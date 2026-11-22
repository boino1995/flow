import asyncio
from collections import deque
from datetime import datetime, timezone
import os
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
import httpx

app = FastAPI(title="Live ClearKey MPD to HLS Restreamer")

HLS_DIR = os.getenv("HLS_DIR", "/tmp/hls")
WORK_DIR = os.getenv("WORK_DIR", "/tmp/dash_work")
DEFAULT_MPD = os.getenv("MPD_URL", "")
DEFAULT_KEYS = os.getenv("CLEAR_KEYS", "")
DEFAULT_UA = os.getenv("USER_AGENT", "Mozilla/5.0")
DEFAULT_REFERER = os.getenv("REFERER", "")
DEFAULT_AUTH = os.getenv("AUTH_HEADER", "")
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "2.0"))
IDLE_TIMEOUT_SEC = int(os.getenv("IDLE_TIMEOUT_SEC", "35"))
LIVE_LOOKBACK_SEGS = int(os.getenv("LIVE_LOOKBACK_SEGS", "2"))

os.makedirs(HLS_DIR, exist_ok=True)
os.makedirs(WORK_DIR, exist_ok=True)

state = {
    "proc": None,
    "poll_task": None,
    "stderr_task": None,
    "active_key": None,
    "active_mpd": None,
    "last_request": 0.0,
    "last_poll_at": None,
    "last_error": None,
    "init_sent": False,
    "segments_piped": 0,
    "bytes_piped": 0,
    "ffmpeg_stderr": deque(maxlen=40),
    "lock": asyncio.Lock(),
}


def build_headers(referer: str = "", auth: str = "", ua: str = "") -> dict[str, str]:
    headers = {"User-Agent": ua or DEFAULT_UA}
    ref = referer or DEFAULT_REFERER
    token = auth or DEFAULT_AUTH
    if ref:
        headers["Referer"] = ref
    if token:
        headers["Authorization"] = token if token.lower().startswith("bearer ") else f"Bearer {token}"
    return headers


def normalize_keys(raw_keys: str) -> list[str]:
    pairs = []
    for item in (raw_keys or "").split(","):
        item = item.strip()
        if ":" not in item:
            continue
        kid, key = item.split(":", 1)
        kid = kid.replace("-", "").strip()
        key = key.replace("-", "").strip()
        if kid and key:
            pairs.append(f"{kid}:{key}")
    return pairs


def resolve_template(template: str, base_url: str, rep_id: str, bandwidth: str, number: int, time_val: int = 0) -> str:
    url = template
    url = url.replace("$RepresentationID$", rep_id)
    url = url.replace("$Bandwidth$", bandwidth)
    url = url.replace("$Number$", str(number))
    url = re.sub(r"\$Number%0(\d+)d\$", lambda m: f"{number:0{int(m.group(1))}d}", url)
    url = url.replace("$Time$", str(time_val))
    return urljoin(base_url, url)


async def decrypt_file_async(src_path: str, dst_path: str, keys: list[str], timeout_sec: float = 10.0) -> None:
    if not keys:
        shutil.copyfile(src_path, dst_path)
        return
    cmd = ["mp4decrypt"]
    for k in keys:
        cmd.extend(["--key", k])
    cmd.extend([src_path, dst_path])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        _, stderr_data = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.wait()
        raise RuntimeError(f"mp4decrypt timed out after {timeout_sec}s")

    if proc.returncode != 0:
        err_msg = stderr_data.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"mp4decrypt failed (rc={proc.returncode}): {err_msg}")


def parse_live_candidates(mpd_xml: str, mpd_url: str) -> tuple[str | None, list[tuple[str, str]]]:
    """
    Returns (init_url, [(seg_token, media_url), ...]) for the primary video Representation.
    Supports both $Number$ / $Number%0Nd$ and $Time$ / SegmentTimeline manifests.
    """
    root = ET.fromstring(mpd_xml)
    ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}

    base_url = mpd_url
    base_el = root.find("mpd:BaseURL", ns)
    if base_el is not None and base_el.text:
        base_url = urljoin(mpd_url, base_el.text.strip())

    video_as = None
    for aset in root.findall(".//mpd:AdaptationSet", ns):
        mime = aset.attrib.get("mimeType", "")
        ctype = aset.attrib.get("contentType", "")
        if "video" in mime or "video" in ctype:
            video_as = aset
            break
    if video_as is None:
        video_as = root.find(".//mpd:AdaptationSet", ns)
    if video_as is None:
        raise RuntimeError("No AdaptationSet found in MPD")

    rep = video_as.find("mpd:Representation", ns)
    if rep is None:
        raise RuntimeError("No Representation found in MPD")

    rep_id = rep.attrib.get("id", "0")
    bandwidth = rep.attrib.get("bandwidth", "0")

    seg_tpl = rep.find("mpd:SegmentTemplate", ns) or video_as.find("mpd:SegmentTemplate", ns)
    if seg_tpl is None:
        raise RuntimeError("SegmentTemplate not found in MPD")

    init_tpl = seg_tpl.attrib.get("initialization", "")
    media_tpl = seg_tpl.attrib.get("media", "")
    if not media_tpl:
        raise RuntimeError("Media template missing in SegmentTemplate")

    start_num = int(seg_tpl.attrib.get("startNumber", "1"))
    init_url = resolve_template(init_tpl, base_url, rep_id, bandwidth, start_num, 0) if init_tpl else None

    timeline = seg_tpl.find("mpd:SegmentTimeline", ns)
    candidates: list[tuple[str, str]] = []

    if timeline is not None:
        cur_time = 0
        seg_num = start_num
        entries: list[tuple[str, str]] = []
        for s in timeline.findall("mpd:S", ns):
            t_attr = s.attrib.get("t")
            if t_attr is not None:
                cur_time = int(t_attr)
            d_val = int(s.attrib.get("d", "0"))
            r_val = int(s.attrib.get("r", "0"))
            repeat_count = r_val + 1 if r_val >= 0 else 1
            for _ in range(repeat_count):
                token = f"t:{cur_time}|n:{seg_num}"
                m_url = resolve_template(media_tpl, base_url, rep_id, bandwidth, seg_num, cur_time)
                entries.append((token, m_url))
                cur_time += d_val
                seg_num += 1
        candidates = entries[-max(1, LIVE_LOOKBACK_SEGS):]
    else:
        duration = float(seg_tpl.attrib.get("duration", "4"))
        timescale = float(seg_tpl.attrib.get("timescale", "1"))
        seg_sec = max(1.0, duration / max(1.0, timescale))
        avail_start_raw = root.attrib.get("availabilityStartTime", "")
        now_epoch = time.time()
        est_current = start_num
        if avail_start_raw:
            try:
                dt = datetime.fromisoformat(avail_start_raw.replace("Z", "+00:00")).astimezone(timezone.utc)
                elapsed = max(0.0, now_epoch - dt.timestamp())
                est_current = start_num + int(elapsed // seg_sec)
            except Exception:
                pass
        first_num = max(start_num, est_current - max(1, LIVE_LOOKBACK_SEGS) + 1)
        for n in range(first_num, est_current + 1):
            candidates.append((f"n:{n}", resolve_template(media_tpl, base_url, rep_id, bandwidth, n, 0)))

    return init_url, candidates


def write_pipe(pipe, data: bytes) -> bool:
    if pipe is None or pipe.closed:
        return False
    try:
        pipe.write(data)
        pipe.flush()
        state["bytes_piped"] += len(data)
        return True
    except (BrokenPipeError, OSError):
        return False


async def download_and_decrypt(
    client: httpx.AsyncClient,
    url: str,
    tag: str,
    keys: list[str],
) -> bytes:
    enc_path = os.path.join(WORK_DIR, f"{tag}_enc.m4s")
    dec_path = os.path.join(WORK_DIR, f"{tag}_dec.m4s")
    try:
        resp = await client.get(url)
        resp.raise_for_status()
        with open(enc_path, "wb") as f:
            f.write(resp.content)
        await decrypt_file_async(enc_path, dec_path, keys)
        with open(dec_path, "rb") as f:
            return f.read()
    finally:
        for p in (enc_path, dec_path):
            try:
                if os.path.exists(p):
                    os.remove(p)
            except OSError:
                pass


async def read_ffmpeg_stderr(proc: subprocess.Popen) -> None:
    try:
        if not proc.stderr:
            return
        loop = asyncio.get_running_loop()
        while proc.poll() is None:
            line = await loop.run_in_executor(None, proc.stderr.readline)
            if not line:
                break
            decoded = line.decode("utf-8", errors="replace").strip()
            if decoded:
                state["ffmpeg_stderr"].append(decoded)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        state["ffmpeg_stderr"].append(f"[stderr-reader] {exc}")


def stop_pipeline_sync() -> None:
    for task_key in ("poll_task", "stderr_task"):
        task = state.get(task_key)
        if task and not task.done():
            task.cancel()
        state[task_key] = None

    proc = state.get("proc")
    if proc:
        try:
            if proc.stdin and not proc.stdin.closed:
                proc.stdin.close()
        except OSError:
            pass
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
    state["proc"] = None
    state["active_key"] = None
    state["active_mpd"] = None
    state["init_sent"] = False


def clean_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)
    for name in os.listdir(path):
        p = os.path.join(path, name)
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.remove(p)
        except OSError:
            pass


async def live_poller_loop(mpd_url: str, keys: list[str], headers: dict[str, str], proc: subprocess.Popen) -> None:
    seen: deque[str] = deque(maxlen=256)
    cached_init_url: str | None = None

    async with httpx.AsyncClient(headers=headers, timeout=10.0, follow_redirects=True) as client:
        while proc.poll() is None:
            state["last_poll_at"] = round(time.time(), 2)
            try:
                mpd_resp = await client.get(mpd_url)
                mpd_resp.raise_for_status()
                init_url, candidates = parse_live_candidates(mpd_resp.text, mpd_url)

                if init_url and (not state["init_sent"] or init_url != cached_init_url):
                    init_bytes = await download_and_decrypt(client, init_url, "init", keys)
                    if not write_pipe(proc.stdin, init_bytes):
                        break
                    state["init_sent"] = True
                    cached_init_url = init_url

                for idx, (token, seg_url) in enumerate(candidates):
                    if proc.poll() is not None:
                        break
                    if token in seen:
                        continue
                    seg_bytes = await download_and_decrypt(client, seg_url, f"seg_{idx}", keys)
                    if not write_pipe(proc.stdin, seg_bytes):
                        return
                    seen.append(token)
                    state["segments_piped"] += 1

                state["last_error"] = None

            except asyncio.CancelledError:
                break
            except Exception as exc:
                err_text = f"{type(exc).__name__}: {exc}"
                state["last_error"] = err_text
                print(f"[poller] warning: {err_text}")

            await asyncio.sleep(POLL_INTERVAL_SEC)


async def ensure_stream_running(mpd_url: str, raw_keys: str, headers: dict[str, str]) -> str:
    keys = normalize_keys(raw_keys)
    session_key = f"{mpd_url}|{'|'.join(keys)}"
    out_m3u8 = os.path.join(HLS_DIR, "index.m3u8")
    out_seg = os.path.join(HLS_DIR, "seg_%03d.ts")

    async with state["lock"]:
        state["last_request"] = time.time()
        proc = state.get("proc")
        if proc is not None and proc.poll() is None and state.get("active_key") == session_key:
            return out_m3u8

        stop_pipeline_sync()
        clean_dir(HLS_DIR)
        clean_dir(WORK_DIR)
        state["last_error"] = None
        state["segments_piped"] = 0
        state["bytes_piped"] = 0
        state["ffmpeg_stderr"].clear()

        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel", "warning",
            "-fflags", "+genpts+discardcorrupt",
            "-i", "pipe:0",
            "-c", "copy",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "6",
            "-hls_flags", "delete_segments+append_list+omit_endlist",
            "-hls_segment_filename", out_seg,
            out_m3u8,
        ]
        proc = subprocess.Popen(
            ffmpeg_cmd,
            stdin=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        state["proc"] = proc
        state["active_key"] = session_key
        state["active_mpd"] = mpd_url
        state["stderr_task"] = asyncio.create_task(read_ffmpeg_stderr(proc))
        state["poll_task"] = asyncio.create_task(live_poller_loop(mpd_url, keys, headers, proc))

        for _ in range(32):
            if os.path.exists(out_m3u8) and os.path.getsize(out_m3u8) > 0:
                break
            if proc.poll() is not None:
                raise RuntimeError("FFmpeg exited before producing HLS segments")
            await asyncio.sleep(0.25)

        return out_m3u8


@app.on_event("startup")
async def start_idle_watchdog():
    async def watchdog():
        while True:
            await asyncio.sleep(10)
            proc = state.get("proc")
            if proc and proc.poll() is None:
                if time.time() - state["last_request"] > IDLE_TIMEOUT_SEC:
                    async with state["lock"]:
                        if time.time() - state["last_request"] > IDLE_TIMEOUT_SEC:
                            stop_pipeline_sync()
                            clean_dir(HLS_DIR)
                            clean_dir(WORK_DIR)

    asyncio.create_task(watchdog())


@app.on_event("shutdown")
async def on_shutdown():
    async with state["lock"]:
        stop_pipeline_sync()


@app.get("/health")
def health():
    proc = state.get("proc")
    return {
        "status": "ok",
        "ffmpeg_running": bool(proc and proc.poll() is None),
        "last_error": state["last_error"],
    }


@app.get("/debug")
def debug():
    proc = state.get("proc")
    hls_files = []
    if os.path.isdir(HLS_DIR):
        for fname in sorted(os.listdir(HLS_DIR)):
            fpath = os.path.join(HLS_DIR, fname)
            if os.path.isfile(fpath):
                hls_files.append({"name": fname, "size": os.path.getsize(fpath)})

    return JSONResponse(
        {
            "ffmpeg_running": bool(proc and proc.poll() is None),
            "ffmpeg_returncode": proc.returncode if proc else None,
            "active_mpd": state["active_mpd"],
            "init_sent": state["init_sent"],
            "segments_piped": state["segments_piped"],
            "bytes_piped": state["bytes_piped"],
            "last_poll_at": state["last_poll_at"],
            "last_error": state["last_error"],
            "hls_files": hls_files,
            "ffmpeg_stderr": list(state["ffmpeg_stderr"]),
        }
    )


@app.get("/stream.m3u8")
async def stream_m3u8(
    mpd: str = Query(default=""),
    keys: str = Query(default=""),
    referer: str = Query(default=""),
    auth: str = Query(default=""),
    ua: str = Query(default=""),
):
    target_mpd = (mpd or DEFAULT_MPD).strip()
    target_keys = (keys or DEFAULT_KEYS).strip()
    if not target_mpd:
        raise HTTPException(status_code=400, detail="Missing 'mpd' query param or MPD_URL env var")

    headers = build_headers(referer=referer, auth=auth, ua=ua)
    try:
        m3u8_path = await ensure_stream_running(target_mpd, target_keys, headers)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc))

    if not os.path.exists(m3u8_path):
        raise HTTPException(status_code=504, detail="HLS playlist not ready yet")

    with open(m3u8_path, "r", encoding="utf-8") as f:
        content = f.read()

    return PlainTextResponse(content=content, media_type="application/vnd.apple.mpegurl")


@app.api_route("/{segment_name}", methods=["GET", "HEAD"])
async def get_segment(segment_name: str, request: Request):
    state["last_request"] = time.time()
    if not re.fullmatch(r"seg_\d+\.ts", segment_name):
        raise HTTPException(status_code=404, detail="Not found")

    seg_path = os.path.join(HLS_DIR, segment_name)
    if not os.path.exists(seg_path):
        raise HTTPException(status_code=404, detail="Segment not found")

    file_size = os.path.getsize(seg_path)
    if file_size <= 0:
        raise HTTPException(status_code=404, detail="Segment not ready")

    clean_headers = [(k, v) for k, v in request.scope.get("headers", []) if k.decode("latin1").lower() != "range"]
    request.scope["headers"] = clean_headers

    if request.method == "HEAD":
        return Response(
            status_code=200,
            headers={
                "Content-Type": "video/mp2t",
                "Content-Length": str(file_size),
                "Accept-Ranges": "none",
                "Cache-Control": "no-cache",
            },
        )

    return FileResponse(
        seg_path,
        media_type="video/mp2t",
        headers={
            "Accept-Ranges": "none",
            "Cache-Control": "no-cache",
        },
    )
