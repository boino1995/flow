import asyncio
import os
import re
import shutil
import subprocess
import time
import xml.etree.ElementTree as ET
from urllib.parse import urljoin
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, PlainTextResponse
import httpx

app = FastAPI(title="ClearKey MPD to HLS Restreamer")

HLS_DIR = os.getenv("HLS_DIR", "/tmp/hls")
DEFAULT_MPD = os.getenv("MPD_URL", "")
DEFAULT_KEYS = os.getenv("CLEAR_KEYS", "")
DEFAULT_UA = os.getenv("USER_AGENT", "Mozilla/5.0")
DEFAULT_REFERER = os.getenv("REFERER", "")
DEFAULT_AUTH = os.getenv("AUTH_HEADER", "")
IDLE_TIMEOUT_SEC = int(os.getenv("IDLE_TIMEOUT_SEC", "35"))

os.makedirs(HLS_DIR, exist_ok=True)

state = {
    "proc": None,
    "active_key": None,
    "last_request": 0.0,
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
    # Accepts "kid:key" or "kid:key,kid2:key2" (strips dashes/spaces)
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


async def fetch_dash_segments(mpd_url: str, keys: list[str], headers: dict[str, str], work_dir: str) -> list[str]:
    """Fetches MPD, downloads init + latest segment(s), decrypts with mp4decrypt, returns decrypted file paths."""
    async with httpx.AsyncClient(headers=headers, timeout=15.0, follow_redirects=True) as client:
        resp = await client.get(mpd_url)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)

    ns = {"mpd": "urn:mpeg:dash:schema:mpd:2011"}
    base_url = mpd_url
    base_el = root.find("mpd:BaseURL", ns)
    if base_el is not None and base_el.text:
        base_url = urljoin(mpd_url, base_el.text.strip())

    # Pick first video AdaptationSet and first Representation
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
        raise RuntimeError("Only SegmentTemplate DASH streams are handled by this starter script")

    init_tpl = seg_tpl.attrib.get("initialization", "")
    media_tpl = seg_tpl.attrib.get("media", "")
    start_num = int(seg_tpl.attrib.get("startNumber", "1"))

    # If SegmentTimeline exists, grab the last entry's start time / repeat
    timeline = seg_tpl.find("mpd:SegmentTimeline", ns)
    cur_time = 0
    seg_number = start_num
    if timeline is not None:
        cur_num = start_num
        for s in timeline.findall("mpd:S", ns):
            t_attr = s.attrib.get("t")
            if t_attr is not None:
                cur_time = int(t_attr)
            d_val = int(s.attrib.get("d", "0"))
            r_val = int(s.attrib.get("r", "0"))
            # Advance to near the live edge
            if r_val > 0:
                cur_time += d_val * r_val
                cur_num += r_val
            seg_number = cur_num
            cur_time += d_val
            cur_num += 1

    decrypted_paths = []
    async with httpx.AsyncClient(headers=headers, timeout=15.0, follow_redirects=True) as client:
        init_dec = os.path.join(work_dir, "init_dec.mp4")
        if init_tpl:
            init_url = resolve_template(init_tpl, base_url, rep_id, bandwidth, seg_number, cur_time)
            init_enc = os.path.join(work_dir, "init_enc.m4s")
            r_init = await client.get(init_url)
            r_init.raise_for_status()
            with open(init_enc, "wb") as f:
                f.write(r_init.content)
            decrypt_file(init_enc, init_dec, keys)
            decrypted_paths.append(init_dec)

        media_url = resolve_template(media_tpl, base_url, rep_id, bandwidth, seg_number, cur_time)
        media_enc = os.path.join(work_dir, f"seg_{seg_number}_enc.m4s")
        media_dec = os.path.join(work_dir, f"seg_{seg_number}_dec.m4s")
        r_seg = await client.get(media_url)
        r_seg.raise_for_status()
        with open(media_enc, "wb") as f:
            f.write(r_seg.content)
        decrypt_file(media_enc, media_dec, keys)
        decrypted_paths.append(media_dec)

    return decrypted_paths


def decrypt_file(src_path: str, dst_path: str, keys: list[str]) -> None:
    if not keys:
        shutil.copyfile(src_path, dst_path)
        return
    cmd = ["mp4decrypt"]
    for k in keys:
        cmd.extend(["--key", k])
    cmd.extend([src_path, dst_path])
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"mp4decrypt failed: {res.stderr.strip()}")


def stop_ffmpeg() -> None:
    proc = state.get("proc")
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
    state["proc"] = None
    state["active_key"] = None


def clean_hls_dir() -> None:
    for name in os.listdir(HLS_DIR):
        p = os.path.join(HLS_DIR, name)
        try:
            if os.path.isdir(p):
                shutil.rmtree(p, ignore_errors=True)
            else:
                os.remove(p)
        except OSError:
            pass


async def ensure_stream_running(mpd_url: str, raw_keys: str, headers: dict[str, str]) -> str:
    keys = normalize_keys(raw_keys)
    session_key = f"{mpd_url}|{'|'.join(keys)}"

    async with state["lock"]:
        state["last_request"] = time.time()
        proc = state.get("proc")
        if proc is not None and proc.poll() is None and state.get("active_key") == session_key:
            return os.path.join(HLS_DIR, "index.m3u8")

        stop_ffmpeg()
        clean_hls_dir()

        work_dir = os.path.join(HLS_DIR, "work")
        os.makedirs(work_dir, exist_ok=True)

        # Seed initial decrypted segment(s) and start rolling FFmpeg HLS muxer (-c copy)
        dec_files = await fetch_dash_segments(mpd_url, keys, headers, work_dir)
        seed_path = os.path.join(work_dir, "seed.mp4")
        with open(seed_path, "wb") as out:
            for p in dec_files:
                with open(p, "rb") as inp:
                    out.write(inp.read())

        out_m3u8 = os.path.join(HLS_DIR, "index.m3u8")
        out_seg = os.path.join(HLS_DIR, "seg_%03d.ts")

        ffmpeg_cmd = [
            "ffmpeg",
            "-loglevel", "error",
            "-re",
            "-i", seed_path,
            "-c", "copy",
            "-f", "hls",
            "-hls_time", "4",
            "-hls_list_size", "6",
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_filename", out_seg,
            out_m3u8,
        ]
        state["proc"] = subprocess.Popen(ffmpeg_cmd)
        state["active_key"] = session_key

        # Wait briefly for index.m3u8 to appear
        for _ in range(30):
            if os.path.exists(out_m3u8) and os.path.getsize(out_m3u8) > 0:
                break
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
                            stop_ffmpeg()
                            clean_hls_dir()

    asyncio.create_task(watchdog())


@app.get("/health")
def health():
    proc = state.get("proc")
    return {
        "status": "ok",
        "ffmpeg_running": bool(proc and proc.poll() is None),
    }


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

    # Ensure relative segment paths work cleanly behind proxies
    return PlainTextResponse(content=content, media_type="application/vnd.apple.mpegurl")


@app.get("/{segment_name}")
async def get_segment(segment_name: str):
    state["last_request"] = time.time()
    if not re.fullmatch(r"seg_\d+\.ts", segment_name):
        raise HTTPException(status_code=404, detail="Not found")
    seg_path = os.path.join(HLS_DIR, segment_name)
    if not os.path.exists(seg_path):
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(seg_path, media_type="video/mp2t")