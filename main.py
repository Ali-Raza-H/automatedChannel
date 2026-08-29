"""Filesystem-first brainrot pipeline CLI and terminal dashboard."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import signal
import subprocess
import threading
import time
import wave
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover
    yaml = None

ROOT = Path(__file__).resolve().parent
CONFIG_PATH = ROOT / "config.yaml"
DATA = ROOT / "data"
RUNTIME_PATH = DATA / "runtime.json"
LOG_PATH = DATA / "logs" / "application.log"

DEFAULTS: dict[str, Any] = {
    "app": {"name": "Brainrot Generator", "log_level": "INFO"},
    "pipeline": {"background_pool_size": 2, "poll_interval_seconds": 5, "stale_job_timeout_minutes": 30},
    "video": {"width": 1080, "height": 1920, "fps": 30, "target_duration_seconds": 75, "min_duration_seconds": 60, "max_duration_seconds": 90},
    "development": {"mock_external_services": True},
    "story": {"target_words": 185, "min_words": 150, "max_words": 230, "genders": {"male": {"enabled": True}, "female": {"enabled": True}}},
    "upload": {"enabled": False},
}
STAGES = ("backgrounds", "stories", "audio", "subtitles", "assembly", "validation", "youtube", "tiktok")


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def merge(dst: dict[str, Any], src: dict[str, Any]) -> None:
    for key, value in src.items():
        if isinstance(value, dict) and isinstance(dst.get(key), dict):
            merge(dst[key], value)
        else:
            dst[key] = value


def load_config() -> dict[str, Any]:
    config = json.loads(json.dumps(DEFAULTS))
    if CONFIG_PATH.exists() and yaml:
        with CONFIG_PATH.open(encoding="utf-8") as fh:
            merge(config, yaml.safe_load(fh) or {})
    return config


def save_config(config: dict[str, Any]) -> None:
    if not yaml:
        raise RuntimeError("PyYAML is required to save configuration")
    temporary = CONFIG_PATH.with_suffix(".yaml.tmp")
    temporary.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    os.replace(temporary, CONFIG_PATH)


class SettingsManager:
    """Thread-safe settings facade used by the TUI and workers."""
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._config = load_config()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._config))

    def get(self, path: str, default: Any = None) -> Any:
        value: Any = self._config
        for part in path.split("."):
            if not isinstance(value, dict) or part not in value:
                return default
            value = value[part]
        return value

    def set(self, path: str, value: Any, persist: bool = True) -> None:
        with self._lock:
            parts = path.split(".")
            target = self._config
            for part in parts[:-1]:
                target = target.setdefault(part, {})
            target[parts[-1]] = value
            if persist:
                save_config(self._config)


def setup_dirs() -> None:
    for kind in ("backgrounds", "stories", "audio", "subtitles"):
        for state in ("available", "processing", "used", "failed"):
            (DATA / kind / state).mkdir(parents=True, exist_ok=True)
    for state in ("ready", "uploading", "uploaded", "failed"):
        (DATA / "videos" / state).mkdir(parents=True, exist_ok=True)
    (DATA / "tmp").mkdir(parents=True, exist_ok=True)
    (DATA / "logs").mkdir(parents=True, exist_ok=True)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2), encoding="utf-8")
    os.replace(temporary, path)


def load_runtime() -> dict[str, Any]:
    default = {"started_at": None, "paused": False, "workers": {}, "current_jobs": {}, "stats": {"generated": 0, "failed": 0, "uploaded": 0}}
    if RUNTIME_PATH.exists():
        try:
            merge(default, json.loads(RUNTIME_PATH.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError):
            pass
    return default


class RuntimeState:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self.value = load_runtime()
        self.value["started_at"] = self.value.get("started_at") or utc_now()

    def update(self, **changes: Any) -> None:
        with self._lock:
            self.value.update(changes)
            atomic_json(RUNTIME_PATH, self.value)

    def worker(self, name: str, status: str, job: str | None = None) -> None:
        with self._lock:
            self.value.setdefault("workers", {})[name] = {"status": status, "updated_at": utc_now()}
            if job is not None:
                self.value.setdefault("current_jobs", {})[name] = job
            elif status in ("idle", "stopped", "failed"):
                self.value.setdefault("current_jobs", {}).pop(name, None)
            atomic_json(RUNTIME_PATH, self.value)

    def increment(self, key: str, amount: int = 1) -> None:
        with self._lock:
            self.value.setdefault("stats", {})[key] = self.value.setdefault("stats", {}).get(key, 0) + amount
            atomic_json(RUNTIME_PATH, self.value)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self.value))


def configure_logging(config: dict[str, Any]) -> None:
    setup_dirs()
    logging.basicConfig(level=getattr(logging, config["app"]["log_level"].upper(), logging.INFO), format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s", handlers=[logging.FileHandler(LOG_PATH, encoding="utf-8"), logging.StreamHandler()])


def next_id(prefix: str) -> str:
    values = []
    for path in DATA.rglob(f"{prefix}_*.json"):
        try: values.append(int(path.stem.rsplit("_", 1)[1]))
        except ValueError: pass
    return f"{prefix}_{max(values, default=0) + 1:06d}"


def claim(kind: str, asset_id: str) -> bool:
    source = DATA / kind / "available" / asset_id
    target = DATA / kind / "processing" / asset_id
    try:
        source.rename(target)
        return True
    except FileNotFoundError:
        return False


def move_state(kind: str, asset_id: str, state: str) -> None:
    source = DATA / kind / "processing" / asset_id
    if source.exists(): source.rename(DATA / kind / state / asset_id)


def ffprobe(path: Path) -> dict[str, Any]:
    result = subprocess.run(["ffprobe", "-v", "error", "-show_streams", "-show_format", "-of", "json", str(path)], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)


def create_mock_background(config: dict[str, Any]) -> str:
    asset_id = next_id("bg"); path = DATA / "backgrounds" / "available" / f"{asset_id}.mp4"
    seconds = config["video"]["max_duration_seconds"]
    subprocess.run(["ffmpeg", "-y", "-f", "lavfi", "-i", "color=c=blue:s=1920x1080:r=30", "-t", str(seconds), "-an", "-c:v", "libx264", "-pix_fmt", "yuv420p", str(path)], capture_output=True, check=True)
    atomic_json(path.with_suffix(".json"), {"id": asset_id, "title": "Development background", "source_url": None, "uploader": None, "license": None, "category": "mock", "duration": seconds, "usage_count": 0, "max_usage_count": 10, "downloaded_at": utc_now()})
    return asset_id


def create_mock_story(gender: str) -> str:
    asset_id = next_id("story")
    story = ("I thought this would be an ordinary day, but one tiny detail changed everything. " * 18).strip()
    atomic_json(DATA / "stories" / "available" / f"{asset_id}.json", {"id": asset_id, "gender": gender, "title": f"The detail I almost missed ({gender})", "story": story, "genre": "storytime", "estimated_duration": 75, "tags": ["storytime", "brainrot"], "created_at": utc_now()})
    return asset_id


def create_mock_audio(story: dict[str, Any], config: dict[str, Any]) -> str:
    asset_id = next_id("audio"); path = DATA / "audio" / "available" / f"{asset_id}.wav"; seconds = config["video"]["target_duration_seconds"]; rate = 24000
    with wave.open(str(path), "wb") as out:
        out.setnchannels(1); out.setsampwidth(2); out.setframerate(rate); out.writeframes(b"\x00\x00" * int(rate * seconds))
    atomic_json(path.with_suffix(".json"), {"id": asset_id, "story_id": story["id"], "provider": "mock", "voice": story["gender"], "duration": seconds, "created_at": utc_now()})
    return asset_id


def create_mock_subtitles(audio: dict[str, Any]) -> str:
    asset_id = next_id("subtitle"); path = DATA / "subtitles" / "available" / f"{asset_id}.ass"
    style = "Style: Default,Arial,64,&H00FFFFFF,&H000000FF,&H00000000,&H00000000,-1,0,0,0,100,100,0,0,1,4,1,2,80,80,160,0"
    path.write_text(f"[Script Info]\nScriptType: v4.00+\n[V4+ Styles]\nFormat: Name, Fontname, Fontsize, PrimaryColour, SecondaryColour, OutlineColour, BackColour, Bold, Italic, Underline, StrikeOut, ScaleX, ScaleY, Spacing, Angle, BorderStyle, Outline, Shadow, Alignment, MarginL, MarginR, MarginV, Encoding\n{style}\n[Events]\nFormat: Layer, Start, End, Style, Name, MarginL, MarginR, MarginV, Effect, Text\nDialogue: 0,0:00:00.00,0:01:15.00,Default,,0,0,0,,I THOUGHT THIS WOULD BE\\NAN ORDINARY DAY\n", encoding="utf-8")
    atomic_json(path.with_suffix(".json"), {"id": asset_id, "audio_id": audio["id"], "model": "mock", "created_at": utc_now()})
    return asset_id


def assemble(bg_id: str, story: dict[str, Any], audio_id: str, subtitle_id: str, config: dict[str, Any]) -> str:
    video_id = next_id("video"); bg = DATA / "backgrounds" / "processing" / f"{bg_id}.mp4"; audio = DATA / "audio" / "processing" / f"{audio_id}.wav"; ass = DATA / "subtitles" / "processing" / f"{subtitle_id}.ass"; out = DATA / "videos" / "ready" / f"{video_id}.mp4"
    w, h, fps = config["video"]["width"], config["video"]["height"], config["video"]["fps"]
    vf = f"scale={w}:{h}:force_original_aspect_ratio=increase,crop={w}:{h},subtitles={ass.as_posix()}"
    subprocess.run(["ffmpeg", "-y", "-i", str(bg), "-i", str(audio), "-t", str(config["video"]["target_duration_seconds"]), "-vf", vf, "-map", "0:v:0", "-map", "1:a:0", "-r", str(fps), "-c:v", "libx264", "-pix_fmt", "yuv420p", "-c:a", "aac", "-b:a", "192k", "-shortest", str(out)], capture_output=True, check=True)
    info = ffprobe(out); stream = next(s for s in info["streams"] if s["codec_type"] == "video")
    atomic_json(out.with_suffix(".json"), {"id": video_id, "story_id": story["id"], "audio_id": audio_id, "subtitle_id": subtitle_id, "background_id": bg_id, "title": story["title"], "gender": story["gender"], "genre": story["genre"], "tags": story["tags"], "duration": float(info["format"]["duration"]), "resolution": f"{stream['width']}x{stream['height']}", "fps": fps, "created_at": utc_now(), "upload": {"youtube": {"status": "pending"}, "tiktok": {"status": "pending"}}})
    return video_id


def generate_once(config: dict[str, Any], runtime: RuntimeState | None = None) -> str:
    setup_dirs(); log = logging.getLogger("pipeline")
    def stage(name: str, job: str) -> None:
        if runtime: runtime.worker(name, "running", job)
        log.info("%s: %s", name, job)
    stage("backgrounds", "downloading"); bg_id = create_mock_background(config); claim("backgrounds", f"{bg_id}.mp4"); claim("backgrounds", f"{bg_id}.json")
    stage("stories", "generating"); story_id = create_mock_story(random.choice(["male", "female"])); claim("stories", f"{story_id}.json"); story = json.loads((DATA / "stories" / "processing" / f"{story_id}.json").read_text())
    stage("audio", story_id); audio_id = create_mock_audio(story, config); claim("audio", f"{audio_id}.wav"); claim("audio", f"{audio_id}.json"); audio = json.loads((DATA / "audio" / "processing" / f"{audio_id}.json").read_text())
    stage("subtitles", audio_id); subtitle_id = create_mock_subtitles(audio); claim("subtitles", f"{subtitle_id}.ass"); claim("subtitles", f"{subtitle_id}.json")
    stage("assembly", "rendering"); video_id = assemble(bg_id, story, audio_id, subtitle_id, config); stage("validation", video_id)
    info = ffprobe(DATA / "videos" / "ready" / f"{video_id}.mp4"); video = next(s for s in info["streams"] if s["codec_type"] == "video")
    if video["width"] != 1080 or video["height"] != 1920 or not any(s["codec_type"] == "audio" for s in info["streams"]): raise ValueError("final video failed required validation")
    for kind, ids in (("stories", [f"{story_id}.json"]), ("audio", [f"{audio_id}.wav", f"{audio_id}.json"]), ("subtitles", [f"{subtitle_id}.ass", f"{subtitle_id}.json"]), ("backgrounds", [f"{bg_id}.mp4", f"{bg_id}.json"])):
        for asset in ids: move_state(kind, asset, "used")
    if runtime: runtime.increment("generated"); [runtime.worker(s, "idle") for s in STAGES]
    log.info("validation: %s passed", video_id)
    return video_id


def count_assets() -> dict[str, dict[str, int]]:
    result = {}
    for kind in ("backgrounds", "stories", "audio", "subtitles"):
        result[kind] = {state: len(list((DATA / kind / state).iterdir())) for state in ("available", "processing", "used", "failed")}
    result["videos"] = {state: len(list((DATA / "videos" / state).glob("*.mp4"))) for state in ("ready", "uploading", "uploaded", "failed")}
    return result


def render_dashboard(runtime: RuntimeState, settings: SettingsManager, logs: deque[str]) -> None:
    os.system("clear")
    snapshot = runtime.snapshot(); assets = count_assets()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║                    BRAINROT GENERATOR — TUI                        ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print(f"║ Status: {'PAUSED' if snapshot.get('paused') else 'RUNNING':<12}  Started: {snapshot.get('started_at') or 'not started':<32}║")
    print("╠══════════════════════ STAGES ════════════════════════════════════════╣")
    for stage_name in STAGES:
        state = snapshot.get("workers", {}).get(stage_name, {}).get("status", "idle")
        job = snapshot.get("current_jobs", {}).get(stage_name, "")
        print(f"║ {stage_name:<14} {state:<12} {job:<40}║")
    print("╠══════════════════════ QUEUES / STATS ════════════════════════════════╣")
    for name, values in assets.items(): print(f"║ {name:<14} " + " ".join(f"{key}={value}" for key, value in values.items()) + " " * max(0, 48 - len(name) - sum(len(f"{key}={value}") + 1 for key, value in values.items())) + "║")
    print(f"║ stats: {snapshot.get('stats', {})!s:<60}║")
    print("╠══════════════════════ RECENT LOGS ════════════════════════════════════╣")
    for line in list(logs)[-5:]: print(f"║ {line[-66:]:<66}║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print("║ [P] pause/resume  [S] settings  [G] generate once  [R] refresh  [Q] quit║")
    print("╚══════════════════════════════════════════════════════════════════════╝")


def settings_menu(settings: SettingsManager) -> None:
    print("\nSettings (blank cancels). Editable: pipeline.poll_interval_seconds, video.target_duration_seconds, upload.enabled")
    path = input("Setting path: ").strip()
    if not path: return
    current = settings.get(path)
    value = input(f"Value [{current}]: ").strip()
    if not value: return
    if value.lower() in ("true", "false"): parsed: Any = value.lower() == "true"
    else:
        try: parsed = int(value)
        except ValueError:
            try: parsed = float(value)
            except ValueError: parsed = value
    settings.set(path, parsed); print("Saved."); time.sleep(1)


def run_tui(config: dict[str, Any]) -> None:
    runtime = RuntimeState(); settings = SettingsManager(); logs: deque[str] = deque(maxlen=100); stop = threading.Event()
    class Buffer(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None: logs.append(self.format(record))
    logging.getLogger().addHandler(Buffer())
    def work() -> None:
        while not stop.is_set():
            if not runtime.snapshot().get("paused"):
                try: generate_once(settings.snapshot(), runtime)
                except Exception: runtime.increment("failed"); logging.getLogger("pipeline").exception("generation failed")
            stop.wait(settings.get("pipeline.poll_interval_seconds", 5))
    thread = threading.Thread(target=work, daemon=True); thread.start()
    try:
        while True:
            render_dashboard(runtime, settings, logs)
            command = input("Command: ").strip().lower()
            if command == "q": break
            if command == "p": runtime.update(paused=not runtime.snapshot().get("paused", False))
            elif command == "s": settings_menu(settings)
            elif command == "g":
                try: generate_once(settings.snapshot(), runtime)
                except Exception: logging.getLogger("pipeline").exception("manual generation failed")
            elif command == "r": continue
    finally:
        stop.set(); thread.join(timeout=2); runtime.update(paused=False)


def main() -> None:
    parser = argparse.ArgumentParser(prog="brainrot")
    parser.add_argument("command", nargs="?", default="run", choices=["run", "generate", "generate-once", "status", "validate"])
    args = parser.parse_args(); setup_dirs(); config = load_config(); configure_logging(config)
    if args.command == "generate-once": print(f"Generated and validated {generate_once(config)}")
    elif args.command in ("run", "generate"): run_tui(config)
    elif args.command == "status":
        print(json.dumps({"assets": count_assets(), "runtime": load_runtime()}, indent=2))
    elif args.command == "validate":
        for path in (DATA / "videos" / "ready").glob("*.mp4"):
            try: print(path.name, "OK", ffprobe(path)["format"].get("duration"))
            except Exception as exc: print(path.name, "FAILED", exc)

if __name__ == "__main__": main()
