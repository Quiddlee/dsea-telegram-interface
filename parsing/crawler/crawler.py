from __future__ import annotations

import hashlib
import json
import logging
import urllib.parse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

import requests

from .. import main as parsing_main

logger = logging.getLogger("crawler")


def _sha256_bytes(data: bytes) -> str:
    h = hashlib.sha256()
    h.update(data)
    return h.hexdigest()


def _safe_write_atomic(path: Path, data: bytes) -> None:
    """
    Write file atomically to avoid partially-written artifacts being read by worker.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_bytes(data)
    tmp_path.replace(path)


def _build_doc_key(source_type: str, source_id: str) -> str:
    """
    Deterministic key for artifact filenames in this MVP.
    Later, replace with DB UUID document.id.
    """
    return hashlib.sha1(f"{source_type}:{source_id}".encode("utf-8")).hexdigest()[:16]


def _normalize_url(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    normalized_path = urllib.parse.quote(parsed.path)
    query = urllib.parse.urlencode(urllib.parse.parse_qsl(parsed.query), doseq=True)
    return urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, normalized_path, parsed.params, query, parsed.fragment)
    )


def _derive_type_and_ext(mime_type: str) -> tuple[str, str]:
    mime = (mime_type or "").split(";")[0].strip().lower()
    if mime in {"text/html", "application/xhtml+xml"}:
        return "html", "html"
    if mime == "text/plain":
        return "text", "txt"
    if mime == "application/pdf":
        return "pdf", "pdf"
    if mime == "image/png":
        return "png", "png"
    if mime == "image/jpeg":
        return "jpg", "jpg"
    if mime == "image/webp":
        return "webp", "webp"
    return "html", "html"


def _looks_like_html(data: bytes) -> bool:
    trimmed = data.lstrip()[:512].lower()
    return trimmed.startswith(b"<!doctype html") or trimmed.startswith(b"<html") or b"<html" in trimmed


def _build_parser_tasks() -> list[tuple[str, Callable[[], Any], str]]:
    return [
        ("call_schedule", parsing_main.call_schedule_parser, parsing_main.URL_CALL_SCHEDULE),
        ("class_schedule", parsing_main.class_schedule_parser, parsing_main.URL_CLASS_SCHEDULE),
        ("session_schedule", parsing_main.session_schedule_parser, parsing_main.URL_SESSION_SCHEDULE),
        ("rating_list", parsing_main.rating_list_parser, parsing_main.URL_SCHOLARSHIP_LIST),
        ("scholarship_list", parsing_main.scholarship_list_parser, parsing_main.URL_SCHOLARSHIP_LIST),
        ("timetable_calendar", parsing_main.timetable_calendar_parser, parsing_main.URL_TIMETABLE_CALENDAR),
    ]


def _save_artifact(
    *,
    raw_bytes: bytes,
    mime_type: str,
    source_url: str,
    run_id: str,
    raw_dir: Path,
    parsed_dir: Path,
    dry_run: bool,
) -> Optional[dict[str, Any]]:
    normalized_url = _normalize_url(source_url)
    effective_mime = mime_type
    if (mime_type or "").split(";")[0].strip().lower() in {"text/html", "application/xhtml+xml"}:
        if _looks_like_html(raw_bytes):
            logger.warning("Skipping HTML response for %s (mime=%s)", source_url, mime_type)
            return None
        effective_mime = "text/plain"

    source_type, ext = _derive_type_and_ext(effective_mime)
    doc_key = _build_doc_key(source_type, normalized_url)

    raw_path = raw_dir / f"{doc_key}.{ext}"
    manifest_path = parsed_dir / f"{doc_key}.json"

    manifest = {
        "type": source_type,
        "version": 1,
        "runId": run_id,
        "source": {
            "url": source_url,
            "sourceId": normalized_url,
            "mimeType": effective_mime,
        },
        "rawPath": f"raw/{doc_key}.{ext}",
        "checksum": _sha256_bytes(raw_bytes),
        "createdAt": datetime.now(timezone.utc).isoformat(),
    }

    if manifest_path.exists():
        try:
            existing = json.loads(manifest_path.read_text(encoding="utf-8"))
            if existing.get("checksum") == manifest["checksum"]:
                logger.info("Unchanged artifact skipped for %s", source_url)
                return existing
        except Exception as exc:
            logger.warning("Failed to read existing manifest for %s: %s", source_url, exc)

    if dry_run:
        logger.info("Dry-run: skipping writes for %s", source_url)
    else:
        _safe_write_atomic(raw_path, raw_bytes)
        _safe_write_atomic(
            manifest_path,
            json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8"),
        )
        logger.info("Saved raw: %s", str(raw_path))
        logger.info("Saved manifest: %s", str(manifest_path))

    return manifest


def run_crawler(
    artifacts_dir: str,
    dry_run: bool = False,
    run_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Execute parsing functions and store downloaded artifacts + manifests.
    """
    started_at = datetime.now(timezone.utc)
    run_id = run_id or started_at.strftime("%Y%m%dT%H%M%SZ")

    base = Path(artifacts_dir)
    raw_dir = base / "raw"
    parsed_dir = base / "parsed"

    logger.info("Crawler started | run_id=%s | dry_run=%s", run_id, dry_run)
    logger.info("Artifacts dir: %s", str(base))

    results: dict[str, Any] = {}
    tasks = _build_parser_tasks()

    for i, (name, func, source_url) in enumerate(tasks, start=1):
        logger.info("[%s/%s] Parser | name=%s | url=%s", i, len(tasks), name, source_url)
        try:
            data = func()
            results[name] = {"data": data, "artifacts": []}

            if name == "call_schedule":
                text_lines, _image_url, page_url = data
                text_bytes = "\n".join(text_lines).encode("utf-8")
                text_source = f"{page_url}#text"
                manifest = _save_artifact(
                    raw_bytes=text_bytes,
                    mime_type="text/plain",
                    source_url=text_source,
                    run_id=run_id,
                    raw_dir=raw_dir,
                    parsed_dir=parsed_dir,
                    dry_run=dry_run,
                )
                if manifest:
                    results[name]["artifacts"].append(manifest)

            elif name in {"class_schedule", "session_schedule"}:
                _title, image_urls, _page_url = data
                for image_url in image_urls:
                    resp = requests.get(image_url, timeout=30)
                    resp.raise_for_status()
                    manifest = _save_artifact(
                        raw_bytes=resp.content,
                        mime_type=resp.headers.get("Content-Type", ""),
                        source_url=image_url,
                        run_id=run_id,
                        raw_dir=raw_dir,
                        parsed_dir=parsed_dir,
                        dry_run=dry_run,
                    )
                    if manifest:
                        results[name]["artifacts"].append(manifest)

            elif name == "rating_list":
                rating_files, _page_url = data
                for _filename, file_url in rating_files:
                    resp = requests.get(file_url, timeout=30)
                    resp.raise_for_status()
                    manifest = _save_artifact(
                        raw_bytes=resp.content,
                        mime_type=resp.headers.get("Content-Type", ""),
                        source_url=file_url,
                        run_id=run_id,
                        raw_dir=raw_dir,
                        parsed_dir=parsed_dir,
                        dry_run=dry_run,
                    )
                    if manifest:
                        results[name]["artifacts"].append(manifest)

            elif name == "scholarship_list":
                file_url, _file_name, _file_text, _page_url = data
                resp = requests.get(file_url, timeout=30)
                resp.raise_for_status()
                manifest = _save_artifact(
                    raw_bytes=resp.content,
                    mime_type=resp.headers.get("Content-Type", ""),
                    source_url=file_url,
                    run_id=run_id,
                    raw_dir=raw_dir,
                    parsed_dir=parsed_dir,
                    dry_run=dry_run,
                )
                if manifest:
                    results[name]["artifacts"].append(manifest)

            elif name == "timetable_calendar":
                _title, files, _page_url = data
                for _name, file_url in files:
                    resp = requests.get(file_url, timeout=30)
                    resp.raise_for_status()
                    manifest = _save_artifact(
                        raw_bytes=resp.content,
                        mime_type=resp.headers.get("Content-Type", ""),
                        source_url=file_url,
                        run_id=run_id,
                        raw_dir=raw_dir,
                        parsed_dir=parsed_dir,
                        dry_run=dry_run,
                    )
                    if manifest:
                        results[name]["artifacts"].append(manifest)
        except Exception as exc:
            logger.exception("Parser failed | name=%s | error=%s", name, str(exc))
            results[name] = {
                "name": name,
                "runId": run_id,
                "source": {"url": source_url},
                "error": str(exc),
            }

    finished_at = datetime.now(timezone.utc)
    logger.info(
        "Crawler finished | run_id=%s | duration_sec=%.2f",
        run_id,
        (finished_at - started_at).total_seconds(),
    )

    return results
