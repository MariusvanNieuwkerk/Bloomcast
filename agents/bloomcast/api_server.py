from __future__ import annotations

import os
import json
from urllib.request import Request, urlopen
from typing import Optional

from fastapi import FastAPI, File, Form, Header, UploadFile
from fastapi.responses import JSONResponse

from main import run_bloomcast
from utils import (
    InMemoryIdempotencyCache,
    now_epoch,
    payload_sha256_from_text,
    sha256_hex,
    upload_output_bytes,
    verify_taskyard_signature,
    maybe_base64,
)


APP_NAME = "BloomCast (Taskyard Agent Service)"
MAX_UPLOAD_BYTES = 250 * 1024 * 1024  # 250MB (Taskyard can provide input_url for large inputs)
MAX_TRANSCRIPT_CHARS = 250_000  # post-cleaning (per Taskyard spec)
TIMESTAMP_SKEW_SECONDS = 300
CONTENT_TYPE_PDF = "application/pdf"
ALLOWED_EXTENSIONS = {".xlsx"}
MAX_UPLOAD_MB = MAX_UPLOAD_BYTES / 1024 / 1024

app = FastAPI(title=APP_NAME, version="1.0.0")
idempotency_cache = InMemoryIdempotencyCache(ttl_seconds=3600)


def _get_secret() -> str:
    secret = os.getenv("TASKYARD_SECRET", "").strip()
    if not secret:
        raise RuntimeError("TASKYARD_SECRET is not set")
    return secret


def _validate_timestamp(ts: str) -> bool:
    try:
        ts_int = int(ts)
    except Exception:
        return False
    now = now_epoch()
    return abs(now - ts_int) <= TIMESTAMP_SKEW_SECONDS


def _download_bytes(url: str, *, max_bytes: int, timeout_seconds: int = 60) -> bytes:
    req = Request(url, method="GET")
    with urlopen(req, timeout=timeout_seconds) as resp:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = resp.read(1024 * 1024)  # 1MB
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise ValueError(f"Downloaded file exceeds limit ({max_bytes} bytes)")
            chunks.append(chunk)
        return b"".join(chunks)


def _payload_sha_candidates_for_input_url(
    *,
    input_url: str,
    input_name: Optional[str],
    input_mime: Optional[str],
    input_size: Optional[int],
) -> list[str]:
    """
    Taskyard may sign either just the URL or a canonical object.
    To stay compatible with different Taskyard payload conventions, we try a small set of deterministic candidates.
    """
    candidates: list[str] = []

    # 1) URL only
    candidates.append(payload_sha256_from_text(input_url))

    # 2) JSON object (stable keys, stable separators)
    obj = {
        "input_url": input_url,
        "input_name": input_name or "",
        "input_mime": input_mime or "",
        "input_size": int(input_size or 0),
    }
    candidates.append(payload_sha256_from_text(json.dumps(obj, sort_keys=True, separators=(",", ":"))))

    # 3) Simple newline-joined canonical text
    joined = "\n".join(
        [
            f"input_url={input_url}",
            f"input_name={input_name or ''}",
            f"input_mime={input_mime or ''}",
            f"input_size={int(input_size or 0)}",
        ]
    )
    candidates.append(payload_sha256_from_text(joined))

    # De-dup while preserving order
    seen = set()
    out: list[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


@app.post("/run")
async def run(
    # Required Taskyard fields
    job_id: str = Form(...),
    completion_mode: str = Form("review"),
    upload_url: Optional[str] = Form(None),
    # Inputs (either file or text)
    input_file: Optional[UploadFile] = File(None),
    transcript_file: Optional[UploadFile] = File(None),
    input_text: Optional[str] = Form(None),
    transcript_text: Optional[str] = Form(None),
    # New large-input mode (Taskyard provides a short-lived signed URL)
    input_url: Optional[str] = Form(None),
    input_name: Optional[str] = Form(None),
    input_mime: Optional[str] = Form(None),
    input_size: Optional[int] = Form(None),
    # Compatibility aliases (some clients send camelCase)
    inputUrl: Optional[str] = Form(None),
    inputName: Optional[str] = Form(None),
    inputMime: Optional[str] = Form(None),
    inputSize: Optional[int] = Form(None),
    # Optional behavior
    return_pdf_base64: Optional[bool] = Form(False),
    # Required Taskyard headers
    x_taskyard_timestamp: str = Header(..., alias="X-Taskyard-Timestamp"),
    x_taskyard_idempotency_key: str = Header(..., alias="X-Taskyard-Idempotency-Key"),
    x_taskyard_signature: str = Header(..., alias="X-Taskyard-Signature"),
):
    # Idempotency first (must return *exact* same response JSON).
    cached = idempotency_cache.get(x_taskyard_idempotency_key)
    if cached is not None:
        return JSONResponse(
            status_code=200,
            content=cached,
            headers={"X-Taskyard-Idempotent-Replay": "true"},
        )

    # Timestamp validation
    if not _validate_timestamp(x_taskyard_timestamp):
        return JSONResponse(status_code=401, content={"error": "Invalid or expired timestamp"})

    # Determine input source
    file_obj = input_file or transcript_file
    text_val = input_text if input_text is not None else transcript_text
    # Normalize input_url fields (snake_case preferred, fall back to camelCase)
    input_url = input_url or inputUrl
    input_name = input_name or inputName
    input_mime = input_mime or inputMime
    input_size = input_size if input_size is not None else inputSize

    input_xlsx_bytes: Optional[bytes] = None
    payload_sha256: Optional[str] = None

    if file_obj is not None:
        raw = await file_obj.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            return JSONResponse(
                status_code=413,
                content={
                    "error": "Input file too large",
                    "max_upload_mb": round(MAX_UPLOAD_MB, 2),
                    "received_mb": round(len(raw) / 1024 / 1024, 2),
                    "hint": "Create a smaller .xlsx by keeping only required sheets/columns (Date/Product/Qty + availability) and reducing history range.",
                },
            )
        # Basic extension check (best-effort; Taskyard should also enforce).
        filename = (file_obj.filename or "").lower()
        if filename and not any(filename.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            return JSONResponse(status_code=415, content={"error": "Unsupported file type. Please upload an .xlsx file."})

        input_xlsx_bytes = raw
        payload_sha256 = sha256_hex(raw)  # file hashing: raw bytes
    elif input_url is not None:
        url = input_url.strip()
        if not url:
            return JSONResponse(status_code=422, content={"error": "input_url is empty"})

        # Best-effort type check using name
        name = (input_name or "").lower()
        if name and not any(name.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            return JSONResponse(status_code=415, content={"error": "Unsupported input_name. Please provide an .xlsx input_url."})

        # Enforce size if provided
        if input_size is not None:
            try:
                size_int = int(input_size)
            except Exception:
                size_int = None
            if size_int is not None and size_int > MAX_UPLOAD_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={
                        "error": "Input file too large",
                        "max_upload_mb": round(MAX_UPLOAD_MB, 2),
                        "received_mb": round(size_int / 1024 / 1024, 2),
                        "hint": "Reduce history range or provide a smaller export.",
                    },
                )

        # Signature payload is derived from the URL metadata (not from file bytes).
        payload_candidates = _payload_sha_candidates_for_input_url(
            input_url=url, input_name=input_name, input_mime=input_mime, input_size=input_size
        )
        payload_sha256 = payload_candidates[0]  # default; we will verify against all
    elif text_val is not None:
        # Pure Data Edition expects an Excel file. (Text input is intentionally unsupported.)
        canonical = text_val.replace("\r\n", "\n").replace("\r", "\n").strip()
        if len(canonical) > MAX_TRANSCRIPT_CHARS:
            return JSONResponse(status_code=413, content={"error": "Input text too large (max 250k chars)"})
        payload_sha256 = payload_sha256_from_text(text_val)
        return JSONResponse(
            status_code=415,
            content={"error": "Text input is not supported for Pure Data Edition. Please upload an .xlsx file."},
        )
    else:
        return JSONResponse(
            status_code=422,
            content={"error": "Missing input. Provide input_file (.xlsx) or input_url."},
        )

    # Signature verification
    try:
        secret = _get_secret()
    except RuntimeError as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

    if payload_sha256 is None:
        return JSONResponse(status_code=422, content={"error": "Could not derive payload_sha256"})

    # Verify signature (try multiple payload candidates for input_url mode).
    if input_url is not None:
        ok = False
        for cand in _payload_sha_candidates_for_input_url(
            input_url=input_url.strip(),
            input_name=input_name,
            input_mime=input_mime,
            input_size=input_size,
        ):
            if verify_taskyard_signature(
                secret=secret,
                ts=x_taskyard_timestamp,
                method="POST",
                path="/run",
                job_id=job_id,
                payload_sha256=cand,
                provided_signature_header=x_taskyard_signature,
            ):
                ok = True
                break
    else:
        ok = verify_taskyard_signature(
            secret=secret,
            ts=x_taskyard_timestamp,
            method="POST",
            path="/run",
            job_id=job_id,
            payload_sha256=payload_sha256,
            provided_signature_header=x_taskyard_signature,
        )
    if not ok:
        return JSONResponse(status_code=401, content={"error": "Invalid signature"})

    # Run core business logic
    if input_xlsx_bytes is None and input_url is not None:
        try:
            input_xlsx_bytes = _download_bytes(input_url.strip(), max_bytes=MAX_UPLOAD_BYTES, timeout_seconds=90)
        except Exception as e:
            return JSONResponse(status_code=422, content={"error": f"Failed to download input_url: {type(e).__name__}: {e}"})

    if input_xlsx_bytes is None:
        return JSONResponse(status_code=422, content={"error": "Missing input (.xlsx). Provide input_file or input_url."})

    pdf_bytes, analysis = run_bloomcast(job_id=job_id, input_xlsx_bytes=input_xlsx_bytes)
    pdf_sha = sha256_hex(pdf_bytes)

    storage: dict = {"upload_used": False}
    if upload_url:
        uploaded, err = upload_output_bytes(
            upload_url=upload_url,
            content=pdf_bytes,
            content_type=CONTENT_TYPE_PDF,
            timeout_seconds=30,
        )
        storage["upload_used"] = uploaded
        if err:
            storage["upload_error"] = err

    response_json = {
        "result_status": "completed" if completion_mode == "completed" else "review",
        "analysis": {
            "summary": str(analysis.get("summary", "")),
            "action_items": list(analysis.get("action_items", [])),
            "decisions": list(analysis.get("decisions", [])),
        },
        "output": {
            "content_type": CONTENT_TYPE_PDF,
            "filename": "bloomcast_weekly_forecast.pdf",
            "sha256": pdf_sha,
            "size_bytes": len(pdf_bytes),
            "storage": storage,
        },
    }

    # Optional fallback for small outputs / debugging
    if (not upload_url or not storage.get("upload_used")) and return_pdf_base64:
        response_json["output"]["pdf_base64"] = maybe_base64(pdf_bytes)

    # Store for idempotency
    idempotency_cache.set(x_taskyard_idempotency_key, response_json)

    return JSONResponse(status_code=200, content=response_json)

