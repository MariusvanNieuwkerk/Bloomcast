from __future__ import annotations

import os
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
MAX_UPLOAD_BYTES = 25 * 1024 * 1024  # 25MB
MAX_TRANSCRIPT_CHARS = 250_000  # post-cleaning (per Taskyard spec)
TIMESTAMP_SKEW_SECONDS = 300
CONTENT_TYPE_PDF = "application/pdf"
ALLOWED_EXTENSIONS = {".xlsx"}

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

    input_xlsx_bytes: Optional[bytes] = None

    if file_obj is not None:
        raw = await file_obj.read()
        if len(raw) > MAX_UPLOAD_BYTES:
            return JSONResponse(status_code=413, content={"error": "Input file too large (max 25MB)"})
        # Basic extension check (best-effort; Taskyard should also enforce).
        filename = (file_obj.filename or "").lower()
        if filename and not any(filename.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            return JSONResponse(status_code=415, content={"error": "Unsupported file type. Please upload an .xlsx file."})

        input_xlsx_bytes = raw
        payload_sha256 = sha256_hex(raw)  # file hashing: raw bytes
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
        return JSONResponse(status_code=422, content={"error": "Missing input_file/input_text"})

    # Signature verification
    try:
        secret = _get_secret()
    except RuntimeError as e:
        return JSONResponse(status_code=500, content={"error": str(e)})

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
    if input_xlsx_bytes is None:
        return JSONResponse(status_code=422, content={"error": "Missing input_file (.xlsx) for Pure Data Edition"})

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

