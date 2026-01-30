from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Optional
from urllib.request import Request, urlopen


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonicalize_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def payload_sha256_from_text(text: str) -> str:
    canonical = canonicalize_text(text)
    return sha256_hex(canonical.encode("utf-8"))


def message_to_sign(*, ts: str, method: str, path: str, job_id: str, payload_sha256: str) -> str:
    return f"{ts}.{method}.{path}.{job_id}.{payload_sha256}"


def compute_signature_hex(secret: str, msg: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), msg.encode("utf-8"), hashlib.sha256)
    return mac.hexdigest()


def parse_signature_header(header_value: str) -> Optional[str]:
    # Expected: "v1=<hex>"
    if not header_value:
        return None
    parts = header_value.split("=", 1)
    if len(parts) != 2:
        return None
    version, sig = parts[0].strip(), parts[1].strip()
    if version != "v1" or not sig:
        return None
    return sig


def verify_taskyard_signature(
    *,
    secret: str,
    ts: str,
    method: str,
    path: str,
    job_id: str,
    payload_sha256: str,
    provided_signature_header: str,
) -> bool:
    provided_sig = parse_signature_header(provided_signature_header)
    if not provided_sig:
        return False
    msg = message_to_sign(ts=ts, method=method, path=path, job_id=job_id, payload_sha256=payload_sha256)
    expected = compute_signature_hex(secret, msg)
    return hmac.compare_digest(expected, provided_sig)


def now_epoch() -> int:
    return int(time.time())


@dataclass
class CachedResponse:
    expires_at: int
    response_json: dict[str, Any]


class InMemoryIdempotencyCache:
    """
    MVP idempotency cache (in-memory, TTL-based).
    Limitation: resets on process restart.
    """

    def __init__(self, *, ttl_seconds: int = 3600):
        self.ttl_seconds = ttl_seconds
        self._store: dict[str, CachedResponse] = {}

    def get(self, key: str) -> Optional[dict[str, Any]]:
        item = self._store.get(key)
        if not item:
            return None
        if now_epoch() > item.expires_at:
            self._store.pop(key, None)
            return None
        # Return a deep-ish copy to prevent accidental mutation.
        return json.loads(json.dumps(item.response_json))

    def set(self, key: str, response_json: dict[str, Any]) -> None:
        self._store[key] = CachedResponse(expires_at=now_epoch() + self.ttl_seconds, response_json=response_json)


def upload_output_bytes(
    *,
    upload_url: str,
    content: bytes,
    content_type: str,
    timeout_seconds: int = 30,
) -> tuple[bool, Optional[str]]:
    try:
        req = Request(
            upload_url,
            data=content,
            method="PUT",
            headers={"Content-Type": content_type},
        )
        with urlopen(req, timeout=timeout_seconds) as resp:
            status = getattr(resp, "status", 200)
            if 200 <= int(status) < 300:
                return True, None
            return False, f"Upload failed: HTTP {status}"
    except Exception as e:
        return False, f"Upload failed: {type(e).__name__}: {e}"


def maybe_base64(content: bytes) -> str:
    return base64.b64encode(content).decode("ascii")

