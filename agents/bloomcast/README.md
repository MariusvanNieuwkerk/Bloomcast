## BloomCast (Taskyard Agent)

BloomCast is an international-ready forecasting agent that acts like an automated Category Manager for flowers & plants.
It optimizes recommended orders using:
- sales history (CSV)
- local weather (mock in MVP)
- local holidays (via `python-holidays` / pip package `holidays`)

### Switch country/store in ~10 seconds
Edit `config.py` (or use env overrides):
- `BLOOMCAST_STORE_CITY` (e.g. `Stockholm` / `Amsterdam`)
- `BLOOMCAST_COUNTRY_CODE` (e.g. `SE` / `NL`)
- `BLOOMCAST_CURRENCY` (e.g. `SEK` / `EUR`)

### Local run (API)

```bash
cd agents/bloomcast
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

export TASKYARD_SECRET="replace_me"
uvicorn api_server:app --host 0.0.0.0 --port 8080 --reload
```

### Deploy (Docker)

```bash
cd agents/bloomcast
docker build -t bloomcast-agent .
docker run -p 8080:8080 -e TASKYARD_SECRET="replace_me" bloomcast-agent
```

### Taskyard webhook details

#### Endpoint
- `POST /run` (exact)

#### Required headers
- `X-Taskyard-Timestamp`: unix seconds (string)
- `X-Taskyard-Idempotency-Key`: stable per job (Taskyard uses `contract_id`)
- `X-Taskyard-Signature`: `v1=<hex>` (HMAC SHA256)

#### Signature (exact)
Compute `payload_sha256`:
- **File input**: `sha256(raw_file_bytes)`
- **Text input**:
  - `canonical_text = text.replace('\r\n','\n').replace('\r','\n').strip()`
  - `payload_sha256 = sha256(utf8(canonical_text))`

Message:
- `msg = "{ts}.POST./run.{job_id}.{payload_sha256}"`

Signature:
- `sig = hex(hmac_sha256(TASKYARD_SECRET, msg))`
- Header value: `X-Taskyard-Signature: v1=<sig>`

#### Example: generate signature (Python)

```python
import hashlib, hmac, time

TASKYARD_SECRET = "replace_me"
ts = str(int(time.time()))
job_id = "contract_id_123"

text = "product_id,product_name,category,units_sold,waste_pct,stock,unit_price,currency\nP-1,Tulip,Flowering,50,5,10,7.5,EUR\n"
canonical = text.replace("\r\n","\n").replace("\r","\n").strip()
payload_sha256 = hashlib.sha256(canonical.encode("utf-8")).hexdigest()

msg = f"{ts}.POST./run.{job_id}.{payload_sha256}"
sig = hmac.new(TASKYARD_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
print("X-Taskyard-Timestamp:", ts)
print("X-Taskyard-Signature:", "v1=" + sig)
```

#### Example curl (text input)

```bash
curl -X POST "http://localhost:8080/run" \
  -H "X-Taskyard-Timestamp: <ts>" \
  -H "X-Taskyard-Idempotency-Key: <stable-key>" \
  -H "X-Taskyard-Signature: v1=<sig>" \
  -F "job_id=<contract_id>" \
  -F "completion_mode=review" \
  -F "return_pdf_base64=true" \
  -F "input_text=<csv-text-here>"
```

### Idempotency (MVP)
- If the same `X-Taskyard-Idempotency-Key` is received again within 1 hour, the agent returns the **exact same JSON** and adds header:
  - `X-Taskyard-Idempotent-Replay: true`
- Limitation: in-memory cache resets on container restart.

