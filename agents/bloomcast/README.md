## BloomCast (Taskyard Agent)

BloomCast **Pure Data Edition** is a deterministic forecasting agent that creates an order proposal purely from an uploaded Excel file (no weather/holiday logic).

### Input file format (required)
Upload an `.xlsx` file with **5 sheets**:
1) `Config` (cols: `Setting`, `Value`) — e.g. `PEER_WEIGHT` (0.2), `BUYER_BOOST` (10)
2) `History_Client` (cols: `Date`, `Product`, `Qty`)
3) `History_Peers` (cols: `Date`, `Product`, `Qty`)
4) `Current_Stock` (cols: `Product`, `StockLevel`)
5) `Buyer_Recs` (cols: `Product`)

### Existing export formats (best-effort autodetect)
If you already have an ERP export, BloomCast will try to autodetect common Dutch/English names, e.g.:
- **Sheets**: `klanthistorie`, `Historie andere klanten`, `Aanbevolen assortiment`, `Basis assortiment`
- **Columns**:
  - Date: `Date`, `Orderdatum`, `Datum`
  - Product: `Product`, `Artikel`, `Artikel nr`, `Omschrijving`
  - Qty: `Qty`, `Aantal`, `Quantity`
  - Stock/availability: `StockLevel` / `Voorraad` or `Leverbaar`

If autodetect fails, you can add overrides to the `Config` sheet:
- `HISTORY_CLIENT_SHEET`, `HISTORY_PEERS_SHEET`, `CURRENT_STOCK_SHEET`, `BUYER_RECS_SHEET`
- `HISTORY_CLIENT_DATE_COL`, `HISTORY_CLIENT_PRODUCT_COL`, `HISTORY_CLIENT_QTY_COL`
- `HISTORY_PEERS_DATE_COL`, `HISTORY_PEERS_PRODUCT_COL`, `HISTORY_PEERS_QTY_COL`
- `BUYER_RECS_PRODUCT_COL`

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

#### Large files (input_url mode)
If Taskyard provides large uploads via storage, it may call the agent with:
- `input_url` (short-lived signed download URL)
- `input_name`, `input_mime`, `input_size`

BloomCast will download the file from `input_url` (HTTP GET) and then process it as usual.

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

#### Example: generate signature for an Excel file (Python)

```python
import hashlib, hmac, time

TASKYARD_SECRET = "replace_me"
ts = str(int(time.time()))
job_id = "contract_id_123"

with open("input.xlsx", "rb") as f:
    payload_sha256 = hashlib.sha256(f.read()).hexdigest()

msg = f"{ts}.POST./run.{job_id}.{payload_sha256}"
sig = hmac.new(TASKYARD_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
print("X-Taskyard-Timestamp:", ts)
print("X-Taskyard-Signature:", "v1=" + sig)
```

#### Example curl (file input)

```bash
curl -X POST "http://localhost:8080/run" \
  -H "X-Taskyard-Timestamp: <ts>" \
  -H "X-Taskyard-Idempotency-Key: <stable-key>" \
  -H "X-Taskyard-Signature: v1=<sig>" \
  -F "job_id=<contract_id>" \
  -F "completion_mode=review" \
  -F "return_pdf_base64=true" \
  -F "input_file=@input.xlsx"
```

### Idempotency (MVP)
- If the same `X-Taskyard-Idempotency-Key` is received again within 1 hour, the agent returns the **exact same JSON** and adds header:
  - `X-Taskyard-Idempotent-Replay: true`
- Limitation: in-memory cache resets on container restart.

