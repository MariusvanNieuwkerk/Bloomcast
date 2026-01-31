from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from fpdf import FPDF

from data_ingestor import IngestedData, ingest_client_data, current_iso_week
from logic_engine import BloomCastOptimizer
from utils import sha256_hex


def generate_bloomcast_pdf_report(
    *,
    optimized_df: pd.DataFrame,
    week: int,
    cfg: dict[str, Any],
) -> bytes:
    now = datetime.now()

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    # Header / branding
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 10, "BloomCast Weekly Forecast (Pure Data Edition)", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Week: {week}  |  Generated: {now.strftime('%Y-%m-%d %H:%M')}", ln=True)
    pdf.cell(
        0,
        6,
        f"Config: PEER_WEIGHT={cfg.get('PEER_WEIGHT')}  BUYER_BOOST={cfg.get('BUYER_BOOST')}",
        ln=True,
    )
    pdf.ln(3)

    # Recommended orders table (deterministic)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Order Proposal (Deterministic)", ln=True)

    columns = [
        ("product", 18, "Art"),
        ("product_name", 55, "Product"),
        ("stock_level", 14, "Avail"),
        ("total", 16, "Proposal"),
        ("breakdown", 0, "Calculation Breakdown"),
    ]

    pdf.set_font("Helvetica", "B", 9)
    for key, width, label in columns:
        w = width if width > 0 else 0
        if w == 0:
            # remaining width
            w = 190 - sum(c[1] for c in columns if c[1] > 0)
        pdf.cell(w, 7, label, border=1)
    pdf.ln()

    pdf.set_font("Helvetica", "", 9)
    view = optimized_df.copy()

    def cell_text(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            # Keep numeric columns neat.
            return f"{v:g}"
        return str(v)

    for _, row in view.iterrows():
        for key, width, _label in columns:
            w = width if width > 0 else 0
            if w == 0:
                w = 190 - sum(c[1] for c in columns if c[1] > 0)
            text = cell_text(row.get(key, ""))
            # Keep the table stable by truncating long text in cells (MVP).
            if key == "breakdown":
                max_len = 120
            elif key == "product_name":
                max_len = 60
            else:
                max_len = 50
            pdf.cell(w, 6, text[:max_len], border=1)
        pdf.ln()

    pdf.ln(2)
    pdf.set_font("Helvetica", "", 9)

    # Export bytes
    out = pdf.output(dest="S")
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    return out.encode("latin-1")


def run_bloomcast(
    *,
    job_id: str,
    input_xlsx_bytes: bytes,
) -> tuple[bytes, dict[str, Any]]:
    """
    Pure business logic for Taskyard:
      input → analyze/optimize → PDF bytes + analysis JSON
    """
    ingested: IngestedData = ingest_client_data(input_xlsx_bytes)
    # Allow overriding the target week for repeatable runs.
    try:
        week = int(ingested.config.get("TARGET_ISO_WEEK") or ingested.config.get("WEEK_NUMBER") or current_iso_week())
    except Exception:
        week = current_iso_week()
    optimizer = BloomCastOptimizer(current_week=week)
    optimized = optimizer.optimize(ingested)

    # Present a focused buying proposal (default: top 60 products).
    try:
        top_n = int(ingested.config.get("PROPOSAL_TOP_N") or 60)
    except Exception:
        top_n = 60
    if top_n < 1:
        top_n = 60

    if not optimized.empty:
        # Prefer only meaningful proposals; keep list compact for buyers.
        if "total" in optimized.columns:
            optimized = optimized[optimized["total"] > 0].copy()
        optimized = optimized.sort_values(["total", "product"], ascending=[False, True]).head(top_n).reset_index(drop=True)

    pdf_bytes = generate_bloomcast_pdf_report(
        optimized_df=optimized,
        week=week,
        cfg=ingested.config,
    )

    # Build analysis payload for Taskyard response.
    summary = f"BloomCast computed a deterministic order proposal for ISO week {week} from the uploaded Excel file."
    decisions: list[str] = [
        "Pure Data Edition: no weather/holiday logic applied.",
        f"PEER_WEIGHT={ingested.config.get('PEER_WEIGHT')}, BUYER_BOOST={ingested.config.get('BUYER_BOOST')}.",
        f"Stock source: {ingested.config.get('STOCK_SOURCE', 'unknown')}.",
        f"Rows proposed: {int(len(optimized))} (top {top_n}).",
    ]
    action_items: list[dict[str, str]] = []
    if not optimized.empty:
        for _, r in optimized.head(5).iterrows():
            action_items.append(
                {
                    "who": "Buyer",
                    "what": f"Place order proposal: {r.get('product')} {r.get('product_name','')}".strip() + f" = {int(r.get('total', 0))} units.",
                    "deadline": "This week",
                }
            )

    analysis = {
        "summary": summary,
        "action_items": action_items,
        "decisions": decisions,
        "context": {
            "config": ingested.config,
            "week": week,
        },
        "job_id": job_id,
        "pdf_sha256": sha256_hex(pdf_bytes),
    }

    return pdf_bytes, analysis

