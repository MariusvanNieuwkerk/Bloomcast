from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from fpdf import FPDF

from data_ingestor import IngestedData, ingest_client_data, current_iso_week
from logic_engine import BloomCastOptimizer
from utils import sha256_hex


def _wrap_text(pdf: FPDF, text: str, max_width_mm: float) -> list[str]:
    """
    Basic word-wrapping using FPDF string width.
    Returns at least one line.
    """
    t = (text or "").replace("\r\n", "\n").replace("\r", "\n")
    t = " ".join(t.split())  # collapse whitespace
    if not t:
        return [""]

    words = t.split(" ")
    lines: list[str] = []
    cur = ""
    for w in words:
        trial = w if not cur else f"{cur} {w}"
        if pdf.get_string_width(trial) <= max_width_mm:
            cur = trial
            continue
        if cur:
            lines.append(cur)
            cur = w
            continue
        # single word longer than width → hard split
        chunk = ""
        for ch in w:
            if pdf.get_string_width(chunk + ch) <= max_width_mm:
                chunk += ch
            else:
                if chunk:
                    lines.append(chunk)
                chunk = ch
        cur = chunk
    if cur:
        lines.append(cur)
    return lines or [""]


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
    pdf.cell(0, 10, "BloomCast Weekly Forecast", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Week: {week}  |  Gegenereerd: {now.strftime('%Y-%m-%d %H:%M')}", ln=True)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 6, "Kort voorstel: top producten om deze week te bestellen.", ln=True)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(90, 90, 90)
    peer_weight = cfg.get("PEER_WEIGHT")
    buyer_boost = cfg.get("BUYER_BOOST")
    pdf.cell(0, 5, f"Instellingen: peer-weging {peer_weight}  |  buyer-boost {buyer_boost}", ln=True)
    pdf.cell(0, 5, "Uitleg (geldt voor alle producten):", ln=True)
    pdf.cell(0, 5, "- Base = wat deze klant normaal verkoopt in deze week (historie)", ln=True)
    pdf.cell(0, 5, "- Extra = als vergelijkbare klanten meer verkopen (peer-signaal)", ln=True)
    pdf.cell(0, 5, "- Buyer tip = extra duwtje als het product is aanbevolen", ln=True)
    stock_mode = str(cfg.get("STOCK_MODE") or "").strip().lower()
    if stock_mode == "availability":
        pdf.cell(0, 5, "Let op: er zijn geen voorraad-aantallen in de export, alleen 'leverbaar'.", ln=True)
    pdf.set_text_color(0, 0, 0)
    pdf.ln(3)

    # Recommended orders table (deterministic)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Bestelvoorstel", ln=True)

    # Column widths in mm (A4 width ~210mm, margins 12mm => usable ~186mm).
    stock_mode = str(cfg.get("STOCK_MODE") or "").strip().lower()
    base_cols = [
        ("product", 18.0, "Artikel"),
        ("product_name", 70.0, "Product"),
    ]
    if stock_mode == "quantity":
        base_cols.append(("stock_level", 14.0, "Voorraad"))
    base_cols += [
        ("total", 16.0, "Advies"),
        ("reason", 0.0, "Waarom"),
    ]
    columns = base_cols

    usable_w = pdf.w - pdf.l_margin - pdf.r_margin
    fixed_w = sum(w for _, w, _ in columns if w > 0)
    remaining_w = max(40.0, usable_w - fixed_w)  # ensure reason stays readable

    def col_width(key: str) -> float:
        for k, w, _ in columns:
            if k == key:
                return remaining_w if w == 0 else w
        return 20.0

    def draw_header() -> None:
        pdf.set_font("Helvetica", "B", 9)
        for key, w, label in columns:
            width = remaining_w if w == 0 else w
            pdf.cell(width, 7, label, border=1, align="C")
        pdf.ln()
        pdf.set_font("Helvetica", "", 9)

    def cell_text(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float):
            return f"{v:g}"
        return str(v)

    draw_header()

    line_h = 5.0
    view = optimized_df.copy()
    # Derive a short reason label per product (phone-friendly).
    if "reason" not in view.columns:
        def _reason(row):
            reasons = []
            try:
                if float(row.get("peer_adjustment", 0) or 0) > 0:
                    reasons.append("Populair bij vergelijkbare klanten")
            except Exception:
                pass
            try:
                if float(row.get("buyer_boost", 0) or 0) > 0:
                    reasons.append("Aanbevolen door buyer")
            except Exception:
                pass
            return " + ".join(reasons) if reasons else "Normale verkoop"
        view["reason"] = view.apply(_reason, axis=1)
    for _, row in view.iterrows():
        # Build wrapped lines for each cell
        cell_lines: dict[str, list[str]] = {}
        max_lines = 1
        for key, w, _label in columns:
            width = remaining_w if w == 0 else w
            pad = 2.0
            text = cell_text(row.get(key, ""))
            # Wrap only for long-text cells; keep others single-line.
            if key in {"product_name", "reason"}:
                lines = _wrap_text(pdf, text, max_width_mm=max(5.0, width - pad))
            else:
                lines = [text]
            cell_lines[key] = lines
            max_lines = max(max_lines, len(lines))

        row_h = line_h * max_lines

        # Page break + repeat header
        if pdf.get_y() + row_h > pdf.page_break_trigger:
            pdf.add_page()
            draw_header()

        y0 = pdf.get_y()
        x0 = pdf.get_x()

        # Draw each cell as a rectangle with manually positioned text lines
        for key, w, _label in columns:
            width = remaining_w if w == 0 else w
            x = pdf.get_x()
            y = pdf.get_y()
            pdf.rect(x, y, width, row_h)

            lines = cell_lines.get(key, [""])
            # Vertical padding
            y_text = y + 1.2
            for i, line in enumerate(lines[:max_lines]):
                yy = y_text + i * line_h
                if key in {"stock_level", "total"}:
                    # Right align numbers
                    text_w = pdf.get_string_width(line)
                    pdf.text(x + width - 1.5 - text_w, yy + 2.7, line)
                else:
                    pdf.text(x + 1.5, yy + 2.7, line)

            pdf.set_xy(x + width, y)

        pdf.set_xy(x0, y0 + row_h)

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
        "Deterministic forecast: no weather/holiday logic applied.",
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

