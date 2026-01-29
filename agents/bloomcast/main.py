from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from io import BytesIO
from pathlib import Path
from zoneinfo import ZoneInfo
from typing import Any, Optional, Union

import pandas as pd
from fpdf import FPDF

from config import BloomCastConfig, CONFIG
from context_engine import ContextFetcher, HolidayContext
from logic_engine import BloomCastOptimizer
from mock_data_generator import generate_sales_history_csv
from utils import sha256_hex


def _load_sales_df_from_csv_bytes(csv_bytes: bytes) -> pd.DataFrame:
    return pd.read_csv(BytesIO(csv_bytes))


def _load_sales_df_from_path(path: Union[str, Path]) -> pd.DataFrame:
    return pd.read_csv(Path(path))


def _format_money(value: float, currency: str) -> str:
    # Lightweight formatting (can be localized later).
    if currency.upper() in {"EUR", "USD", "GBP"}:
        return f"{currency.upper()} {value:,.2f}"
    return f"{value:,.2f} {currency.upper()}"


def generate_bloomcast_pdf_report(
    *,
    config: BloomCastConfig,
    optimized_df: pd.DataFrame,
    weather: dict[str, Any],
    holiday: HolidayContext,
) -> bytes:
    tz = ZoneInfo(config.TIMEZONE)
    now = datetime.now(tz)
    week = now.isocalendar().week

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=True, margin=12)
    pdf.add_page()

    # Header / branding
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 10, "BloomCast Weekly Forecast", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Store: {config.STORE_CITY} ({config.COUNTRY_CODE})", ln=True)
    pdf.cell(0, 6, f"Week: {week}  |  Generated: {now.strftime('%Y-%m-%d %H:%M')} ({config.TIMEZONE})", ln=True)
    pdf.ln(3)

    # Context block
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Local Context", ln=True)
    pdf.set_font("Helvetica", "", 11)
    pdf.cell(0, 6, f"Weather: {weather.get('temp')}°C, {weather.get('condition')}", ln=True)
    if holiday.upcoming:
        pdf.cell(0, 6, f"Holiday: {holiday.name} in {holiday.days_until} days ({holiday.date_iso})", ln=True)
    else:
        pdf.cell(0, 6, "Holiday: none within next 14 days", ln=True)
    pdf.ln(3)

    # Recommended orders table
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 7, "Recommended Orders", ln=True)

    columns = [
        ("product_name", 36, "Product"),
        ("category", 18, "Category"),
        ("units_sold", 16, "Sold"),
        ("waste_pct", 18, "Waste %"),
        ("stock", 14, "Stock"),
        ("recommended_order_units", 18, "Order"),
        ("action", 26, "Action"),
        ("reasoning", 0, "Reasoning"),
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
    if "action" in view.columns:
        priority = {"STOP": 0, "STOCK UP": 1, "INCREASE 50%": 2, "KEEP": 3}
        view["_p"] = view["action"].map(priority).fillna(99)
        view = view.sort_values(["_p", "product_name"], ascending=[True, True]).drop(columns=["_p"])

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
            max_len = 80 if key == "reasoning" else 40
            pdf.cell(w, 6, text[:max_len], border=1)
        pdf.ln()

    pdf.ln(2)
    pdf.set_font("Helvetica", "", 9)

    # Small totals line
    if "recommended_order_value" in optimized_df.columns and "currency" in optimized_df.columns:
        currency = str(optimized_df["currency"].iloc[0]) if len(optimized_df) else config.CURRENCY
        total_value = float(optimized_df["recommended_order_value"].sum())
        pdf.cell(0, 6, f"Estimated order value: {_format_money(total_value, currency)}", ln=True)

    # Export bytes
    out = pdf.output(dest="S")
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    return out.encode("latin-1")


def run_bloomcast(
    *,
    job_id: str,
    config: BloomCastConfig = CONFIG,
    input_csv_bytes: Optional[bytes] = None,
    input_csv_path: Optional[Union[str, Path]] = None,
) -> tuple[bytes, dict[str, Any]]:
    """
    Pure business logic for Taskyard:
      input → analyze/optimize → PDF bytes + analysis JSON
    """
    if input_csv_bytes is not None:
        sales_df = _load_sales_df_from_csv_bytes(input_csv_bytes)
    elif input_csv_path is not None:
        sales_df = _load_sales_df_from_path(input_csv_path)
    else:
        # Self-contained demo mode: generate sales_history.csv and load it.
        csv_path = generate_sales_history_csv(output_path="sales_history.csv", config=config)
        sales_df = _load_sales_df_from_path(csv_path)

    fetcher = ContextFetcher(config=config)
    weather = fetcher.get_weather(config.STORE_CITY)
    holiday = fetcher.get_holidays(config.COUNTRY_CODE)

    optimizer = BloomCastOptimizer()
    optimized = optimizer.optimize(sales_df, weather=weather, holiday=holiday)

    pdf_bytes = generate_bloomcast_pdf_report(
        config=config,
        optimized_df=optimized,
        weather=weather,
        holiday=holiday,
    )

    # Build analysis payload for Taskyard response.
    counts = optimized["action"].value_counts().to_dict() if "action" in optimized.columns else {}
    summary = (
        f"BloomCast generated a weekly forecast for {config.STORE_CITY} ({config.COUNTRY_CODE}). "
        f"Actions: {counts}."
    )
    decisions: list[str] = [
        f"Weather context: {weather.get('temp')}°C and {weather.get('condition')}.",
    ]
    if holiday.upcoming:
        decisions.append(f"Upcoming holiday detected: {holiday.name} ({holiday.date_iso}).")
    else:
        decisions.append("No national holiday within the next 14 days.")

    action_items: list[dict[str, str]] = []
    if "action" in optimized.columns:
        for _, r in optimized[optimized["action"] == "STOP"].head(6).iterrows():
            action_items.append(
                {
                    "who": "Category Manager",
                    "what": f"Review high-waste item: {r.get('product_name')} (waste {r.get('waste_pct')}%).",
                    "deadline": "This week",
                }
            )

    analysis = {
        "summary": summary,
        "action_items": action_items,
        "decisions": decisions,
        "context": {
            "config": asdict(config),
            "weather": weather,
            "holiday": asdict(holiday),
        },
        "job_id": job_id,
        "pdf_sha256": sha256_hex(pdf_bytes),
    }

    return pdf_bytes, analysis

