from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Dict, Tuple, Union

import pandas as pd


@dataclass(frozen=True)
class IngestedData:
    config: Dict[str, float]
    history_client_weekly: pd.DataFrame
    history_peers_weekly: pd.DataFrame
    current_stock: pd.DataFrame
    buyer_recs: pd.DataFrame


def _to_week(df: pd.DataFrame, date_col: str = "Date") -> pd.DataFrame:
    out = df.copy()
    out[date_col] = pd.to_datetime(out[date_col], errors="coerce")
    out = out.dropna(subset=[date_col])

    # ISO week number (1-53) for deterministic week-aligned comparisons.
    iso = out[date_col].dt.isocalendar()
    out["IsoYear"] = iso["year"].astype(int)
    out["IsoWeek"] = iso["week"].astype(int)
    return out


def _aggregate_qty_by_product_week(df: pd.DataFrame) -> pd.DataFrame:
    # Expected cols: Product, Qty, IsoYear, IsoWeek
    out = df.copy()
    out["Qty"] = pd.to_numeric(out["Qty"], errors="coerce").fillna(0)
    out["Product"] = out["Product"].astype(str).str.strip()
    out = out[out["Product"] != ""]

    agg = (
        out.groupby(["Product", "IsoYear", "IsoWeek"], as_index=False)["Qty"]
        .sum()
        .sort_values(["Product", "IsoYear", "IsoWeek"])
        .reset_index(drop=True)
    )
    return agg


def ingest_client_data(filepath: Union[str, Path, bytes]) -> IngestedData:
    """
    Ingest the client Excel file with 5 sheets:
      1) Config (Setting, Value) -> PEER_WEIGHT, BUYER_BOOST, ...
      2) History_Client (Date, Product, Qty)
      3) History_Peers (Date, Product, Qty)
      4) Current_Stock (Product, StockLevel)
      5) Buyer_Recs (Product)

    Action:
      - Convert dates to ISO Week Numbers
      - Aggregate quantities by Product + Week Number
      - Return processed DataFrames + config dict
    """
    if isinstance(filepath, (bytes, bytearray)):
        excel_obj = BytesIO(bytes(filepath))
    else:
        excel_obj = filepath

    # Read Config
    cfg_df = pd.read_excel(excel_obj, sheet_name="Config", engine="openpyxl")
    cfg_df.columns = [str(c).strip() for c in cfg_df.columns]
    cfg_df = cfg_df.rename(columns={"setting": "Setting", "value": "Value"})

    config: Dict[str, float] = {}
    if "Setting" in cfg_df.columns and "Value" in cfg_df.columns:
        for _, r in cfg_df.iterrows():
            k = str(r.get("Setting", "")).strip()
            if not k:
                continue
            v = r.get("Value", None)
            try:
                config[k] = float(v)
            except Exception:
                # Non-numeric config entries are ignored in MVP
                continue

    # Provide deterministic defaults if not present
    config.setdefault("PEER_WEIGHT", 0.2)
    config.setdefault("BUYER_BOOST", 10.0)

    # Rewind for subsequent reads if we used BytesIO
    if isinstance(excel_obj, BytesIO):
        excel_obj.seek(0)

    hc = pd.read_excel(excel_obj, sheet_name="History_Client", engine="openpyxl")
    if isinstance(excel_obj, BytesIO):
        excel_obj.seek(0)
    hp = pd.read_excel(excel_obj, sheet_name="History_Peers", engine="openpyxl")
    if isinstance(excel_obj, BytesIO):
        excel_obj.seek(0)
    stock = pd.read_excel(excel_obj, sheet_name="Current_Stock", engine="openpyxl")
    if isinstance(excel_obj, BytesIO):
        excel_obj.seek(0)
    buyer = pd.read_excel(excel_obj, sheet_name="Buyer_Recs", engine="openpyxl")

    # Normalize column names
    for df in (hc, hp):
        df.columns = [str(c).strip() for c in df.columns]
    stock.columns = [str(c).strip() for c in stock.columns]
    buyer.columns = [str(c).strip() for c in buyer.columns]

    # Date -> ISO week and aggregate
    hc_week = _aggregate_qty_by_product_week(_to_week(hc, "Date"))
    hp_week = _aggregate_qty_by_product_week(_to_week(hp, "Date"))

    # Normalize stock
    stock = stock.copy()
    stock["Product"] = stock["Product"].astype(str).str.strip()
    if "StockLevel" in stock.columns:
        stock["StockLevel"] = pd.to_numeric(stock["StockLevel"], errors="coerce").fillna(0)
    else:
        stock["StockLevel"] = 0
    stock = stock[stock["Product"] != ""].reset_index(drop=True)

    # Normalize buyer recs
    buyer = buyer.copy()
    buyer["Product"] = buyer["Product"].astype(str).str.strip()
    buyer = buyer[buyer["Product"] != ""].drop_duplicates(subset=["Product"]).reset_index(drop=True)

    return IngestedData(
        config=config,
        history_client_weekly=hc_week,
        history_peers_weekly=hp_week,
        current_stock=stock,
        buyer_recs=buyer,
    )


def current_iso_week() -> int:
    return int(datetime.now().isocalendar().week)

