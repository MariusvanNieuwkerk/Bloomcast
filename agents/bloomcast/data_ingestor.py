from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, Optional, Union

import pandas as pd


@dataclass(frozen=True)
class IngestedData:
    config: Dict[str, Any]
    history_client_weekly: pd.DataFrame
    history_peers_weekly: pd.DataFrame
    current_stock: pd.DataFrame
    buyer_recs: pd.DataFrame
    product_catalog: pd.DataFrame  # cols: Product, ProductName


def _norm(s: str) -> str:
    return str(s).strip().lower()


def _normalize_product_value(v: Any) -> str:
    """
    Normalize product identifiers so different sheets match:
    - 1234.0 -> "1234"
    - " 1234 " -> "1234"
    - None/NaN -> ""
    """
    if v is None:
        return ""
    try:
        if pd.isna(v):  # type: ignore[arg-type]
            return ""
    except Exception:
        pass
    if isinstance(v, bool):
        return ""
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float) and float(v).is_integer():
        return str(int(v))
    s = str(v).strip()
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


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
    out["Product"] = out["Product"].apply(_normalize_product_value)
    out = out[out["Product"] != ""]

    agg = (
        out.groupby(["Product", "IsoYear", "IsoWeek"], as_index=False)["Qty"]
        .sum()
        .sort_values(["Product", "IsoYear", "IsoWeek"])
        .reset_index(drop=True)
    )
    return agg


def _find_sheet(excel: pd.ExcelFile, *, preferred: Optional[str], candidates: list[str]) -> Optional[str]:
    sheet_names = list(excel.sheet_names)
    sheet_map = {_norm(s): s for s in sheet_names}

    if preferred:
        pref = _norm(preferred)
        if pref in sheet_map:
            return sheet_map[pref]

    # Exact candidates (case-insensitive)
    for cand in candidates:
        c = _norm(cand)
        if c in sheet_map:
            return sheet_map[c]

    # Contains candidates
    for s in sheet_names:
        sn = _norm(s)
        for cand in candidates:
            if _norm(cand) in sn:
                return s
    return None


def _read_sheet(excel_obj: Union[str, Path, BytesIO], sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(excel_obj, sheet_name=sheet_name, engine="openpyxl")
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _pick_first_col(df: pd.DataFrame, aliases: list[str]) -> Optional[str]:
    cols = list(df.columns)
    by_norm = {_norm(c): c for c in cols}
    for a in aliases:
        if _norm(a) in by_norm:
            return by_norm[_norm(a)]
    # contains match
    for c in cols:
        cn = _norm(c)
        for a in aliases:
            if _norm(a) in cn:
                return c
    return None


def _override_or_detect(df: pd.DataFrame, override: Any, aliases: list[str]) -> Optional[str]:
    if override is not None:
        o = str(override).strip()
        if o and o in df.columns:
            return o
    return _pick_first_col(df, aliases)


def _extract_history_long(df: pd.DataFrame, *, date_col: str, product_col: str, qty_col: str) -> pd.DataFrame:
    out = df[[date_col, product_col, qty_col]].copy()
    out = out.rename(columns={date_col: "Date", product_col: "Product", qty_col: "Qty"})
    out["Product"] = out["Product"].apply(_normalize_product_value)
    return out


def _extract_peers_history_long(
    df: pd.DataFrame, *, date_col: str, product_col: str, qty_col: str, peer_col: Optional[str]
) -> pd.DataFrame:
    cols = [date_col, product_col, qty_col]
    if peer_col:
        cols.append(peer_col)
    out = df[cols].copy()
    rename_map = {date_col: "Date", product_col: "Product", qty_col: "Qty"}
    if peer_col:
        rename_map[peer_col] = "Peer"
    out = out.rename(columns=rename_map)
    out["Product"] = out["Product"].apply(_normalize_product_value)
    if "Peer" in out.columns:
        out["Peer"] = out["Peer"].astype(str).str.strip()
        out = out[out["Peer"] != ""]
    return out


def _extract_product_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """
    Best-effort extraction of (Product, ProductName) from any sheet containing an ID and an Omschrijving/Description.
    """
    product_col = _pick_first_col(df, ["Product", "Artikel", "Artikel nr", "Artikelnr", "Artikelnummer"])
    name_col = _pick_first_col(df, ["Omschrijving", "ProductName", "Product name", "Description"])
    if not product_col or not name_col:
        return pd.DataFrame(columns=["Product", "ProductName"])

    out = df[[product_col, name_col]].copy()
    out = out.rename(columns={product_col: "Product", name_col: "ProductName"})
    out["Product"] = out["Product"].apply(_normalize_product_value)
    out["ProductName"] = out["ProductName"].astype(str).str.strip()
    out = out[(out["Product"] != "") & (out["ProductName"] != "")]
    out = out.drop_duplicates(subset=["Product"], keep="first").reset_index(drop=True)
    return out


def _history_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    # Normalized columns expected: Date, Product, Qty
    weekly = _aggregate_qty_by_product_week(_to_week(df, "Date"))
    return weekly


def _peers_history_to_weekly(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalized columns expected: Date, Product, Qty, optional Peer.
    If Peer exists, keep it so we can compute a true peer average per customer.
    """
    out = df.copy()
    out = _to_week(out, "Date")
    out["Qty"] = pd.to_numeric(out["Qty"], errors="coerce").fillna(0)
    out["Product"] = out["Product"].apply(_normalize_product_value)
    out = out[out["Product"] != ""]
    if "Peer" in out.columns:
        out["Peer"] = out["Peer"].astype(str).str.strip()
        out = out[out["Peer"] != ""]
        agg = (
            out.groupby(["Product", "Peer", "IsoYear", "IsoWeek"], as_index=False)["Qty"]
            .sum()
            .sort_values(["Product", "Peer", "IsoYear", "IsoWeek"])
            .reset_index(drop=True)
        )
        return agg
    return _aggregate_qty_by_product_week(out)


def _build_stock_from_assortment(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    # Try to convert an assortment sheet into (Product, StockLevel) as an "availability" proxy.
    product_col = _pick_first_col(df, ["Product", "Artikel", "Artikel nr", "Artikelnr", "Artikelnummer", "Omschrijving"])
    if not product_col:
        return None

    leverbaar_col = _pick_first_col(df, ["Leverbaar", "Available", "Beschikbaar"])
    stock_col = _pick_first_col(
        df,
        [
            "StockLevel",
            "Stock",
            "Voorraad",
            "Voorraadniveau",
            "Voorraad aanwezig",
            "Beschikbare voorraad",
            "Available stock",
            "On hand",
        ],
    )

    out = pd.DataFrame()
    out["Product"] = df[product_col].apply(_normalize_product_value)
    out = out[out["Product"] != ""]

    if stock_col:
        out["StockLevel"] = pd.to_numeric(df[stock_col], errors="coerce").fillna(0.0)
        return out[["Product", "StockLevel"]].reset_index(drop=True)

    if leverbaar_col:
        # Convert WAAR/TRUE/1 to 1, else 0
        s = df[leverbaar_col]
        if s.dtype == bool:
            out["StockLevel"] = s.astype(int)
        else:
            out["StockLevel"] = s.astype(str).str.strip().str.lower().isin(["waar", "true", "1", "yes", "ja"]).astype(int)
        return out[["Product", "StockLevel"]].reset_index(drop=True)

    return None


def _looks_like_availability(series: pd.Series) -> bool:
    """
    Heuristic: treat as availability if it's basically boolean/0/1.
    This prevents showing a voorraadkolom full of '1' when the export only has leverbaarheid.
    """
    try:
        if series.dtype == bool:
            return True
    except Exception:
        pass
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return False
    # If almost everything is 0/1, assume it's availability.
    unique = set(s.unique().tolist())
    if unique.issubset({0, 1}):
        return True
    # Some exports encode availability as 0/1 floats with noise; be conservative.
    if s.min() >= 0 and s.max() <= 1 and (s.round().isin([0, 1]).mean() >= 0.98):
        return True
    return False


def ingest_client_data(filepath: Union[str, Path, bytes]) -> IngestedData:
    """
    Ingest client Excel in either of these shapes:

    A) Template format (recommended)
      - Config, History_Client, History_Peers, Current_Stock, Buyer_Recs

    B) Existing ERP/Export formats (best-effort autodetect)
      - e.g. Dutch sheet names like 'klanthistorie', 'Historie andere klanten', 'Aanbevolen assortiment'
      - and Dutch columns like 'Orderdatum', 'Artikel', 'Aantal', 'Leverbaar'

    Action:
      - Convert dates to ISO Week Numbers
      - Aggregate quantities by Product + Week Number
      - Return processed DataFrames + config dict
    """
    if isinstance(filepath, (bytes, bytearray)):
        excel_obj = BytesIO(bytes(filepath))
    else:
        excel_obj = filepath

    excel = pd.ExcelFile(excel_obj, engine="openpyxl")

    # Read Config if present (supports numeric + string overrides)
    config: Dict[str, Any] = {}
    cfg_sheet = _find_sheet(excel, preferred=None, candidates=["Config", "Configuratie", "Instellingen", "Settings"])
    if cfg_sheet:
        cfg_df = _read_sheet(excel_obj, cfg_sheet)
        cfg_df = cfg_df.rename(columns={"setting": "Setting", "value": "Value"})
        setting_col = _pick_first_col(cfg_df, ["Setting", "Instelling", "Key"])
        value_col = _pick_first_col(cfg_df, ["Value", "Waarde"])
        if setting_col and value_col:
            for _, r in cfg_df.iterrows():
                k = str(r.get(setting_col, "")).strip()
                if not k:
                    continue
                v = r.get(value_col, None)
                # Store raw; optimizer will parse numbers where needed.
                config[k] = v

    # Provide deterministic defaults if not present
    config.setdefault("PEER_WEIGHT", 0.2)
    config.setdefault("BUYER_BOOST", 10.0)

    # Resolve sheet names (allow overrides via Config keys)
    # If the user provides these keys in Config, they will be used:
    #  - HISTORY_CLIENT_SHEET, HISTORY_PEERS_SHEET, CURRENT_STOCK_SHEET, BUYER_RECS_SHEET
    hc_sheet = _find_sheet(
        excel,
        preferred=str(config.get("HISTORY_CLIENT_SHEET", "")).strip() or None,
        candidates=["History_Client", "Klanthistorie", "Klant historie", "Client history", "Historie klant"],
    )
    hp_sheet = _find_sheet(
        excel,
        preferred=str(config.get("HISTORY_PEERS_SHEET", "")).strip() or None,
        candidates=["History_Peers", "Historie andere klanten", "Peer history", "Peers", "Andere klanten"],
    )
    buyer_sheet = _find_sheet(
        excel,
        preferred=str(config.get("BUYER_RECS_SHEET", "")).strip() or None,
        candidates=["Buyer_Recs", "Aanbevolen assortiment", "Aanbevolen Assortiment", "Buyer", "Recommendations"],
    )
    stock_sheet = _find_sheet(
        excel,
        preferred=str(config.get("CURRENT_STOCK_SHEET", "")).strip() or None,
        candidates=["Current_Stock", "Voorraad", "Stock", "Basis assortiment", "Basisassortiment", "Assortiment"],
    )

    if not hc_sheet or not hp_sheet:
        raise ValueError(
            "Could not find required history sheets. Provide template sheet names or set HISTORY_CLIENT_SHEET / HISTORY_PEERS_SHEET in Config."
        )

    # History sheets (autodetect columns; allow overrides)
    hc_raw = _read_sheet(excel_obj, hc_sheet)
    hp_raw = _read_sheet(excel_obj, hp_sheet)

    date_aliases = ["Date", "Orderdatum", "Verzenddatum", "Datum", "Verkoopdatum", "DateTime"]
    qty_aliases = ["Qty", "Aantal", "Quantity", "Verkoopaantal", "Aantal stuks"]
    product_aliases = ["Product", "Artikel", "Artikel nr", "Artikelnr", "Artikelnummer", "Omschrijving"]
    peer_aliases = [
        "Peer",
        "Klant",
        "Klantnaam",
        "Klant nr",
        "Klantnr",
        "Debiteur",
        "Debiteurnr",
        "Customer",
        "CustomerName",
        "Customer No",
        "CustomerNo",
        "Account",
    ]

    hc_date = _override_or_detect(hc_raw, config.get("HISTORY_CLIENT_DATE_COL"), date_aliases)
    hc_qty = _override_or_detect(hc_raw, config.get("HISTORY_CLIENT_QTY_COL"), qty_aliases)
    hc_prod = _override_or_detect(hc_raw, config.get("HISTORY_CLIENT_PRODUCT_COL"), product_aliases)
    if not (hc_date and hc_qty and hc_prod):
        raise ValueError("Could not detect Date/Product/Qty columns for client history sheet.")

    hp_date = _override_or_detect(hp_raw, config.get("HISTORY_PEERS_DATE_COL"), date_aliases)
    hp_qty = _override_or_detect(hp_raw, config.get("HISTORY_PEERS_QTY_COL"), qty_aliases)
    hp_prod = _override_or_detect(hp_raw, config.get("HISTORY_PEERS_PRODUCT_COL"), product_aliases)
    hp_peer = _override_or_detect(hp_raw, config.get("HISTORY_PEERS_PEER_COL"), peer_aliases)
    if not (hp_date and hp_qty and hp_prod):
        raise ValueError("Could not detect Date/Product/Qty columns for peers history sheet.")

    hc_norm = _extract_history_long(hc_raw, date_col=hc_date, product_col=hc_prod, qty_col=hc_qty)
    hp_norm = _extract_peers_history_long(hp_raw, date_col=hp_date, product_col=hp_prod, qty_col=hp_qty, peer_col=hp_peer)

    hc_week = _history_to_weekly(hc_norm)
    hp_week = _peers_history_to_weekly(hp_norm)

    # Buyer recs
    if buyer_sheet:
        buyer_raw = _read_sheet(excel_obj, buyer_sheet)
        buyer_product_col = _override_or_detect(buyer_raw, config.get("BUYER_RECS_PRODUCT_COL"), product_aliases)
        if buyer_product_col:
            buyer = buyer_raw[[buyer_product_col]].copy().rename(columns={buyer_product_col: "Product"})
        else:
            buyer = pd.DataFrame(columns=["Product"])
    else:
        buyer = pd.DataFrame(columns=["Product"])

    buyer["Product"] = buyer["Product"].apply(_normalize_product_value)
    buyer = buyer[buyer["Product"] != ""].drop_duplicates(subset=["Product"]).reset_index(drop=True)

    # Current stock / availability
    stock_df: Optional[pd.DataFrame] = None
    stock_catalog = pd.DataFrame(columns=["Product", "ProductName"])
    if stock_sheet:
        stock_raw = _read_sheet(excel_obj, stock_sheet)
        stock_catalog = _extract_product_catalog(stock_raw)
        # Determine whether we have true quantities or only availability.
        detected_stock_qty_col = _pick_first_col(
            stock_raw,
            [
                "StockLevel",
                "Stock",
                "Voorraad",
                "Voorraadniveau",
                "Voorraad aanwezig",
                "Beschikbare voorraad",
                "Available stock",
                "On hand",
            ],
        )
        detected_avail_col = _pick_first_col(stock_raw, ["Leverbaar", "Available", "Beschikbaar"])
        if detected_stock_qty_col and not _looks_like_availability(stock_raw[detected_stock_qty_col]):
            config["STOCK_MODE"] = "quantity"
            config["STOCK_COL"] = detected_stock_qty_col
        elif detected_avail_col:
            config["STOCK_MODE"] = "availability"
            config["STOCK_COL"] = detected_avail_col
        elif detected_stock_qty_col:
            # Quantity-like column exists but behaves like 0/1 → treat as availability.
            config["STOCK_MODE"] = "availability"
            config["STOCK_COL"] = detected_stock_qty_col
        else:
            config["STOCK_MODE"] = "unknown"

        # If it already matches template, use it.
        if "Product" in stock_raw.columns and "StockLevel" in stock_raw.columns:
            stock_df = stock_raw[["Product", "StockLevel"]].copy()
        else:
            stock_df = _build_stock_from_assortment(stock_raw)

    if stock_df is None:
        # Fallback: build a "presence-only" stock list from products seen in histories.
        # This keeps the workflow running, but should be documented to users.
        products = sorted(set(hc_week["Product"].astype(str)) | set(hp_week["Product"].astype(str)))
        stock_df = pd.DataFrame({"Product": products, "StockLevel": [1.0] * len(products)})
        config.setdefault("STOCK_SOURCE", "fallback_presence_only")
    else:
        config.setdefault("STOCK_SOURCE", str(stock_sheet))

    stock_df["Product"] = stock_df["Product"].apply(_normalize_product_value)
    stock_df["StockLevel"] = pd.to_numeric(stock_df["StockLevel"], errors="coerce").fillna(0.0)
    stock_df = stock_df[stock_df["Product"] != ""].reset_index(drop=True)

    # Build product catalog (prefer stock sheet names, then client history, then peers)
    catalog_client = _extract_product_catalog(hc_raw)
    catalog_peers = _extract_product_catalog(hp_raw)
    catalog_buyer = pd.DataFrame(columns=["Product", "ProductName"])
    if buyer_sheet:
        try:
            buyer_raw = _read_sheet(excel_obj, buyer_sheet)
            catalog_buyer = _extract_product_catalog(buyer_raw)
        except Exception:
            catalog_buyer = pd.DataFrame(columns=["Product", "ProductName"])

    combined = pd.concat([stock_catalog, catalog_client, catalog_peers, catalog_buyer], ignore_index=True)
    combined["Product"] = combined["Product"].apply(_normalize_product_value)
    combined["ProductName"] = combined["ProductName"].astype(str).str.strip()
    combined = combined[(combined["Product"] != "") & (combined["ProductName"] != "")]
    combined = combined.drop_duplicates(subset=["Product"], keep="first").reset_index(drop=True)

    return IngestedData(
        config=config,
        history_client_weekly=hc_week,
        history_peers_weekly=hp_week,
        current_stock=stock_df,
        buyer_recs=buyer,
        product_catalog=combined,
    )


def current_iso_week() -> int:
    return int(datetime.now().isocalendar().week)

