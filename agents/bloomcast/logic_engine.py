from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict, Optional

import pandas as pd

from data_ingestor import IngestedData, current_iso_week


@dataclass(frozen=True)
class ProposalRow:
    product: str
    product_name: str
    base: float
    peer_avg: float
    peer_gap: float
    peer_adjustment: float
    buyer_boost: float
    total: int
    stock_level: float
    breakdown: str


class BloomCastOptimizer:
    """
    Pure Data Edition (deterministic):
      1) Baseline: average client sales for current ISO week
      2) Opportunity: if peers > client: add (peer_avg - client_avg) * PEER_WEIGHT
      3) Trend: if in Buyer_Recs: add BUYER_BOOST
      4) Filter: if not in Current_Stock or StockLevel == 0 -> remove
    """

    def __init__(self, *, current_week: Optional[int] = None):
        self.current_week = int(current_week) if current_week is not None else current_iso_week()

    @staticmethod
    def _avg_for_week(history_weekly: pd.DataFrame, week: int) -> Dict[str, float]:
        """
        history_weekly columns: Product, IsoYear, IsoWeek, Qty
        Returns: product -> mean(Qty over IsoYear) for the given IsoWeek
        """
        df = history_weekly.copy()
        df["IsoWeek"] = pd.to_numeric(df["IsoWeek"], errors="coerce").fillna(-1).astype(int)
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0.0)
        df["Product"] = df["Product"].astype(str).str.strip()
        df = df[(df["IsoWeek"] == int(week)) & (df["Product"] != "")]
        if df.empty:
            return {}
        avg = df.groupby("Product")["Qty"].mean().to_dict()
        return {str(k): float(v) for k, v in avg.items()}

    def optimize(self, ingested: IngestedData) -> pd.DataFrame:
        cfg = ingested.config or {}
        peer_weight = float(cfg.get("PEER_WEIGHT", 0.2))
        buyer_boost = float(cfg.get("BUYER_BOOST", 10.0))

        client_avg_map = self._avg_for_week(ingested.history_client_weekly, self.current_week)
        peer_avg_map = self._avg_for_week(ingested.history_peers_weekly, self.current_week)

        stock = ingested.current_stock.copy()
        stock["Product"] = stock["Product"].astype(str).str.strip()
        stock["StockLevel"] = pd.to_numeric(stock["StockLevel"], errors="coerce").fillna(0.0)
        stock_map = dict(zip(stock["Product"], stock["StockLevel"]))

        buyer_set = set(ingested.buyer_recs["Product"].astype(str).str.strip().tolist())

        name_map: Dict[str, str] = {}
        try:
            cat = ingested.product_catalog.copy()
            cat["Product"] = cat["Product"].astype(str).str.strip()
            cat["ProductName"] = cat["ProductName"].astype(str).str.strip()
            name_map = dict(zip(cat["Product"], cat["ProductName"]))
        except Exception:
            name_map = {}

        # Candidate products: union of history products (client+peers)
        products = set(client_avg_map.keys()) | set(peer_avg_map.keys())
        rows: list[ProposalRow] = []

        for product in sorted(products):
            stock_level = float(stock_map.get(product, 0.0))
            # Step 4: Filter
            if stock_level <= 0.0:
                continue

            base = float(client_avg_map.get(product, 0.0))
            peer_avg = float(peer_avg_map.get(product, 0.0))

            # Step 2: Opportunity
            peer_gap = max(0.0, peer_avg - base)
            peer_adjustment = peer_gap * peer_weight

            # Step 3: Trend
            boost = float(buyer_boost if product in buyer_set else 0.0)

            total = base + peer_adjustment + boost
            total_int = int(max(0, math.floor(total + 0.5)))  # round-half-up to nearest integer

            breakdown = (
                f"{base:.2f} (Base) + {peer_adjustment:.2f} (Peer Gap * {peer_weight:g}) + "
                f"{boost:.2f} (Buyer Boost) = {total_int} Total"
            )

            rows.append(
                ProposalRow(
                    product=product,
                    product_name=name_map.get(product, ""),
                    base=base,
                    peer_avg=peer_avg,
                    peer_gap=peer_gap,
                    peer_adjustment=peer_adjustment,
                    buyer_boost=boost,
                    total=total_int,
                    stock_level=stock_level,
                    breakdown=breakdown,
                )
            )

        out = pd.DataFrame([r.__dict__ for r in rows])
        if not out.empty:
            out = out.sort_values(["total", "product"], ascending=[False, True]).reset_index(drop=True)
        return out

