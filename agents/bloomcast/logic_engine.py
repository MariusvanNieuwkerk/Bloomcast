from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Optional

import pandas as pd

from context_engine import HolidayContext


@dataclass(frozen=True)
class Recommendation:
    action: str
    reasoning: str
    recommended_order_units: int


class BloomCastOptimizer:
    """
    Applies BloomCast business rules to sales data + context.
    """

    def __init__(self, *, festive_keywords: Optional[set[str]] = None):
        self.festive_keywords = festive_keywords or {
            "tulip",
            "hydrangea",
            "rose",
            "orchid",
            "poinsettia",
            "daffodil",
            "sunflower",
        }

    def optimize(
        self,
        sales_df: pd.DataFrame,
        *,
        weather: dict[str, Any],
        holiday: HolidayContext,
    ) -> pd.DataFrame:
        df = sales_df.copy()

        # Normalize expected numeric columns.
        for col in ["units_sold", "waste_pct", "stock", "unit_price"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        def base_recommended_order(row: pd.Series) -> int:
            # Simple baseline: aim for 10% above last period’s sales.
            target_stock = float(row.get("units_sold", 0)) * 1.10
            current_stock = float(row.get("stock", 0))
            return max(0, int(math.ceil(target_stock - current_stock)))

        def is_festive_relevant(row: pd.Series) -> bool:
            category = str(row.get("category", "")).strip().lower()
            name = str(row.get("product_name", "")).strip().lower()
            if category == "flowering":
                return True
            return any(k in name for k in self.festive_keywords)

        rec_actions: list[str] = []
        rec_reasons: list[str] = []
        rec_units: list[int] = []

        temp = float(weather.get("temp", 0))

        for _, row in df.iterrows():
            recommended = base_recommended_order(row)

            # 1) Bleeder Rule (highest priority)
            if float(row.get("waste_pct", 0)) > 20.0:
                rec_actions.append("STOP")
                rec_reasons.append("Waste too high - BloomCast Alert")
                rec_units.append(0)
                continue

            # 2) Festive Rule
            if holiday.upcoming and is_festive_relevant(row):
                # Stock up aggressively for seasonal uplift.
                boosted = int(math.ceil(recommended * 1.75))
                boosted = max(boosted, recommended + 5) if recommended > 0 else 10
                rec_actions.append("STOCK UP")
                if holiday.name:
                    rec_reasons.append(f"Festive demand: {holiday.name} within {holiday.days_until} days")
                else:
                    rec_reasons.append("Festive demand: upcoming holiday")
                rec_units.append(boosted)
                continue

            # 3) Sunny Rule
            category = str(row.get("category", "")).strip().lower()
            if temp > 20.0 and category == "outdoor":
                boosted = int(math.ceil(recommended * 1.50))
                rec_actions.append("INCREASE 50%")
                rec_reasons.append("Sunny Forecast")
                rec_units.append(boosted)
                continue

            # Default
            rec_actions.append("KEEP")
            rec_reasons.append("Stable baseline forecast")
            rec_units.append(int(recommended))

        df["recommended_order_units"] = rec_units
        df["action"] = rec_actions
        df["reasoning"] = rec_reasons

        # Helpful derived column for reporting.
        if "unit_price" in df.columns:
            df["recommended_order_value"] = (df["recommended_order_units"] * df["unit_price"]).round(2)

        return df

