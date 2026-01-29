from __future__ import annotations

from dataclasses import asdict
from pathlib import Path
import random
from typing import Optional, Union

import pandas as pd

from config import BloomCastConfig, CONFIG


def _currency_price_band(currency: str) -> tuple[float, float]:
    # Very rough, just to make the demo feel local.
    if currency.upper() in {"SEK", "NOK", "DKK"}:
        return (39.0, 299.0)
    if currency.upper() in {"EUR", "GBP", "USD"}:
        return (3.5, 29.0)
    return (5.0, 50.0)


def generate_sales_history_csv(
    output_path: Union[str, Path] = "sales_history.csv",
    *,
    config: BloomCastConfig = CONFIG,
    rows: int = 18,
    seed: Optional[int] = None,
) -> Path:
    """
    Generates a dummy sales_history.csv relevant to the config.

    Required columns (per brief):
      - product_id, product_name, category, units_sold, waste_pct, stock

    Additional columns to support the “currency / pricing” requirement:
      - unit_price, currency
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    stable_seed = seed if seed is not None else abs(hash(tuple(asdict(config).values()))) % (2**32)
    rng = random.Random(stable_seed)

    catalog = [
        ("P-1001", "Hydrangea", "Flowering"),
        ("P-1002", "Tulip", "Flowering"),
        ("P-1003", "Olive Tree", "Outdoor"),
        ("P-1004", "Rose", "Flowering"),
        ("P-1005", "Orchid", "Indoor"),
        ("P-1006", "Lavender", "Outdoor"),
        ("P-1007", "Sunflower", "Flowering"),
        ("P-1008", "Basil Plant", "Indoor"),
        ("P-1009", "Monstera", "Indoor"),
        ("P-1010", "Geranium", "Outdoor"),
        ("P-1011", "Poinsettia", "Flowering"),
        ("P-1012", "Daffodil", "Flowering"),
    ]

    min_p, max_p = _currency_price_band(config.CURRENCY)

    records: list[dict] = []
    for i in range(rows):
        product_id, product_name, category = catalog[i % len(catalog)]

        units_sold = rng.randint(4, 140)
        waste_pct = round(rng.uniform(2.0, 35.0), 1)  # includes >20% for Bleeder rule demo
        stock = rng.randint(0, 120)
        unit_price = round(rng.uniform(min_p, max_p), 2)

        records.append(
            {
                "product_id": product_id,
                "product_name": product_name,
                "category": category,
                "units_sold": units_sold,
                "waste_pct": waste_pct,
                "stock": stock,
                "unit_price": unit_price,
                "currency": config.CURRENCY.upper(),
            }
        )

    df = pd.DataFrame.from_records(records)
    df.to_csv(output_path, index=False)
    return output_path


if __name__ == "__main__":
    path = generate_sales_history_csv(config=CONFIG)
    print(f"Generated: {path.resolve()}")

