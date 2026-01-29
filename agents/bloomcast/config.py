from __future__ import annotations

from dataclasses import dataclass
import os


@dataclass(frozen=True)
class BloomCastConfig:
    # Change these three fields to switch country/store in ~10 seconds.
    STORE_CITY: str = os.getenv("BLOOMCAST_STORE_CITY", "Stockholm")
    COUNTRY_CODE: str = os.getenv("BLOOMCAST_COUNTRY_CODE", "SE")  # e.g. "SE" or "NL"
    CURRENCY: str = os.getenv("BLOOMCAST_CURRENCY", "SEK")  # e.g. "SEK" or "EUR"

    # Helpful for week number + “next 14 days” accuracy.
    TIMEZONE: str = os.getenv("BLOOMCAST_TIMEZONE", "Europe/Stockholm")


CONFIG = BloomCastConfig()

