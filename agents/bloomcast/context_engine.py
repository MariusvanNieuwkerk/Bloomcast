from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Optional

import holidays

from config import BloomCastConfig, CONFIG


@dataclass(frozen=True)
class HolidayContext:
    upcoming: bool
    name: Optional[str] = None
    date_iso: Optional[str] = None
    days_until: Optional[int] = None


class ContextFetcher:
    def __init__(self, *, config: BloomCastConfig = CONFIG):
        self.config = config

    def get_holidays(self, country_code: Optional[str] = None, *, days_ahead: int = 14) -> HolidayContext:
        """
        Uses python-holidays to find the next holiday in the specified country.

        Brief requirement:
          - Return True + holiday name if a holiday is within the next 14 days.

        We return a structured object for easier downstream logic + reporting.
        """
        cc = (country_code or self.config.COUNTRY_CODE).upper()
        tz = ZoneInfo(self.config.TIMEZONE)
        today = datetime.now(tz).date()
        end = (datetime.now(tz) + timedelta(days=days_ahead)).date()

        try:
            h = holidays.country_holidays(cc)
        except Exception:
            # Unsupported country code → act as “no upcoming holiday”.
            return HolidayContext(upcoming=False)

        upcoming: list[tuple[datetime, str]] = []
        d = today
        while d <= end:
            if d in h:
                name = str(h.get(d))
                upcoming.append((datetime(d.year, d.month, d.day, tzinfo=tz), name))
            d = d + timedelta(days=1)

        if not upcoming:
            return HolidayContext(upcoming=False)

        first_dt, first_name = sorted(upcoming, key=lambda x: x[0])[0]
        days_until = (first_dt.date() - today).days
        return HolidayContext(
            upcoming=True,
            name=first_name,
            date_iso=first_dt.date().isoformat(),
            days_until=days_until,
        )

    def get_weather(self, city: Optional[str] = None) -> dict[str, Any]:
        """
        MVP: return mock weather context.
        """
        _city = city or self.config.STORE_CITY
        # Slightly different defaults per city to make the demo feel “real”.
        normalized = _city.strip().lower()
        if "stockholm" in normalized:
            return {"temp": 2, "condition": "Cloudy"}
        if "amsterdam" in normalized:
            return {"temp": 9, "condition": "Partly Cloudy"}
        return {"temp": 22, "condition": "Sunny"}

