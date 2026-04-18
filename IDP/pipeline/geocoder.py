"""
geocoder.py — LocationIQ geocoding for facility records.

Resolves facility name + city + state + country to latitude/longitude
using the LocationIQ REST API directly (geopy's LocationIQ class is not
available in geopy >= 2.x).

geopy.distance.geodesic is still available via:
    from geopy.distance import geodesic
    geodesic((lat1, lon1), (lat2, lon2)).km

Rate limit: LocationIQ free tier = 2 req/sec → 0.55s sleep between calls.
"""

import logging
import os
import time
from typing import Optional

import requests
from requests.exceptions import Timeout, ConnectionError as ReqConnectionError
import threading

logger = logging.getLogger(__name__)

_LOCATIONIQ_URL = "https://us1.locationiq.com/v1/search"
_API_LOCK = threading.Lock()


class FacilityGeocoder:
    """Geocode healthcare facilities using the LocationIQ REST API.

    Cascading query strategy (stops at first successful result):
      1. "{name}, {city}, {state}, {country}"  — most specific
      2. "{city}, {state}, {country}"
      3. "{city}, {country}"                   — extracts state from response
      4. "{name}, {country}"                   — last resort; extracts city+state
      5. All fail → lat=None, lon=None
    """

    _RATE_DELAY = 0.55   # seconds between API calls (stays under 2 req/sec)
    _RETRY_DELAY = 2.0   # seconds before a single retry on timeout

    def __init__(self) -> None:
        self._api_key = os.getenv("LOCATION_IQ_ACCESS_TOKEN")
        if not self._api_key:
            raise ValueError(
                "LOCATION_IQ_ACCESS_TOKEN is not set in environment. "
                "Add it to your .env file."
            )

    # ── Public API ──────────────────────────────────────────────────────

    def geocode_facility(
        self,
        name: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: Optional[str] = "Ghana",
    ) -> dict:
        """Resolve facility location to GPS coordinates.

        Returns
        -------
        dict with keys:
          latitude       : float | None
          longitude      : float | None
          resolved_city  : str | None   — backfill value when inferred from API
          resolved_state : str | None   — backfill value when inferred from API
        """
        result: dict = {
            "latitude": None,
            "longitude": None,
        }

        country = (country or "Ghana").strip()
        queries = self._build_query_cascade(name, city, state, country)
        facility_label = name or city or "unknown facility"
        attempted: list[str] = []

        for query in queries:
            attempted.append(query)
            loc = self._call_api_with_retry(query)
            if loc is None:
                logger.warning(
                    "[Geocode] No result for query '%s' (facility: '%s')",
                    query, facility_label,
                )
                continue

            result["latitude"] = float(loc["lat"])
            result["longitude"] = float(loc["lon"])
            return result

        logger.warning(
            "[Geocode] ✘ ALL queries failed for '%s'. "
            "Tried %d queries: %s — storing null coordinates.",
            facility_label,
            len(attempted),
            " | ".join(f"({i+1}) '{q}'" for i, q in enumerate(attempted)),
        )
        return result

    # ── Private helpers ─────────────────────────────────────────────────

    def _build_query_cascade(
        self,
        name: Optional[str],
        city: Optional[str],
        state: Optional[str],
        country: str,
    ) -> list[str]:
        """Return ordered list of query strings to try."""
        queries: list[str] = []
        n = (name or "").strip()
        c = (city or "").strip()
        s = (state or "").strip()

        # 1. Full: name + city + state + country (most specific)
        if n and c and s:
            queries.append(f"{n}, {c}, {s}, {country}")

        # 2. No name: city + state + country
        if c and s:
            queries.append(f"{c}, {s}, {country}")

        # 3. No state: city + country
        if c:
            queries.append(f"{c}, {country}")

        # 4. No city/state: name + country
        if n and not c:
            queries.append(f"{n}, {country}")

        return queries

    def _call_api_with_retry(self, query: str) -> Optional[dict]:
        """Call LocationIQ REST API with one automatic retry on timeout."""
        params = {
            "key": self._api_key,
            "q": query,
            "format": "json",
            "limit": 1,
            "countrycodes": "gh",   # restrict to Ghana
        }

        for attempt in range(2):
            try:
                with _API_LOCK:
                    time.sleep(self._RATE_DELAY)
                    response = requests.get(
                        _LOCATIONIQ_URL, params=params, timeout=10
                    )

                if response.status_code == 200:
                    data = response.json()
                    if data:
                        return data[0]
                    logger.warning(
                        "[Geocode] Empty result (200 OK, no match) for query '%s'", query
                    )
                    return None
                elif response.status_code == 429:
                    logger.warning(
                        "[Geocode] Rate limited (429) on query '%s' — backing off 2s.", query
                    )
                    time.sleep(2.0)
                    continue
                else:
                    logger.warning(
                        "[Geocode] HTTP %d for query '%s' — body: %s",
                        response.status_code, query, response.text[:200],
                    )
                    return None
            except Timeout:
                if attempt == 0:
                    logger.warning(
                        "[Geocode] Timeout on query '%s' (attempt 1) — retrying after %.1fs.",
                        query, self._RETRY_DELAY,
                    )
                    time.sleep(self._RETRY_DELAY)
                else:
                    logger.warning(
                        "[Geocode] Timeout on query '%s' after retry — giving up.", query
                    )
            except ReqConnectionError as exc:
                logger.warning(
                    "[Geocode] Connection error for query '%s': %s", query, exc
                )
                return None

        return None
