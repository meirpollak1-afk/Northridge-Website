#!/usr/bin/env python3
"""
scripts/update_rates.py

Fetches the 5 benchmark rates from the FRED API and writes rates.json.
Called by .github/workflows/update-rates.yml every 6 hours.

Requires environment variable: FRED_API_KEY
Never hardcode the key here — it is injected by GitHub Actions as a secret.

Series fetched:
  prime  → WPRIME   Bank Prime Loan Rate (weekly, Fed H.15)
  sofr   → SOFR     Secured Overnight Financing Rate
  t3     → DGS3     3-Year Treasury Constant Maturity
  t5     → DGS5     5-Year Treasury Constant Maturity
  t10    → DGS10    10-Year Treasury Constant Maturity
"""

import json
import os
import sys
from datetime import date, timezone, datetime
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

FRED_BASE = "https://api.stlouisfed.org/fred/series/observations"

SERIES = [
    ("prime", "WPRIME"),
    ("sofr",  "SOFR"),
    ("t3",    "DGS3"),
    ("t5",    "DGS5"),
    ("t10",   "DGS10"),
]

OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "rates.json")


def fetch_series(series_id: str, api_key: str) -> dict | None:
    """
    Fetch the most recent valid observation for one FRED series.
    Requests the last 10 rows (descending) and walks back until it finds
    a real numeric value — skipping "." entries that FRED uses for
    weekends, holidays, and missing data.

    Returns {"value": "6.75", "date": "2026-03-14"} or None on failure.
    """
    url = (
        f"{FRED_BASE}"
        f"?series_id={series_id}"
        f"&api_key={api_key}"
        f"&limit=10"
        f"&sort_order=desc"
        f"&file_type=json"
    )

    try:
        with urlopen(url, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        print(f"  ERROR {series_id}: HTTP {exc.code} — {exc.reason}", file=sys.stderr)
        return None
    except URLError as exc:
        print(f"  ERROR {series_id}: network error — {exc.reason}", file=sys.stderr)
        return None
    except Exception as exc:
        print(f"  ERROR {series_id}: {exc}", file=sys.stderr)
        return None

    observations = data.get("observations", [])
    for obs in observations:
        raw = obs.get("value", "")
        if raw and raw != ".":
            try:
                value = float(raw)
                if value > 0:
                    return {
                        "value": f"{value:.2f}",
                        "date":  obs["date"],
                    }
            except (ValueError, TypeError):
                continue

    print(f"  WARNING {series_id}: no valid observation in last 10 rows", file=sys.stderr)
    return None


def main():
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        print("FATAL: FRED_API_KEY environment variable is not set.", file=sys.stderr)
        print("Add it as a repository secret in GitHub → Settings → Secrets.", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching rates — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")

    result = {}
    dates  = []
    any_failed = False

    for key, series_id in SERIES:
        print(f"  {series_id} ({key}) ...", end=" ")
        entry = fetch_series(series_id, api_key)
        if entry:
            result[key] = entry
            dates.append(entry["date"])
            print(f"{entry['value']}%  ({entry['date']})")
        else:
            # Keep existing value if present, so a transient FRED outage
            # doesn't wipe a previously good value from rates.json.
            existing = _load_existing(key)
            if existing:
                result[key] = existing
                print(f"kept existing {existing['value']}%")
            else:
                print("FAILED — no fallback available")
            any_failed = True

    if not result:
        print("FATAL: All series failed and no existing data to preserve.", file=sys.stderr)
        sys.exit(1)

    # Use the most recent date across successful fetches, or today as fallback
    if dates:
        dates.sort()
        last_updated = dates[-1]
    else:
        last_updated = date.today().isoformat()

    result["lastUpdated"] = last_updated

    # Write rates.json (compact but readable)
    output_path = os.path.abspath(OUTPUT_PATH)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
        f.write("\n")

    print(f"\nWrote {output_path}")
    print(f"lastUpdated: {last_updated}")

    if any_failed:
        print("\nWARNING: One or more series failed. Kept previous values where available.")

    # Exit 0 even with partial failures — we don't want the workflow to error
    # if FRED has a temporary outage; the commit will still include good data.


def _load_existing(key: str) -> dict | None:
    """Read the current rates.json and return the existing entry for key, or None."""
    output_path = os.path.abspath(OUTPUT_PATH)
    try:
        with open(output_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entry = data.get(key)
        if entry and entry.get("value"):
            return entry
    except (FileNotFoundError, json.JSONDecodeError):
        pass
    return None


if __name__ == "__main__":
    main()
