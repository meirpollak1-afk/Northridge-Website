#!/usr/bin/env python3
"""
scripts/update_rates.py

Fetches 5 benchmark rates from official sources and writes rates.json.
Run by .github/workflows/update-rates.yml every 6 hours.

Sources used — chosen to be reliable, free, and require no API key except Prime:
  prime  ← FRED WPRIME         (needs FRED_API_KEY secret)
  sofr   ← NY Fed JSON API     (no key required)
  t3     ← US Treasury XML     (no key required)
  t5     ← US Treasury XML     (no key required)
  t10    ← US Treasury XML     (no key required)

NY Fed SOFR endpoint (official, no key):
  https://markets.newyorkfed.org/api/rates/sofr/last/5.json
  Returns JSON with a "refRates" array. We look for type=="SOFR" (overnight rate).

US Treasury yield curve XML (official, no key):
  https://home.treasury.gov/resource-center/data-chart-center/interest-rates/pages/xml
  ?data=daily_treasury_yield_curve&field_tdr_date_value_month=YYYYMM
  Returns Atom XML. The most recent entry contains fields like BC_3YEAR, BC_5YEAR, BC_10YEAR.
  We fetch the current month; if it returns no entries yet (e.g. first days of month)
  we fall back to the previous month.

FRED (needs FRED_API_KEY secret):
  https://api.stlouisfed.org/fred/series/observations?series_id=WPRIME&...
  Used only for Prime Rate since WPRIME has no free keyless equivalent.
"""

import json
import os
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

OUTPUT_PATH = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "rates.json")
)

# ── helpers ───────────────────────────────────────────────────────────────────

def http_get(url, timeout=20):
    """Fetch a URL with a browser-like User-Agent and return the response body as bytes."""
    req = Request(url, headers={"User-Agent": "Mozilla/5.0 (rates-updater/1.0)"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_existing_rates():
    try:
        with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


# ── source 1: NY Fed SOFR (no API key needed) ─────────────────────────────────

def fetch_sofr():
    """
    Fetch overnight SOFR from the NY Fed public JSON API.
    Endpoint: https://markets.newyorkfed.org/api/rates/sofr/last/5.json
    Returns {"value": "4.30", "date": "2026-03-14"} or None.
    """
    url = "https://markets.newyorkfed.org/api/rates/sofr/last/5.json"
    print(f"  [sofr] fetching {url}")
    try:
        body = http_get(url)
        data = json.loads(body)
    except (HTTPError, URLError) as exc:
        print(f"  [sofr] NETWORK ERROR: {exc}", file=sys.stderr)
        return None
    except json.JSONDecodeError as exc:
        print(f"  [sofr] JSON PARSE ERROR: {exc}", file=sys.stderr)
        return None

    ref_rates = data.get("refRates", [])
    print(f"  [sofr] received {len(ref_rates)} entries")

    # Walk entries looking for type=="SOFR" (overnight, not SOFR30DAYAVG etc.)
    for entry in ref_rates:
        if entry.get("type") != "SOFR":
            continue
        rate = entry.get("percentRate")
        dt   = entry.get("effectiveDate", "")
        if rate is None:
            continue
        try:
            value = float(rate)
            if value > 0:
                formatted = f"{value:.2f}"
                print(f"  [sofr] {dt}: {formatted}%  ← using this")
                return {"value": formatted, "date": dt}
        except (ValueError, TypeError):
            continue

    print("  [sofr] WARNING: no valid SOFR entry found", file=sys.stderr)
    return None


# ── source 2: US Treasury XML (no API key needed) ────────────────────────────

def fetch_treasury_xml(year, month):
    """
    Fetch the US Treasury daily par yield curve XML for a given YYYYMM.
    Returns the parsed XML root, or None on error.
    """
    ym = f"{year}{month:02d}"
    url = (
        "https://home.treasury.gov/resource-center/data-chart-center"
        f"/interest-rates/pages/xml?data=daily_treasury_yield_curve"
        f"&field_tdr_date_value_month={ym}"
    )
    print(f"  [treasury] fetching {url}")
    try:
        body = http_get(url)
        root = ET.fromstring(body)
        return root
    except (HTTPError, URLError) as exc:
        print(f"  [treasury] NETWORK ERROR: {exc}", file=sys.stderr)
        return None
    except ET.ParseError as exc:
        print(f"  [treasury] XML PARSE ERROR: {exc}", file=sys.stderr)
        return None


def parse_treasury_latest(root):
    """
    Parse the XML root and return the most recent entry's yield values.
    The XML uses OData namespaces; field names are like d:BC_3YEAR, d:BC_5YEAR, d:BC_10YEAR.
    Returns dict mapping field_name → {"value": "3.95", "date": "2026-03-14"}, or {}.
    """
    if root is None:
        return {}

    # OData namespaces used by Treasury XML
    ns = {
        "atom": "http://www.w3.org/2005/Atom",
        "m":    "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
        "d":    "http://schemas.microsoft.com/ado/2007/08/dataservices",
    }

    entries = root.findall("atom:entry", ns)
    print(f"  [treasury] {len(entries)} entries in feed")
    if not entries:
        return {}

    # Entries are in ascending date order; take the last (most recent)
    last_entry = entries[-1]

    props = last_entry.find("atom:content/m:properties", ns)
    if props is None:
        print("  [treasury] WARNING: no properties found in last entry", file=sys.stderr)
        return {}

    # Extract the date from NEW_DATE field
    date_el = props.find("d:NEW_DATE", ns)
    raw_date = ""
    if date_el is not None and date_el.text:
        # Format is "2026-03-14T00:00:00" — take first 10 chars
        raw_date = date_el.text.strip()[:10]

    print(f"  [treasury] most recent entry date: {raw_date}")

    result = {}
    fields = {
        "BC_3YEAR":  "t3",
        "BC_5YEAR":  "t5",
        "BC_10YEAR": "t10",
    }
    for xml_field, key in fields.items():
        el = props.find(f"d:{xml_field}", ns)
        if el is not None and el.text and el.text.strip():
            try:
                value = float(el.text.strip())
                if value > 0:
                    formatted = f"{value:.2f}"
                    print(f"  [treasury] {key} ({xml_field}): {formatted}%")
                    result[key] = {"value": formatted, "date": raw_date}
                else:
                    print(f"  [treasury] {key} ({xml_field}): {el.text.strip()} (skip)")
            except (ValueError, TypeError):
                print(f"  [treasury] {key} ({xml_field}): '{el.text}' (not numeric, skip)")
        else:
            print(f"  [treasury] {key} ({xml_field}): missing or empty")

    return result


def fetch_all_treasury():
    """
    Fetch Treasury yields, trying current month then falling back to previous month.
    Returns dict with keys t3, t5, t10 each mapped to {"value", "date"}.
    """
    today = date.today()

    # Try current month first
    root = fetch_treasury_xml(today.year, today.month)
    result = parse_treasury_latest(root)

    # If we got all three, we're done
    if len(result) >= 3:
        return result

    # Fall back to previous month (handles early days of new month before any entries)
    prev = (today.replace(day=1) - timedelta(days=1))
    print(f"  [treasury] current month incomplete, trying {prev.year}-{prev.month:02d}")
    root_prev = fetch_treasury_xml(prev.year, prev.month)
    result_prev = parse_treasury_latest(root_prev)

    # Merge: current month values take precedence
    merged = {**result_prev, **result}
    return merged


# ── source 3: FRED WPRIME (requires FRED_API_KEY) ────────────────────────────

def fetch_prime(api_key):
    """
    Fetch the Bank Prime Loan Rate from FRED series WPRIME.
    Returns {"value": "6.75", "date": "2026-03-12"} or None.
    """
    url = (
        "https://api.stlouisfed.org/fred/series/observations"
        f"?series_id=WPRIME"
        f"&api_key={api_key}"
        f"&limit=10"
        f"&sort_order=desc"
        f"&file_type=json"
    )
    print(f"  [prime] fetching FRED WPRIME")
    try:
        body = http_get(url)
        data = json.loads(body)
    except (HTTPError, URLError) as exc:
        print(f"  [prime] NETWORK ERROR: {exc}", file=sys.stderr)
        return None
    except json.JSONDecodeError as exc:
        print(f"  [prime] JSON PARSE ERROR: {exc}", file=sys.stderr)
        return None

    if "error_code" in data:
        print(
            f"  [prime] FRED ERROR {data.get('error_code')}: {data.get('error_message')}",
            file=sys.stderr,
        )
        return None

    for obs in data.get("observations", []):
        raw = obs.get("value", "").strip()
        if not raw or raw == ".":
            continue
        try:
            value = float(raw)
            if value > 0:
                formatted = f"{value:.2f}"
                print(f"  [prime] {obs['date']}: {formatted}%  ← using this")
                return {"value": formatted, "date": obs["date"]}
        except (ValueError, TypeError):
            continue

    print("  [prime] WARNING: no valid observation found", file=sys.stderr)
    return None


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    api_key = os.environ.get("FRED_API_KEY", "").strip()
    if not api_key:
        print("FATAL: FRED_API_KEY environment variable is not set.", file=sys.stderr)
        print("Set it under GitHub → your repo → Settings → Secrets → Actions.", file=sys.stderr)
        sys.exit(1)

    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    print(f"=== update_rates.py  {now_utc} ===")
    print(f"Output: {OUTPUT_PATH}\n")

    existing = load_existing_rates()
    result = {}
    fresh_dates = []
    failed_keys = []

    # ── SOFR (NY Fed, no key) ────────────────────────────────────────────────
    print("--- SOFR ---")
    sofr = fetch_sofr()
    print()
    if sofr:
        result["sofr"] = sofr
        fresh_dates.append(sofr["date"])
    else:
        prev = existing.get("sofr")
        if prev:
            result["sofr"] = prev
            print(f"  [sofr] kept previous: {prev['value']}%")
        else:
            failed_keys.append("sofr")

    # ── Treasury 3yr, 5yr, 10yr (Treasury.gov, no key) ──────────────────────
    print("--- US Treasury ---")
    tsy = fetch_all_treasury()
    print()
    for key in ("t3", "t5", "t10"):
        if key in tsy:
            result[key] = tsy[key]
            fresh_dates.append(tsy[key]["date"])
        else:
            prev = existing.get(key)
            if prev:
                result[key] = prev
                print(f"  [{key}] kept previous: {prev['value']}%")
            else:
                failed_keys.append(key)

    # ── Prime Rate (FRED WPRIME, needs API key) ───────────────────────────────
    print("--- Prime Rate (FRED WPRIME) ---")
    prime = fetch_prime(api_key)
    print()
    if prime:
        result["prime"] = prime
        fresh_dates.append(prime["date"])
    else:
        prev = existing.get("prime")
        if prev:
            result["prime"] = prev
            print(f"  [prime] kept previous: {prev['value']}%")
        else:
            failed_keys.append("prime")

    if not result:
        print("FATAL: no data at all — rates.json not updated.", file=sys.stderr)
        sys.exit(1)

    # lastUpdated = most recent date among live fetches
    if fresh_dates:
        fresh_dates.sort()
        last_updated = fresh_dates[-1]
    else:
        last_updated = date.today().isoformat()

    result["lastUpdated"] = last_updated

    # Write atomically
    tmp = OUTPUT_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)
        f.write("\n")
    os.replace(tmp, OUTPUT_PATH)

    print("=== Summary ===")
    order = [("prime","WPRIME (FRED)"), ("sofr","NY Fed SOFR"),
             ("t3","Treasury 3yr"), ("t5","Treasury 5yr"), ("t10","Treasury 10yr")]
    for key, label in order:
        entry = result.get(key)
        tag = "" if key not in failed_keys else " (kept previous)"
        if entry:
            print(f"  {key:5s}  {label:22s}  {entry['value']:>6}%  {entry['date']}{tag}")
        else:
            print(f"  {key:5s}  {label:22s}  MISSING")
    print(f"  lastUpdated: {last_updated}")
    print(f"\nWrote {OUTPUT_PATH}")

    if failed_keys:
        print(f"\nWARNING: {len(failed_keys)} key(s) failed live fetch ({', '.join(failed_keys)}).",
              file=sys.stderr)


if __name__ == "__main__":
    main()
