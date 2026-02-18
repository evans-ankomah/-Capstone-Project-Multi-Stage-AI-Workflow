import csv
from datetime import datetime

INPUT_FILE = "data/raw_orders.csv"
OUTPUT_FILE = "data/clean_orders.csv"
SUMMARY_FILE = "data/daily_summary.csv"


def normalize_country(value):
    if not value:
        return "UNKNOWN"
    cleaned = value.strip().lower()
    mapping = {
        "us": "USA",
        "usa": "USA",
        "u.s.a": "USA",
        "united states": "USA",
        "uk": "UK",
        "u.k": "UK",
        "united kingdom": "UK",
        "gh": "GHANA",
        "ghana": "GHANA",
    }
    return mapping.get(cleaned, cleaned.upper())


def to_float(text):
    try:
        return float(text)
    except Exception:
        return None


def parse_date(text):
    if not text:
        return None
    patterns = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"]
    for p in patterns:
        try:
            return datetime.strptime(text.strip(), p).strftime("%Y-%m-%d")
        except Exception:
            continue
    return None


def main():
    seen_orders = set()
    cleaned_rows = []
    daily_totals = {}

    with open(INPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            order_id = (row.get("order_id") or "").strip()
            customer_id = (row.get("customer_id") or "").strip()
            country = normalize_country(row.get("country"))
            order_date = parse_date(row.get("order_date"))
            amount = to_float((row.get("amount") or "").replace(",", ""))
            status = (row.get("status") or "").strip().upper()

            if not order_id or not customer_id or not order_date or amount is None:
                continue
            if amount <= 0:
                continue
            if status not in {"PAID", "SHIPPED"}:
                continue
            if order_id in seen_orders:
                continue

            seen_orders.add(order_id)
            cleaned_rows.append(
                {
                    "order_id": order_id,
                    "customer_id": customer_id,
                    "country": country,
                    "order_date": order_date,
                    "amount": round(amount, 2),
                    "status": status,
                }
            )

            if order_date not in daily_totals:
                daily_totals[order_date] = 0.0
            daily_totals[order_date] += amount

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["order_id", "customer_id", "country", "order_date", "amount", "status"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in cleaned_rows:
            writer.writerow(row)

    with open(SUMMARY_FILE, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["order_date", "total_revenue"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for date_key in sorted(daily_totals.keys()):
            writer.writerow({"order_date": date_key, "total_revenue": round(daily_totals[date_key], 2)})

    print("Rows in:", len(cleaned_rows))
    print("Summary days:", len(daily_totals))


if __name__ == "__main__":
    main()

