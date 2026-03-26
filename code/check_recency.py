import os
import duckdb
from datetime import date

def main():
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/market.duckdb"
    threshold = int(os.environ.get("DAYS_THRESHOLD", "30"))
    topn = int(os.environ.get("TOPN", "20"))
    top_date_print = int(os.environ.get("TOP_DATE_PRINT", "20"))
    top_diff_print = int(os.environ.get("TOP_DIFF_PRINT", "20"))
    con = duckdb.connect(db_path)
    rows = con.execute("SELECT symbol, max(date) AS last_date, count(*) AS rows FROM daily_data GROUP BY symbol ORDER BY symbol").fetchall()
    today = date.today()
    items = []
    for sym, last_date, cnt in rows:
        if last_date is None:
            diff = None
        else:
            diff = (today - last_date).days
        items.append((sym, last_date, cnt, diff))
    total = len(items)
    far_items = [x for x in items if x[3] is not None and x[3] > threshold]
    print("total_symbols=", total, "far_threshold_days=", threshold, "far_count=", len(far_items))
    buckets = [(0,7),(8,30),(31,90),(91,99999)]
    for lo, hi in buckets:
        c = sum(1 for x in items if x[3] is not None and lo <= x[3] <= hi)
        print("bucket_", lo, "_", hi, "=", c)
    items_sorted = sorted([x for x in items if x[3] is not None], key=lambda x: x[3], reverse=True)
    for sym, last_date, cnt, diff in items_sorted[:topn]:
        print("symbol=", sym, "last_date=", last_date, "rows=", cnt, "days_diff=", diff)
    # counts by exact last_date (which day没更新)
    from collections import Counter
    date_counts = Counter([x[1] for x in items if x[1] is not None])
    print("distinct_last_dates=", len(date_counts))
    # print top by staleness (older dates first)
    dates_sorted_by_stale = sorted(date_counts.items(), key=lambda t: (today - t[0]).days, reverse=True)
    for d, cnt in dates_sorted_by_stale[:top_date_print]:
        print("last_date=", d, "symbols=", cnt, "days_diff=", (today - d).days)
    # counts by exact days_diff
    diff_counts = Counter([x[3] for x in items if x[3] is not None])
    diffs_sorted = sorted(diff_counts.items(), key=lambda t: t[0], reverse=True)
    for diff, cnt in diffs_sorted[:top_diff_print]:
        print("days_diff=", diff, "symbols=", cnt)
    print("done")

if __name__ == "__main__":
    main()
