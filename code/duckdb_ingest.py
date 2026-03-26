import os
import sys
import csv
import re
from datetime import datetime
import duckdb
import traceback

def parse_float(x):
    return float(x)

def valid_row(row):
    if len(row) != 12:
        return False
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", row[0]):
        return False
    try:
        float(row[1]); float(row[2]); float(row[3]); float(row[4])
        float(row[5]); float(row[6]); float(row[7]); float(row[8])
    except:
        return False
    if not row[9] or not row[10] or not row[11]:
        return False
    return True

def to_tuple(row):
    return (
        row[0],
        parse_float(row[1]),
        parse_float(row[2]),
        parse_float(row[3]),
        parse_float(row[4]),
        parse_float(row[5]),
        parse_float(row[6]),
        parse_float(row[7]),
        parse_float(row[8]),
        row[9],
        row[10],
        row[11],
    )

def ensure_schema(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS daily_data (
            date DATE,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            amount DOUBLE,
            outstanding_share DOUBLE,
            turnover DOUBLE,
            symbol VARCHAR,
            name VARCHAR,
            adjust VARCHAR
        )
    """)
    con.execute("""
        CREATE TABLE IF NOT EXISTS import_log (
            symbol VARCHAR,
            file_path VARCHAR,
            rows_total INTEGER,
            rows_valid INTEGER,
            rows_invalid INTEGER,
            first_date DATE,
            last_date DATE,
            imported_rows INTEGER,
            import_time TIMESTAMP,
            status VARCHAR,
            error_message VARCHAR
        )
    """)
    cols = [r[1] for r in con.execute("PRAGMA table_info('import_log')").fetchall()]
    if "status" not in cols:
        con.execute("ALTER TABLE import_log ADD COLUMN status VARCHAR")
    if "error_message" not in cols:
        con.execute("ALTER TABLE import_log ADD COLUMN error_message VARCHAR")

def process_file(con, path):
    rows_total = 0
    rows_valid = []
    rows_invalid = 0
    invalid_samples = []
    first_date = None
    last_date = None
    symbol_value = None
    status = "success"
    err = None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header is None or len(header) != 12:
                raise ValueError("bad_header")
            for row in reader:
                rows_total += 1
                if not valid_row(row):
                    rows_invalid += 1
                    if len(invalid_samples) < 3:
                        invalid_samples.append(",".join(row))
                    continue
                t = to_tuple(row)
                rows_valid.append(t)
                d = t[0]
                if first_date is None or d < first_date:
                    first_date = d
                if last_date is None or d > last_date:
                    last_date = d
                symbol_value = t[9]
        imported_rows = 0
        if rows_valid:
            con.executemany("INSERT INTO daily_data VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", rows_valid)
            imported_rows = len(rows_valid)
    except Exception as e:
        status = "failed"
        err = str(e)
        imported_rows = 0
    con.execute(
        "INSERT INTO import_log (symbol,file_path,rows_total,rows_valid,rows_invalid,first_date,last_date,imported_rows,import_time,status,error_message) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            symbol_value or "",
            path,
            rows_total,
            len(rows_valid),
            rows_invalid,
            first_date,
            last_date,
            imported_rows,
            datetime.now(),
            status,
            err,
        ],
    )
    last_in_db = None
    if symbol_value:
        last_in_db = con.execute("SELECT max(date) FROM daily_data WHERE symbol = ?", [symbol_value]).fetchone()[0]
    print("file_start=", os.path.basename(path))
    print("file_done=", os.path.basename(path), "symbol=", symbol_value, "status=", status, "total=", rows_total, "valid=", len(rows_valid), "invalid=", rows_invalid, "file_last=", last_date, "db_last=", last_in_db, "error=", err)
    if invalid_samples:
        for s in invalid_samples:
            print("invalid_sample=", s)

def main():
    base_dir = "/Users/yuping/Downloads/ossdata/hangqing/daily_data"
    db_path = "/Users/yuping/Downloads/ossdata/duckdb/market.duckdb"
    con = duckdb.connect(db_path)
    ensure_schema(con)
    files = [os.path.join(base_dir, x) for x in os.listdir(base_dir) if x.endswith(".csv")]
    files.sort()
    limit = None
    if "LIMIT_FILES" in os.environ:
        try:
            limit = int(os.environ["LIMIT_FILES"])
        except:
            limit = None
    count = 0
    total_imported = 0
    total_invalid = 0
    success_files = 0
    failed_files = 0
    start_ts = datetime.now()
    for p in files:
        if limit is not None and count >= limit:
            break
        process_file(con, p)
        stats = con.execute("SELECT imported_rows, rows_invalid, status FROM import_log WHERE file_path = ? ORDER BY import_time DESC LIMIT 1", [p]).fetchone()
        total_imported += stats[0] or 0
        total_invalid += stats[1] or 0
        if stats[2] == "failed":
            failed_files += 1
        else:
            success_files += 1
        count += 1
    elapsed = (datetime.now() - start_ts).total_seconds()
    print("processed_files=", count, "success_files=", success_files, "failed_files=", failed_files, "total_imported_rows=", total_imported, "total_invalid_rows=", total_invalid, "elapsed_sec=", int(elapsed))
    summary = con.execute("SELECT symbol, max(date) AS last_date, count(*) AS rows FROM daily_data GROUP BY symbol ORDER BY symbol").fetchall()
    for s in summary[:50]:
        print("summary_symbol=", s[0], "last_date=", s[1], "rows=", s[2])
    print("done")

if __name__ == "__main__":
    main()
