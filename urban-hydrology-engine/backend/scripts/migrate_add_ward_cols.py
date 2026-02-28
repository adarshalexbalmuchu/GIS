"""
Migration — add ward_no, zone_name columns.
Safe to run multiple times.
"""

import os
import psycopg2

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://hydro:hydro123@localhost:5432/hydrology",
)


def main():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    cur = conn.cursor()

    stmts = [
        "ALTER TABLE wards ADD COLUMN IF NOT EXISTS ward_no VARCHAR;",
        "ALTER TABLE wards ADD COLUMN IF NOT EXISTS zone_name VARCHAR;",
        "ALTER TABLE hotspots ADD COLUMN IF NOT EXISTS zone_name VARCHAR;",
    ]
    for sql in stmts:
        cur.execute(sql)
        print(f"  OK: {sql.strip()}")

    cur.close()
    conn.close()
    print("Migration complete")


if __name__ == "__main__":
    main()
