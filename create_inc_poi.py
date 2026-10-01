import os
import sys
import time
from pathlib import Path

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv


load_dotenv()

SQL_PATH = (
    Path(__file__).parent
    / "sql"
    / "create_inc_poi.sql"
)

EXPECTED_SNAPSHOTS = {"2024", "2026"}
EXPECTED_SRID = 5179

VALID_CHANGE_TYPES = {
    "appear",
    "disappear",
    "updated",
}


def connect_postgresql():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=5,
        autocommit=True,
        row_factory=dict_row,
    )


def validate_source_snapshots(conn):
    rows = conn.execute(
        """
        SELECT
            snapshot,
            COUNT(*) AS row_count,
            COUNT(DISTINCT nf_id)
                AS unique_nf_id_count,
            ARRAY_AGG(
                DISTINCT ST_SRID(geom)
            ) FILTER (
                WHERE geom IS NOT NULL
            ) AS geometry_srids
        FROM mart.poi
        WHERE snapshot IN ('2024', '2026')
        GROUP BY snapshot
        ORDER BY snapshot
        """
    ).fetchall()

    found_snapshots = {
        str(row["snapshot"]) for row in rows
    }

    if found_snapshots != EXPECTED_SNAPSHOTS:
        raise RuntimeError(
            "필요한 snapshot이 없습니다. "
            f"확인 결과: {found_snapshots}"
        )

    for row in rows:
        snapshot = str(row["snapshot"])
        row_count = row["row_count"]
        unique_count = row["unique_nf_id_count"]
        srids = row["geometry_srids"]

        print(
            f"{snapshot}: "
            f"전체={row_count:,}, "
            f"고유 NF_ID={unique_count:,}, "
            f"SRID={srids}"
        )

        if row_count != unique_count:
            raise RuntimeError(
                f"{snapshot} snapshot에 "
                "중복 또는 NULL NF_ID가 있습니다."
            )

        if srids != [EXPECTED_SRID]:
            raise RuntimeError(
                f"{snapshot} Geometry SRID 오류: "
                f"{srids}"
            )

    print("원본 snapshot 사전 검증 성공")


def load_sql_statements():
    if not SQL_PATH.exists():
        raise FileNotFoundError(
            f"SQL 파일이 없습니다: {SQL_PATH}"
        )

    sql_text = SQL_PATH.read_text(
        encoding="utf-8-sig"
    )

    statements = [
        statement.strip()
        for statement in sql_text.split(";")
        if statement.strip()
    ]

    if not statements:
        raise RuntimeError("SQL 파일이 비어 있습니다.")

    print("SQL 파일:", SQL_PATH)
    print("실행할 SQL 문 수:", len(statements))

    return statements


def execute_sql(conn, statements):
    start_time = time.perf_counter()

    # 모든 SQL을 하나의 트랜잭션으로 실행한다.
    # 도중에 실패하면 전체 작업을 롤백한다.
    with conn.transaction():
        for number, statement in enumerate(
            statements,
            start=1,
        ):
            print(
                f"SQL 실행 중: "
                f"{number}/{len(statements)}"
            )
            conn.execute(statement)

    elapsed = time.perf_counter() - start_time

    print(
        f"mart.inc_poi 생성 시간: "
        f"{elapsed:.2f}초"
    )


def validate_inc_poi(conn):
    counts = conn.execute(
        """
        SELECT
            change_type,
            COUNT(*) AS row_count
        FROM mart.inc_poi
        GROUP BY change_type
        ORDER BY change_type
        """
    ).fetchall()

    if not counts:
        raise RuntimeError(
            "mart.inc_poi가 비어 있습니다."
        )

    detected_types = {
        row["change_type"] for row in counts
    }

    unknown_types = (
        detected_types - VALID_CHANGE_TYPES
    )

    if unknown_types:
        raise RuntimeError(
            "알 수 없는 변화 유형: "
            f"{sorted(unknown_types)}"
        )

    duplicate_count = conn.execute(
        """
        SELECT COUNT(*) AS duplicate_count
        FROM (
            SELECT nf_id
            FROM mart.inc_poi
            GROUP BY nf_id
            HAVING COUNT(*) > 1
        ) AS duplicated
        """
    ).fetchone()["duplicate_count"]

    if duplicate_count != 0:
        raise RuntimeError(
            f"중복 NF_ID 그룹 수: {duplicate_count}"
        )

    total_count = 0

    print("변화 유형별 결과:")

    for row in counts:
        print(
            f"- {row['change_type']}: "
            f"{row['row_count']:,}"
        )
        total_count += row["row_count"]

    print(f"전체 증분 데이터: {total_count:,}")
    print("mart.inc_poi 사후 검증 성공")


statements = load_sql_statements()

with connect_postgresql() as connection:
    validate_source_snapshots(connection)

    if "--apply" not in sys.argv:
        print("아직 PostgreSQL을 변경하지 않았습니다.")
        print(
            "실제 실행 명령: "
            "python create_inc_poi.py --apply"
        )
        sys.exit(0)

    execute_sql(connection, statements)
    validate_inc_poi(connection)

print("mart.inc_poi 자동 생성 완료")