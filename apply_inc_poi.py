import argparse
import os
import time
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from neo4j import GraphDatabase


load_dotenv()

VALID_CHANGE_TYPES = {
    "appear",
    "disappear",
    "updated",
}

# PostgreSQL에는 존재하지만 Neo4j 속성으로 직접 넣지 않을 컬럼
EXCLUDED_PROPERTIES = {
    "geom",         # PostGIS Geometry
    "change_type",  # CDC 처리용 메타데이터
    "snapshot",     # Neo4j 기존 모델에는 사용하지 않음
}


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "mart.inc_poi를 기존 Neo4j GeoKG에 "
            "배치 단위로 반영합니다."
        )
    )

    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 Neo4j 변경을 수행합니다.",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Neo4j 트랜잭션당 처리 건수",
    )

    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError(
            "batch-size는 1 이상이어야 합니다."
        )

    return args


def connect_postgresql():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=5,
        row_factory=dict_row,
    )


def connect_neo4j():
    return GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(
            os.getenv("NEO4J_USER"),
            os.getenv("NEO4J_PASSWORD"),
        ),
    )


def validate_inc_poi(pg_conn):
    counts = pg_conn.execute(
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
            f"알 수 없는 변화 유형: {unknown_types}"
        )

    total_count = sum(
        row["row_count"] for row in counts
    )

    print("PostgreSQL 증분 데이터:")

    for row in counts:
        print(
            f"- {row['change_type']}: "
            f"{row['row_count']:,}"
        )

    print(f"- 전체: {total_count:,}")

    invalid_count = pg_conn.execute(
        """
        SELECT COUNT(*) AS invalid_count
        FROM mart.inc_poi
        WHERE nf_id IS NULL
           OR change_type IS NULL
           OR (
               change_type IN ('appear', 'updated')
               AND (
                   emd_cd IS NULL
                   OR longitude IS NULL
                   OR latitude IS NULL
               )
           )
        """
    ).fetchone()["invalid_count"]

    if invalid_count:
        raise RuntimeError(
            "필수값이 누락된 증분 데이터: "
            f"{invalid_count:,}건"
        )

    duplicate_count = pg_conn.execute(
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

    if duplicate_count:
        raise RuntimeError(
            f"중복 NF_ID 그룹: {duplicate_count:,}개"
        )

    print("mart.inc_poi 사전 검증 성공")

    return {
        row["change_type"]: row["row_count"]
        for row in counts
    }


def load_snapshot_ids(pg_conn, snapshot):
    rows = pg_conn.execute(
        """
        SELECT nf_id
        FROM mart.poi
        WHERE snapshot = %s
        """,
        (snapshot,),
    ).fetchall()

    return {
        row["nf_id"] for row in rows
    }


def load_neo4j_ids(session):
    return set(
        session.run(
            """
            MATCH (p:POI2024)
            RETURN p.nf_id AS nf_id
            """
        ).value("nf_id")
    )


def validate_neo4j_structure(
    pg_conn,
    session,
    t1_ids,
    t2_ids,
):
    constraint_count = session.run(
        """
        SHOW CONSTRAINTS
        YIELD labelsOrTypes, properties
        WHERE 'POI2024' IN labelsOrTypes
          AND 'nf_id' IN properties
        RETURN count(*) AS constraint_count
        """
    ).single()["constraint_count"]

    if constraint_count == 0:
        raise RuntimeError(
            "POI2024.nf_id 유일성 제약조건이 없습니다."
        )

    neo4j_ids = load_neo4j_ids(session)

    unexpected_ids = (
        neo4j_ids - (t1_ids | t2_ids)
    )

    if unexpected_ids:
        raise RuntimeError(
            "두 snapshot에 없는 NF_ID가 Neo4j에 "
            f"존재합니다: {list(unexpected_ids)[:5]}"
        )

    if neo4j_ids == t1_ids:
        graph_state = "2024 baseline"
    elif neo4j_ids == t2_ids:
        graph_state = "2026 target"
    else:
        graph_state = "partial/mixed"

    print("현재 Neo4j 상태:", graph_state)
    print("현재 POI2024 노드:", f"{len(neo4j_ids):,}")
    print(
        "POI2024 NF_ID 제약조건 확인 성공"
    )

    required_emd_codes = {
        str(row["emd_cd"])
        for row in pg_conn.execute(
            """
            SELECT DISTINCT emd_cd
            FROM mart.inc_poi
            WHERE change_type IN (
                'appear',
                'updated'
            )
            """
        ).fetchall()
    }

    adm_rows = session.run(
        """
        MATCH (a:admDongList)
        WHERE toString(a.emd_cd) IN $codes
        RETURN
            toString(a.emd_cd) AS emd_cd,
            count(a) AS node_count
        """,
        codes=sorted(required_emd_codes),
    ).data()

    adm_counts = {
        row["emd_cd"]: row["node_count"]
        for row in adm_rows
    }

    invalid_emd_codes = [
        emd_cd
        for emd_cd in required_emd_codes
        if adm_counts.get(emd_cd) != 1
    ]

    if invalid_emd_codes:
        raise RuntimeError(
            "admDongList가 없거나 중복된 행정동 코드: "
            f"{invalid_emd_codes}"
        )

    print(
        "행정동 연결 대상 검증 성공:",
        sorted(required_emd_codes),
    )


def convert_value(value):
    if value is None:
        return None

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, UUID):
        return str(value)

    if isinstance(
        value,
        (
            str,
            int,
            float,
            bool,
            bytes,
            date,
            datetime,
            datetime_time,
        ),
    ):
        return value

    return str(value)


def prepare_active_row(row):
    properties = {
        key: convert_value(value)
        for key, value in row.items()
        if key not in EXCLUDED_PROPERTIES
    }

    return {
        "nf_id": str(row["nf_id"]),
        "emd_cd": str(row["emd_cd"]),
        "longitude": float(row["longitude"]),
        "latitude": float(row["latitude"]),
        "properties": properties,
    }


def stream_batches(
    pg_conn,
    change_type,
    batch_size,
):
    cursor_name = (
        f"inc_poi_{change_type}_cursor"
    )

    with pg_conn.cursor(
        name=cursor_name
    ) as cursor:
        cursor.execute(
            """
            SELECT *
            FROM mart.inc_poi
            WHERE change_type = %s
            ORDER BY nf_id
            """,
            (change_type,),
        )

        while True:
            rows = cursor.fetchmany(batch_size)

            if not rows:
                break

            yield rows


def delete_batch(tx, nf_ids):
    result = tx.run(
        """
        UNWIND $nf_ids AS nf_id
        MATCH (p:POI2024 {nf_id: nf_id})
        DETACH DELETE p
        """,
        nf_ids=nf_ids,
    )

    summary = result.consume()

    return summary.counters.nodes_deleted


def upsert_batch(tx, rows):
    upsert_result = tx.run(
        """
        UNWIND $rows AS row

        MERGE (p:POI2024 {nf_id: row.nf_id})
        SET p += row.properties
        SET p.location = point({
            longitude: row.longitude,
            latitude: row.latitude
        })

        RETURN count(p) AS processed
        """,
        rows=rows,
    )

    processed = (
        upsert_result.single()["processed"]
    )

    if processed != len(rows):
        raise RuntimeError(
            "노드 반영 건수 불일치: "
            f"요청={len(rows)}, 처리={processed}"
        )

    tx.run(
        """
        UNWIND $rows AS row

        MATCH (p:POI2024 {nf_id: row.nf_id})
        OPTIONAL MATCH
            (p)-[old:IN]-(:admDongList)
        DELETE old
        """,
        rows=rows,
    ).consume()

    relationship_result = tx.run(
        """
        UNWIND $rows AS row

        MATCH (p:POI2024 {nf_id: row.nf_id})
        MATCH (a:admDongList)
        WHERE toString(a.emd_cd) = row.emd_cd

        MERGE (p)-[r:IN]->(a)

        RETURN count(r) AS linked
        """,
        rows=rows,
    )

    linked = (
        relationship_result.single()["linked"]
    )

    if linked != len(rows):
        raise RuntimeError(
            "IN 관계 반영 건수 불일치: "
            f"요청={len(rows)}, 연결={linked}"
        )

    return processed, linked


def apply_disappear(
    pg_conn,
    session,
    batch_size,
    expected_count,
):
    processed = 0
    deleted = 0
    start_time = time.perf_counter()

    for rows in stream_batches(
        pg_conn,
        "disappear",
        batch_size,
    ):
        nf_ids = [
            str(row["nf_id"]) for row in rows
        ]

        deleted += session.execute_write(
            delete_batch,
            nf_ids,
        )

        processed += len(rows)

        print(
            f"\rdisappear 처리: "
            f"{processed:,}/{expected_count:,}",
            end="",
            flush=True,
        )

    print()

    elapsed = time.perf_counter() - start_time

    print(
        f"disappear 완료: 대상={processed:,}, "
        f"실제 삭제={deleted:,}, "
        f"시간={elapsed:.2f}초"
    )


def apply_active_type(
    pg_conn,
    session,
    change_type,
    batch_size,
    expected_count,
):
    processed = 0
    linked = 0
    start_time = time.perf_counter()

    for rows in stream_batches(
        pg_conn,
        change_type,
        batch_size,
    ):
        prepared_rows = [
            prepare_active_row(row)
            for row in rows
        ]

        batch_processed, batch_linked = (
            session.execute_write(
                upsert_batch,
                prepared_rows,
            )
        )

        processed += batch_processed
        linked += batch_linked

        print(
            f"\r{change_type} 처리: "
            f"{processed:,}/{expected_count:,}",
            end="",
            flush=True,
        )

    print()

    elapsed = time.perf_counter() - start_time

    if processed != expected_count:
        raise RuntimeError(
            f"{change_type} 최종 처리 건수 불일치"
        )

    print(
        f"{change_type} 완료: "
        f"노드={processed:,}, "
        f"IN 관계={linked:,}, "
        f"시간={elapsed:.2f}초"
    )


def validate_target_state(
    session,
    expected_t2_ids,
):
    actual_ids = load_neo4j_ids(session)

    missing_ids = expected_t2_ids - actual_ids
    unexpected_ids = actual_ids - expected_t2_ids

    if missing_ids or unexpected_ids:
        raise RuntimeError(
            "2026 NF_ID 집합 불일치: "
            f"누락={len(missing_ids):,}, "
            f"초과={len(unexpected_ids):,}"
        )

    relationship_result = session.run(
        """
        MATCH (p:POI2024)
        OPTIONAL MATCH
            (p)-[r:IN]->(:admDongList)

        WITH p, count(r) AS relationship_count

        RETURN
            count(p) AS poi_count,

            sum(
                CASE
                    WHEN relationship_count = 0
                    THEN 1
                    ELSE 0
                END
            ) AS without_in,

            sum(
                CASE
                    WHEN relationship_count > 1
                    THEN 1
                    ELSE 0
                END
            ) AS multiple_in
        """
    ).single()

    if (
        relationship_result["without_in"] != 0
        or relationship_result["multiple_in"] != 0
    ):
        raise RuntimeError(
            "IN 관계 구조 오류: "
            f"누락={relationship_result['without_in']}, "
            f"중복={relationship_result['multiple_in']}"
        )

    print("2026 target NF_ID 집합 일치")
    print(
        "최종 POI2024 노드:",
        f"{relationship_result['poi_count']:,}",
    )
    print("모든 POI의 IN 관계 1개 확인")


args = parse_arguments()

with connect_postgresql() as pg_connection:
    counts = validate_inc_poi(pg_connection)

    t1_ids = load_snapshot_ids(
        pg_connection,
        "2024",
    )
    t2_ids = load_snapshot_ids(
        pg_connection,
        "2026",
    )

    with connect_neo4j() as neo4j_driver:
        neo4j_driver.verify_connectivity()

        with neo4j_driver.session(
            database=os.getenv("NEO4J_DATABASE")
        ) as neo4j_session:
            validate_neo4j_structure(
                pg_connection,
                neo4j_session,
                t1_ids,
                t2_ids,
            )

            print(
                "Neo4j 배치 크기:",
                f"{args.batch_size:,}",
            )

            if not args.apply:
                print(
                    "아직 Neo4j를 변경하지 않았습니다."
                )
                print(
                    "실제 실행 명령: "
                    "python apply_inc_poi.py --apply"
                )
                raise SystemExit(0)

            total_start = time.perf_counter()

            apply_disappear(
                pg_connection,
                neo4j_session,
                args.batch_size,
                counts.get("disappear", 0),
            )

            apply_active_type(
                pg_connection,
                neo4j_session,
                "appear",
                args.batch_size,
                counts.get("appear", 0),
            )

            apply_active_type(
                pg_connection,
                neo4j_session,
                "updated",
                args.batch_size,
                counts.get("updated", 0),
            )

            validate_target_state(
                neo4j_session,
                t2_ids,
            )

            total_elapsed = (
                time.perf_counter() - total_start
            )

            print(
                "전체 Neo4j 반영 시간:",
                f"{total_elapsed:.2f}초",
            )
            print(
                "mart.inc_poi → Neo4j "
                "배치 반영 완료"
            )