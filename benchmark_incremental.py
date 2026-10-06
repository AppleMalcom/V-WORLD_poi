"""Incremental(tombstone) 방식 Neo4j 갱신 성능 벤치마크.

2024 baseline에서 2026 target으로 증분 갱신하는 데 걸리는 시간과
Neo4j 그래프 쓰기 연산량을 반복 측정한다. 매 반복 전 그래프를
2024 baseline으로 되돌리므로 Neo4j를 중지하지 않고 자동 반복할 수 있다.

측정 구간은 '증분 반영' 뿐이며, 리셋·검증 시간은 포함하지 않는다.
disappear 처리는 apply_inc_poi_tombstone.py와 동일하게 노드를
:POITombstone으로 전환하여 소멸 이력을 보존한다.

사용 예:
    python benchmark_incremental.py                 # 점검만(변경 없음)
    python benchmark_incremental.py --apply         # 6회 반복(1회 워밍업)
    python benchmark_incremental.py --apply --repeat 6 --warmup 1
"""

import argparse
import csv
import os
import statistics
import time
from datetime import date, datetime, time as datetime_time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from neo4j import GraphDatabase


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

VALID_CHANGE_TYPES = {"appear", "disappear", "updated"}

# PostgreSQL에는 있지만 Neo4j 속성으로 직접 넣지 않을 컬럼
EXCLUDED_PROPERTIES = {"geom", "change_type", "snapshot"}

BASELINE_SNAPSHOT = "2024"
TARGET_SNAPSHOT = "2026"
METHOD_NAME = "incremental_tombstone"


# ---------------------------------------------------------------------------
# 인자 및 연결
# ---------------------------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Incremental(tombstone) 방식의 Neo4j 갱신 시간을 "
            "반복 측정한다."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 Neo4j 변경을 수행하며 벤치마크를 실행한다.",
    )
    parser.add_argument(
        "--repeat",
        type=int,
        default=6,
        help="총 반복 횟수(워밍업 포함). 기본 6회.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=1,
        help="앞에서 버릴 워밍업 횟수. 기본 1회.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Neo4j 트랜잭션당 처리 건수. 기본 1000.",
    )
    args = parser.parse_args()

    if args.batch_size <= 0:
        raise ValueError("batch-size는 1 이상이어야 한다.")
    if args.repeat <= 0:
        raise ValueError("repeat은 1 이상이어야 한다.")
    if args.warmup < 0 or args.warmup >= args.repeat:
        raise ValueError(
            "warmup은 0 이상이고 repeat보다 작아야 한다."
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
        auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD")),
    )


# ---------------------------------------------------------------------------
# 공통 유틸 (apply_inc_poi_tombstone.py와 동일한 변환 규칙)
# ---------------------------------------------------------------------------
def convert_value(value):
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    if isinstance(
        value,
        (str, int, float, bool, bytes, date, datetime, datetime_time),
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


def load_snapshot_ids(pg_conn, snapshot):
    rows = pg_conn.execute(
        "SELECT nf_id FROM mart.poi WHERE snapshot = %s",
        (snapshot,),
    ).fetchall()
    return {row["nf_id"] for row in rows}


def load_neo4j_poi_ids(session):
    return set(
        session.run(
            "MATCH (p:POI2024) RETURN p.nf_id AS nf_id"
        ).value("nf_id")
    )


def stream_snapshot_rows(pg_conn, snapshot, batch_size):
    with pg_conn.cursor(name=f"bench_snapshot_{snapshot}") as cursor:
        cursor.execute(
            "SELECT * FROM mart.poi WHERE snapshot = %s ORDER BY nf_id",
            (snapshot,),
        )
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows


def stream_inc_rows(pg_conn, change_type, batch_size):
    with pg_conn.cursor(name=f"bench_inc_{change_type}") as cursor:
        cursor.execute(
            "SELECT * FROM mart.inc_poi WHERE change_type = %s ORDER BY nf_id",
            (change_type,),
        )
        while True:
            rows = cursor.fetchmany(batch_size)
            if not rows:
                break
            yield rows


# ---------------------------------------------------------------------------
# Cypher 쓰기 블록 (반환: SummaryCounters)
# ---------------------------------------------------------------------------
def tombstone_batch(tx, nf_ids):
    """disappear: :POI2024 → :POITombstone 전환, 이력 속성 기록."""
    result = tx.run(
        """
        UNWIND $nf_ids AS nf_id
        MATCH (p:POI2024 {nf_id: nf_id})
        OPTIONAL MATCH (p)-[r:IN]-(:admDongList)
        WITH p, collect(r) AS old_in_relationships
        FOREACH (rel IN old_in_relationships | DELETE rel)
        REMOVE p:POI2024
        SET p:POITombstone,
            p.last_seen_snapshot = $baseline,
            p.absent_since_snapshot = $target
        RETURN count(p) AS tombstoned
        """,
        nf_ids=nf_ids,
        baseline=BASELINE_SNAPSHOT,
        target=TARGET_SNAPSHOT,
    )
    summary = result.consume()
    return summary.counters


def upsert_batch(tx, rows):
    """appear/updated: 노드 MERGE + location + IN 관계 재연결."""
    counters_total = {}

    r1 = tx.run(
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
    s1 = r1.consume()

    r2 = tx.run(
        """
        UNWIND $rows AS row
        MATCH (p:POI2024 {nf_id: row.nf_id})
        OPTIONAL MATCH (p)-[old:IN]-(:admDongList)
        DELETE old
        """,
        rows=rows,
    )
    s2 = r2.consume()

    r3 = tx.run(
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
    s3 = r3.consume()

    return [s1.counters, s2.counters, s3.counters]


def reset_delete_label_batch(tx, label, batch_size):
    """리셋: 특정 라벨 노드를 LIMIT 단위로 DETACH DELETE.

    :POITombstone에는 nf_id 인덱스가 없으므로 nf_id 매칭 대신
    라벨 스캔 + LIMIT 배치로 삭제한다. 반환값은 이번 호출에서
    삭제된 노드 수이며, 0이 되면 호출부가 반복을 멈춘다.
    """
    result = tx.run(
        f"""
        MATCH (p:{label})
        WITH p LIMIT $batch_size
        DETACH DELETE p
        """,
        batch_size=batch_size,
    )
    return result.consume().counters.nodes_deleted


def reset_upsert_batch(tx, rows):
    """리셋: 2024 전체 노드를 MERGE + location + IN 재연결."""
    tx.run(
        """
        UNWIND $rows AS row
        MERGE (p:POI2024 {nf_id: row.nf_id})
        SET p += row.properties
        SET p.location = point({
            longitude: row.longitude,
            latitude: row.latitude
        })
        """,
        rows=rows,
    ).consume()

    tx.run(
        """
        UNWIND $rows AS row
        MATCH (p:POI2024 {nf_id: row.nf_id})
        MATCH (a:admDongList)
        WHERE toString(a.emd_cd) = row.emd_cd
        MERGE (p)-[r:IN]->(a)
        """,
        rows=rows,
    ).consume()


# ---------------------------------------------------------------------------
# 카운터 누적
# ---------------------------------------------------------------------------
def new_counter_accumulator():
    return {
        "nodes_created": 0,
        "nodes_deleted": 0,
        "relationships_created": 0,
        "relationships_deleted": 0,
        "labels_added": 0,
        "labels_removed": 0,
        "properties_set": 0,
    }


def accumulate(acc, counters):
    acc["nodes_created"] += counters.nodes_created
    acc["nodes_deleted"] += counters.nodes_deleted
    acc["relationships_created"] += counters.relationships_created
    acc["relationships_deleted"] += counters.relationships_deleted
    acc["labels_added"] += counters.labels_added
    acc["labels_removed"] += counters.labels_removed
    acc["properties_set"] += counters.properties_set


# ---------------------------------------------------------------------------
# 리셋 / 반영 / 검증
# ---------------------------------------------------------------------------
def reset_to_baseline(pg_conn, session, batch_size):
    """그래프를 깨끗한 2024 baseline으로 되돌린다(측정 제외)."""
    # 1) 기존 POI/Tombstone 노드를 라벨 단위로 모두 삭제한다.
    #    (:POITombstone에는 nf_id 인덱스가 없으므로 라벨 스캔으로 지운다)
    for label in ("POI2024", "POITombstone"):
        while True:
            deleted = session.execute_write(
                reset_delete_label_batch, label, batch_size
            )
            if deleted == 0:
                break

    # 2) 2024 스냅샷 전체를 다시 적재한다.
    for rows in stream_snapshot_rows(
        pg_conn, BASELINE_SNAPSHOT, batch_size
    ):
        prepared = [prepare_active_row(row) for row in rows]
        session.execute_write(reset_upsert_batch, prepared)


def assert_graph_equals(session, expected_ids, label):
    actual = load_neo4j_poi_ids(session)
    missing = expected_ids - actual
    extra = actual - expected_ids
    if missing or extra:
        raise RuntimeError(
            f"{label} 상태 불일치: 누락={len(missing):,}, "
            f"초과={len(extra):,}"
        )


def run_incremental_once(pg_conn, session, counts, batch_size):
    """증분 반영 1회. 반환: (경과초, 누적 counters)."""
    acc = new_counter_accumulator()
    start = time.perf_counter()

    # 1) disappear → tombstone
    for rows in stream_inc_rows(pg_conn, "disappear", batch_size):
        nf_ids = [str(row["nf_id"]) for row in rows]
        counters = session.execute_write(tombstone_batch, nf_ids)
        accumulate(acc, counters)

    # 2) appear, 3) updated → upsert
    for change_type in ("appear", "updated"):
        for rows in stream_inc_rows(pg_conn, change_type, batch_size):
            prepared = [prepare_active_row(row) for row in rows]
            counters_list = session.execute_write(upsert_batch, prepared)
            for counters in counters_list:
                accumulate(acc, counters)

    elapsed = time.perf_counter() - start
    return elapsed, acc


# ---------------------------------------------------------------------------
# 결과 출력/저장
# ---------------------------------------------------------------------------
def summarize(times):
    mean = statistics.mean(times)
    stdev = statistics.stdev(times) if len(times) > 1 else 0.0
    return mean, stdev


def write_csv(path, records, batch_size):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as file:
        writer = csv.writer(file)
        writer.writerow([
            "method", "run", "role", "batch_size",
            "elapsed_sec",
            "nodes_created", "nodes_deleted",
            "relationships_created", "relationships_deleted",
            "labels_added", "labels_removed", "properties_set",
        ])
        for rec in records:
            acc = rec["counters"]
            writer.writerow([
                METHOD_NAME, rec["run"], rec["role"], batch_size,
                f"{rec['elapsed']:.6f}",
                acc["nodes_created"], acc["nodes_deleted"],
                acc["relationships_created"], acc["relationships_deleted"],
                acc["labels_added"], acc["labels_removed"],
                acc["properties_set"],
            ])


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main():
    args = parse_arguments()

    with connect_postgresql() as pg_conn:
        # inc_poi 변화 유형별 건수 확인
        inc_counts = {
            row["change_type"]: row["row_count"]
            for row in pg_conn.execute(
                """
                SELECT change_type, COUNT(*) AS row_count
                FROM mart.inc_poi
                GROUP BY change_type
                """
            ).fetchall()
        }
        unknown = set(inc_counts) - VALID_CHANGE_TYPES
        if unknown:
            raise RuntimeError(f"알 수 없는 변화 유형: {unknown}")
        if not inc_counts:
            raise RuntimeError("mart.inc_poi가 비어 있습니다.")

        t1_ids = load_snapshot_ids(pg_conn, BASELINE_SNAPSHOT)
        t2_ids = load_snapshot_ids(pg_conn, TARGET_SNAPSHOT)

        print(f"방식: {METHOD_NAME}")
        print(f"2024 baseline NF_ID: {len(t1_ids):,}")
        print(f"2026 target NF_ID:   {len(t2_ids):,}")
        print("증분 변화 유형별 건수:")
        for change_type in ("appear", "disappear", "updated"):
            print(f"- {change_type}: {inc_counts.get(change_type, 0):,}")
        print(f"배치 크기: {args.batch_size:,}")
        print(
            f"반복: 총 {args.repeat}회 "
            f"(워밍업 {args.warmup}회 제외, 유효 "
            f"{args.repeat - args.warmup}회)"
        )

        if not args.apply:
            print(
                "\n점검만 완료했습니다. Neo4j는 변경하지 않았습니다.\n"
                "실제 벤치마크: python benchmark_incremental.py --apply"
            )
            return

        with connect_neo4j() as driver:
            driver.verify_connectivity()
            with driver.session(
                database=os.getenv("NEO4J_DATABASE")
            ) as session:
                records = []
                valid_times = []

                for run in range(1, args.repeat + 1):
                    role = "warmup" if run <= args.warmup else "measure"
                    print(f"\n===== run {run}/{args.repeat} ({role}) =====")

                    # 리셋(측정 제외)
                    print("리셋: 2024 baseline 재구성 중...", flush=True)
                    reset_to_baseline(pg_conn, session, args.batch_size)
                    assert_graph_equals(session, t1_ids, "2024 baseline")
                    print("2024 baseline 확인 완료")

                    # 증분 반영(측정)
                    elapsed, acc = run_incremental_once(
                        pg_conn, session, inc_counts, args.batch_size
                    )

                    # 결과 상태 검증
                    assert_graph_equals(session, t2_ids, "2026 target")
                    print(
                        f"반영 완료: {elapsed:.3f}초 | "
                        f"노드 생성={acc['nodes_created']:,}, "
                        f"라벨 제거(tombstone)={acc['labels_removed']:,}, "
                        f"관계 생성={acc['relationships_created']:,}, "
                        f"관계 삭제={acc['relationships_deleted']:,}"
                    )

                    records.append({
                        "run": run,
                        "role": role,
                        "elapsed": elapsed,
                        "counters": acc,
                    })
                    if role == "measure":
                        valid_times.append(elapsed)

                # 요약
                print("\n" + "=" * 48)
                print(f"방식: {METHOD_NAME}")
                print(f"유효 측정 횟수: {len(valid_times)}")
                print("측정값(초): " + ", ".join(
                    f"{t:.3f}" for t in valid_times
                ))
                if valid_times:
                    mean, stdev = summarize(valid_times)
                    print(f"평균: {mean:.3f}초")
                    print(f"표준편차: {stdev:.3f}초")
                    print(f"최소: {min(valid_times):.3f}초")
                    print(f"최대: {max(valid_times):.3f}초")

                # CSV 저장
                stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                csv_path = (
                    ROOT / "results"
                    / f"benchmark_{METHOD_NAME}_{stamp}.csv"
                )
                write_csv(csv_path, records, args.batch_size)
                print(f"\nCSV 저장: {csv_path}")


if __name__ == "__main__":
    main()
