"""Batch(전체 재구축) 방식 Neo4j 갱신 성능 벤치마크.

증분(benchmark_incremental.py)과의 공정 비교를 위해 반복 횟수,
배치 크기, 리셋 방식, 측정 구간 구성, CSV 포맷을 모두 동일하게
맞춘다. 유일한 차이는 '갱신'을 변화분만 반영하는 대신 2024 그래프를
통째로 비우고 2026 전체 스냅샷으로 재구축한다는 점이다.

측정 구간(= Batch 반영):
    1) 기존 :POI2024 / :POITombstone 노드 전량 DETACH DELETE
    2) mart.poi의 2026 전체를 MERGE + location + IN 관계로 재구축
리셋(2024 baseline 재구성)과 검증 시간은 측정에 포함하지 않는다.

Batch 방식은 소멸 POI를 보존하지 않으므로 :POITombstone 노드를
남기지 않는다(이력 소실). 이는 증분 방식과의 질적 차이이다.

사용 예:
    python benchmark_batch.py                 # 점검만(변경 없음)
    python benchmark_batch.py --apply         # 6회 반복(1회 워밍업)
    python benchmark_batch.py --apply --repeat 6 --warmup 1
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

# PostgreSQL에는 있지만 Neo4j 속성으로 직접 넣지 않을 컬럼
EXCLUDED_PROPERTIES = {"geom", "change_type", "snapshot"}

BASELINE_SNAPSHOT = "2024"
TARGET_SNAPSHOT = "2026"
METHOD_NAME = "batch_rebuild"


# ---------------------------------------------------------------------------
# 인자 및 연결
# ---------------------------------------------------------------------------
def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "Batch(전체 재구축) 방식의 Neo4j 갱신 시간을 반복 측정한다."
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
        raise ValueError("warmup은 0 이상이고 repeat보다 작아야 한다.")

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
# 공통 유틸 (benchmark_incremental.py와 동일한 변환 규칙)
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


# ---------------------------------------------------------------------------
# Cypher 쓰기 블록
# ---------------------------------------------------------------------------
def delete_label_batch(tx, label, batch_size):
    """라벨 노드를 LIMIT 단위로 DETACH DELETE. 반환: 삭제된 노드 수."""
    result = tx.run(
        f"""
        MATCH (p:{label})
        WITH p LIMIT $batch_size
        DETACH DELETE p
        """,
        batch_size=batch_size,
    )
    return result.consume().counters


def rebuild_upsert_batch(tx, rows):
    """2026 전체 노드를 MERGE + location + IN 재구축."""
    s1 = tx.run(
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

    s2 = tx.run(
        """
        UNWIND $rows AS row
        MATCH (p:POI2024 {nf_id: row.nf_id})
        MATCH (a:admDongList)
        WHERE toString(a.emd_cd) = row.emd_cd
        MERGE (p)-[r:IN]->(a)
        """,
        rows=rows,
    ).consume()

    return [s1.counters, s2.counters]


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
def delete_all_poi_nodes(session, batch_size):
    """현재 :POI2024 / :POITombstone 노드를 모두 삭제한다.

    반환: 누적 counters (측정 구간에서 재사용).
    """
    acc = new_counter_accumulator()
    for label in ("POI2024", "POITombstone"):
        while True:
            counters = session.execute_write(
                delete_label_batch, label, batch_size
            )
            accumulate(acc, counters)
            if counters.nodes_deleted == 0:
                break
    return acc


def reset_to_baseline(pg_conn, session, batch_size):
    """그래프를 깨끗한 2024 baseline으로 되돌린다(측정 제외)."""
    delete_all_poi_nodes(session, batch_size)
    for rows in stream_snapshot_rows(
        pg_conn, BASELINE_SNAPSHOT, batch_size
    ):
        prepared = [prepare_active_row(row) for row in rows]
        session.execute_write(rebuild_upsert_batch, prepared)


def assert_graph_equals(session, expected_ids, label):
    actual = load_neo4j_poi_ids(session)
    missing = expected_ids - actual
    extra = actual - expected_ids
    if missing or extra:
        raise RuntimeError(
            f"{label} 상태 불일치: 누락={len(missing):,}, "
            f"초과={len(extra):,}"
        )


def run_batch_once(pg_conn, session, batch_size):
    """Batch 전체 재구축 1회. 반환: (경과초, 누적 counters).

    측정 구간: 기존 노드 전량 삭제 + 2026 전체 재구축.
    """
    acc = new_counter_accumulator()
    start = time.perf_counter()

    # 1) 기존 POI/Tombstone 노드 전량 삭제
    delete_acc = delete_all_poi_nodes(session, batch_size)
    for key in acc:
        acc[key] += delete_acc[key]

    # 2) 2026 전체 스냅샷으로 재구축
    for rows in stream_snapshot_rows(
        pg_conn, TARGET_SNAPSHOT, batch_size
    ):
        prepared = [prepare_active_row(row) for row in rows]
        counters_list = session.execute_write(
            rebuild_upsert_batch, prepared
        )
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
        t1_ids = load_snapshot_ids(pg_conn, BASELINE_SNAPSHOT)
        t2_ids = load_snapshot_ids(pg_conn, TARGET_SNAPSHOT)

        print(f"방식: {METHOD_NAME}")
        print(f"2024 baseline NF_ID: {len(t1_ids):,}")
        print(f"2026 target NF_ID:   {len(t2_ids):,}")
        print(f"배치 크기: {args.batch_size:,}")
        print(
            f"반복: 총 {args.repeat}회 "
            f"(워밍업 {args.warmup}회 제외, 유효 "
            f"{args.repeat - args.warmup}회)"
        )

        if not args.apply:
            print(
                "\n점검만 완료했습니다. Neo4j는 변경하지 않았습니다.\n"
                "실제 벤치마크: python benchmark_batch.py --apply"
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

                    # Batch 재구축(측정)
                    elapsed, acc = run_batch_once(
                        pg_conn, session, args.batch_size
                    )

                    # 결과 상태 검증
                    assert_graph_equals(session, t2_ids, "2026 target")
                    print(
                        f"재구축 완료: {elapsed:.3f}초 | "
                        f"노드 삭제={acc['nodes_deleted']:,}, "
                        f"노드 생성={acc['nodes_created']:,}, "
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
