import argparse
import os
import subprocess
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from neo4j import GraphDatabase


PROJECT_DIR = Path(__file__).resolve().parent
load_dotenv(PROJECT_DIR / ".env")


def run_step(title, filename, *options):
    print(f"\n===== {title} =====", flush=True)

    result = subprocess.run(
        [sys.executable, str(PROJECT_DIR / filename), *options],
        cwd=PROJECT_DIR,
    )

    if result.returncode != 0:
        raise SystemExit(
            f"{title} 실패: 종료 코드 {result.returncode}"
        )


def graph_state():
    with psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        connect_timeout=5,
    ) as connection:
        baseline_ids = {
            row[0]
            for row in connection.execute(
                "SELECT nf_id FROM mart.poi WHERE snapshot = '2024'"
            )
        }
        target_ids = {
            row[0]
            for row in connection.execute(
                "SELECT nf_id FROM mart.poi WHERE snapshot = '2026'"
            )
        }

    with GraphDatabase.driver(
        os.getenv("NEO4J_URI"),
        auth=(
            os.getenv("NEO4J_USER"),
            os.getenv("NEO4J_PASSWORD"),
        ),
    ) as driver:
        with driver.session(
            database=os.getenv("NEO4J_DATABASE")
        ) as session:
            graph_ids = set(
                session.run(
                    "MATCH (p:POI2024) RETURN p.nf_id AS nf_id"
                ).value("nf_id")
            )

    if graph_ids == baseline_ids:
        return "2024 baseline"
    if graph_ids == target_ids:
        return "2026 target"
    return "partial/mixed"


def main():
    parser = argparse.ArgumentParser(
        description="POI 증분 갱신 모듈 1·2·3 통합 실행"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="2024 baseline에서 실제 증분 갱신 실행",
    )
    args = parser.parse_args()

    state = graph_state()
    print(f"현재 Neo4j 상태: {state}", flush=True)

    if args.apply:
        if state != "2024 baseline":
            raise SystemExit(
                "실행 중단: Neo4j가 2024 baseline이 아닙니다."
            )

        run_step(
            "모듈 1: 증분 테이블 생성",
            "create_inc_poi.py",
            "--apply",
        )
        run_step(
            "모듈 2: Neo4j 증분 반영",
            "apply_inc_poi.py",
            "--apply",
        )
        run_step(
            "모듈 3: 최종 정합성 검증",
            "verify_inc_poi.py",
        )
        print("\n통합 파이프라인 완료")
    else:
        run_step(
            "모듈 1 사전 점검",
            "create_inc_poi.py",
        )
        run_step(
            "모듈 2 사전 점검",
            "apply_inc_poi.py",
        )

        if state == "2026 target":
            run_step(
                "모듈 3 현재 상태 검증",
                "verify_inc_poi.py",
            )

        print("\n읽기 전용 점검 완료")


if __name__ == "__main__":
    main()