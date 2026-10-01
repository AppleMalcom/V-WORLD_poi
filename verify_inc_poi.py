import math
import os
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv
from neo4j import GraphDatabase


load_dotenv()

BATCH_SIZE = 1000
SPATIAL_TOLERANCE_M = 0.1001

EXCLUDED_PROPERTIES = {
    "geom",
    "change_type",
    "snapshot",
    "longitude",
    "latitude",
}

verification = {
    "checked": 0,
    "errors": 0,
    "samples": [],
}


def add_error(message):
    verification["errors"] += 1

    if len(verification["samples"]) < 20:
        verification["samples"].append(message)


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


def canonical(value):
    if value is None or value == "":
        return None

    if isinstance(value, Decimal):
        return float(value)

    if isinstance(value, UUID):
        return str(value)

    if isinstance(value, (date, datetime, time)):
        return value.isoformat()

    # Neo4j temporal type
    if type(value).__module__.startswith("neo4j.time"):
        return str(value)

    return value


def distance_m(lon1, lat1, lon2, lat2):
    earth_radius = 6_371_008.8

    lon1 = math.radians(float(lon1))
    lat1 = math.radians(float(lat1))
    lon2 = math.radians(float(lon2))
    lat2 = math.radians(float(lat2))

    delta_lon = lon2 - lon1
    delta_lat = lat2 - lat1

    value = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1)
        * math.cos(lat2)
        * math.sin(delta_lon / 2) ** 2
    )

    return 2 * earth_radius * math.asin(
        math.sqrt(value)
    )


def stream_rows(
    pg_conn,
    cursor_name,
    query,
    parameters=(),
):
    with pg_conn.cursor(name=cursor_name) as cursor:
        cursor.execute(query, parameters)

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)

            if not rows:
                break

            yield rows


def load_neo4j_nodes(session, nf_ids):
    rows = session.run(
        """
        MATCH (p:POI2024)
        WHERE p.nf_id IN $nf_ids

        OPTIONAL MATCH
            (p)-[r:IN]->(a:admDongList)

        RETURN
            p.nf_id AS nf_id,
            properties(p) AS properties,
            count(r) AS in_count,
            collect(
                DISTINCT toString(a.emd_cd)
            ) AS related_emd_codes
        """,
        nf_ids=nf_ids,
    ).data()

    return {
        row["nf_id"]: row
        for row in rows
    }


def verify_target_snapshot(pg_conn, neo4j_session):
    print("1. 2026 전체 watched properties 검증")

    processed = 0

    query = """
        SELECT
            nf_id,
            poi_nm,
            poi_cl_dc,
            emd_cd,
            longitude,
            latitude
        FROM mart.poi
        WHERE snapshot = '2026'
        ORDER BY nf_id
    """

    for rows in stream_rows(
        pg_conn,
        "verify_target_cursor",
        query,
    ):
        expected_by_id = {
            str(row["nf_id"]): row
            for row in rows
        }

        actual_by_id = load_neo4j_nodes(
            neo4j_session,
            list(expected_by_id),
        )

        for nf_id, expected in expected_by_id.items():
            verification["checked"] += 1
            actual = actual_by_id.get(nf_id)

            if actual is None:
                add_error(
                    f"{nf_id}: Neo4j 노드 없음"
                )
                continue

            properties = actual["properties"]

            for key in [
                "poi_nm",
                "poi_cl_dc",
                "emd_cd",
            ]:
                expected_value = canonical(
                    expected[key]
                )
                actual_value = canonical(
                    properties.get(key)
                )

                if expected_value != actual_value:
                    add_error(
                        f"{nf_id}: {key} 불일치 "
                        f"(PG={expected_value}, "
                        f"Neo4j={actual_value})"
                    )

            actual_longitude = properties.get(
                "longitude"
            )
            actual_latitude = properties.get(
                "latitude"
            )

            if (
                actual_longitude is None
                or actual_latitude is None
            ):
                add_error(
                    f"{nf_id}: 경도 또는 위도 없음"
                )
            else:
                coordinate_distance = distance_m(
                    expected["longitude"],
                    expected["latitude"],
                    actual_longitude,
                    actual_latitude,
                )

                if (
                    coordinate_distance
                    > SPATIAL_TOLERANCE_M
                ):
                    add_error(
                        f"{nf_id}: 좌표 거리 불일치 "
                        f"({coordinate_distance:.6f}m)"
                    )

            location = properties.get("location")

            if location is None:
                add_error(
                    f"{nf_id}: location Point 없음"
                )
            else:
                location_distance = distance_m(
                    expected["longitude"],
                    expected["latitude"],
                    location.longitude,
                    location.latitude,
                )

                if (
                    location_distance
                    > SPATIAL_TOLERANCE_M
                ):
                    add_error(
                        f"{nf_id}: location 불일치 "
                        f"({location_distance:.6f}m)"
                    )

            if actual["in_count"] != 1:
                add_error(
                    f"{nf_id}: IN 관계 수="
                    f"{actual['in_count']}"
                )
            elif set(
                actual["related_emd_codes"]
            ) != {str(expected["emd_cd"])}:
                add_error(
                    f"{nf_id}: IN 관계 행정동 불일치 "
                    f"(PG={expected['emd_cd']}, "
                    f"Neo4j="
                    f"{actual['related_emd_codes']})"
                )

        processed += len(rows)

        print(
            f"\r2026 POI 검증: {processed:,}",
            end="",
            flush=True,
        )

    print()


def verify_changed_full_properties(
    pg_conn,
    neo4j_session,
):
    print(
        "2. appear/updated 전체 속성 검증"
    )

    processed = 0

    query = """
        SELECT *
        FROM mart.inc_poi
        WHERE change_type IN (
            'appear',
            'updated'
        )
        ORDER BY nf_id
    """

    for rows in stream_rows(
        pg_conn,
        "verify_changed_cursor",
        query,
    ):
        expected_by_id = {
            str(row["nf_id"]): row
            for row in rows
        }

        actual_by_id = load_neo4j_nodes(
            neo4j_session,
            list(expected_by_id),
        )

        for nf_id, expected in expected_by_id.items():
            actual = actual_by_id.get(nf_id)

            if actual is None:
                add_error(
                    f"{nf_id}: 변경 노드 없음"
                )
                continue

            actual_properties = actual["properties"]

            for key, expected_value in expected.items():
                if key in EXCLUDED_PROPERTIES:
                    continue

                expected_value = canonical(
                    expected_value
                )
                actual_value = canonical(
                    actual_properties.get(key)
                )

                if expected_value != actual_value:
                    add_error(
                        f"{nf_id}: 전체 속성 {key} 불일치 "
                        f"(PG={expected_value}, "
                        f"Neo4j={actual_value})"
                    )

        processed += len(rows)

        print(
            f"\r변경 POI 전체 속성 검증: "
            f"{processed:,}",
            end="",
            flush=True,
        )

    print()


def verify_disappear(pg_conn, neo4j_session):
    print("3. disappear 삭제 검증")

    processed = 0

    query = """
        SELECT nf_id
        FROM mart.inc_poi
        WHERE change_type = 'disappear'
        ORDER BY nf_id
    """

    for rows in stream_rows(
        pg_conn,
        "verify_disappear_cursor",
        query,
    ):
        nf_ids = [
            str(row["nf_id"])
            for row in rows
        ]

        remaining_ids = neo4j_session.run(
            """
            MATCH (p:POI2024)
            WHERE p.nf_id IN $nf_ids
            RETURN p.nf_id AS nf_id
            """,
            nf_ids=nf_ids,
        ).value("nf_id")

        for nf_id in remaining_ids:
            add_error(
                f"{nf_id}: disappear 노드가 남아 있음"
            )

        processed += len(rows)

        print(
            f"\rdisappear 검증: {processed:,}",
            end="",
            flush=True,
        )

    print()


with connect_postgresql() as pg_connection:
    with connect_neo4j() as neo4j_driver:
        neo4j_driver.verify_connectivity()

        with neo4j_driver.session(
            database=os.getenv("NEO4J_DATABASE")
        ) as neo4j_session:
            verify_target_snapshot(
                pg_connection,
                neo4j_session,
            )

            verify_changed_full_properties(
                pg_connection,
                neo4j_session,
            )

            verify_disappear(
                pg_connection,
                neo4j_session,
            )


print()
print("검증한 2026 POI:", f"{verification['checked']:,}")
print("오류:", f"{verification['errors']:,}")

if verification["errors"]:
    print("\n오류 예시:")

    for error in verification["samples"]:
        print("-", error)

    raise SystemExit(1)

print(
    "RDB와 GDB의 증분 갱신 결과가 "
    "정상적으로 일치합니다."
)