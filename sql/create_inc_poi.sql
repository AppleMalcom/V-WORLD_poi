-- ============================================================
-- 2024–2026 POI 전체 snapshot 비교 및 증분 테이블 생성
--
-- 식별 키:
--   nf_id
--
-- Watched properties:
--   poi_nm, poi_cl_dc, emd_cd, geom
--
-- 출력:
--   mart.inc_poi
-- ============================================================


-- 기존 증분 테이블 제거
DROP TABLE IF EXISTS mart.inc_poi;


-- 증분 POI 테이블 생성
CREATE TABLE mart.inc_poi AS
WITH t1 AS (
    -- Baseline state: 2024 snapshot
    SELECT
        nf_id,
        poi_nm,
        poi_cl_dc,
        emd_cd,
        geom
    FROM mart.poi
    WHERE snapshot = '2024'
),
t2 AS (
    -- Target state: 2026 snapshot
    SELECT
        nf_id,
        poi_nm,
        poi_cl_dc,
        emd_cd,
        geom
    FROM mart.poi
    WHERE snapshot = '2026'
),
classified AS (
    -- NF_ID를 기준으로 변화 유형 판정
    SELECT
        COALESCE(t2.nf_id, t1.nf_id) AS nf_id,

        CASE
            -- 2024에는 없고 2026에만 존재
            WHEN t1.nf_id IS NULL
                THEN 'appear'

            -- 2024에는 있고 2026에는 없음
            WHEN t2.nf_id IS NULL
                THEN 'disappear'

            -- 동일 NF_ID의 watched properties 변경
              WHEN
                    t1.poi_nm IS DISTINCT FROM t2.poi_nm
                OR t1.poi_cl_dc IS DISTINCT FROM t2.poi_cl_dc
                OR t1.emd_cd IS DISTINCT FROM t2.emd_cd
                OR (
                    (t1.geom IS NULL)
                    IS DISTINCT FROM
                    (t2.geom IS NULL)
                )
                OR (
                    t1.geom IS NOT NULL
                    AND t2.geom IS NOT NULL
                    AND NOT ST_DWithin(
                        t1.geom,
                        t2.geom,
                        0.1
                   )
                )

                    THEN 'updated'

            -- 식별자와 watched properties가 모두 동일
            ELSE 'no_change'
        END AS change_type

    FROM t1
    FULL OUTER JOIN t2
        ON t1.nf_id = t2.nf_id
),
changes AS (
    -- GeoKG 갱신이 필요 없는 객체 제외
    SELECT
        nf_id,
        change_type
    FROM classified
    WHERE change_type <> 'no_change'
)

-- 변화가 탐지된 POI의 전체 속성 저장
SELECT
    p.*,
    c.change_type
FROM changes AS c
JOIN mart.poi AS p
    ON p.nf_id = c.nf_id
   AND p.snapshot = CASE
       -- 소멸 객체는 마지막으로 존재했던 2024 속성 저장
       WHEN c.change_type = 'disappear'
           THEN '2024'

       -- 신규 및 수정 객체는 최신 2026 속성 저장
       ELSE '2026'
   END;


-- 변화 유형별 조회용 인덱스
CREATE INDEX idx_inc_poi_change_type
ON mart.inc_poi (change_type);


-- NF_ID 검색 및 중복 방지용 고유 인덱스
CREATE UNIQUE INDEX idx_inc_poi_nf_id
ON mart.inc_poi (nf_id);


-- PostgreSQL 쿼리 최적화를 위한 통계 갱신
ANALYZE mart.inc_poi;