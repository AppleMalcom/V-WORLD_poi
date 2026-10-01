---

# POI GeoKG 증분 갱신 파이프라인

이 프로젝트는 PostgreSQL/PostGIS에 저장된 **2024년·2026년 V-World POI 스냅샷**을 비교하고, 그 결과를 기존 Neo4j GeoKG에 반영하는 연구용 프로토타입이다.

**이 저장소에는 코드만 공개한다.** POI 원본 데이터, Neo4j dump, 접속 정보는 포함하지 않는다. 따라서 동일한 실험 결과를 재현하려면 해당 데이터에 대한 적법한 접근 권한과 초기 2024 GeoKG가 별도로 필요하다. 이 저장소는 두 POI 스냅샷이 PostgreSQL의 mart.poi에 적재되고 2024년 기준 GeoKG가 Neo4j에 구축된 상태를 전제로 한다. 공개 코드는 이후의 변화 탐지, 증분 반영, 정합성 검증을 자동화한다.

## 처리 과정

| 단계 | 파일 | 역할 |
| --- | --- | --- |
| 모듈 1: 변화 탐지 | `create_inc_poi.py`, `sql/create_inc_poi.sql` | 두 스냅샷을 검증하고 PostgreSQL의 `mart.inc_poi`를 생성 |
| 모듈 2: 그래프 갱신 | `apply_inc_poi.py` | 증분 데이터를 Neo4j의 POI 노드와 `IN` 관계에 반영 |
| 모듈 3: 결과 검증 | `verify_inc_poi.py` | 갱신된 그래프와 2026 스냅샷의 정합성을 검증 |
| 통합 실행 | `run_pipeline.py` | 모듈 1→2→3을 순서대로 실행 |
| 선택적 초기화 | `reset_to_t1.py` | 현재 로컬 Neo4j를 백업한 뒤 비공개 T1 dump로 복원 |

원본 POI 파일의 수집·전처리·RDB 적재와 최초 GeoKG 생성은 이 저장소의 자동화 범위에 포함되지 않는다.

## 변화 판정 기준

`nf_id`가 스냅샷 사이에서 안정적으로 유지되는 고유 식별자라고 가정한다. `nf_id`는 두 스냅샷을 연결하는 키이며, 감시 속성에는 포함하지 않는다.

감시 속성은 `poi_nm`, `poi_cl_dc`, `emd_cd`, `geom`이다. 공간좌표는 EPSG:5179에서 `ST_DWithin`으로 비교하며, **0.1m 이하의 위치 차이만으로는 수정(`updated`)으로 판정하지 않는다.**

| 2024 스냅샷 | 2026 스냅샷 | 판정 |
| --- | --- | --- |
| 없음 | 있음 | `appear` |
| 있음 | 없음 | `disappear` |
| 있음 | 있음, 감시 속성 변경 | `updated` |
| 있음 | 있음, 감시 속성 동일 | 증분 테이블에서 제외 |

`mart.inc_poi`에는 변화가 탐지된 POI의 **전체 원본 속성**을 저장한다. `appear`·`updated`는 2026년 속성을, `disappear`는 마지막으로 존재했던 2024년 속성을 저장한다.

## 실행 전제

- Python 3.11, PostgreSQL/PostGIS, Neo4j가 필요하다. 실험에서는 Python 3.11.9와 Neo4j Desktop 5.26.16을 사용했다.
- `mart.poi`에 2024·2026 스냅샷이 적재되어 있어야 한다. 각 스냅샷의 `nf_id`는 중복 및 NULL이 없어야 하며, `geom`의 SRID는 5179여야 한다.
- 기존 Neo4j에는 2024 기준 `:POI2024` 노드, `:POI2024(nf_id)` 고유성 제약조건, 필요한 `:admDongList` 노드가 있어야 한다.
- POI와 행정동은 `(:POI2024)-[:IN]->(:admDongList)` 형태로 연결된다.
- PostgreSQL에 SSH 터널로 접속한다면 Python 실행 전에 별도로 터널을 열어야 한다.

증분 갱신 후에도 노드 라벨은 `POI2024`로 유지된다. 이는 현재 데이터의 연도를 뜻하는 것이 아니라, **기존 그래프에서 사용하던 노드 라벨**이다.

## 설치 및 설정

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

프로젝트 최상위에 자신의 접속 정보로 `.env` 파일을 만든다. **실제 비밀번호가 포함된 `.env`는 GitHub에 올리지 않는다.**

```dotenv
PG_HOST=127.0.0.1
PG_PORT=<PostgreSQL 포트>
PG_DATABASE=<데이터베이스 이름>
PG_USER=<사용자 이름>
PG_PASSWORD=<비밀번호>

NEO4J_URI=neo4j://127.0.0.1:7687
NEO4J_DATABASE=neo4j
NEO4J_USER=<사용자 이름>
NEO4J_PASSWORD=<비밀번호>

# 아래 두 항목은 선택적 T1 복원에만 필요
NEO4J_HOME=<로컬 Neo4j DBMS 폴더>
NEO4J_JAVA_HOME=<로컬 Java 실행 환경 폴더>
```

## 파이프라인 실행

아래 명령은 연결 상태와 사전 조건만 확인하며 데이터베이스를 변경하지 않는다.

```powershell
python run_pipeline.py
```

2024 baseline에서 2026 target으로 실제 갱신하려면 다음을 실행한다.

```powershell
python run_pipeline.py --apply
```

이 명령은 `mart.inc_poi` 생성 → Neo4j 증분 반영 → 최종 검증 순으로 진행한다. **모듈 1은 기존 `mart.inc_poi`를 삭제하고 다시 만들며, 모듈 2는 Neo4j 그래프를 변경한다.** Neo4j의 `nf_id` 집합이 2024 baseline과 일치하지 않으면 통합 실행을 중단한다.

각 모듈은 개별 실행도 가능하다.

```powershell
python create_inc_poi.py --apply
python apply_inc_poi.py --apply
python verify_inc_poi.py
```

`verify_inc_poi.py`는 읽기 전용 검증이다.

## 선택적 T1 복원

`reset_to_t1.py`는 다음 위치의 비공개 baseline dump를 사용하도록 설정되어 있다.

```text
backups/t1/neo4j_t1_corrected_20261001.dump
```

**dump 파일은 이 저장소에서 제공하지 않는다.** 로컬 Neo4j의 경로를 확인하고 Neo4j Desktop에서 해당 인스턴스를 중지한 다음 실행해야 한다.

```powershell
python reset_to_t1.py          # 점검만 수행
python reset_to_t1.py --apply  # 현재 DB 백업 후 T1으로 교체
```

복원 후 Neo4j를 다시 시작하고 `python apply_inc_poi.py`를 **`--apply` 없이** 실행해 2024 baseline 상태를 확인한다.

## 실험 결과

| 항목 | 건수 |
| --- | ---: |
| 2024 POI | 125,010 |
| 2026 POI | 105,494 |
| `appear` | 49,315 |
| `disappear` | 68,831 |
| `updated` | 1,456 |
| 전체 증분 데이터 | 119,602 |

실험 환경에서 Neo4j 증분 반영에는 약 **34초**가 걸렸다. 최종 검증에서는 2026 POI 105,494건, 신규·수정 POI의 전체 속성 50,771건, 소멸 POI 68,831건을 확인했으며 **오류 0건**이었다. 이 시간은 해당 환경의 관측값이며 일반적인 성능 보장은 아니다.

## 데이터 공개 범위와 한계

원본 POI 데이터와 Neo4j dump는 공개하지 않는다. `.gitignore`는 `.env`, `.venv/`, `data/`, `backups/`를 제외하지만, **GitHub 웹 업로드에는 이 규칙이 자동 적용되지 않으므로 파일을 직접 선별해야 한다.**

이 코드는 두 개의 고정된 스냅샷과 안정적인 `nf_id`, 이미 구축된 2024 GeoKG를 전제로 한다. 원본 데이터의 품질 검증, 실제 시설 단위 식별자 매칭, 최초 그래프 구축, 실시간 스트리밍 CDC는 연구 범위 밖이다.
