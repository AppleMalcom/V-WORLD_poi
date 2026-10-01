import argparse
import hashlib
import os
import shutil
import socket
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parent
BACKUPS = ROOT / "backups"
T1_DUMP = BACKUPS / "t1" / "neo4j_t1_corrected_20261001.dump"

load_dotenv(ROOT / ".env")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for block in iter(lambda: file.read(8 * 1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def bolt_is_running():
    uri = urlparse(os.getenv("NEO4J_URI", "neo4j://127.0.0.1:7687"))
    if uri.hostname not in ("127.0.0.1", "localhost"):
        raise RuntimeError("로컬 Neo4j만 복원할 수 있습니다.")

    try:
        with socket.create_connection((uri.hostname, uri.port or 7687), timeout=1):
            return True
    except OSError:
        return False


def run_admin(admin, java_home, *arguments):
    environment = os.environ.copy()
    environment["JAVA_HOME"] = str(java_home)
    environment["PATH"] = (
        str(java_home / "bin")
        + os.pathsep
        + environment.get("PATH", "")
    )
    subprocess.run(
        [str(admin), "database", *arguments],
        cwd=admin.parent.parent,
        env=environment,
        check=True,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Neo4j를 2024 T1 상태로 복원"
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="현재 DB 백업 및 T1 복원 실행",
    )
    args = parser.parse_args()

    if not T1_DUMP.is_file() or T1_DUMP.stat().st_size == 0:
        raise RuntimeError(f"T1 dump를 찾지 못했습니다: {T1_DUMP}")

    home_text = os.getenv("NEO4J_HOME")
    java_text = os.getenv("NEO4J_JAVA_HOME")
    if not home_text or not java_text:
        raise RuntimeError(
            ".env에 NEO4J_HOME과 NEO4J_JAVA_HOME을 설정하세요."
        )

    neo4j_home = Path(home_text)
    java_home = Path(java_text)
    admin = neo4j_home / "bin" / "neo4j-admin.bat"

    if not admin.is_file():
        raise RuntimeError(f"관리 도구를 찾지 못했습니다: {admin}")
    if not (java_home / "bin" / "java.exe").is_file():
        raise RuntimeError(f"Java 21을 찾지 못했습니다: {java_home}")

    # Windows의 Neo4j 관리자 명령이 한글 경로를 처리하지 못할 수 있어
    # 영문 경로에 임시 dump를 만든다.
    staging_root = neo4j_home / "data" / "dumps"
    if not str(staging_root).isascii():
        raise RuntimeError("Neo4j 임시 경로에 한글이 포함되어 있습니다.")

    print(f"복원 대상: {neo4j_home}")
    print(f"T1 dump: {T1_DUMP}")
    print(f"Neo4j 실행 중: {bolt_is_running()}")

    if not args.apply:
        print("점검만 완료했습니다. DB는 변경하지 않았습니다.")
        return

    if bolt_is_running():
        raise RuntimeError(
            "Neo4j Desktop에서 인스턴스를 Stop한 후 다시 실행하세요."
        )

    staging_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(
        prefix="t1-reset-", dir=staging_root
    ) as temporary:
        stage = Path(temporary)
        current_dir = stage / "current"
        t1_dir = stage / "t1"
        current_dir.mkdir()
        t1_dir.mkdir()

        # 1. 현재 DB를 먼저 백업한다.
        print("현재 DB 백업 중...", flush=True)
        run_admin(
            admin, java_home,
            "dump", "neo4j", f"--to-path={current_dir}",
        )

        current_dump = current_dir / "neo4j.dump"
        if not current_dump.is_file():
            raise RuntimeError("현재 DB dump 생성에 실패했습니다.")

        saved_dir = BACKUPS / "pre_restore"
        saved_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        saved_dump = saved_dir / f"neo4j_before_reset_{timestamp}.dump"

        shutil.copy2(current_dump, saved_dump)
        if file_hash(current_dump) != file_hash(saved_dump):
            raise RuntimeError("현재 DB 백업 검증에 실패했습니다.")

        print(f"현재 DB 백업 완료: {saved_dump}", flush=True)

        # 2. T1 dump를 Neo4j가 읽을 수 있는 영문 경로로 복사한다.
        staged_t1 = t1_dir / "neo4j.dump"
        shutil.copy2(T1_DUMP, staged_t1)
        if file_hash(T1_DUMP) != file_hash(staged_t1):
            raise RuntimeError("T1 dump 복사 검증에 실패했습니다.")

        run_admin(
            admin, java_home,
            "load", "--info", f"--from-path={t1_dir}", "neo4j",
        )

        # 3. 기존 neo4j DB를 T1으로 교체한다.
        print("T1 복원 중...", flush=True)
        run_admin(
            admin, java_home,
            "load", "neo4j",
            f"--from-path={t1_dir}",
            "--overwrite-destination=true",
        )

    print("복원 완료. Neo4j Desktop에서 인스턴스를 Start하세요.")
    print("그다음 python apply_inc_poi.py 로 2024 baseline을 확인하세요.")


if __name__ == "__main__":
    main()