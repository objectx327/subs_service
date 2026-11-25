import os
import sys
from datetime import timedelta
from pathlib import Path
from uuid import uuid4

import pendulum
import psycopg
from psycopg.types.json import Jsonb

from airflow.sdk import dag, task, get_current_context
from airflow.timetables.interval import CronDataIntervalTimetable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR / "py"))

from load_subscriptions import (
    read_manifest,
    read_csv_sources,
    read_subscriptions,
    fetch_payments,
    save_staging,
    save_source_error,
    save_final_checks,
    save_event,
    validate_staging,
    apply_core,
    refresh_marts,
    check_period,
    check_quality,
    quality_gate,
    verify_publication_approval,
    check_published_model,
)

CONN_ID = "subs_postgres"
DATA_DIR = PROJECT_DIR / "subscription_source" / "data"
API_URL = os.getenv("PAYMENTS_API_URL", "http://127.0.0.1:8765/payments")
DAG_START = pendulum.datetime(2026, 1, 1, tz="UTC")


def connect_db():
    settings = PostgresHook(postgres_conn_id=CONN_ID).get_connection(CONN_ID)
    conn = psycopg.connect(
        host=settings.host,
        port=settings.port or 5432,
        dbname=settings.schema,
        user=settings.login,
        password=settings.password,
        autocommit=True,
    )
    conn.execute("SET TIME ZONE 'UTC'")
    return conn

@task(task_id="receive_sources", retries=2, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=5))
def receive_sources_task():
    context = get_current_context()
    dag_run = context["dag_run"]
    conf = dag_run.conf or {}
    attempt = context["ti"].try_number
    run_type = getattr(dag_run.run_type, "value", str(dag_run.run_type))

    if run_type == "manual":
        batch_id = conf.get("batch_id")
        if not isinstance(batch_id, str) or not batch_id.strip():
            raise AirflowFailException("Для ручного запуска нужен batch_id")
    else:
        interval_start = context["data_interval_start"].in_timezone("UTC")
        interval_end = context["data_interval_end"].in_timezone("UTC")
        matches = []

        for folder in sorted(DATA_DIR.iterdir()):
            if not folder.is_dir() or folder.name.startswith("test_"):
                continue
            if folder.name == "04_full_reference":
                continue
            manifest_path = folder / "manifest.json"
            if not manifest_path.is_file():
                continue
            info = read_manifest(manifest_path)
            if info["manifest_errors"]:
                continue

            delta_matches = (
                info["mode"] == "delta"
                and info["manifest_from_inclusive"] == interval_start
                and info["manifest_until_exclusive"] == interval_end
            )
            initial_full_matches = (
                info["mode"] == "full"
                and info["manifest_from_inclusive"] is None
                and info["manifest_until_exclusive"] == interval_end
                and interval_start == DAG_START
            )
            if delta_matches or initial_full_matches:
                matches.append(folder.name)

        if len(matches) != 1:
            raise AirflowFailException(
                f"Для интервала {interval_start} - {interval_end} ожидалась одна поставка, найдено: {matches}"
            )
        batch_id = matches[0]

    batch_dir = (DATA_DIR / batch_id).resolve()
    if batch_dir.parent != DATA_DIR.resolve() or not batch_dir.is_dir():
        raise AirflowFailException("Неизвестная папка поставки")

    with connect_db() as conn:
        with conn.transaction():
            conn.execute(
                "INSERT INTO etl.pipeline_state (pipeline_name) VALUES (%s) "
                "ON CONFLICT (pipeline_name) DO NOTHING",
                ("subs_service",),
            )
            existing = conn.execute(
                """
                SELECT load_id, batch_id, status, staging_complete, manifest_data
                FROM etl.load_runs WHERE run_id=%s
                ORDER BY started_at DESC LIMIT 1 FOR UPDATE
                """,
                (dag_run.run_id,),
            ).fetchone()
            if existing is None:
                load_id = uuid4()
                saved_manifest = None
                conn.execute(
                    """
                    INSERT INTO etl.load_runs (load_id, run_id, batch_id, source_path)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (load_id, dag_run.run_id, batch_id, str(batch_dir)),
                )
            else:
                load_id, old_batch, old_status, completed, saved_manifest = existing
                if old_batch != batch_id:
                    raise AirflowFailException("У данного DAG Run уже другая поставка")
                if old_status == "published":
                    raise AirflowSkipException("Этот DAG Run уже опубликован")
                if not completed:
                    conn.execute(
                        "UPDATE etl.load_runs SET status='running', finished_at=NULL, last_error=NULL WHERE load_id=%s",
                        (load_id,),
                    )

        try:
            manifest = read_manifest(batch_dir / "manifest.json")
            if manifest["manifest_errors"]:
                raise AirflowFailException("Некорректный manifest: " + "; ".join(manifest["manifest_errors"][:3]))
            if saved_manifest is not None and saved_manifest != manifest["manifest_data"]:
                raise AirflowFailException("Манифест изменился внутри данного DAG Run")

            with conn.transaction():
                conn.execute(
                    """
                    UPDATE etl.load_runs
                    SET manifest_data=%s, mode=%s, period_from=%s, period_to=%s,
                        manifest_complete=%s
                    WHERE load_id=%s
                    """,
                    (Jsonb(manifest["manifest_data"]), manifest["mode"],
                     manifest["manifest_from_inclusive"], manifest["manifest_until_exclusive"],
                     manifest["complete"], load_id),
                )
                check_period(conn, manifest, batch_id)

            save_event(conn, load_id, "receive", "SUCCESS", attempt=attempt)
        except Exception as error:
            reason = f"receive_sources: {type(error).__name__}"
            if isinstance(error, (ValueError, AirflowFailException)):
                reason += f": {str(error)[:500]}"
            with conn.transaction():
                conn.execute(
                    """
                    UPDATE etl.load_runs
                    SET status='failed', last_error=%s, finished_at=clock_timestamp()
                    WHERE load_id=%s
                    """,
                    (reason, load_id),
                )
            save_event(conn, load_id, "receive", "FAILED", message=reason, attempt=attempt)
            raise

    return str(load_id)


@task(task_id="stage_sources", retries=2, retry_delay=timedelta(seconds=30), execution_timeout=timedelta(minutes=10))
def stage_sources_task(load_id):
    attempt = get_current_context()["ti"].try_number
    source_error = {}
    with connect_db() as conn:
        try:
            row = conn.execute(
                "SELECT batch_id, source_path, status, staging_complete FROM etl.load_runs WHERE load_id=%s", (load_id,),
            ).fetchone()
            if row is None:
                raise AirflowFailException("load_id не найден")
            
            batch_id, source_path, status, staging_complete = row
            if staging_complete:
                save_event(conn, load_id, "stage", "SUCCESS", attempt=attempt,
                           message="Ранее зафиксированный staging использован повторно")
                return load_id
            if status == "published":
                raise AirflowFailException("Опубликованную загрузку нельзя повторно записывать в staging")
            bad_source = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM stg.source_records WHERE load_id=%s AND raw_text IS NOT NULL)",
                (load_id,),
            ).fetchone()[0]
            if bad_source:
                raise AirflowFailException("Сохранен поврежденный источник: создайте новый DAG Run")
            if status != "running":
                with conn.transaction():
                    conn.execute(
                        """
                        UPDATE etl.load_runs
                        SET status='running', last_error=NULL, finished_at=NULL
                        WHERE load_id=%s
                        """,
                        (load_id,),
                    )

            batch_dir = Path(source_path)
            csv_records = read_csv_sources(batch_dir, source_error)
            subscriptions = read_subscriptions(batch_dir, source_error)
            payments = fetch_payments(batch_id, API_URL, limit=5, source_error=source_error)
            counts = save_staging(conn, load_id, batch_id, csv_records, subscriptions, payments)
            save_event(conn, load_id, "stage", "SUCCESS", counters=counts, attempt=attempt)
        except Exception as error:
            reason = f"stage_sources: {type(error).__name__}"
            with conn.transaction():
                if source_error:
                    save_source_error(conn, load_id, batch_id, source_error)
                conn.execute(
                    """
                    UPDATE etl.load_runs
                    SET status='failed', last_error=%s, finished_at=clock_timestamp()
                    WHERE load_id=%s AND status <> 'published'
                    """,
                    (reason, load_id),
                )

            save_event(conn, load_id, "stage", "FAILED", message=reason, attempt=attempt)
            if source_error:
                raise AirflowFailException(source_error["reason"]) from error
            raise
        
    return load_id


@task(task_id="prepare_data", retries=0, execution_timeout=timedelta(minutes=10))
def prepare_data_task(load_id):
    with connect_db() as conn:
        row = conn.execute("SELECT source_path, manifest_data FROM etl.load_runs WHERE load_id=%s", (load_id,)).fetchone()
        if row is None:
            raise AirflowFailException("load_id не найден")
        manifest = read_manifest(Path(row[0]) / "manifest.json")
        if manifest["manifest_errors"] or manifest["manifest_data"] != row[1]:
            raise AirflowFailException("Манифест некорректен или изменился")
        result = validate_staging(conn, load_id, manifest, strict=False)
        status = "WARN" if result["rejected_rows"] or result["delivery_errors"] else "SUCCESS"
        save_event(conn, load_id, "prepare", status,
                   counters={**result["counts"], "rejected_rows": result["rejected_rows"],
                             "delivery_errors": len(result["delivery_errors"])},
                   attempt=get_current_context()["ti"].try_number)
    return load_id


@task(task_id="check_records", retries=0, execution_timeout=timedelta(minutes=5))
def check_records_task(load_id):
    attempt = get_current_context()["ti"].try_number
    with connect_db() as conn:
        return check_quality(conn, load_id, "records", attempt)


@task(task_id="check_totals", retries=0, execution_timeout=timedelta(minutes=5))
def check_totals_task(load_id):
    attempt = get_current_context()["ti"].try_number
    with connect_db() as conn:
        return check_quality(conn, load_id, "totals", attempt)


@task(task_id="quality_gate", retries=0, execution_timeout=timedelta(minutes=5))
def quality_gate_task(load_id, records_result, totals_result):
    attempt = get_current_context()["ti"].try_number
    with connect_db() as conn:
        try:
            approval = quality_gate(conn, load_id, records_result, totals_result)
        except Exception as error:
            save_event(conn, load_id, "quality_gate", "FAILED", attempt=attempt,
                       message=f"Публикация заблокирована: {type(error).__name__}")
            raise AirflowFailException(f"DQ не разрешили публикацию load_id={load_id}") from error
        save_event(conn, load_id, "quality_gate", "WARN" if approval["warnings"] else "SUCCESS",
                   attempt=attempt, counters={"warnings": approval["warnings"]})
        return approval


@task(task_id="publish_data", retries=0, execution_timeout=timedelta(minutes=20))
def publish_data_task(approval):
    load_id = approval["load_id"]
    attempt = get_current_context()["ti"].try_number
    final_results = []
    with connect_db() as conn:
        try:
            with conn.transaction():
                row = conn.execute(
                    "SELECT batch_id, source_path, status, manifest_data FROM etl.load_runs WHERE load_id=%s FOR UPDATE",
                    (load_id,),
                ).fetchone()
                if row is None:
                    raise AirflowFailException("load_id не найден")
                batch_id, source_path, status, saved_manifest = row
                if status == "published":
                    return {"load_id": load_id}
                manifest = read_manifest(Path(source_path) / "manifest.json")
                if manifest["manifest_errors"] or manifest["manifest_data"] != saved_manifest:
                    raise AirflowFailException("Манифест некорректен или изменился")

                watermark = check_period(conn, manifest, batch_id, lock=True)
                verify_publication_approval(conn, load_id, approval)
                prepared = validate_staging(conn, load_id, manifest)["prepared"]
                core_stats = apply_core(conn, prepared)
                mart_stats = refresh_marts(conn, manifest["manifest_until_exclusive"], final_results)
                check_published_model(conn, mart_stats, core_stats, manifest["counts"], prepared, final_results)
                save_final_checks(conn, load_id, attempt, final_results)
                upper = manifest["manifest_until_exclusive"]
                if watermark is None or upper > watermark:
                    conn.execute(
                        """
                        UPDATE etl.pipeline_state
                        SET last_success_until=%s, last_load_id=%s, last_batch_id=%s, updated_at=clock_timestamp()
                        WHERE pipeline_name=%s
                        """,
                        (upper, load_id, batch_id, "subs_service"),
                    )
                counts = conn.execute(
                    "SELECT counts FROM etl.load_runs WHERE load_id=%s", (load_id,)
                ).fetchone()[0]
                conn.execute(
                    """
                    UPDATE etl.load_runs
                    SET status='published', counts=%s, finished_at=clock_timestamp(), last_error=NULL
                    WHERE load_id=%s
                    """,
                    (Jsonb({"staging": counts.get("staging", counts),
                            "core": core_stats, "mart": mart_stats}), load_id),
                )
                save_event(conn, load_id, "publish", "SUCCESS", attempt=attempt)

        except Exception as error:
            reason = f"publish: {type(error).__name__}"
            if isinstance(error, (ValueError, AirflowFailException)):
                reason += f": {str(error)[:500]}"
            with conn.transaction():
                if final_results:
                    save_final_checks(conn, load_id, attempt, final_results)
                conn.execute(
                    """
                    UPDATE etl.load_runs SET status='failed', finished_at=clock_timestamp(),last_error=%s
                    WHERE load_id=%s AND status<>'published'
                    """,
                    (reason, load_id),
                )
            save_event(conn, load_id, "publish", "FAILED", message=reason, attempt=attempt)
            raise
    return {"load_id": load_id}
    

@dag(
    dag_id="subs_service_etl",
    start_date=DAG_START,
    end_date=pendulum.datetime(2026, 3, 1, tz="UTC"),
    schedule=CronDataIntervalTimetable("0 0 1 * *", timezone="UTC"),
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    is_paused_upon_creation=True,
    tags=["subscriptions", "etl"],
)
def subs_service_etl_dag():
    received = receive_sources_task()
    staged = stage_sources_task(received)
    prepared = prepare_data_task(staged)
    records_result = check_records_task(prepared)
    totals_result = check_totals_task(prepared)
    approved = quality_gate_task(prepared, records_result, totals_result)
    publish_data_task(approved)


subs_service_etl_dag()