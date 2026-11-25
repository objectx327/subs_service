create schema if not exists etl;

-- один запуск обработки одной поставки (в том числе неудачный или повторный)
create table if not exists etl.load_runs (
    load_id           uuid primary key,
    run_id            text not null,                 -- manual__... или идентификатор запуска Airflow
    batch_id          text not null,                 -- 01_full, 02_delta, 03_delta и т. д.
    source_path       text not null,                 -- путь к каталогу поставки (без секретов)
    mode              text,                          -- full/delta; NULL, если manifest не удалось разобрать
    period_from       timestamptz,                   -- manifest.from_inclusive, NULL у full
    period_to         timestamptz,                   -- manifest.until_exclusive
    manifest_complete boolean,                       -- manifest.complete; NULL, если еще не проверен
    manifest_data     jsonb,                         -- разобранный manifest для последующей сверки
    status            text not null default 'running',
    counts            jsonb not null default '{}'::jsonb,
    last_error        text,
    started_at        timestamptz not null default clock_timestamp(),
    finished_at       timestamptz,
    staging_complete  boolean not null default false,
    constraint ck_load_run_id_not_empty check (btrim(run_id) <> ''),
    constraint ck_load_batch_not_empty check (btrim(batch_id) <> ''),
    constraint ck_load_source_not_empty check (btrim(source_path) <> ''),
    constraint ck_load_mode check (mode is null or mode in ('full', 'delta')),
    constraint ck_load_period check (
        period_from is null or period_to is null or period_from < period_to
    ),
    constraint ck_load_status check (
        status in ('running', 'staged', 'prepared', 'checked', 'published', 'blocked', 'failed')
    ),
    constraint ck_load_counts_object check (jsonb_typeof(counts) = 'object'),
    constraint ck_load_manifest_object check (
        manifest_data is null or jsonb_typeof(manifest_data) = 'object'
    ),
    constraint ck_load_finished check (finished_at is null or finished_at >= started_at)
);


-- история работы этапов: одна строка = одно событие на этапе конкретной загрузки
create table if not exists etl.load_events (
    event_id       bigint generated always as identity primary key,
    load_id        uuid not null references etl.load_runs(load_id),
    stage          text not null,                    -- receive, stage, validate, prepare_core, publish
    status         text not null,                    -- STARTED / SUCCESS / WARN / FAILED
    attempt        integer not null default 1,
    counters       jsonb not null default '{}'::jsonb,
    message        text,
    created_at     timestamptz not null default clock_timestamp(),

    constraint ck_event_stage check (btrim(stage) <> ''),
    constraint ck_event_status check (status in ('STARTED', 'SUCCESS', 'WARN', 'FAILED')),
    constraint ck_event_attempt check (attempt >= 1),
    constraint ck_event_counters check (jsonb_typeof(counters) = 'object')
);


-- причины отклонения отдельных записей, сама исходная запись остаётся в stg.source_records
-- одна строка = одна отклоненая staging-запись
create table if not exists etl.rejections (
    stg_id        bigint primary key references stg.source_records(stg_id),
    reasons       text[] not null,
    rejected_at   timestamptz not null default clock_timestamp(),

    constraint ck_rejections_reasons check (cardinality(reasons) > 0)
);


create table if not exists etl.pipeline_state (
    pipeline_name      text primary key,
    last_success_until timestamptz,
    last_load_id       uuid references etl.load_runs(load_id),
    last_batch_id      text,
    updated_at         timestamptz not null default clock_timestamp(),

    constraint ck_pipeline_name check (btrim(pipeline_name) <> ''),
    constraint ck_pipeline_watermark_pair check (
        (last_success_until is null and last_load_id is null and last_batch_id is null)
        or
        (last_success_until is not null and last_load_id is not null and last_batch_id is not null)
    )
);

--проверки dq
create table if not exists etl.dq_results (
    result_id        bigint generated always as identity primary key,
    load_id          uuid not null references etl.load_runs(load_id),
    run_id           text not null,
    batch_id         text not null,
    task_id          text not null,           -- check_records / check_totals
    attempt          integer not null,
    rule_name        text not null,
    rule_version     text not null default '1',
    source_type      text,                    -- null для проверки всей поставки
    period_from      timestamptz,             -- null для full
    period_to        timestamptz,
    severity         text not null,
    status           text not null,
    expected         jsonb,
    actual           jsonb,
    checked_rows     bigint,
    bad_rows         bigint,
    reason           text,
    created_at       timestamptz not null default clock_timestamp(),

    constraint uq_dq_rule_per_attempt unique (load_id, task_id, attempt, rule_name),
    constraint ck_dq_attempt check (attempt >= 1),
    constraint ck_dq_severity check (severity in ('CRITICAL', 'WARNING')),
    constraint ck_dq_status check (status in ('PASS', 'WARN', 'FAIL', 'ERROR', 'NOT_EVALUATED')),
    constraint ck_dq_checked_rows check (checked_rows is null or checked_rows >= 0),
    constraint ck_dq_bad_rows check (bad_rows is null or bad_rows >= 0),
    constraint ck_dq_period check (period_from is null or period_to is null or period_from < period_to),
    constraint ck_dq_rule_name check (btrim(rule_name) <> ''),
    constraint ck_dq_task_id check (btrim(task_id) <> '')
);

--допуск к публикации
create table if not exists etl.publication_approvals (
    load_id       uuid primary key references etl.load_runs(load_id),
    run_id        text not null,
    attempt       integer not null,
    dq_result_ids bigint[] not null,
    approved_at   timestamptz not null default clock_timestamp(),

    constraint ck_approval_attempt check (attempt >= 1),
    constraint ck_approval_checks check (cardinality(dq_result_ids) > 0)
);