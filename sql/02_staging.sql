create table if not exists stg.source_records (
    stg_id        bigint generated always as identity primary key,
    load_id       uuid not null,
    batch_id      text not null,
    source_type   text not null,
    source_name   text not null,
    record_number integer not null,
    page_number   integer,
    raw_payload   jsonb,
    raw_text      text,
    received_at   timestamptz not null default current_timestamp,

    constraint uq_stg_source_record unique (load_id, source_type, record_number),
    constraint ck_stg_source_type check (
        source_type in ('customers', 'plans', 'subscriptions', 'invoices', 'payments', 'refunds', 'manifest')
    ),
    constraint ck_stg_record_number check (record_number > 0),
    constraint ck_stg_page_number check (page_number is null or page_number > 0),
    constraint ck_stg_payload check (raw_payload is not null or raw_text is not null),
    constraint ck_stg_batch_not_empty check (btrim(batch_id) <> ''),
    constraint ck_stg_name_not_empty check (btrim(source_name) <> '')
);

create index if not exists ix_stg_batch_source
    on stg.source_records (batch_id, source_type, load_id);
	
comment on table stg.source_records is 'Исходные данные';
comment on column stg.source_records.stg_id is 'PK, идентификатор staging-строки';
comment on column stg.source_records.load_id is 'Идентификатор загрузки';
comment on column stg.source_records.batch_id is 'Поставка';
comment on column stg.source_records.source_type is 'Тип источника';
comment on column stg.source_records.source_name is 'Исходный файл';
comment on column stg.source_records.record_number is 'Порядковый номер записи в источнике внутри загрузки';
comment on column stg.source_records.page_number is 'Номер страницы API; NULL для файлов';
comment on column stg.source_records.raw_payload is 'Исходная запись';
comment on column stg.source_records.raw_text is 'Исходный текст, если запись невозможно разобрать как JSON';
comment on column stg.source_records.received_at is 'Время получения записи';