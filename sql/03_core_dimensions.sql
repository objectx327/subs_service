create table if not exists core.dim_customer (
    customer_sk        bigint generated always as identity primary key,
    customer_id        text not null unique,
    segment            text not null,
    acquisition_channel text,
    source_version     integer not null,
    source_updated_at  timestamptz not null,

    constraint ck_customer_version check (source_version >= 1),
    constraint ck_customer_id check (btrim(customer_id) <> ''),
    constraint ck_customer_segment check (btrim(segment) <> '')
);

comment on table core.dim_customer is 'Клиенты';
comment on column core.dim_customer.customer_sk is 'PK, суррогатный ключ';
comment on column core.dim_customer.customer_id is 'Идентификатор клиента в источнике';
comment on column core.dim_customer.segment is 'Сегмент клиента';
comment on column core.dim_customer.acquisition_channel is 'Канал привлечения';
comment on column core.dim_customer.source_version is 'Версия записи источника';
comment on column core.dim_customer.source_updated_at is '	Время изменения у источника';

-----------
create table if not exists core.dim_subscription (
    subscription_sk bigint generated always as identity primary key,
    subscription_id text not null unique,
    customer_sk     bigint not null references core.dim_customer(customer_sk),

    constraint ck_subscription_id check (btrim(subscription_id) <> '')
);

comment on table core.dim_subscription is 'Подписки';
comment on column core.dim_subscription.subscription_sk is 'PK, суррогатный ключ';
comment on column core.dim_subscription.subscription_id is 'Идентификатор подписки в источнике';
comment on column core.dim_subscription.customer_sk is 'FK на dim_customer';

-----------
create table if not exists core.dim_plan (
    plan_sk           bigint generated always as identity primary key,
    catalog_event_id  text not null unique,
    plan_id           text not null,
    tier              text not null,
    currency          varchar(3) not null,
    monthly_price     numeric(14, 2) not null,
    valid_from        timestamptz not null,
    valid_to          timestamptz,
    source_version    integer not null,
    source_updated_at timestamptz not null,

    constraint uq_plan_version_start unique (plan_id, valid_from),
    constraint ck_plan_id check (btrim(plan_id) <> ''),
    constraint ck_catalog_event_id check (btrim(catalog_event_id) <> ''),
    constraint ck_plan_tier check (tier in ('basic', 'plus', 'team')),
    constraint ck_plan_currency check (currency in ('EUR', 'USD')),
    constraint ck_plan_price check (monthly_price >= 0),
    constraint ck_plan_period check (valid_to is null or valid_to > valid_from),
    constraint ck_plan_version check (source_version >= 1)
);

comment on table core.dim_plan is 'Тарифы';
comment on column core.dim_plan.plan_sk is 'PK, суррогатный ключ';
comment on column core.dim_plan.catalog_event_id is 'Идентификатор записи каталога в источнике';
comment on column core.dim_plan.plan_id is 'Устойчивый бизнес-ключ тарифа';
comment on column core.dim_plan.tier is 'basic, plus, team';
comment on column core.dim_plan.currency is 'Валюта, EUR или USD';
comment on column core.dim_plan.monthly_price is 'Месячная цена';
comment on column core.dim_plan.valid_from is 'Начало действия версии включительно';
comment on column core.dim_plan.valid_to is 'Конец действия исключительно; NULL для последней версии';
comment on column core.dim_plan.source_version is 'Версия записи каталога';
comment on column core.dim_plan.source_updated_at is 'Время регистрации версии у источника';

------------
create table if not exists core.dim_date (
    date_key      integer primary key,
    calendar_date date not null unique,
    year          smallint not null,
    quarter       smallint not null,
    month         smallint not null,
    day_of_month  smallint not null,
    day_of_week   smallint not null,
    iso_week      smallint not null,

    constraint ck_date_quarter check (quarter between 1 and 4),
    constraint ck_date_month check (month between 1 and 12),
    constraint ck_date_day check (day_of_month between 1 and 31),
    constraint ck_date_dow check (day_of_week between 1 and 7),
    constraint ck_date_week check (iso_week between 1 and 53)
);

comment on table core.dim_date is 'Календарь';
comment on column core.dim_date.date_key is 'PK в формате YYYYMMDD';
comment on column core.dim_date.calendar_date is 'Календарная дата';
comment on column core.dim_date.year is 'Год';
comment on column core.dim_date.quarter is 'Квартал (1–4)';
comment on column core.dim_date.month is 'Месяц (1–12)';
comment on column core.dim_date.day_of_month is 'День месяца';
comment on column core.dim_date.day_of_week is 'День недели (1 = понедельник)';
comment on column core.dim_date.iso_week is 'Номер недели ISO';

--Генерация календаря - пока с 1 декабря 2025 года по 31 декабря 2026 года
insert into core.dim_date (
    date_key, calendar_date, year, quarter, month,
    day_of_month, day_of_week, iso_week
)
select
    to_char(d.calendar_date, 'yyyymmdd')::integer,
    d.calendar_date,
    extract(year from d.calendar_date)::smallint,
    extract(quarter from d.calendar_date)::smallint,
    extract(month from d.calendar_date)::smallint,
    extract(day from d.calendar_date)::smallint,
    extract(isodow from d.calendar_date)::smallint,
    extract(week from d.calendar_date)::smallint
from (
    select date '2025-12-01' + n as calendar_date
    from generate_series(0, date '2026-12-31' - date '2025-12-01') as g(n)
) as d
on conflict (calendar_date) do nothing;
