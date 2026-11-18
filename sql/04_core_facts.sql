create table if not exists core.fact_subscription_event (
    event_sk           bigint generated always as identity primary key,
    event_id           text not null unique,
    subscription_sk    bigint not null references core.dim_subscription(subscription_sk),
    plan_id            text not null,
    state              text not null,
    effective_at       timestamptz not null,
    effective_date_key integer not null references core.dim_date(date_key),
    source_version     integer not null,
    source_updated_at  timestamptz not null,

    constraint ck_event_id check (btrim(event_id) <> ''),
    constraint ck_event_plan check (btrim(plan_id) <> ''),
    constraint ck_event_state check (state in ('active', 'paused', 'cancelled')),
    constraint ck_event_version check (source_version >= 1)
);

comment on table core.fact_subscription_event is 'События подписок';
comment on column core.fact_subscription_event.event_sk is 'PK, суррогатный ключ';
comment on column core.fact_subscription_event.event_id is 'Идентификатор события в источнике';
comment on column core.fact_subscription_event.subscription_sk is 'FK на dim_subscription';
comment on column core.fact_subscription_event.plan_id is 'Назначенный тариф (бизнес-ключ)';
comment on column core.fact_subscription_event.state is 'Состояние подписки';
comment on column core.fact_subscription_event.effective_at is 'Время начала действия состояния';
comment on column core.fact_subscription_event.effective_date_key is 'FK на dim_date';
comment on column core.fact_subscription_event.source_version is 'Версия события';
comment on column core.fact_subscription_event.source_updated_at is 'Время регистрации события';

-------------
create table if not exists core.fact_invoice (
    invoice_sk         bigint generated always as identity primary key,
    invoice_id         text not null unique,
    subscription_sk    bigint not null references core.dim_subscription(subscription_sk),
    plan_sk            bigint not null references core.dim_plan(plan_sk),
    issued_at          timestamptz not null,
    issued_date_key    integer not null references core.dim_date(date_key),
    due_date           date not null,
    due_date_key       integer not null references core.dim_date(date_key),
    amount             numeric(14, 2) not null,
    currency           varchar(3) not null,
    source_version     integer not null,
    source_updated_at  timestamptz not null,

    constraint ck_invoice_id check (btrim(invoice_id) <> ''),
    constraint ck_invoice_amount check (amount >= 0),
    constraint ck_invoice_currency check (currency in ('EUR', 'USD')),
    constraint ck_invoice_version check (source_version >= 1),
    constraint ck_invoice_due_date check (due_date >= (issued_at at time zone 'UTC')::date)
);

comment on table core.fact_invoice is '';
comment on column core.fact_invoice.invoice_sk is 'PK, суррогатный ключ';
comment on column core.fact_invoice.invoice_id is 'Идентификатор счета в источнике';
comment on column core.fact_invoice.subscription_sk is 'FK на подписку';
comment on column core.fact_invoice.plan_sk is 'FK на историческую версию тарифа';
comment on column core.fact_invoice.issued_at is 'Время выставления';
comment on column core.fact_invoice.issued_date_key is 'FK на календарь';
comment on column core.fact_invoice.due_date is 'Последний день оплаты без просрочки';
comment on column core.fact_invoice.due_date_key is 'FK на календарь';
comment on column core.fact_invoice.amount is 'Сумма начисления';
comment on column core.fact_invoice.currency is 'Валюта счета';
comment on column core.fact_invoice.source_version is 'Версия записи';
comment on column core.fact_invoice.source_updated_at is 'Время регистрации версии';

------------
create table if not exists core.fact_payment (
    payment_sk         bigint generated always as identity primary key,
    payment_id         text not null unique,
    invoice_sk         bigint not null references core.fact_invoice(invoice_sk),
    paid_at            timestamptz not null,
    paid_date_key      integer not null references core.dim_date(date_key),
    amount             numeric(14, 2) not null,
    currency           varchar(3) not null,
    status             text not null,
    source_version     integer not null,
    source_updated_at  timestamptz not null,

    constraint ck_payment_id check (btrim(payment_id) <> ''),
    constraint ck_payment_amount check (amount >= 0),
    constraint ck_payment_currency check (currency in ('EUR', 'USD')),
    constraint ck_payment_status check (status in ('succeeded', 'failed')),
    constraint ck_payment_version check (source_version >= 1)
);

comment on table core.fact_payment is 'Платежи';
comment on column core.fact_payment.payment_sk is 'PK, суррогатный ключ';
comment on column core.fact_payment.payment_id is 'Идентификатор платежа в источнике';
comment on column core.fact_payment.invoice_sk is 'FK на счет';
comment on column core.fact_payment.paid_at is 'Время операции';
comment on column core.fact_payment.paid_date_key is 'FK на календарь';
comment on column core.fact_payment.amount is 'Сумма платежа';
comment on column core.fact_payment.currency is 'Валюта';
comment on column core.fact_payment.status is 'succeeded или failed';
comment on column core.fact_payment.source_version is 'Версия записи';
comment on column core.fact_payment.source_updated_at is 'Время регистрации версии';

------------
create table if not exists core.fact_refund (
    refund_sk          bigint generated always as identity primary key,
    refund_id          text not null unique,
    payment_sk         bigint not null references core.fact_payment(payment_sk),
    refunded_at        timestamptz not null,
    refunded_date_key  integer not null references core.dim_date(date_key),
    amount             numeric(14, 2) not null,
    currency           varchar(3) not null,
    status             text not null,
    source_version     integer not null,
    source_updated_at  timestamptz not null,

    constraint ck_refund_id check (btrim(refund_id) <> ''),
    constraint ck_refund_amount check (amount >= 0),
    constraint ck_refund_currency check (currency in ('EUR', 'USD')),
    constraint ck_refund_status check (status in ('succeeded')),
    constraint ck_refund_version check (source_version >= 1)
);

comment on table core.fact_refund is 'Возвраты';
comment on column core.fact_refund.refund_sk is 'PK, суррогатный ключ';
comment on column core.fact_refund.refund_id is 'Идентификатор возврата в источнике';
comment on column core.fact_refund.payment_sk is 'FK на исходный платеж';
comment on column core.fact_refund.refunded_at is 'Время возврата';
comment on column core.fact_refund.refunded_date_key is 'FK на календарь';
comment on column core.fact_refund.amount is 'Сумма возврата';
comment on column core.fact_refund.currency is 'Валюта';
comment on column core.fact_refund.status is 'Статус возврата';
comment on column core.fact_refund.source_version is 'Версия записи';
comment on column core.fact_refund.source_updated_at is 'Время регистрации версии';