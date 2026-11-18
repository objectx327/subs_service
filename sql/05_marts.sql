create table if not exists mart.subscription_daily (
    date_key                 integer not null references core.dim_date(date_key),
    subscription_sk          bigint not null references core.dim_subscription(subscription_sk),
    customer_sk              bigint not null references core.dim_customer(customer_sk),
    plan_sk                  bigint not null references core.dim_plan(plan_sk),
    state                    text not null,
    state_valid_from         timestamptz not null,
    state_valid_to           timestamptz,
    subscription_started_at  timestamptz not null,
    subscription_ended_at    timestamptz,
    is_active                boolean not null,

    constraint pk_subscription_daily primary key (date_key, subscription_sk),
    constraint ck_sub_daily_state check (state in ('active', 'paused', 'cancelled')),
    constraint ck_sub_daily_active check (is_active = (state = 'active')),
    constraint ck_sub_daily_period check (
        state_valid_to is null or state_valid_to > state_valid_from
    ),
    constraint ck_sub_daily_started check (subscription_started_at <= state_valid_from),
    constraint ck_sub_daily_ended check (
        subscription_ended_at is null or subscription_ended_at >= subscription_started_at
    )
);

comment on table mart.subscription_daily is 'Состояние подписок на дату';
comment on column mart.subscription_daily.date_key is 'FK на dim_date, отчетная дата';
comment on column mart.subscription_daily.subscription_sk is 'FK на dim_subscription';
comment on column mart.subscription_daily.customer_sk is 'FK на dim_customer';
comment on column mart.subscription_daily.plan_sk is 'FK на версию dim_plan, действовавшую на дату';
comment on column mart.subscription_daily.state is 'Состояние на конец дня';
comment on column mart.subscription_daily.state_valid_from is 'Начало действия текущего на дату состояния';
comment on column mart.subscription_daily.state_valid_to is 'Конец действия состояния; NULL, если следующего события нет';
comment on column mart.subscription_daily.subscription_started_at is 'Первое начало действия подписки';
comment on column mart.subscription_daily.subscription_ended_at is 'Время окончательной отмены, если она произошла к отчетной дате';
comment on column mart.subscription_daily.is_active is 'Признак активной подписки на дату';

-----------
create table if not exists mart.invoice_daily (
    date_key            integer not null references core.dim_date(date_key),
    invoice_sk          bigint not null references core.fact_invoice(invoice_sk),
    invoice_amount      numeric(14, 2) not null,
    paid_amount         numeric(14, 2) not null,
    refund_amount       numeric(14, 2) not null,
    net_paid_amount     numeric(14, 2) not null,
    balance_amount      numeric(14, 2) not null,
    outstanding_amount  numeric(14, 2) not null,
    overpayment_amount  numeric(14, 2) not null,
    payment_status      text not null,
    days_overdue        integer not null,
    is_overdue          boolean not null,

    constraint pk_invoice_daily primary key (date_key, invoice_sk),
    constraint ck_inv_daily_nonnegative check (
        invoice_amount >= 0
        and paid_amount >= 0
        and refund_amount >= 0
        and outstanding_amount >= 0
        and overpayment_amount >= 0
    ),
    constraint ck_inv_daily_net check (net_paid_amount = paid_amount - refund_amount),
    constraint ck_inv_daily_balance check (balance_amount = invoice_amount - net_paid_amount),
    constraint ck_inv_daily_outstanding check (outstanding_amount = greatest(balance_amount, 0)),
    constraint ck_inv_daily_overpayment check (overpayment_amount = greatest(-balance_amount, 0)),
    constraint ck_inv_daily_status check (payment_status in ('paid', 'partial', 'unpaid')),
    constraint ck_inv_daily_overdue check (days_overdue >= 0)
);

comment on table mart.invoice_daily is 'Расчеты и задолженносьт на дату';
comment on column mart.invoice_daily.date_key is 'FK на dim_date, отчетная дата';
comment on column mart.invoice_daily.invoice_sk is 'FK на fact_invoice';
comment on column mart.invoice_daily.invoice_amount is 'Исходное начисление';
comment on column mart.invoice_daily.paid_amount is 'Успешные оплаты на дату';
comment on column mart.invoice_daily.refund_amount is 'Успешные возвраты на дату';
comment on column mart.invoice_daily.net_paid_amount is 'Оплаты минус возвраты';
comment on column mart.invoice_daily.balance_amount is 'Сальдо счета со знаком';
comment on column mart.invoice_daily.outstanding_amount is 'Неоплаченный остаток, не меньше нуля';
comment on column mart.invoice_daily.overpayment_amount is 'Переплата, не меньше нуля';
comment on column mart.invoice_daily.payment_status is 'Статус платежа: paid, partial, unpaid';
comment on column mart.invoice_daily.days_overdue is 'Число дней просрочки';
comment on column mart.invoice_daily.is_overdue is 'Есть ли просроченный остаток';