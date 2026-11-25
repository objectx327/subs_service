-- B1–B4: отчетная дата 31.01.2026. Для 31.03.2026 заменить 20260131 на 20260331 в WHERE.
-- B1. Активные подписки по тарифам.
select p.plan_id, p.tier, p.currency, count(*) as active_subscriptions
from mart.subscription_daily m
join core.dim_plan p on p.plan_sk = m.plan_sk
where m.date_key = 20260131 and m.is_active
group by p.plan_id, p.tier, p.currency
order by p.plan_id;

-- B2. Количество счетов по статусам, отдельно по валютам.
select i.currency, m.payment_status, count(*) as invoices,
       sum(m.outstanding_amount) as outstanding
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where m.date_key = 20260131
group by i.currency, m.payment_status
order by i.currency, m.payment_status;

-- B3. Начисления, зачтенные оплаты, возвраты и задолженность на дату.
select i.currency, sum(m.invoice_amount) as invoiced,
       sum(m.paid_amount) as paid, sum(m.refund_amount) as refunded,
       sum(m.net_paid_amount) as net_paid,
       sum(m.outstanding_amount) as outstanding,
       sum(m.overpayment_amount) as overpayment
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where m.date_key = 20260131
group by i.currency order by i.currency;

-- B4. Распределение непогашенного долга по срокам просрочки.
select i.currency,
       case when m.days_overdue = 0 then '0: не просрочен'
            when m.days_overdue <= 30 then '1: 1–30 дней'
            when m.days_overdue <= 60 then '2: 31–60 дней'
            else '3: более 60 дней' end as overdue_bucket,
       count(*) as invoices, sum(m.outstanding_amount) as outstanding
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where m.date_key = 20260131 and m.outstanding_amount > 0
group by i.currency, overdue_bucket
order by i.currency, overdue_bucket;


-- B5. Изменения тарифа и статуса конкретных подписок.
select m.date_key, s.subscription_id, p.plan_id, p.monthly_price, m.state
from mart.subscription_daily m
join core.dim_subscription s using (subscription_sk)
join core.dim_plan p using (plan_sk)
where s.subscription_id in ('S0006', 'S0010', 'S0008')
  and m.date_key in (20260131, 20260201, 20260209, 20260210,
                     20260218, 20260219, 20260314, 20260315)
order by s.subscription_id, m.date_key;

-- B6. Исторические снимки: выполнить до и после каждой дельты.
select i.invoice_id, i.currency, m.date_key, m.invoice_amount,
       m.paid_amount, m.refund_amount, m.outstanding_amount,
       m.payment_status, m.days_overdue
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where i.invoice_id in ('I0001-01', 'I0002-01', 'I0003-01', 'I0015-01')
  and m.date_key in (20260102, 20260103, 20260111, 20260112,
                     20260131, 20260207, 20260208, 20260228, 20260331)
order by i.invoice_id, m.date_key;

-- B7. Соседние даты: суммы начислений, оплат, возвратов и долга.
select
    m.date_key,
    i.currency,
    sum(m.invoice_amount) as invoiced,
    sum(m.paid_amount) as paid,
    sum(m.refund_amount) as refunded,
    sum(m.outstanding_amount) as outstanding
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where m.date_key in (20260331, 20260401)
group by m.date_key, i.currency
order by i.currency, m.date_key;

-- B8. Соседние даты: состояние подписок.
select date_key, state, count(*) as subscriptions
from mart.subscription_daily
where date_key in (20260331, 20260401)
group by date_key, state
order by date_key, state;

-- B9. Соседние даты: дни просрочки I0001-01.
select
    i.invoice_id,
    i.due_date,
    m.date_key,
    m.outstanding_amount,
    m.days_overdue,
    m.is_overdue
from mart.invoice_daily m
join core.fact_invoice i using (invoice_sk)
where i.invoice_id = 'I0001-01'
  and m.date_key in (20260331, 20260401)
order by m.date_key;
