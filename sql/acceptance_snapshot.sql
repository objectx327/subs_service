-- Снимок бизнес-данных для точного сравнения строк двух загрузок
set time zone 'UTC';

create schema if not exists acceptance;

create or replace view acceptance.current_snapshot as
with snapshot as (
    select 'dim_date' as dataset, d.date_key::text as business_key,
           null::integer as snapshot_date, to_jsonb(d) as payload
    from core.dim_date d
    union all
    select 'dim_customer', c.customer_id, null, to_jsonb(c) - 'customer_sk'
    from core.dim_customer c
    union all
    select 'dim_subscription', s.subscription_id, null,
           (to_jsonb(s) - 'subscription_sk' - 'customer_sk') || jsonb_build_object('customer_id', c.customer_id)
    from core.dim_subscription s join core.dim_customer c using (customer_sk)
    union all
    select 'dim_plan', p.catalog_event_id, null, to_jsonb(p) - 'plan_sk'
    from core.dim_plan p
    union all
    select 'fact_subscription_event', e.event_id, null,
           (to_jsonb(e) - 'event_sk' - 'subscription_sk') || jsonb_build_object('subscription_id', s.subscription_id)
    from core.fact_subscription_event e join core.dim_subscription s using (subscription_sk)
    union all
    select 'fact_invoice', i.invoice_id, null,
           (to_jsonb(i) - 'invoice_sk' - 'subscription_sk' - 'plan_sk') ||
           jsonb_build_object('subscription_id', s.subscription_id, 'catalog_event_id', p.catalog_event_id)
    from core.fact_invoice i join core.dim_subscription s using (subscription_sk)
    join core.dim_plan p using (plan_sk)
    union all
    select 'fact_payment', p.payment_id, null,
           (to_jsonb(p) - 'payment_sk' - 'invoice_sk') || jsonb_build_object('invoice_id', i.invoice_id)
    from core.fact_payment p join core.fact_invoice i using (invoice_sk)
    union all
    select 'fact_refund', r.refund_id, null,
           (to_jsonb(r) - 'refund_sk' - 'payment_sk') || jsonb_build_object('payment_id', p.payment_id)
    from core.fact_refund r join core.fact_payment p using (payment_sk)
    union all
    select 'subscription_daily', m.date_key::text || ':' || s.subscription_id, m.date_key,
           (to_jsonb(m) - 'subscription_sk' - 'customer_sk' - 'plan_sk') ||
           jsonb_build_object('subscription_id', s.subscription_id, 'customer_id', c.customer_id,
                              'catalog_event_id', p.catalog_event_id)
    from mart.subscription_daily m join core.dim_subscription s using (subscription_sk)
    join core.dim_customer c on c.customer_sk = m.customer_sk join core.dim_plan p using (plan_sk)
    union all
    select 'invoice_daily', m.date_key::text || ':' || i.invoice_id, m.date_key,
           (to_jsonb(m) - 'invoice_sk') || jsonb_build_object('invoice_id', i.invoice_id)
    from mart.invoice_daily m join core.fact_invoice i using (invoice_sk)
)
select dataset, business_key, snapshot_date, payload
from snapshot
order by dataset, business_key;