with
plan_periods as (
    select
        plan_sk,
        plan_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by plan_id order by valid_from
        ) as next_valid_from
    from core.dim_plan
),
expected_invoice_plans as (
    select
        i.invoice_sk,
        i.invoice_id,
        i.plan_sk as actual_plan_sk,
        e.plan_id as event_plan_id,
        count(p.plan_sk) as matching_plan_count,
        bool_or(p.plan_sk = i.plan_sk) as actual_plan_matches,
        bool_or(p.currency = i.currency) as currency_matches
    from core.fact_invoice i
    left join lateral (
        select event.plan_id
        from core.fact_subscription_event event
        where event.subscription_sk = i.subscription_sk
          and event.effective_at <= i.issued_at
        order by event.effective_at desc, event.event_id desc
        limit 1
    ) as e on true
    left join core.dim_plan p
        on p.plan_id = e.plan_id
       and p.valid_from <= i.issued_at
       and (p.valid_to > i.issued_at or p.valid_to is null)
    group by i.invoice_sk, i.invoice_id, i.plan_sk, e.plan_id
),
issues as (
    select
        'plan_period_boundaries'::text as check_name,
        p.plan_id::text as business_key,
        'valid_to is not equal to the next valid_from (or NULL for the last version)'::text as details
    from plan_periods p
    where p.valid_to is distinct from p.next_valid_from
    union all
    select
        'plan_period_overlap'::text,
        a.plan_id::text,
        ('overlap between plan_sk=' || a.plan_sk || ' and plan_sk=' || b.plan_sk)::text
    from core.dim_plan a
    join core.dim_plan b
      on a.plan_id = b.plan_id
     and a.plan_sk < b.plan_sk
     and tstzrange(a.valid_from, a.valid_to, '[)')
         && tstzrange(b.valid_from, b.valid_to, '[)')
    union all
    select
        'invoice_historical_plan'::text,
        p.invoice_id::text,
        ('plan matches=' || p.matching_plan_count ||
         '; assigned plan_sk=' || p.actual_plan_sk)::text
    from expected_invoice_plans p
    where p.matching_plan_count <> 1
       or not coalesce(p.actual_plan_matches, false)
       or not coalesce(p.currency_matches, false)
    union all
    select
        'subscription_event_plan_missing'::text,
        e.event_id::text,
        ('plan_id=' || e.plan_id)::text
    from core.fact_subscription_event e
    where not exists (
        select 1 from core.dim_plan p
        where p.plan_id = e.plan_id
          and p.valid_from <= e.effective_at
          and (p.valid_to > e.effective_at or p.valid_to is null)
    )
    union all
    select
        'payment_invoice_currency'::text,
        p.payment_id::text,
        ('payment=' || p.currency || '; invoice=' || i.currency)::text
    from core.fact_payment p
    join core.fact_invoice i on i.invoice_sk = p.invoice_sk
    where p.currency <> i.currency
    union all
    select
        'refund_payment_currency_or_status'::text,
        r.refund_id::text,
        ('refund=' || r.currency || '; payment=' || p.currency ||
         '; payment_status=' || p.status)::text
    from core.fact_refund r
    join core.fact_payment p on p.payment_sk = r.payment_sk
    where r.currency <> p.currency
       or p.status <> 'succeeded'
       or r.refunded_at < p.paid_at
    union all
    select
        'refund_exceeds_payment'::text,
        p.payment_id::text,
        ('successful refunds=' || sum(r.amount) || '; payment=' || p.amount)::text
    from core.fact_payment p
    join core.fact_refund r on r.payment_sk = p.payment_sk
    where r.status = 'succeeded'
    group by p.payment_sk, p.payment_id, p.amount
    having sum(r.amount) > p.amount
    union all
    select
        'fact_date_key_mismatch'::text,
        e.event_id::text,
        'event date_key differs from UTC effective_at'::text
    from core.fact_subscription_event e
    join core.dim_date d on d.date_key = e.effective_date_key
    where d.calendar_date <> (e.effective_at at time zone 'UTC')::date
    union all
    select
        'fact_date_key_mismatch'::text,
        i.invoice_id::text,
        'invoice date_key or due date_key differs from its source date'::text
    from core.fact_invoice i
    join core.dim_date issued on issued.date_key = i.issued_date_key
    join core.dim_date due on due.date_key = i.due_date_key
    where issued.calendar_date <> (i.issued_at at time zone 'UTC')::date
       or due.calendar_date <> i.due_date
    union all
    select
        'fact_date_key_mismatch'::text,
        p.payment_id::text,
        'payment date_key differs from UTC paid_at'::text
    from core.fact_payment p
    join core.dim_date d on d.date_key = p.paid_date_key
    where d.calendar_date <> (p.paid_at at time zone 'UTC')::date
    union all
    select
        'fact_date_key_mismatch'::text,
        r.refund_id::text,
        'refund date_key differs from UTC refunded_at'::text
    from core.fact_refund r
    join core.dim_date d on d.date_key = r.refunded_date_key
    where d.calendar_date <> (r.refunded_at at time zone 'UTC')::date
    union all
    select
        'subscription_daily_customer'::text,
        (s.date_key || ':' || s.subscription_sk)::text,
        'customer_sk differs from dim_subscription'::text
    from mart.subscription_daily s
    join core.dim_subscription sub on sub.subscription_sk = s.subscription_sk
    where s.customer_sk <> sub.customer_sk
    union all
    select
        'subscription_daily_plan'::text,
        (s.date_key || ':' || s.subscription_sk)::text,
        'selected plan version is not valid for the UTC report day-end'::text
    from mart.subscription_daily s
    join core.dim_date d on d.date_key = s.date_key
    join core.dim_plan p on p.plan_sk = s.plan_sk
    where not (
        p.valid_from < ((d.calendar_date + 1)::timestamp at time zone 'UTC')
        and (
            p.valid_to is null
            or p.valid_to >= ((d.calendar_date + 1)::timestamp at time zone 'UTC')
        )
    )
    union all
    select
        'subscription_daily_state_or_plan'::text,
        (s.date_key || ':' || s.subscription_sk)::text,
        'latest subscription event does not match snapshot state, plan or start'::text
    from mart.subscription_daily s
    join core.dim_date d on d.date_key = s.date_key
    join core.dim_plan p on p.plan_sk = s.plan_sk
    left join lateral (
        select e.event_sk, e.plan_id, e.state, e.effective_at
        from core.fact_subscription_event e
        where e.subscription_sk = s.subscription_sk
          and e.effective_at < ((d.calendar_date + 1)::timestamp at time zone 'UTC')
        order by e.effective_at desc, e.event_id desc
        limit 1
    ) as last_event on true
    where last_event.event_sk is null
       or s.state <> last_event.state
       or p.plan_id <> last_event.plan_id
       or s.state_valid_from <> last_event.effective_at
    union all
    select
        'invoice_daily_status_or_overdue'::text,
        (m.date_key || ':' || i.invoice_id)::text,
        'payment_status, days_overdue or is_overdue differs from documented rules'::text
    from mart.invoice_daily m
    join core.fact_invoice i on i.invoice_sk = m.invoice_sk
    join core.dim_date d on d.date_key = m.date_key
    where m.invoice_amount <> i.amount
       or i.issued_at >= ((d.calendar_date + 1)::timestamp at time zone 'UTC')
       or m.payment_status <> case
            when m.outstanding_amount = 0 then 'paid'
            when m.net_paid_amount > 0 then 'partial'
            else 'unpaid'
         end
       or m.days_overdue <> case
            when m.outstanding_amount > 0 and d.calendar_date > i.due_date
                then d.calendar_date - i.due_date
            else 0
         end
       or m.is_overdue <> (
            m.outstanding_amount > 0 and d.calendar_date > i.due_date
         )
)
select check_name, business_key, details
from issues
order by check_name, business_key;