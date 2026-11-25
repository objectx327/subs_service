-- Проверочные SQL-запросы из README.md в порядке первого появления
-- Запускать нужную проверку отдельно после соответствующего этапа испытаний
-- Некоторые запросы требуют заранее созданных снимков acceptance.* (см. README)
-- При повторении испытаний нужно заменить исторические UUID на актуальные
set time zone 'UTC';

-- Q1. История и статус загрузок для диагностики и восстановления.
SELECT load_id, batch_id, status, staging_complete, manifest_complete, last_error
FROM etl.load_runs 
--WHERE batch_id = 'test_critical'
ORDER BY started_at DESC LIMIT 1;

-- Q2. Интервалы, задержка и статус регламентных запусков.
-- period_from у full равен NULL: нижняя граница манифеста, а не logical date.
SELECT
    run_id,
    batch_id,
    period_from,
    period_to,
    started_at,
    started_at - period_to AS delay,
    status
FROM etl.load_runs
WHERE run_id LIKE 'scheduled__%'
ORDER BY period_to;

-- Q3. Результаты DQ по своевременности поставок.
SELECT
    batch_id,
    rule_name,
    severity,
    status,
    expected,
    actual,
    reason
FROM etl.dq_results
WHERE rule_name = 'delivery_timeliness'
ORDER BY created_at;

-- Q4. Текущее состояние прогресса (watermark).
SELECT *
FROM etl.pipeline_state;

-- Q5. Подробные сведения и счетчики выбранной загрузки.
select load_id, run_id, batch_id, status, staging_complete, manifest_complete,
       period_from, period_to, started_at, finished_at, last_error, counts
from etl.load_runs order by started_at desc limit 1;

-- Q6. Количество бизнес-записей и снимков.
select 'customers' as dataset, count(*) from core.dim_customer
union all select 'subscriptions', count(*) from core.dim_subscription
union all select 'plans', count(*) from core.dim_plan
union all select 'events', count(*) from core.fact_subscription_event
union all select 'invoices', count(*) from core.fact_invoice
union all select 'payments', count(*) from core.fact_payment
union all select 'refunds', count(*) from core.fact_refund
union all select 'subscription_daily', count(*) from mart.subscription_daily
union all select 'invoice_daily', count(*) from mart.invoice_daily;

-- Q7. Покрытие периода каждой витриной.
select 'subscription_daily' as dataset, min(date_key), max(date_key),
       count(distinct date_key) as days from mart.subscription_daily
union all
select 'invoice_daily', min(date_key), max(date_key), count(distinct date_key)
from mart.invoice_daily;

-- Q8. Контроль финансовых сумм core по валютам (только успешные платежи и возвраты).
select 'invoices' as dataset, currency, count(*) as rows, sum(amount) as amount
from core.fact_invoice group by currency
union all
select 'payments_succeeded', currency, count(*), sum(amount)
from core.fact_payment where status = 'succeeded' group by currency
union all
select 'refunds_succeeded', currency, count(*), sum(amount)
from core.fact_refund where status = 'succeeded' group by currency
order by dataset, currency;

-- Q9. Количество исходных записей последней загрузки по типам источников.
-- JSON subscriptions хранится группами; здесь считаются строки staging, а не события.
select source_type, count(*) as staging_rows
from stg.source_records
where load_id = (select load_id from etl.load_runs order by started_at desc limit 1)
group by source_type order by source_type;

-- Q10. Результаты DQ выбранной загрузки по задачам, попыткам и правилам.
-- Для истории восстановления можно убрать reason и использовать ORDER BY attempt, task_id, rule_name.
select task_id, attempt, rule_name, severity, status, expected, actual, reason
from etl.dq_results
where load_id = (select load_id from etl.load_runs order by started_at desc limit 1)
order by task_id, attempt, rule_name;

-- Q11. История событий этапов загрузки и попыток.
select stage, attempt, status, counters, message, created_at
from etl.load_events
where load_id = (select load_id from etl.load_runs order by started_at desc limit 1)    --AND status IN ('SUCCESS', 'FAILED', 'WARN').
order by event_id;

-- Q12. Актуальные версии, суммы и статусы контрольных платежей.
-- Для проверки старой или конфликтующей версии заменить WHERE на:
-- WHERE payment_id = 'P0001-01-1'
select payment_id, source_version, source_updated_at, amount, currency, status
from core.fact_payment where payment_id in ('P0001-01-1', 'P0002-01-2')
order by payment_id;

-- Q13. Состояния подписок на последнюю отчетную дату.
select date_key, state, count(*) as subscriptions
from mart.subscription_daily
where date_key = (select max(date_key) from mart.subscription_daily)
group by date_key, state order by state;

-- Q14. SCD2: число текущих версий каждого тарифа (ожидается пустой набор нарушений).
select plan_id, count(*) filter (where valid_to is null) as current_versions
from core.dim_plan group by plan_id
having count(*) filter (where valid_to is null) <> 1;

-- Q15. История цены и границ действия контрольного тарифа.
select plan_id, monthly_price, valid_from, valid_to
from core.dim_plan where plan_id = 'basic_EUR' order by valid_from;

-- Q16. Исторические версии тарифов, связанные с контрольными счетами.
-- Полный контроль исторических связей также описан в README (06_checks.sql).
select i.invoice_id, i.issued_at, p.plan_id, p.valid_from, p.valid_to, p.monthly_price
from core.fact_invoice i join core.dim_plan p using (plan_sk)
where i.invoice_id in ('I0006-01', 'I0006-02', 'I0006-03',
                       'I0003-01', 'I0003-02', 'I0003-03')
order by i.invoice_id;

-- Q17. Количество записей снимка по наборам данных.
-- Варианты FROM:
-- FROM acceptance.current_snapshot
-- Для проверки новых снимков дополнить этот FROM одним из условий перед GROUP BY:
-- WHERE snapshot_date = 20260401
-- WHERE snapshot_date = 20260402
select dataset, count(*) as rows_count
from acceptance.before_replay
group by dataset
order by dataset;

-- Q18. Сохраненное исходное состояние watermark.
select *
from acceptance.watermark_before_replay;

-- Q19. Строки эталона, отсутствующие в текущем бизнес-состоянии (односторонняя сверка).
select *
from acceptance.before_replay
except all
select *
from acceptance.current_snapshot;

-- Q20. Двусторонняя сверка бизнес-данных с эталоном (количество расхождений).
-- Для негативных испытаний заменить acceptance.before_replay на
-- acceptance.before_control_tests в обоих местах.
-- При сравнении full/delta заменить подписи direction:
-- 'before_minus_after' -> 'incremental_minus_full'
-- 'after_minus_before' -> 'full_minus_incremental'.
with before_minus_after as (
    select *
    from acceptance.before_replay
    except all
    select *
    from acceptance.current_snapshot
),
after_minus_before as (
    select *
    from acceptance.current_snapshot
    except all
    select *
    from acceptance.before_replay
)
select 'before_minus_after' as direction,
       count(*) as differences
from before_minus_after
union all
select 'after_minus_before',
       count(*)
from after_minus_before;

-- Q21. Счетчики вставленных, обновленных и пропущенных записей core.
select key as dataset,
       value->>'inserted' as inserted,
       value->>'updated' as updated,
       value->>'skipped' as skipped
from etl.load_runs,
     jsonb_each(counts->'core')
where load_id = (
    select load_id
    from etl.load_runs
    order by started_at desc
    limit 1
)
order by dataset;

-- Q22. Совпадение watermark до и после повторной загрузки.
-- При сравнении full/delta можно заменить алиасы watermark_before, watermark_after,
-- watermark_unchanged на incremental_watermark, full_watermark, watermark_equal.
select
    b.last_success_until as watermark_before,
    p.last_success_until as watermark_after,
    b.last_success_until = p.last_success_until
        as watermark_unchanged
from acceptance.watermark_before_replay b
join etl.pipeline_state p
    using (pipeline_name);

-- Q23. Общее количество записей в контрольном снимке.
-- Для подсчета после пустой дельты: FROM acceptance.current_snapshot.
select count(*)
from acceptance.before_replay;

-- Q24. Режим, период, полнота и статус независимой full-загрузки.
select load_id, run_id, batch_id, mode,
       period_from, period_to,
       status, staging_complete, manifest_complete
from etl.load_runs
where batch_id = '04_full_reference'
order by started_at desc;

-- Q25. Статус, контрольные количества и суммы манифеста пустой или неполной поставки.
-- Для неполной поставки заменить фильтр на:
-- WHERE batch_id = 'test_incomplete'
-- Сокращенный SELECT: load_id, batch_id, status, staging_complete, manifest_complete,
-- manifest_data->'counts' AS expected_counts, last_error.
select
    load_id,
    batch_id,
    mode,
    period_from,
    period_to,
    status,
    staging_complete,
    manifest_complete,
    manifest_data->'counts' as expected_counts,
    manifest_data->'amounts_by_currency' as expected_amounts,
    counts
from etl.load_runs
where batch_id = '05_empty_delta'
order by started_at desc
limit 1;

-- Q26. Общее количество исходных записей выбранной загрузки.
-- Варианты внутри подзапроса по etl.load_runs:
-- WHERE batch_id = 'test_incomplete'
-- Для последней загрузки независимо от batch убрать условие WHERE batch_id.
select count(*) as staging_rows
from stg.source_records
where load_id = (
    select load_id
    from etl.load_runs
    where batch_id = '05_empty_delta'
    order by started_at desc
    limit 1
);

-- Q27. Результаты выбранных правил DQ для контрольного испытания.
-- Варианты (заменить выбор load_id и условия после подзапроса):
-- test_critical: WHERE batch_id = 'test_critical' внутри подзапроса;
--     AND severity = 'CRITICAL' AND status <> 'PASS'
-- test_incomplete: WHERE batch_id = 'test_incomplete' внутри подзапроса;
--     AND rule_name IN ('manifest_complete', 'staging_complete', 'manifest_counts', 'empty_input_confirmed')
--     ORDER BY rule_name
-- test_version_conflict: WHERE batch_id = 'test_version_conflict' внутри подзапроса;
--     AND rule_name = 'source_version_conflict'
-- test_warning: вместо подзапроса использовать
-- WHERE load_id = 'ab108b8a-d2b0-4399-8802-66dbef0d9383'::uuid
--     AND rule_name = 'missing_channel_share'
--     ORDER BY attempt
-- Для test_warning также добавить attempt в SELECT.
select
    rule_name,
    severity,
    status,
    expected,
    actual,
    reason
from etl.dq_results
where load_id = (
    select load_id
    from etl.load_runs
    where batch_id = '05_empty_delta'
    order by started_at desc
    limit 1
)
and rule_name = 'empty_input_confirmed';

-- Q28. Продвижение watermark после пустой дельты.
select
    b.last_success_until as watermark_before,
    p.last_success_until as watermark_after,
    p.last_success_until - b.last_success_until as advancement,
    p.last_batch_id
from acceptance.watermark_before_replay b
join etl.pipeline_state p
    using (pipeline_name);

-- Q29. Двусторонняя сверка core и исторических снимков до заданной даты.
-- Для test_stale_version в subs_reference заменить одновременно:
-- OR snapshot_date <= 20260401
-- acceptance.before_replay -> acceptance.before_control_tests (в обоих местах).
-- Эталон должен соответствовать выбранной отчетной границе.
with current_filtered as (
    select *
    from acceptance.current_snapshot
    where snapshot_date is null
       or snapshot_date <= 20260331
),
before_minus_after as (
    select *
    from acceptance.before_replay
    except all
    select *
    from current_filtered
),
after_minus_before as (
    select *
    from current_filtered
    except all
    select *
    from acceptance.before_replay
)
select 'before_minus_after' as direction,
       count(*) as differences
from before_minus_after
union all
select 'after_minus_before',
       count(*)
from after_minus_before;

-- Q30. Отклоненные исходные записи и причины отклонения.
-- Для поврежденного ответа API убрать WHERE batch_id из подзапроса,
-- чтобы выбрать последнюю загрузку, и расширить SELECT полями
-- s.source_name, s.page_number, s.raw_text (сохранив s.source_type, s.raw_payload, r.reasons).
select
    s.source_type,
    s.raw_payload,
    r.reasons
from stg.source_records s
join etl.rejections r using (stg_id)
where s.load_id = (
    select load_id
    from etl.load_runs
    where batch_id = 'test_critical'
    order by started_at desc
    limit 1
);

-- Q31. Отсутствие ошибочного платежа P-TEST в core.
select *
from core.fact_payment
where payment_id = 'P-TEST';

-- Q32. Наличие допуска к публикации и статус выбранной загрузки.
-- Для последней загрузки убрать WHERE batch_id = 'test_critical' из подзапроса.
select r.load_id, r.status,
       a.approved_at, a.dq_result_ids
from etl.load_runs r
left join etl.publication_approvals a using (load_id)
where r.load_id = (
    select load_id
    from etl.load_runs
    where batch_id = 'test_critical'
    order by started_at desc
    limit 1
);

-- Q33. Сравнение всех контрольных полей прогресса до и после испытания.
select
    b.last_success_until as watermark_before,
    p.last_success_until as watermark_after,
    b.last_success_until = p.last_success_until
        as watermark_unchanged,
    b.last_load_id = p.last_load_id
        as last_load_unchanged,
    b.last_batch_id as batch_before,
    p.last_batch_id as batch_after,
    b.updated_at = p.updated_at
        as updated_at_unchanged
from acceptance.watermark_before_control_tests b
join etl.pipeline_state p
    using (pipeline_name);

-- Q34. Сопоставление версии и суммы платежа в staging и core.
-- Для старой версии: внутри подзапроса WHERE batch_id = 'test_stale_version'.
select
    s.raw_payload->>'payment_id' as payment_id,
    s.raw_payload->>'version' as staging_version,
    s.raw_payload->>'amount' as staging_amount,
    p.source_version as core_version,
    p.amount as core_amount
from stg.source_records s
join core.fact_payment p
    on p.payment_id = s.raw_payload->>'payment_id'
where s.load_id = (
    select load_id
    from etl.load_runs
    where batch_id = 'test_version_conflict'
    order by started_at desc
    limit 1
)
and s.source_type = 'payments';

-- Q35. Исходные записи staging конкретной загрузки.
select stg_id,
       source_type,
       raw_payload
from stg.source_records
where load_id = '8ccd2fca-7f50-4c3e-a1b1-ecacd2f27ee0';

-- Q36. Наличие идентификаторов и статусов результатов DQ конкретной загрузки.
select result_id,
       task_id,
       rule_name,
       severity,
       status
from etl.dq_results
where load_id = '8ccd2fca-7f50-4c3e-a1b1-ecacd2f27ee0'
order by task_id, rule_name;

-- Q37. Количество выполненных обязательных DQ-проверок по группам.
select
    count(*) filter (
        where task_id = 'check_records'
    ) as records_checks,
    count(*) filter (
        where task_id = 'check_totals'
    ) as totals_checks,
    count(*) as total_checks
from etl.dq_results
where load_id = '8ccd2fca-7f50-4c3e-a1b1-ecacd2f27ee0';

-- Q38. Принадлежность результатов DQ другой загрузки текущему load_id.
select
    d.result_id,
    d.load_id,
    d.batch_id,
    d.task_id,
    d.rule_name,
    d.status,
    d.load_id = '8ccd2fca-7f50-4c3e-a1b1-ecacd2f27ee0'::uuid
        as belongs_to_current_load
from etl.dq_results d
where d.load_id = (
    select load_id
    from etl.load_runs
    where batch_id = '03_delta'
      and status = 'published'
    order by started_at desc
    limit 1
)
and d.task_id in ('check_records', 'check_totals')
order by d.task_id, d.rule_name;

-- Q39. Наличие записи допуска в publication_approvals для конкретного load_id.
select *
from etl.publication_approvals
where load_id = '8ccd2fca-7f50-4c3e-a1b1-ecacd2f27ee0';

-- Q40. Статус и счетчики обработки платежей при поступлении старой версии.
select batch_id,
       status,
       counts->'core'->'payments' as payment_stats
from etl.load_runs
where batch_id = 'test_stale_version'
order by started_at desc
limit 1;

-- Q41. Версия и канал привлечения контрольного клиента C0001.
select customer_id, source_version, acquisition_channel
from core.dim_customer
where customer_id = 'C0001';

-- Q42. Содержимое сохраненного снимка staging перед восстановлением.
select stg_id, source_type, raw_payload
from acceptance.recovery_staging;

-- Q43. Отсутствие изменений staging при восстановлении (двусторонняя сверка).
-- При новом испытании заменить UUID в обоих местах на load_id восстановления.
with before_minus_after as (
    select *
    from acceptance.recovery_staging
    except all
    select *
    from stg.source_records
    where load_id = 'ab108b8a-d2b0-4399-8802-66dbef0d9383'::uuid
),
after_minus_before as (
    select *
    from stg.source_records
    where load_id = 'ab108b8a-d2b0-4399-8802-66dbef0d9383'::uuid
    except all
    select *
    from acceptance.recovery_staging
)
select 'before_minus_after' as direction,
       count(*) as differences
from before_minus_after
union all
select 'after_minus_before',
       count(*)
from after_minus_before;

-- Q44. Количество запусков с одним load_id при восстановлении.
select count(*) as load_runs
from etl.load_runs
where load_id = 'ab108b8a-d2b0-4399-8802-66dbef0d9383'::uuid;

-- Q45. Количество клиентов и клиентов без канала привлечения.
select
    count(*) as total_customers,
    count(*) filter (
        where acquisition_channel is null
    ) as missing_channel
from core.dim_customer;

-- Q46. Детализация изменившихся строк core и прежних снимков после предупреждения.
with current_filtered as (
    select *
    from acceptance.current_snapshot
    where snapshot_date is null
       or snapshot_date <= 20260401
),
before_minus_after as (
    select *
    from acceptance.before_control_tests
    except all
    select *
    from current_filtered
),
after_minus_before as (
    select *
    from current_filtered
    except all
    select *
    from acceptance.before_control_tests
)
select 'before' as direction, *
from before_minus_after
union all
select 'after', *
from after_minus_before;
