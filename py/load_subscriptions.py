import argparse
import csv
import json
import logging
import os
import requests
import psycopg
from psycopg.types.json import Jsonb
from psycopg import sql
from uuid import UUID, uuid4
from dotenv import load_dotenv
import time
import sys
from datetime import datetime, timezone, date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from urllib.parse import urljoin

#########
#Параметры
#########
base_dir = Path(__file__).resolve().parent.parent
source_data_dir = base_dir / "subscription_source" / "data"
log_dir = base_dir / "logs"
logger = logging.getLogger(__name__)

CSV_COLUMNS = {
    "customers": ("customer_id", "segment", "acquisition_channel", "version", "updated_at"),
    "plans": ("catalog_event_id", "plan_id", "effective_from", "tier", "currency",
              "monthly_price", "version", "updated_at"),
    "invoices": ("invoice_id", "subscription_id", "issued_at", "due_date", "amount",
                 "currency", "version", "updated_at"),
    "refunds": ("refund_id", "payment_id", "refunded_at", "amount", "currency",
                "status", "version", "updated_at"),
}

SOURCE_NAMES = ("customers", "plans", "events", "invoices", "payments", "refunds")

MONEY_SOURCES = ("invoices", "payments", "refunds")

#правила dq
RULE_VERSION = "1"

ROW_SQL_RULES = {
    "record_validation": ("CRITICAL", "all", """
        WITH counts AS (
            SELECT
                (SELECT COUNT(*) FROM stg.source_records WHERE load_id=%(load_id)s) AS total,
                (SELECT COUNT(*) FROM etl.rejections r
                 JOIN stg.source_records s ON s.stg_id=r.stg_id
                 WHERE s.load_id=%(load_id)s) AS bad
        )
        SELECT CASE WHEN bad=0 THEN 'PASS' ELSE 'FAIL' END AS status,
               jsonb_build_object('rejected_rows',0) AS expected,
               jsonb_build_object('rejected_rows',bad) AS actual,
               total AS checked_rows, bad AS bad_rows,
               'Первичная проверка типов, полей, дат и допустимых значений Python' AS reason
        FROM counts
    """),
    "business_keys_unique": ("CRITICAL", "all", """
        WITH keys AS (
            SELECT source_type, CASE source_type
                WHEN 'customers' THEN raw_payload->>'customer_id'
                WHEN 'plans' THEN raw_payload->>'catalog_event_id'
                WHEN 'invoices' THEN raw_payload->>'invoice_id'
                WHEN 'payments' THEN raw_payload->>'payment_id'
                WHEN 'refunds' THEN raw_payload->>'refund_id'
            END AS business_key
            FROM stg.source_records
            WHERE load_id=%(load_id)s AND source_type <> 'subscriptions'
            UNION ALL
            SELECT 'subscriptions', raw_payload->>'subscription_id'
            FROM stg.source_records
            WHERE load_id = %(load_id)s
            AND source_type = 'subscriptions'
            UNION ALL
            SELECT 'events', event->>'event_id'
            FROM stg.source_records s
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(s.raw_payload->'events')='array'
                     THEN s.raw_payload->'events' ELSE '[]'::jsonb END
            ) AS event
            WHERE s.load_id=%(load_id)s AND s.source_type='subscriptions'
        ), duplicates AS (
            SELECT source_type, business_key, COUNT(*) AS n
            FROM keys WHERE business_key IS NOT NULL
            GROUP BY source_type, business_key HAVING COUNT(*)>1
        )
        SELECT CASE WHEN COALESCE(SUM(n-1),0)=0 THEN 'PASS' ELSE 'FAIL' END AS status,
               jsonb_build_object('duplicate_records',0) AS expected,
               jsonb_build_object('duplicate_records',COALESCE(SUM(n-1),0),
                                  'duplicate_keys',COUNT(*)) AS actual,
               (SELECT COUNT(*) FROM keys) AS checked_rows,
               COALESCE(SUM(n-1),0)::bigint AS bad_rows,
               'Бизнес-ключи не должны повторяться внутри одной поставки' AS reason
        FROM duplicates
    """),
    "reference_integrity": ("CRITICAL", "all", """
        WITH missing AS (
            SELECT s.stg_id FROM stg.source_records s
            WHERE s.load_id=%(load_id)s AND s.source_type='subscriptions'
              AND NOT EXISTS (SELECT 1 FROM core.dim_customer c
                              WHERE c.customer_id=s.raw_payload->>'customer_id')
              AND NOT EXISTS (SELECT 1 FROM stg.source_records c
                              WHERE c.load_id=%(load_id)s AND c.source_type='customers'
                                AND c.raw_payload->>'customer_id'=s.raw_payload->>'customer_id')
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            WHERE s.load_id=%(load_id)s AND s.source_type='invoices'
              AND NOT EXISTS (SELECT 1 FROM core.dim_subscription c
                              WHERE c.subscription_id=s.raw_payload->>'subscription_id')
              AND NOT EXISTS (SELECT 1 FROM stg.source_records c
                              WHERE c.load_id=%(load_id)s AND c.source_type='subscriptions'
                                AND c.raw_payload->>'subscription_id'=s.raw_payload->>'subscription_id')
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            WHERE s.load_id=%(load_id)s AND s.source_type='payments'
              AND NOT EXISTS (SELECT 1 FROM core.fact_invoice c
                              WHERE c.invoice_id=s.raw_payload->>'invoice_id')
              AND NOT EXISTS (SELECT 1 FROM stg.source_records c
                              WHERE c.load_id=%(load_id)s AND c.source_type='invoices'
                                AND c.raw_payload->>'invoice_id'=s.raw_payload->>'invoice_id')
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            WHERE s.load_id=%(load_id)s AND s.source_type='refunds'
              AND NOT EXISTS (SELECT 1 FROM core.fact_payment c
                              WHERE c.payment_id=s.raw_payload->>'payment_id')
              AND NOT EXISTS (SELECT 1 FROM stg.source_records c
                              WHERE c.load_id=%(load_id)s AND c.source_type='payments'
                                AND c.raw_payload->>'payment_id'=s.raw_payload->>'payment_id')
        )
        SELECT CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END AS status,
               jsonb_build_object('missing_links',0) AS expected,
               jsonb_build_object('missing_links',COUNT(*)) AS actual,
               (SELECT COUNT(*) FROM stg.source_records WHERE load_id=%(load_id)s) AS checked_rows,
               COUNT(*) AS bad_rows,
               'Связи подписка-клиент, счет-подписка, платеж-счет, возврат-платеж' AS reason
        FROM missing
    """),
    "event_plan_exists": ("CRITICAL", "events", """
        WITH events AS (
            SELECT event->>'event_id' AS event_id, event->>'plan_id' AS plan_id
            FROM stg.source_records s
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(s.raw_payload->'events')='array'
                     THEN s.raw_payload->'events' ELSE '[]'::jsonb END
            ) AS event
            WHERE s.load_id=%(load_id)s AND s.source_type='subscriptions'
        ), missing AS (
            SELECT event_id FROM events e
            WHERE NOT EXISTS (SELECT 1 FROM core.dim_plan p WHERE p.plan_id=e.plan_id)
              AND NOT EXISTS (SELECT 1 FROM stg.source_records p
                              WHERE p.load_id=%(load_id)s AND p.source_type='plans'
                                AND p.raw_payload->>'plan_id'=e.plan_id)
        )
        SELECT CASE WHEN (SELECT COUNT(*) FROM missing)=0 THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('missing_plans',0),
               jsonb_build_object('missing_plans',(SELECT COUNT(*) FROM missing)),
               (SELECT COUNT(*) FROM events), (SELECT COUNT(*) FROM missing),
               'Версия тарифа по времени будет дополнительно проверена внутри транзакции публикации'
    """),
    "source_version_conflict": ("CRITICAL", "all", """
        WITH conflict AS (
            SELECT s.stg_id FROM stg.source_records s
            JOIN core.dim_customer c ON c.customer_id=s.raw_payload->>'customer_id'
            WHERE s.load_id=%(load_id)s AND s.source_type='customers'
              AND (s.raw_payload->>'version')::int=c.source_version
              AND (c.segment IS DISTINCT FROM s.raw_payload->>'segment'
                   OR c.acquisition_channel IS DISTINCT FROM NULLIF(BTRIM(s.raw_payload->>'acquisition_channel'),''))
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            JOIN core.dim_plan p ON p.catalog_event_id=s.raw_payload->>'catalog_event_id'
            WHERE s.load_id=%(load_id)s AND s.source_type='plans'
              AND (s.raw_payload->>'version')::int=p.source_version
              AND (p.plan_id IS DISTINCT FROM s.raw_payload->>'plan_id'
                OR p.tier IS DISTINCT FROM s.raw_payload->>'tier'
                OR p.currency IS DISTINCT FROM s.raw_payload->>'currency'
                OR p.monthly_price IS DISTINCT FROM (s.raw_payload->>'monthly_price')::numeric
                OR p.valid_from IS DISTINCT FROM (s.raw_payload->>'effective_from')::timestamptz)
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            JOIN core.fact_invoice i ON i.invoice_id=s.raw_payload->>'invoice_id'
            JOIN core.dim_subscription sub ON sub.subscription_sk=i.subscription_sk
            WHERE s.load_id=%(load_id)s AND s.source_type='invoices'
              AND (s.raw_payload->>'version')::int=i.source_version
              AND (sub.subscription_id IS DISTINCT FROM s.raw_payload->>'subscription_id'
                OR i.amount IS DISTINCT FROM (s.raw_payload->>'amount')::numeric
                OR i.currency IS DISTINCT FROM s.raw_payload->>'currency'
                OR i.due_date::text IS DISTINCT FROM s.raw_payload->>'due_date'
                OR i.issued_at IS DISTINCT FROM (s.raw_payload->>'issued_at')::timestamptz)
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            JOIN core.fact_payment p ON p.payment_id=s.raw_payload->>'payment_id'
            JOIN core.fact_invoice i ON i.invoice_sk=p.invoice_sk
            WHERE s.load_id=%(load_id)s AND s.source_type='payments'
              AND (s.raw_payload->>'version')::int=p.source_version
              AND (i.invoice_id IS DISTINCT FROM s.raw_payload->>'invoice_id'
                OR p.amount IS DISTINCT FROM (s.raw_payload->>'amount')::numeric
                OR p.currency IS DISTINCT FROM s.raw_payload->>'currency'
                OR p.status IS DISTINCT FROM s.raw_payload->>'status'
                OR p.paid_at IS DISTINCT FROM (s.raw_payload->>'paid_at')::timestamptz)
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            JOIN core.fact_refund r ON r.refund_id=s.raw_payload->>'refund_id'
            JOIN core.fact_payment p ON p.payment_sk=r.payment_sk
            WHERE s.load_id=%(load_id)s AND s.source_type='refunds'
              AND (s.raw_payload->>'version')::int=r.source_version
              AND (p.payment_id IS DISTINCT FROM s.raw_payload->>'payment_id'
                OR r.amount IS DISTINCT FROM (s.raw_payload->>'amount')::numeric
                OR r.currency IS DISTINCT FROM s.raw_payload->>'currency'
                OR r.status IS DISTINCT FROM s.raw_payload->>'status'
                OR r.refunded_at IS DISTINCT FROM (s.raw_payload->>'refunded_at')::timestamptz)
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(s.raw_payload->'events')='array'
                     THEN s.raw_payload->'events' ELSE '[]'::jsonb END
            ) AS e
            JOIN core.fact_subscription_event ev ON ev.event_id=e->>'event_id'
            JOIN core.dim_subscription sub ON sub.subscription_sk=ev.subscription_sk
            WHERE s.load_id=%(load_id)s AND s.source_type='subscriptions'
              AND (e->>'version')::int=ev.source_version
              AND (sub.subscription_id IS DISTINCT FROM s.raw_payload->>'subscription_id'
                OR ev.plan_id IS DISTINCT FROM e->>'plan_id'
                OR ev.state IS DISTINCT FROM e->>'state'
                OR ev.effective_at IS DISTINCT FROM (e->>'effective_at')::timestamptz)
        )
        SELECT CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('conflicts',0), jsonb_build_object('conflicts',COUNT(*)),
               (SELECT COUNT(*) FROM stg.source_records WHERE load_id=%(load_id)s), COUNT(*),
               'Одинаковый бизнес-ключ и source_version не могут иметь разные бизнес-поля'
        FROM conflict
    """),
    "currency_links": ("CRITICAL", "payments_refunds", """
        WITH bad AS (
            SELECT s.stg_id FROM stg.source_records s
            LEFT JOIN stg.source_records i ON i.load_id=s.load_id AND i.source_type='invoices'
                 AND i.raw_payload->>'invoice_id'=s.raw_payload->>'invoice_id'
            LEFT JOIN core.fact_invoice fi ON fi.invoice_id=s.raw_payload->>'invoice_id'
            WHERE s.load_id=%(load_id)s AND s.source_type='payments'
              AND COALESCE(i.raw_payload->>'currency',fi.currency) IS NOT NULL
              AND s.raw_payload->>'currency' IS DISTINCT FROM COALESCE(i.raw_payload->>'currency',fi.currency)
            UNION ALL
            SELECT s.stg_id FROM stg.source_records s
            LEFT JOIN stg.source_records p ON p.load_id=s.load_id AND p.source_type='payments'
                 AND p.raw_payload->>'payment_id'=s.raw_payload->>'payment_id'
            LEFT JOIN core.fact_payment fp ON fp.payment_id=s.raw_payload->>'payment_id'
            WHERE s.load_id=%(load_id)s AND s.source_type='refunds'
              AND COALESCE(p.raw_payload->>'currency',fp.currency) IS NOT NULL
              AND s.raw_payload->>'currency' IS DISTINCT FROM COALESCE(p.raw_payload->>'currency',fp.currency)
        )
        SELECT CASE WHEN COUNT(*)=0 THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('currency_mismatches',0),
               jsonb_build_object('currency_mismatches',COUNT(*)),
               (SELECT COUNT(*) FROM stg.source_records WHERE load_id=%(load_id)s), COUNT(*),
               'Валюта платежа совпадает с валютой счета, возврата - с валютой платежа'
        FROM bad
    """),
}

TOTAL_SQL_RULES = {
    "manifest_complete": ("CRITICAL", None, """
        SELECT CASE WHEN manifest_complete IS TRUE THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('complete',true),
               jsonb_build_object('complete',manifest_complete),
               NULL::bigint, CASE WHEN manifest_complete IS TRUE THEN 0 ELSE 1 END::bigint,
               'Полнота подтверждается manifest.complete, не максимальной датой событий'
        FROM etl.load_runs WHERE load_id=%(load_id)s
    """),
    "staging_complete": ("CRITICAL", None, """
        WITH x AS (
            SELECT staging_complete,
                   (SELECT COUNT(*) FROM stg.source_records s WHERE s.load_id=r.load_id) AS staged
            FROM etl.load_runs r WHERE load_id=%(load_id)s
        )
        SELECT CASE WHEN staging_complete THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('staging_complete',true),
               jsonb_build_object('staging_complete',staging_complete,'staged',staged),
               staged, CASE WHEN staging_complete THEN 0 ELSE 1 END::bigint,
               'Успешный staging допускает ноль строк только при подтвержденном пустом batch'
        FROM x
    """),
    "manifest_counts": ("CRITICAL", None, """
        WITH raw AS (
            SELECT source_type, raw_payload FROM stg.source_records WHERE load_id=%(load_id)s
        ), actual AS (
            SELECT source_type AS name, COUNT(*)::bigint AS n
            FROM raw WHERE source_type <> 'subscriptions' GROUP BY source_type
            UNION ALL
            SELECT 'events', COALESCE(SUM(
                CASE WHEN jsonb_typeof(raw_payload->'events')='array'
                     THEN jsonb_array_length(raw_payload->'events') ELSE 0 END
            ),0)::bigint FROM raw WHERE source_type='subscriptions'
        ), named AS (
            SELECT name, COALESCE(SUM(n),0) AS n
            FROM actual GROUP BY name
        ), compare AS (
            SELECT sources.name,
                   COALESCE(n.n,0) AS observed,
                   (r.manifest_data->'counts'->>sources.name)::bigint AS expected
            FROM etl.load_runs r
            CROSS JOIN (VALUES ('customers'),('plans'),('events'),('invoices'),('payments'),('refunds')) sources(name)
            LEFT JOIN named n ON n.name=sources.name
            WHERE r.load_id=%(load_id)s
        ), summary AS (
            SELECT COALESCE(jsonb_object_agg(name,expected),'{}'::jsonb) AS expected,
                   COALESCE(jsonb_object_agg(name,observed),'{}'::jsonb) AS actual,
                   COUNT(*) FILTER (WHERE observed IS DISTINCT FROM expected) AS bad,
                   SUM(observed) AS total FROM compare
        )
        SELECT CASE WHEN bad=0 THEN 'PASS' ELSE 'FAIL' END,
               expected,actual,total,bad,
               'counts.events считает элементы вложенных events[], а не объекты subscriptions'
        FROM summary
    """),
    "manifest_amounts": ("CRITICAL", None, """
        WITH raw AS (
            SELECT source_type, raw_payload AS r
            FROM stg.source_records
            WHERE load_id=%(load_id)s AND source_type IN ('invoices','payments','refunds')
        ), typed AS (
            SELECT source_type, r->>'currency' AS currency,
                   CASE WHEN (r->>'amount') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                        THEN (r->>'amount')::numeric ELSE NULL END AS amount,
                   r->>'status' AS status
            FROM raw
        ), unusable AS (
            SELECT COUNT(*) AS n FROM typed
            WHERE (source_type='invoices' OR status='succeeded')
              AND (amount IS NULL OR currency NOT IN ('EUR','USD') OR currency IS NULL)
        ), sums AS (
            SELECT source_type, currency, SUM(amount)::numeric(14,2)::text AS total
            FROM typed WHERE currency IN ('EUR','USD')
              AND (source_type='invoices' OR status='succeeded')
            GROUP BY source_type,currency
        ), source_totals AS (
            SELECT sources.name,
                COALESCE(jsonb_object_agg(s.currency,s.total)
                    FILTER (WHERE s.currency IS NOT NULL),'{}'::jsonb) AS totals
            FROM (VALUES ('invoices'),('payments'),('refunds')) sources(name)
            LEFT JOIN sums s ON s.source_type=sources.name
            GROUP BY sources.name
        ), actual_json AS (
            SELECT jsonb_object_agg(name,totals) AS value FROM source_totals
        ), expected_json AS (
            SELECT jsonb_object_agg(s.name, COALESCE((
                SELECT jsonb_object_agg(c.currency, c.amount::numeric(14,2)::text)
                FROM jsonb_each_text(s.totals) AS c(currency, amount)
            ), '{}'::jsonb)) AS value
            FROM etl.load_runs r
            CROSS JOIN LATERAL jsonb_each(r.manifest_data->'amounts_by_currency') AS s(name, totals)
            WHERE r.load_id=%(load_id)s
        )
        SELECT CASE WHEN (SELECT n FROM unusable)=0
                    AND a.value=e.value THEN 'PASS' ELSE 'FAIL' END,
               e.value,a.value,
               (SELECT COUNT(*) FROM raw),
               (SELECT n FROM unusable),
               'Сверка исходных сумм успешных платежей/возвратов и всех счетов отдельно по валютам'
        FROM actual_json a CROSS JOIN expected_json e
    """),
    "period_coverage": ("CRITICAL", None, """
        WITH r AS (
            SELECT mode,period_from,period_to,manifest_data
            FROM etl.load_runs WHERE load_id=%(load_id)s
        ), updates AS (
            SELECT s.raw_payload->>'updated_at' AS stamp
            FROM stg.source_records s WHERE s.load_id=%(load_id)s AND s.source_type <> 'subscriptions'
            UNION ALL
            SELECT event->>'updated_at' FROM stg.source_records s
            CROSS JOIN LATERAL jsonb_array_elements(
                CASE WHEN jsonb_typeof(s.raw_payload->'events')='array'
                     THEN s.raw_payload->'events' ELSE '[]'::jsonb END
            ) AS event
            WHERE s.load_id=%(load_id)s AND s.source_type='subscriptions'
        ), invalid AS (
            SELECT COUNT(*) AS n FROM updates u CROSS JOIN r
            WHERE u.stamp IS NULL
               OR NOT (u.stamp ~ '^\\d{4}-\\d{2}-\\d{2}T')
               OR (CASE WHEN u.stamp ~ '^\\d{4}-\\d{2}-\\d{2}T'
                        THEN u.stamp::timestamptz END) >= r.period_to
               OR (r.period_from IS NOT NULL AND
                   (CASE WHEN u.stamp ~ '^\\d{4}-\\d{2}-\\d{2}T'
                         THEN u.stamp::timestamptz END) < r.period_from)
        )
        SELECT CASE WHEN invalid.n=0 AND r.period_to IS NOT NULL
                     AND ((r.mode='full' AND r.period_from IS NULL)
                       OR (r.mode='delta' AND r.period_from<r.period_to))
                    THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('period_from',r.period_from,'period_to',r.period_to),
               jsonb_build_object('rows_outside_interval',invalid.n),
               (SELECT COUNT(*) FROM updates), invalid.n,
               'Границы manifest по updated_at включительно/исключительно; UTC'
        FROM r CROSS JOIN invalid
    """),
    "empty_input_confirmed": ("CRITICAL", None, """
        WITH r AS (
            SELECT manifest_complete,manifest_data,staging_complete
            FROM etl.load_runs WHERE load_id=%(load_id)s
        ), x AS (
            SELECT COUNT(*) AS n FROM stg.source_records WHERE load_id=%(load_id)s
        )
        SELECT CASE WHEN x.n>0 THEN 'PASS'
                    WHEN r.manifest_complete IS TRUE AND r.staging_complete IS TRUE
                     AND (SELECT COALESCE(SUM((value)::bigint),0)
                          FROM jsonb_each_text(r.manifest_data->'counts'))=0
                     AND r.manifest_data->'amounts_by_currency'='{"invoices":{},"payments":{},"refunds":{}}'::jsonb
                    THEN 'PASS' ELSE 'FAIL' END,
               jsonb_build_object('empty_requires','complete=true, staging=true, zero counts and zero sums'),
               jsonb_build_object('raw_rows',x.n,'complete',r.manifest_complete),
               x.n, CASE WHEN x.n>0 OR (r.manifest_complete IS TRUE AND r.staging_complete IS TRUE
                         AND (SELECT COALESCE(SUM((value)::bigint),0)
                              FROM jsonb_each_text(r.manifest_data->'counts'))=0
                         AND r.manifest_data->'amounts_by_currency'='{"invoices":{},"payments":{},"refunds":{}}'::jsonb)
                    THEN 0 ELSE 1 END::bigint,
               'Пустая дельта не удаляет ранее загруженные бизнес-записи'
        FROM r CROSS JOIN x
    """),
    "delivery_timeliness": ("WARNING", None, """
        SELECT CASE WHEN started_at <= period_to + INTERVAL '24 hours' THEN 'PASS'
                    ELSE 'WARN' END,
               jsonb_build_object('max_delay_hours',24),
               jsonb_build_object('period_to',period_to,'started_at',started_at,
                                  'late',started_at > period_to + INTERVAL '24 hours'),
               NULL::bigint, NULL::bigint,
               'Для исторических учебных запусков ожидается WARN, он не запрещает backfill'
        FROM etl.load_runs WHERE load_id=%(load_id)s
    """),
    "missing_channel_share": ("WARNING", "customers", """
        WITH staged AS (
            SELECT s.raw_payload->>'customer_id' AS customer_id,
                   NULLIF(BTRIM(s.raw_payload->>'acquisition_channel'),'') AS channel
            FROM stg.source_records s
            WHERE s.load_id=%(load_id)s AND s.source_type='customers'
        ), combined AS (
            SELECT c.customer_id, c.acquisition_channel AS channel
            FROM core.dim_customer c
            WHERE NOT EXISTS (SELECT 1 FROM staged s WHERE s.customer_id=c.customer_id)
            UNION ALL
            SELECT customer_id,channel FROM staged
        ), counts AS (
            SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE channel IS NULL) AS missing
            FROM combined
        ), share AS (
            SELECT total,missing, CASE WHEN total>0 THEN missing::numeric/total
                                      ELSE 0::numeric END AS ratio FROM counts
        )
        SELECT CASE WHEN ratio=0 THEN 'PASS'
                    WHEN ratio<=0.05 THEN 'WARN' ELSE 'FAIL' END,
               jsonb_build_object('max_ratio',0.05),
               jsonb_build_object('missing',missing,'total',total,'ratio',ratio),
               total,missing,
               'Незначительное отсутствие необязательного канала - предупреждение, >5%% блокирует'
        FROM share
    """),
}


REQUIRED_TEXT_FIELDS = {
    "customers": ("customer_id", "segment"),
    "plans": ("catalog_event_id", "plan_id", "tier", "currency"),
    "subscriptions": ("subscription_id", "customer_id"),
    "events": ("event_id", "subscription_id", "customer_id", "state", "plan_id"),
    "invoices": ("invoice_id", "subscription_id", "currency"),
    "payments": ("payment_id", "invoice_id", "currency", "status"),
    "refunds": ("refund_id", "payment_id", "currency", "status"),
}

TIMESTAMP_FIELDS = {
    "plans": "effective_from",
    "events": "effective_at",
    "invoices": "issued_at",
    "payments": "paid_at",
    "refunds": "refunded_at",
}

AMOUNT_FIELDS = {
    "plans": "monthly_price",
    "invoices": "amount",
    "payments": "amount",
    "refunds": "amount",
}
######
#Настройка логирования
####

def configure_logging():
    log_dir.mkdir(parents=True, exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(
                log_dir / "load.log",
                encoding="utf-8"
            ),
            logging.StreamHandler(sys.stdout)
        ]
    )

#Проверка обязательных полей
def validate_fields(obj, fieldname, fieldtype):
    if not isinstance(obj, dict):
        return None, f"{obj}: Ожидался JSON-объект"
    
    if fieldname not in obj:
        return None, f"Отсутствует поле: {fieldname}"
    
    value = obj[fieldname]
    if value is None:
        return None, f"{fieldname}: значение отсутствует"
    
    if not isinstance(value, fieldtype): 
        return None, f"{fieldname}: ожидался тип {fieldtype.__name__}"
    
    return value, None

#Приведение данных
#Строка
def required_text(value, field): 
    if not isinstance(value, str): 
        return None, f"{field}: ожидалась строка" 
    
    if value.strip() == "": 
        return None, f"{field}: пустое значение" 
    return value, None


#Преобразование суммы в Decimal
def parse_amount(value, field, allow_zero=False, allow_negative=False): 
    value, error = required_text(value, field)
    if error:
        return None, error

    try:
        amount = Decimal(value)
    except InvalidOperation:
        return None, f"{field}: некорректное десятичное число"

    if not amount.is_finite():
        return None, f"{field}: требуется конечное число"

    if not allow_negative and (amount < 0 or (amount == 0 and not allow_zero)):
        return None, f"{field}: недопустимая сумма"
    
    if amount.as_tuple().exponent < -2:
        return amount, f"{field}: больше двух знаков после точки"
    if amount > Decimal("999999999999.99"):
        return None, f"{field}: превышен диапазон NUMERIC(14,2)"
    return amount, None


def parse_datetime(value, field):
    value, error = required_text(value, field)
    if error:
        return None, error
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None, f"{field}: некорректная дата"
    
    if result.utcoffset() is None:
        return None, f"{field}: не указано смещение UTC"
    
    return result.astimezone(timezone.utc), None

#####Читает и проверяет manifest, режим поставки, границы и ожидаемые показатели
def read_manifest(path):
    path = Path(path)
    config = {
        "delivery": None,
        "mode": None,
        "manifest_from_inclusive": None, 
        "manifest_until_exclusive": None,
        "complete": None, 
        "counts": {},
        "amounts_by_currency": {}, 
        "manifest_errors": [],
        "manifest_data": None,
    }

    errors = config["manifest_errors"]

    try:
        with path.open("r", encoding="utf-8") as file:
            manifest = json.load(file)
    except json.JSONDecodeError as error:
        errors.append(f"manifest: некорректный JSON ({error.msg})")
        return config
    except (OSError, UnicodeError):
        errors.append("manifest.json: ошибка чтения или кодировки")
        return config
    
    if not isinstance(manifest, dict):
        errors.append("manifest должен быть JSON-объектом")
        return config
    

    config["manifest_data"] = manifest

    for field in ("delivery", "mode"):
        value, error = validate_fields(manifest, field, str)
        if not error:
            value, error = required_text(value, field)
        if error:
            errors.append(error)
        else:
            config[field] = value

    if config["delivery"] is not None and config["delivery"] != path.parent.name:
        errors.append("delivery: не совпадает с именем папки поставки")

    if config["mode"] not in ("full", "delta"):
        errors.append("mode: допускается full или delta")

    if "from_inclusive" not in manifest:
        errors.append("from_inclusive: поле отсутствует")
    elif manifest["from_inclusive"] is not None:
        value, error = parse_datetime(manifest["from_inclusive"], "from_inclusive")
        if error:
            errors.append(error)
        else:
            config["manifest_from_inclusive"] = value

    value, error = parse_datetime(manifest.get("until_exclusive"), "until_exclusive")
    if error:
        errors.append(error)
    else:
        config["manifest_until_exclusive"] = value

    start = config["manifest_from_inclusive"]
    end = config["manifest_until_exclusive"]
    if config["mode"] == "full" and manifest.get("from_inclusive") is not None:
        errors.append("full: from_inclusive должен быть null")
    if config["mode"] == "delta" and start is None:
        errors.append("delta: требуется корректный from_inclusive")
    if start is not None and end is not None and start >= end:
        errors.append("from_inclusive должен быть раньше until_exclusive")

    complete = manifest.get("complete")
    if type(complete) is bool:
        config["complete"] = complete
    else:
        errors.append("complete: требуется логическое true/false")

    counts = manifest.get("counts")
    if not isinstance(counts, dict):
        errors.append("counts: ожидался объект")
    else:
        for source in SOURCE_NAMES:
            count = counts.get(source)
            if type(count) is not int or count < 0:
                errors.append(f"counts.{source}: требуется неотрицательное целое")
            else:
                config["counts"][source] = count

    amounts = manifest.get("amounts_by_currency")
    if not isinstance(amounts, dict):
        errors.append("amounts_by_currency: ожидался объект")
    else:
        for source in MONEY_SOURCES:
            currency_amounts = amounts.get(source)
            if not isinstance(currency_amounts, dict):
                errors.append(f"amounts_by_currency.{source}: ожидался объект")
                continue
            config["amounts_by_currency"][source] = {}
            for currency, raw_amount in currency_amounts.items():
                if currency not in ("EUR", "USD"):
                    errors.append(f"amounts_by_currency.{source}: неизвестная валюта {currency}")
                    continue
                amount, error = parse_amount(raw_amount, f"{source}.{currency}", allow_zero=True, allow_negative=True)
                if error:
                    errors.append(error)
                else:
                    config["amounts_by_currency"][source][currency] = str(amount)
    logger.info("event=manifest_read batch=%s mode=%s errors=%s",
                config["delivery"], config["mode"], len(errors))

    return config

#Читает четыре CSV, проверяет заголовки и структуру строк    
def read_csv_sources(batch_dir, source_error=None):
    batch_dir = Path(batch_dir)
    records = {}
    for source, expected_columns in CSV_COLUMNS.items():
        path = batch_dir / f"{source}.csv"
        try:
            with path.open("r", encoding="utf-8", newline="") as file:
                reader = csv.DictReader(file, delimiter=",", strict=True)
                header = reader.fieldnames
                if header is None or len(header) != len(set(header)) or set(header) != set(expected_columns):
                    raise ValueError(f"{path.name}: некорректный заголовок CSV")
                rows = []
                for number, row in enumerate(reader, 1):
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f"{path.name}: неверное число столбцов в записи {number}")
                    rows.append(row)
        except (ValueError, csv.Error, OSError) as error:
            if source_error is not None:
                try:
                    raw_text = path.read_text(encoding="utf-8", errors="backslashreplace")
                except OSError:
                    raw_text = str(path)
                source_error.update({
                    "source_type": source, "source_name": path.name,
                    "raw_text": raw_text, "page_number": None,
                    "reason": f"{path.name}: {error}",
                })
            raise
        records[source] = rows
        logger.info("event=csv_read source=%s rows=%s", source, len(rows))
    return records

#Читает вложенный JSON и проверяет subscriptions[]
def read_subscriptions(batch_dir, source_error=None):
    path = Path(batch_dir) / "subscriptions.json"
    try:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        if not isinstance(data, dict) or not isinstance(data.get("subscriptions"), list):
            raise ValueError("subscriptions.json: ожидается объект с массивом subscriptions")
        subscriptions = data["subscriptions"]
        event_count = 0
        for number, subscription in enumerate(subscriptions, 1):
            if not isinstance(subscription, dict) or not isinstance(subscription.get("events"), list):
                raise ValueError(f"subscriptions.json: некорректная структура подписки {number}")
            for event in subscription["events"]:
                if not isinstance(event, dict):
                    raise ValueError(f"subscriptions.json: событие в подписке {number} должно быть объектом")
            event_count += len(subscription["events"])
    except (ValueError, OSError) as error:
        if source_error is not None:
            try:
                raw_text = path.read_text(encoding="utf-8", errors="backslashreplace")
            except OSError:
                raw_text = str(path)
            source_error.update({
                "source_type": "subscriptions", "source_name": path.name,
                "raw_text": raw_text, "page_number": None,
                "reason": f"{path.name}: {error}",
            })
        raise
    logger.info("event=subscriptions_read subscriptions=%s events=%s", len(subscriptions), event_count)
    return subscriptions


#Получает все страницы API с timeout и ограниченными повторами
def fetch_payments(batch_id, api_url, limit=10, fault=None, trial=None, source_error=None):
    max_attempts = 3
    retry_statuses = (429, 500, 502, 503, 504)

    if not 1 <= limit <= 50:
        raise ValueError("limit должен быть от 1 до 50")

    params = {"batch": batch_id, "page": 1, "limit": limit}
    if fault is not None:
        params["fault"] = fault
    if trial is not None:
        params["trial"] = trial

    url = api_url
    payments = []
    expected_total = None
    visited = set()
    page_number = 0

    response = None
    try:
        while url is not None:
            if url in visited:
                raise ValueError("API: ссылка next повторяется, обнаружен цикл")
            visited.add(url)
            page_number += 1

            for attempt in range(1, 4):
                try:
                    response = requests.get(url, params=params, timeout=(5, 30))
                except (requests.Timeout, requests.ConnectionError) as error:
                    if attempt == max_attempts:
                        raise RuntimeError("API: ошибка соединения после трех попыток") from error
                    logger.warning("event=api_retry page=%s attempt=%s cause=%s", page_number, attempt, type(error).__name__)
                    time.sleep(attempt)
                    continue

                if response.status_code in retry_statuses:
                    if attempt == max_attempts:
                        response.raise_for_status()

                    pause = attempt
                    retry_after = response.headers.get("Retry-After")
                    if retry_after is not None:
                        try:
                            pause = max(pause, int(retry_after))
                        except ValueError:
                            pass

                    logger.warning("event=api_retry page=%s attempt=%s http_status=%s pause=%s", page_number, attempt, response.status_code, pause)
                    time.sleep(pause)
                    continue

                response.raise_for_status()
                try:
                    page = response.json()
                except ValueError as error:
                    raise ValueError(f"API: некорректный JSON на странице {page_number}") from error 
                break

            if not isinstance(page, dict):
                raise ValueError(f"API: страница {page_number} должна быть JSON-объектом")
            if not isinstance(page.get("items"), list):
                raise ValueError(f"API: поле items на странице {page_number} должно быть массивом")
            if type(page.get("total")) is not int or page["total"] < 0:
                raise ValueError(f"API: некорректное total на странице {page_number}")
            if "next" not in page or not (page["next"] is None or 
                                        (isinstance(page["next"], str) and page["next"].startswith("/payments?"))):
                raise ValueError(f"API: некорректная ссылка next на странице {page_number}")

            if expected_total is None:
                expected_total = page["total"]
            elif expected_total != page["total"]:
                raise ValueError("API: total изменился в ходе пагинации")

            payments.extend(page["items"])
            if len(payments) > expected_total:
                raise ValueError("API: получено больше записей, чем указано в total")

            logger.info("event=api_page page=%s rows=%s received=%s total=%s", page_number, len(page["items"]), len(payments), expected_total)
            next_page = page["next"]
            if next_page is not None and len(payments) == expected_total:
                raise ValueError("API: next указан после получения всех записей")

            url = urljoin(api_url, next_page) if next_page is not None else None 
            params = None 

        if len(payments) != expected_total:
            raise ValueError(f"API: получено {len(payments)} платежей вместо {expected_total}")
    except ValueError as error:
        if source_error is not None and response is not None:
            source_error.update({
                "source_type": "payments", "source_name": "GET /payments",
                "raw_text": response.text, "page_number": page_number,
                "reason": str(error),
            })
        raise
    
    logger.info("event=api_read_complete batch=%s pages=%s payments=%s", batch_id, page_number, len(payments))
    return payments

#Проверяет поля, типы, суммы и временные метки записи
def validate_record(source, raw):	
    if source not in REQUIRED_TEXT_FIELDS:
        raise ValueError(f"Неизвестный источник: {source}")
    if not isinstance(raw, dict):
        return None, [f"{source}: ожидался объект"]

    prepared = {}
    errors = []

    for field in REQUIRED_TEXT_FIELDS[source]:
        value, error = validate_fields(raw, field, str)
        if not error:
            value, error = required_text(value, field)
        if error:
            errors.append(error)
        else:
            prepared[field] = value

    if source == "customers":
        channel = raw.get("acquisition_channel")
        if channel is None or (isinstance(channel, str) and not channel.strip()):
            prepared["acquisition_channel"] = None
        elif isinstance(channel, str):
            prepared["acquisition_channel"] = channel
        else:
            errors.append("acquisition_channel: ожидалась строка или пустое значение")

    if source == "subscriptions":
        events = raw.get("events")
        if not isinstance(events, list):
            errors.append("events: ожидался массив")
        else:
            prepared["events"] = []
            for number, event in enumerate(events, 1):
                if not isinstance(event, dict):
                    errors.append(f"events[{number}]: ожидался объект")
                    continue
                event_raw = {**event,
                             "subscription_id": raw.get("subscription_id"),
                             "customer_id": raw.get("customer_id")}
                event_data, event_errors = validate_record("events", event_raw)
                if event_errors:
                    errors.extend(f"events[{number}]: {error}" for error in event_errors)
                else:
                    prepared["events"].append(event_data)

    else:
        version = raw.get("version")
        if source in CSV_COLUMNS and isinstance(version, str) and version.isdecimal():
            version = int(version)
        if type(version) is not int or version < 1:
            errors.append("version: требуется положительное целое число")
        else:
            prepared["version"] = version

        value, error = parse_datetime(raw.get("updated_at"), "updated_at")
        if error:
            errors.append(error)
        else:
            prepared["updated_at"] = value

    if source in TIMESTAMP_FIELDS:
        field = TIMESTAMP_FIELDS[source]
        value, error = parse_datetime(raw.get(field), field)
        if error:
            errors.append(error)
        else:
            prepared[field] = value

    if source in AMOUNT_FIELDS:
        field = AMOUNT_FIELDS[source]
        value, error = parse_amount(raw.get(field), field, allow_zero=True)
        if error:
            errors.append(error)
        else:
            prepared[field] = value

    if source == "invoices":
        value = raw.get("due_date")
        try:
            prepared["due_date"] = date.fromisoformat(value) if isinstance(value, str) and len(value) == 10 else None
        except ValueError:
            prepared["due_date"] = None
        if prepared["due_date"] is None:
            errors.append("due_date: требуется дата YYYY-MM-DD")
        elif "issued_at" in prepared and prepared["due_date"] < prepared["issued_at"].date():
            errors.append("due_date раньше даты выставления счета")

    if source == "plans" and prepared.get("tier") not in (None, "basic", "plus", "team"):
        errors.append("tier: недопустимое значение")
    if source == "events" and prepared.get("state") not in (None, "active", "paused", "cancelled"):
        errors.append("state: недопустимое значение")
    if source in ("payments", "refunds") and prepared.get("status") not in (None, "succeeded", "failed"):
        errors.append("status: недопустимое значение")
    if source in ("plans", "invoices", "payments", "refunds"):
        if prepared.get("currency") not in (None, "EUR", "USD"):
            errors.append("currency: допустимы EUR и USD")

    return (prepared if not errors else None), errors


####
#Регистрация запуска
def start_load(conn, batch_id, batch_dir):
    load_id = uuid4()
    run_id = f"manual__{uuid4().hex}"

    with conn.transaction():
        conn.execute(
            """
            INSERT INTO etl.pipeline_state (pipeline_name)
            VALUES (%s)
            ON CONFLICT (pipeline_name) DO NOTHING
            """,
            ("subs_service",)
        )
        
        conn.execute(
            """
            INSERT INTO etl.load_runs (load_id, run_id, batch_id, source_path)
            VALUES (%s, %s, %s, %s)
            """,
            (load_id, run_id, batch_id, str(batch_dir)),
        )

    logger.info("event=load_started load_id=%s batch=%s", load_id, batch_id)
    return load_id


#Записывает события этапов в etl.load_events
def save_event(conn, load_id, stage, status, counters=None, message=None, attempt=1):
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO etl.load_events (load_id, stage, status, attempt, counters, message)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (load_id, stage, status, attempt, Jsonb(counters or {}), message),
        )
    logger.info("event=load_stage load_id=%s stage=%s status=%s", load_id, stage, status)

#Сохранение исходных записей
#Ошибка чтения сохраняется отдельной транзакцией, без успешного staging
def save_source_error(conn, load_id, batch_id, source_error):
    with conn.transaction():
        stg_id = conn.execute(
            """
            INSERT INTO stg.source_records
                (load_id, batch_id, source_type, source_name, record_number, page_number, raw_text)
            VALUES (%s, %s, %s, %s, 1, %s, %s)
            RETURNING stg_id
            """,
            (load_id, batch_id, source_error["source_type"], source_error["source_name"],
             source_error["page_number"], source_error["raw_text"].replace("\x00", "\\x00")),
        ).fetchone()[0]
        conn.execute(
            "INSERT INTO etl.rejections (stg_id, reasons) VALUES (%s, %s)",
            (stg_id, [source_error["reason"].replace("\x00", "\\x00")]),
        )

def save_staging(conn, load_id, batch_id, csv_records, subscriptions, payments):
    all_records = {**csv_records, "subscriptions": subscriptions, "payments": payments}
    counts = {source: len(records) for source, records in all_records.items()}
    counts["events"] = sum(len(sub["events"]) for sub in subscriptions)

    rows = []
    for source, records in all_records.items():
        if source == "payments":
            source_name = "GET /payments"
        elif source == "subscriptions":
            source_name = "subscriptions.json"
        else:
            source_name = f"{source}.csv"

        for number, raw in enumerate(records, 1):
            rows.append((load_id, batch_id, source, source_name, number, Jsonb(raw)))

    with conn.transaction():
        run = conn.execute(
            "SELECT status FROM etl.load_runs WHERE load_id = %s FOR UPDATE",
            (load_id,),
        ).fetchone()
        if run is None or run[0] != "running":
            raise ValueError("Staging допускается только для зарегистрированной загрузки running")

        existing = conn.execute(
            "SELECT COUNT(*) FROM stg.source_records WHERE load_id = %s",
            (load_id,),
        ).fetchone()[0]
        if existing:
            raise RuntimeError("В staging уже существуют записи этого load_id")

        if rows:
            with conn.cursor() as cur:
                cur.executemany(
                    """
                    INSERT INTO stg.source_records (load_id, batch_id, source_type, source_name, record_number, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    rows,
                )

        conn.execute(
            """
            UPDATE etl.load_runs
            SET status = 'staged', staging_complete = TRUE, counts = %s
            WHERE load_id = %s
            """,
            (Jsonb(counts), load_id),
        )
    logger.info("event=staging_saved load_id=%s records=%s events=%s",
                load_id, len(rows), counts["events"])
    return counts


#Проверяет staging, сохраняет отклонения и сверяет показатели поставки
def validate_staging(conn, load_id, manifest, strict=True):
    records = conn.execute(
        """
        SELECT stg_id, source_type, raw_payload
        FROM stg.source_records WHERE load_id = %s
        ORDER BY source_type, record_number
        """, (load_id,)
    ).fetchall()

    prepared = {name: [] for name in (*SOURCE_NAMES, "subscriptions")}
    actual_counts = {name: 0 for name in SOURCE_NAMES}
    actual_amounts = {name: {} for name in MONEY_SOURCES}
    rejections = []
    errors = []
    lower = manifest["manifest_from_inclusive"]
    upper = manifest["manifest_until_exclusive"]

    for stg_id, source, raw in records:
        if source == "subscriptions":
            actual_counts["events"] += len(raw.get("events", [])) if isinstance(raw, dict) and isinstance(raw.get("events"), list) else 0
        else:
            actual_counts[source] += 1

        item, row_errors = validate_record(source, raw)
        if row_errors:
            rejections.append((stg_id, row_errors))
            continue

        items = item["events"] if source == "subscriptions" else [item]
        for record in items:
            # Для full нижняя граница отсутствует; для delta обе границы обязательны
            updated = record["updated_at"]
            if updated >= upper or (lower is not None and updated < lower):
                row_errors.append("updated_at: вне периода manifest")

        if row_errors:
            rejections.append((stg_id, row_errors))
            continue

        if source == "subscriptions":
            prepared["events"].extend(item["events"])
            prepared.setdefault("subscriptions", []).append(item)
        else:
            prepared[source].append(item)

    for _, source, raw in records:
        if source not in MONEY_SOURCES or not isinstance(raw, dict):
            continue
        if source != "invoices" and raw.get("status") != "succeeded":
            continue
        currency, amount = raw.get("currency"), raw.get("amount")
        try:
            if currency not in ("EUR", "USD"):
                raise ValueError("валюта отсутствует или неизвестна")
            value = Decimal(amount)
            if not value.is_finite():
                raise ValueError("сумма не конечная")
        except (ValueError, TypeError, InvalidOperation):
            errors.append(f"{source}: нельзя сверить контрольную сумму")
            continue
        sums = actual_amounts[source]
        sums[currency] = sums.get(currency, Decimal("0")) + value

    for name in SOURCE_NAMES:
        if actual_counts[name] != manifest["counts"].get(name):
            errors.append(f"counts.{name}: прочитано {actual_counts[name]}, ожидалось {manifest['counts'].get(name)}")
    for name in MONEY_SOURCES:
        expected = {curr: Decimal(amount) for curr, amount in manifest["amounts_by_currency"].get(name, {}).items()}
        if actual_amounts[name] != expected:
            errors.append(f"amounts_by_currency.{name}: суммы не совпадают с manifest")

    if manifest["complete"] is not True:
        errors.append("manifest.complete=false: полнота не подтверждена")

    known = {
        "customers": {r[0] for r in conn.execute("SELECT customer_id FROM core.dim_customer").fetchall()},
        "subscriptions": {r[0] for r in conn.execute("SELECT subscription_id FROM core.dim_subscription").fetchall()},
        "invoices": {r[0] for r in conn.execute("SELECT invoice_id FROM core.fact_invoice").fetchall()},
        "payments": {r[0] for r in conn.execute("SELECT payment_id FROM core.fact_payment").fetchall()},
    }
    known["customers"].update(r["customer_id"] for r in prepared["customers"])
    known["subscriptions"].update(r["subscription_id"] for r in prepared["subscriptions"])
    known["invoices"].update(r["invoice_id"] for r in prepared["invoices"])
    known["payments"].update(r["payment_id"] for r in prepared["payments"])
    rejected_by_id = {stg_id: reasons for stg_id, reasons in rejections}

    for stg_id, source, raw in records:
        if not isinstance(raw, dict):
            continue
        relation = {
            "subscriptions": ("customer_id", "customers"),
            "invoices": ("subscription_id", "subscriptions"),
            "payments": ("invoice_id", "invoices"),
            "refunds": ("payment_id", "payments"),
        }.get(source)
        if relation is None:
            continue
        field, referenced = relation
        key = raw.get(field)
        if isinstance(key, str) and key.strip() and key not in known[referenced]:
            rejected_by_id.setdefault(stg_id, []).append(f"{field}: отсутствует связанная запись ({key})")

    rejections = list(rejected_by_id.items())

    with conn.transaction():
        for stg_id, row_errors in rejections:
            conn.execute(
                """
                INSERT INTO etl.rejections (stg_id, reasons)
                VALUES (%s, %s)
                ON CONFLICT (stg_id) DO UPDATE SET reasons=EXCLUDED.reasons
                """,
                (stg_id, row_errors),
            )
        if not rejections and not errors:
            conn.execute("UPDATE etl.load_runs SET status='prepared' WHERE load_id=%s AND status<>'published'", (load_id,))

    if rejections or errors:
        message = f"Ошибочных записей: {len(rejections)}; ошибок поставки: {len(errors)}"
        logger.error("event=validation_failed load_id=%s %s", load_id, message)
        if strict:
            raise ValueError(message + ("; " + "; ".join(errors[:3]) if errors else ""))

    missing_channel = sum(row["acquisition_channel"] is None for row in prepared["customers"])
    if missing_channel:
        logger.warning("event=validation_warning load_id=%s missing_acquisition_channel=%s",
                       load_id, missing_channel)
    logger.info("event=validation_complete load_id=%s counts=%s", load_id, actual_counts)
    return {"prepared": prepared, "counts": actual_counts,
            "rejected_rows": len(rejections), "delivery_errors": errors}


def upsert_versioned(cur, table, key, values, business_fields):
    fields = list(values)
    #соберем динамически
    query = sql.SQL("SELECT {} FROM {} WHERE {}=%s FOR UPDATE").format(
        sql.SQL(", ").join(sql.Identifier(f) for f in fields),
        sql.Identifier(*table.split(".")),
        sql.Identifier(key),
    )
    cur.execute(query, (values[key],))
    existing_row = cur.fetchone()
    if existing_row is not None:
        old = dict(zip(fields, existing_row))
        if values["source_version"] < old["source_version"]:
            return "skipped"
        if values["source_version"] == old["source_version"]:
            if any(old[f] != values[f] for f in business_fields):
                raise ValueError(f"Конфликт одинаковой версии: {table}.{key}={values[key]}")
            return "skipped"
        
        updates = [f for f in fields if f != key]
        query = sql.SQL("UPDATE {} SET {} WHERE {}=%s").format(
            sql.Identifier(*table.split(".")),
            sql.SQL(", ").join(sql.SQL("{}=%s").format(sql.Identifier(f)) for f in updates),
            sql.Identifier(key),
        )
        cur.execute(query, tuple(values[f] for f in updates) + (values[key],))
        return "updated"

    query = sql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
        sql.Identifier(*table.split(".")),
        sql.SQL(", ").join(sql.Identifier(f) for f in fields),
        sql.SQL(", ").join(sql.Placeholder() for _ in fields),
    )
    cur.execute(query, tuple(values.values()))
    return "inserted"


#Применяет новые и измененые записи к core по правилам версий
def apply_core(conn, prepared):
    stats = {name: {"inserted": 0, "updated": 0, "skipped": 0} for name in SOURCE_NAMES}
    with conn.cursor() as cur:

        #клиенты
        for row in prepared["customers"]:
            values = dict(
                    customer_id=row["customer_id"], 
                    segment=row["segment"],
                    acquisition_channel=row["acquisition_channel"],
                    source_version=row["version"], 
                    source_updated_at=row["updated_at"]
                )
            result = upsert_versioned(cur, "core.dim_customer", "customer_id", values, ("segment", "acquisition_channel"))
            stats["customers"][result] += 1

        #каталог
        if prepared["plans"]:
            cur.execute(
                """
                UPDATE core.dim_plan
                SET valid_to = valid_from + INTERVAL '1 microsecond'
                """
            )
            for row in prepared["plans"]:
                values = dict(
                        catalog_event_id=row["catalog_event_id"], 
                        plan_id=row["plan_id"],
                        tier=row["tier"], 
                        currency=row["currency"],
                        monthly_price=row["monthly_price"], 
                        valid_from=row["effective_from"],
                        valid_to=row["effective_from"] + timedelta(microseconds=1),
                        source_version=row["version"], 
                        source_updated_at=row["updated_at"]
                    )
                result = upsert_versioned(cur, "core.dim_plan", "catalog_event_id", values, ("plan_id", "tier", "currency", "monthly_price", "valid_from"))
                stats["plans"][result] += 1

            cur.execute(
                """
                WITH periods AS (
                                SELECT plan_sk,
                                        LEAD(valid_from) OVER (
                                            PARTITION BY plan_id ORDER BY valid_from
                                        ) AS next_from
                                FROM core.dim_plan
                            )
                UPDATE core.dim_plan p
                SET valid_to = x.next_from
                FROM periods x WHERE p.plan_sk=x.plan_sk
                """
            )

        #подписки
        for sub in prepared["subscriptions"]:
            customer = cur.execute("SELECT customer_sk FROM core.dim_customer WHERE customer_id=%s", (sub["customer_id"],)).fetchone()
            if customer is None:
                raise ValueError(f"Подписка {sub['subscription_id']}: клиент отсутствует")
            
            previous = cur.execute("SELECT customer_sk FROM core.dim_subscription WHERE subscription_id=%s", (sub["subscription_id"],)).fetchone()
            if previous is None:
                cur.execute("INSERT INTO core.dim_subscription (subscription_id, customer_sk) VALUES (%s,%s)", (sub["subscription_id"], customer[0]))
            elif previous[0] != customer[0]:
                raise ValueError(f"Подписка {sub['subscription_id']}: противоречивый customer_id")

        #события подписок
        for row in prepared["events"]:
            sub = cur.execute("SELECT subscription_sk FROM core.dim_subscription WHERE subscription_id=%s", (row["subscription_id"],)).fetchone()
            if sub is None:
                raise ValueError(f"Событие {row['event_id']}: подписка отсутствует")
            
            if cur.execute(
                """
                SELECT 1 FROM core.dim_plan
                WHERE plan_id=%s AND valid_from<=%s
                    AND (valid_to>%s OR valid_to IS NULL) LIMIT 1
                """,
                (row["plan_id"], row["effective_at"], row["effective_at"])
            ).fetchone() is None:
                raise ValueError(f"Событие {row['event_id']}: тариф не действовал на дату события")
            
            values = dict(
                    event_id=row["event_id"], 
                    subscription_sk=sub[0], 
                    plan_id=row["plan_id"],
                    state=row["state"], 
                    effective_at=row["effective_at"],
                    effective_date_key=int(row["effective_at"].strftime("%Y%m%d")),
                    source_version=row["version"], 
                    source_updated_at=row["updated_at"]
                )
            result = upsert_versioned(cur, "core.fact_subscription_event", "event_id", values, ("subscription_sk", "plan_id", "state", "effective_at"))
            stats["events"][result] += 1

        #счета
        for row in prepared["invoices"]:
            sub = cur.execute("SELECT subscription_sk FROM core.dim_subscription WHERE subscription_id=%s",
                              (row["subscription_id"],)).fetchone()
            if sub is None:
                raise ValueError(f"Счет {row['invoice_id']}: подписка отсутствует")
            
            plan = cur.execute(
                """
                SELECT p.plan_sk, p.currency
                FROM core.fact_subscription_event e
                JOIN core.dim_plan p ON p.plan_id=e.plan_id
                     AND p.valid_from<=%s AND (p.valid_to>%s OR p.valid_to IS NULL)
                WHERE e.subscription_sk=%s AND e.effective_at<=%s
                ORDER BY e.effective_at DESC,
                    COALESCE((substring(e.event_id FROM '-([0-9]+)$'))::int, 0) DESC,
                    e.event_id DESC
                LIMIT 1
                """, (row["issued_at"], row["issued_at"], sub[0], row["issued_at"])
            ).fetchone()
            
            if plan is None or plan[1] != row["currency"]:
                raise ValueError(f"Счет {row['invoice_id']}: нет подходящего тарифа/валюты")
            
            values = dict(
                    invoice_id=row["invoice_id"], 
                    subscription_sk=sub[0], 
                    plan_sk=plan[0],
                    issued_at=row["issued_at"],
                    issued_date_key=int(row["issued_at"].strftime("%Y%m%d")),
                    due_date=row["due_date"], 
                    due_date_key=int(row["due_date"].strftime("%Y%m%d")),
                    amount=row["amount"], 
                    currency=row["currency"],
                    source_version=row["version"], 
                    source_updated_at=row["updated_at"]
                )
            result = upsert_versioned(cur, "core.fact_invoice", "invoice_id", values, ("subscription_sk", "issued_at", "due_date", "amount", "currency"))
            stats["invoices"][result] += 1

        old_invoices = cur.execute(
            """
            SELECT i.invoice_sk, i.invoice_id, i.subscription_sk, i.issued_at, i.currency, i.plan_sk
            FROM core.fact_invoice i
            """
        ).fetchall()

        for sk, invoice_id, sub_sk, issued_at, currency, old_plan_sk in old_invoices:
            current = cur.execute(
                """
                SELECT p.plan_sk, p.currency
                FROM core.fact_subscription_event e
                JOIN core.dim_plan p ON p.plan_id=e.plan_id
                     AND p.valid_from<=%s AND (p.valid_to>%s OR p.valid_to IS NULL)
                WHERE e.subscription_sk=%s AND e.effective_at<=%s
                ORDER BY e.effective_at DESC,
                    COALESCE((substring(e.event_id FROM '-([0-9]+)$'))::int, 0) DESC,
                    e.event_id DESC LIMIT 1
                """, 
                (issued_at, issued_at, sub_sk, issued_at)
            ).fetchone()
            if current is None or current[1] != currency:
                raise ValueError(f"Счет {invoice_id}: нарушена историческая привязка тарифа")
            if current[0] != old_plan_sk:
                cur.execute("UPDATE core.fact_invoice SET plan_sk=%s WHERE invoice_sk=%s", (current[0], sk))

        #платежи
        for row in prepared["payments"]:
            invoice = cur.execute("SELECT invoice_sk, currency FROM core.fact_invoice WHERE invoice_id=%s", (row["invoice_id"],)).fetchone()
            if invoice is None or invoice[1] != row["currency"]:
                raise ValueError(f"Платеж {row['payment_id']}: нет счета/валюта не совпадает")
            
            values = dict(
                    payment_id=row["payment_id"], 
                    invoice_sk=invoice[0], 
                    paid_at=row["paid_at"],
                    paid_date_key=int(row["paid_at"].strftime("%Y%m%d")),
                    amount=row["amount"], 
                    currency=row["currency"], 
                    status=row["status"],
                    source_version=row["version"], 
                    source_updated_at=row["updated_at"]
                )
            result = upsert_versioned(cur, "core.fact_payment", "payment_id", values, ("invoice_sk", "paid_at", "amount", "currency", "status"))
            stats["payments"][result] += 1
        
        #возвраты
        for row in prepared["refunds"]:
            payment = cur.execute("SELECT payment_sk, currency FROM core.fact_payment WHERE payment_id=%s", (row["payment_id"],)).fetchone()
            if payment is None or payment[1] != row["currency"]:
                raise ValueError(f"Возврат {row['refund_id']}: нет платежа/валюта не совпадает")
            
            values = dict(
                    refund_id=row["refund_id"], 
                    payment_sk=payment[0], 
                    refunded_at=row["refunded_at"],
                    refunded_date_key=int(row["refunded_at"].strftime("%Y%m%d")),
                    amount=row["amount"], 
                    currency=row["currency"], 
                    status=row["status"],
                    source_version=row["version"], 
                    source_updated_at=row["updated_at"]
                )
            result = upsert_versioned(cur, "core.fact_refund", "refund_id", values, ("payment_sk", "refunded_at", "amount", "currency", "status"))
            stats["refunds"][result] += 1

    logger.info("event=core_applied stats=%s", stats)
    return stats


#Пересчитывает затронутые даты обеих витрин
def refresh_marts(conn, until_exclusive, final_results, fail_inside_publication=False):
    row = conn.execute(
        """
        SELECT LEAST(
            (SELECT MIN((effective_at AT TIME ZONE 'UTC')::date)
            FROM core.fact_subscription_event),
            (SELECT MIN((issued_at AT TIME ZONE 'UTC')::date)
            FROM core.fact_invoice)
        )
        """
        ).fetchone()

    start = row[0]
    final_results.append(("nonempty_history", None, True, start is not None,
                          "Для построения витрин нужна история подписок или счетов"))
    if start is None:
        raise ValueError("Нельзя строить витрины: история подписок и счета отсутствуют")

    earliest, latest = conn.execute(
        """
        SELECT MIN(d.calendar_date), MAX(d.calendar_date)
        FROM core.dim_date d WHERE d.date_key IN (
            SELECT date_key FROM mart.subscription_daily
            UNION SELECT date_key FROM mart.invoice_daily)
        """
        ).fetchone()
    if earliest is not None:
        start = min(start, earliest)
    end = until_exclusive.astimezone(timezone.utc).date()
    if latest is not None:
        end = max(end, latest + timedelta(days=1))
    final_results.append(("mart_period", None, True, start < end,
                          "Начало пересчета должно быть раньше конца периода"))
    if start >= end:
        raise ValueError("Нет дат для построения витрин")

    #проверка покрытия каленаря
    actual_days = conn.execute(
        """
        SELECT count(*) FROM core.dim_date
        WHERE calendar_date >=%s AND calendar_date <%s
        """,
        (start, end)
    ).fetchone()[0]
    final_results.append(("calendar_coverage", "dim_date", (end - start).days, actual_days,
                          "Календарь покрывает весь период пересчета"))
    if actual_days != (end - start).days:
        raise ValueError(f"В dim_date нет всех дней периода {start} - {end}")

    conn.execute(
        """
        DELETE FROM mart.subscription_daily WHERE date_key IN
        (SELECT date_key FROM core.dim_date WHERE calendar_date >=%s AND calendar_date <%s)
        """, (start, end)
    )
    conn.execute(
        """
        DELETE FROM mart.invoice_daily WHERE date_key IN
        (SELECT date_key FROM core.dim_date WHERE calendar_date >=%s AND calendar_date <%s)
        """, (start, end)
    )

    if fail_inside_publication:
        raise RuntimeError("Тестовый сбой внутри публикации после DELETE витрин")

    subs_count = conn.execute(
        """
        WITH days AS (
            SELECT date_key, calendar_date,
                   (calendar_date + 1)::timestamp AT TIME ZONE 'UTC' AS day_end
            FROM core.dim_date WHERE calendar_date >=%s AND calendar_date <%s
        ), windows AS (
            SELECT e.*,
                   LEAD(e.effective_at) OVER (
                     PARTITION BY e.subscription_sk
                     ORDER BY e.effective_at,
                       COALESCE((substring(e.event_id FROM '-([0-9]+)$'))::int,0),
                       e.event_id
                   ) AS next_at,
                   MIN(e.effective_at) OVER (PARTITION BY e.subscription_sk) AS started_at
            FROM core.fact_subscription_event e
        )
        INSERT INTO mart.subscription_daily (
            date_key, subscription_sk, customer_sk, plan_sk, state,
            state_valid_from, state_valid_to, subscription_started_at,
            subscription_ended_at, is_active
        )
        SELECT d.date_key, s.subscription_sk, s.customer_sk, p.plan_sk, e.state,
               e.effective_at, e.next_at, e.started_at,
               CASE WHEN e.state='cancelled' THEN e.effective_at END,
               (e.state='active')
        FROM days d CROSS JOIN core.dim_subscription s
        JOIN LATERAL (
            SELECT * FROM windows w
            WHERE w.subscription_sk=s.subscription_sk AND w.effective_at<d.day_end
            ORDER BY w.effective_at DESC,
                COALESCE((substring(w.event_id FROM '-([0-9]+)$'))::int,0) DESC,
                w.event_id DESC
            LIMIT 1
        ) e ON TRUE
        JOIN core.dim_plan p ON p.plan_id=e.plan_id
            AND p.valid_from<d.day_end
            AND (p.valid_to IS NULL OR p.valid_to>=d.day_end)
        """, (start, end)
    ).rowcount

    invoice_count = conn.execute(
        """
        WITH days AS (
            SELECT date_key, calendar_date,
                   (calendar_date + 1)::timestamp AT TIME ZONE 'UTC' AS day_end
            FROM core.dim_date WHERE calendar_date >=%s AND calendar_date <%s
        ), invoice_days AS (
            SELECT d.date_key, d.calendar_date, d.day_end,
                   i.invoice_sk, i.amount AS invoice_amount, i.due_date,
                   COALESCE(p.paid,0) AS paid,
                   COALESCE(r.refunded,0) AS refunded
            FROM days d
            JOIN core.fact_invoice i ON i.issued_at<d.day_end
            LEFT JOIN LATERAL (
                SELECT SUM(x.amount) AS paid
                FROM core.fact_payment x
                WHERE x.invoice_sk=i.invoice_sk AND x.status='succeeded'
                      AND x.paid_at<d.day_end
            ) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT SUM(x.amount) AS refunded
                FROM core.fact_refund x
                JOIN core.fact_payment payment ON x.payment_sk=payment.payment_sk
                WHERE payment.invoice_sk=i.invoice_sk
                  AND payment.status='succeeded' AND payment.paid_at<d.day_end
                  AND x.status='succeeded' AND x.refunded_at<d.day_end
            ) r ON TRUE
        ), balance AS (
            SELECT *, paid-refunded AS net_paid,
                      invoice_amount-paid+refunded AS balance_amount
            FROM invoice_days
        ), result AS (
            SELECT *, GREATEST(balance_amount,0) AS outstanding_amount,
                      GREATEST(-balance_amount,0) AS overpayment_amount
            FROM balance
        )
        INSERT INTO mart.invoice_daily (
            date_key, invoice_sk, invoice_amount, paid_amount, refund_amount,
            net_paid_amount, balance_amount, outstanding_amount,
            overpayment_amount, payment_status, days_overdue, is_overdue
        )
        SELECT date_key, invoice_sk, invoice_amount, paid, refunded,
               net_paid, balance_amount, outstanding_amount, overpayment_amount,
               CASE WHEN outstanding_amount=0 THEN 'paid'
                    WHEN net_paid>0 THEN 'partial' ELSE 'unpaid' END,
               CASE WHEN outstanding_amount>0 AND calendar_date>due_date
                    THEN calendar_date-due_date ELSE 0 END,
               (outstanding_amount>0 AND calendar_date>due_date)
        FROM result
        """, (start, end)
    ).rowcount

    expected_subs = conn.execute(
        """
        SELECT count(*) FROM core.dim_date d
        CROSS JOIN core.dim_subscription s
        WHERE d.calendar_date >=%s AND d.calendar_date <%s
          AND EXISTS (SELECT 1 FROM core.fact_subscription_event e
                      WHERE e.subscription_sk=s.subscription_sk
                        AND e.effective_at < ((d.calendar_date+1)::timestamp AT TIME ZONE 'UTC'))
        """, (start, end)
    ).fetchone()[0]
    final_results.append(("subscription_daily_count", "subscription_daily", expected_subs, subs_count,
                          "Витрина подписок содержит все ожидаемые снимки"))
    if subs_count != expected_subs:
        raise ValueError("Витрина подписок: не найдена версия тарифа на некоторые даты")

    logger.info("event=marts_refreshed from=%s to=%s subscriptions=%s invoices=%s",
                start, end, subs_count, invoice_count)

    return {
            "from": str(start), 
            "to_exclusive": str(end),
            "subscription_daily": subs_count, 
            "invoice_daily": invoice_count
        }


def check_quality(conn, load_id, group, attempt):
    load_id = UUID(str(load_id))
    rules = ROW_SQL_RULES if group == "records" else TOTAL_SQL_RULES if group == "totals" else None
    if rules is None:
        raise ValueError("group должен быть records или totals")
    if attempt < 1:
        raise ValueError("attempt должен быть положительным")

    task_id = "check_records" if group == "records" else "check_totals"
    run = conn.execute(
        """
        SELECT run_id,batch_id,period_from,period_to,staging_complete
        FROM etl.load_runs WHERE load_id=%s
        """, (load_id,)
    ).fetchone()
    if run is None or not run[4]:
        raise ValueError("Сначала необходимо зафиксировать staging")
    run_id, batch_id, period_from, period_to, _ = run

    result_ids = []
    for rule_name, (severity, source_type, query) in rules.items():
        try:
             with conn.transaction():
                row = conn.execute(query, {"load_id": load_id}).fetchone()
                if row is None or len(row) != 6:
                    raise ValueError("SQL-правило не вернуло шесть полей")
                status, expected, actual, checked_rows, bad_rows, reason = row
                if status not in ("PASS", "WARN", "FAIL", "ERROR", "NOT_EVALUATED"):
                    raise ValueError("SQL-правило вернуло неизвестный статус")
        except Exception as error:
            status = "ERROR"
            expected = None
            actual = {
                "error_type": type(error).__name__,
                "error_message": str(error)[:1000],
            }
            checked_rows, bad_rows = None, None
            reason = f"SQL-ошибка: {type(error).__name__}: {str(error)[:1000]}"

            logger.exception(
                "event=dq_rule_error load_id=%s rule=%s",
                load_id, rule_name
            )

        with conn.transaction():
            result_id = conn.execute(
                """
                INSERT INTO etl.dq_results
                       (load_id,run_id,batch_id,task_id,attempt,rule_name,rule_version,
                        source_type,period_from,period_to,severity,status,expected,
                        actual,checked_rows,bad_rows,reason)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (load_id,task_id,attempt,rule_name)
                DO UPDATE SET severity=EXCLUDED.severity, status=EXCLUDED.status,
                       expected=EXCLUDED.expected, actual=EXCLUDED.actual,
                       checked_rows=EXCLUDED.checked_rows, bad_rows=EXCLUDED.bad_rows,
                       reason=EXCLUDED.reason, created_at=clock_timestamp()
                RETURNING result_id
                """,
                (load_id,run_id,batch_id,task_id,attempt,rule_name,RULE_VERSION,
                 source_type,period_from,period_to,severity,status,
                 Jsonb(expected) if expected is not None else None,
                 Jsonb(actual) if actual is not None else None,
                 checked_rows,bad_rows,reason),
            ).fetchone()[0]
        result_ids.append(result_id)
        logger.info("event=dq_rule load_id=%s task=%s rule=%s severity=%s status=%s",
                    load_id, task_id, rule_name, severity, status)

    return {"load_id": str(load_id), "task_id": task_id,
            "attempt": attempt, "result_ids": result_ids}


def quality_gate(conn, load_id, records_result, totals_result):
    load_id = UUID(str(load_id))
    row = conn.execute(
        """
        SELECT run_id,batch_id,period_from,period_to,staging_complete,manifest_complete
        FROM etl.load_runs WHERE load_id=%s
        """, (load_id,)
    ).fetchone()
    if row is None:
        raise ValueError("Загрузка не найдена")
    run_id, batch_id, period_from, period_to, staged, complete = row

    errors = []
    references = (records_result, totals_result)
    expected_rules = ({("check_records", name) for name in ROW_SQL_RULES}
                      | {("check_totals", name) for name in TOTAL_SQL_RULES})
    selected = []
    expected_attempts = {}
    for ref, task_name in zip(references, ("check_records", "check_totals")):
        if (ref.get("load_id") != str(load_id) or ref.get("task_id") != task_name
                or type(ref.get("attempt")) is not int or ref["attempt"] < 1):
            errors.append("Результаты относятся к другой загрузке, задаче или попытке")
        else:
            expected_attempts[task_name] = ref["attempt"]
        selected.extend(ref.get("result_ids", []))

    if len(selected) != len(expected_rules) or len(set(selected)) != len(selected):
        errors.append("Список результатов DQ неполон или содержит повторы")

    db_rows = conn.execute(
        """
        SELECT result_id,load_id,run_id,batch_id,task_id,attempt,rule_name,rule_version,period_from,period_to,severity,status
        FROM etl.dq_results WHERE result_id = ANY(%s)
        """, (selected,)
    ).fetchall() if selected else []
    if len(db_rows) != len(expected_rules):
        errors.append("Не найдены все результаты обязательных проверок")

    found = set()
    warnings = 0
    for r in db_rows:
        (result_id, r_load, r_run, r_batch, task_id, attempt, name,
         version, r_from, r_to, severity, status) = r
        key = (task_id, name)
        if key in found or key not in expected_rules:
            errors.append(f"Лишнее или повторное правило: {task_id}.{name}")
        found.add(key)
        if (r_load != load_id or r_run != run_id or r_batch != batch_id
                or r_from != period_from or r_to != period_to
                or version != RULE_VERSION or attempt != expected_attempts.get(task_id)):
            errors.append(f"DQ result_id={result_id} не соответствует загрузке/периоду/попытке")
        expected_severity = (ROW_SQL_RULES if task_id == "check_records"
                             else TOTAL_SQL_RULES if task_id == "check_totals" else {}).get(name)
        if expected_severity is None or severity != expected_severity[0]:
            errors.append(f"Неверная критичность: {name}")
        if severity == "WARNING" and status == "WARN":
            warnings += 1
        elif status != "PASS":
            errors.append(f"{name}: {status}")

    if found != expected_rules:
        errors.append("Отсутствуют обязательные правила DQ")
    if not staged or not complete:
        errors.append("Staging или полнота поставки не подтверждены")

    with conn.transaction():
        if errors:
            conn.execute("DELETE FROM etl.publication_approvals WHERE load_id=%s", (load_id,))
            conn.execute("UPDATE etl.load_runs SET status='blocked',last_error=%s WHERE load_id=%s AND status<>'published'",
                         ("; ".join(dict.fromkeys(errors))[:1000], load_id))
        else:
            conn.execute(
                """
                INSERT INTO etl.publication_approvals
                       (load_id,run_id,attempt,dq_result_ids)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (load_id) DO UPDATE SET
                       run_id=EXCLUDED.run_id, attempt=EXCLUDED.attempt,
                       dq_result_ids=EXCLUDED.dq_result_ids,
                       approved_at=clock_timestamp()
                """,
                (load_id, run_id, max(expected_attempts.values()), selected),
            )
            conn.execute("UPDATE etl.load_runs SET status='checked',last_error=NULL WHERE load_id=%s AND status<>'published'", (load_id,))

    if errors:
        raise ValueError("Публикация заблокирована: " + "; ".join(dict.fromkeys(errors))[:600])
    return {"load_id": str(load_id), "result_ids": selected, "warnings": warnings}


def verify_publication_approval(conn, load_id, approval=None):
    load_id = UUID(str(load_id))
    run = conn.execute(
        "SELECT run_id,status,staging_complete,manifest_complete FROM etl.load_runs WHERE load_id=%s FOR UPDATE",
        (load_id,),
    ).fetchone()
    if run is None or run[1] != "checked" or not run[2] or not run[3]:
        raise ValueError("Загрузка не находится в состоянии checked")
    
    saved = conn.execute(
        """
        SELECT run_id,attempt,dq_result_ids FROM etl.publication_approvals
        WHERE load_id=%s FOR SHARE
        """, (load_id,),
    ).fetchone()
    if saved is None or saved[0] != run[0]:
        raise ValueError("Нет действующего разрешения для этого load_id")
    _, approval_attempt, ids = saved
    if approval is not None and (approval.get("load_id") != str(load_id)
                                 or set(approval.get("result_ids", [])) != set(ids)):
        raise ValueError("Передано другое разрешение публикации")
    
    required = ({("check_records", name) for name in ROW_SQL_RULES}
                | {("check_totals", name) for name in TOTAL_SQL_RULES})
    rows = conn.execute(
        """
        SELECT task_id,rule_name,rule_version,load_id,run_id,severity,status,attempt
        FROM etl.dq_results WHERE result_id=ANY(%s)
        """, (ids,)
    ).fetchall()
    if len(ids) != len(required) or len(rows) != len(required):
        raise ValueError("Разрешение содержит неполный набор DQ")
    
    found = set()
    for task_id, name, version, result_load, result_run, severity, status, attempt in rows:
        key = (task_id, name)
        rules = ROW_SQL_RULES if task_id == "check_records" else TOTAL_SQL_RULES
        if (key not in required or key in found or version != RULE_VERSION
                or result_load != load_id or result_run != run[0]
                or rules[name][0] != severity or attempt > approval_attempt
                or not (status == "PASS" or (severity == "WARNING" and status == "WARN"))):
            raise ValueError("Недействительный DQ-допуск")
        found.add(key)

    if found != required:
        raise ValueError("Отсутствуют обязательные результаты DQ")


def check_published_model(conn, refresh_stats, core_stats, source_counts, prepared, final_results):
    for name in SOURCE_NAMES:
        processed = sum(core_stats[name].values())
        final_results.append((f"core_count_{name}", name, source_counts[name], processed,
                              "Количество вставленных, обновленных и пропущенных записей"))

    # Сравниваем входящие версии со значениями core после apply_core, устаревшие версии пропускаем, одинаковые версии тоже проверяем
    money_queries = {
        "invoices": ("invoice_id", "SELECT source_version, amount, currency FROM core.fact_invoice WHERE invoice_id=%s"),
        "payments": ("payment_id", "SELECT source_version, amount, currency FROM core.fact_payment WHERE payment_id=%s"),
        "refunds": ("refund_id", "SELECT source_version, amount, currency FROM core.fact_refund WHERE refund_id=%s"),
    }
    for source, (key, query) in money_queries.items():
        expected = {"EUR": Decimal("0"), "USD": Decimal("0")}
        actual = {"EUR": Decimal("0"), "USD": Decimal("0")}
        bad_rows = 0
        for row in prepared[source]:
            saved = conn.execute(query, (row[key],)).fetchone()
            if saved is not None and saved[0] > row["version"]:
                continue
            expected[row["currency"]] += row["amount"]
            if saved is not None:
                actual[saved[2]] += saved[1]
            if saved != (row["version"], row["amount"], row["currency"]):
                bad_rows += 1

        final_results.append((f"core_money_rows_{source}", source, 0, bad_rows,
                              "Версия, сумма и валюта каждой принятой записи совпадают"))
        for currency in ("EUR", "USD"):
            final_results.append((f"core_amounts_{source}_{currency}", source,
                                  format(expected[currency], ".2f"), format(actual[currency], ".2f"),
                                  f"Сумма принятых записей staging и core в {currency}"))

    bad_plans = conn.execute(
        """
        WITH periods AS (
            SELECT plan_id,valid_from,valid_to,
                   LEAD(valid_from) OVER (PARTITION BY plan_id ORDER BY valid_from) AS next_from
            FROM core.dim_plan
        )
        SELECT COUNT(*) FROM periods WHERE valid_to IS DISTINCT FROM next_from
        """
    ).fetchone()[0]
    bad_invoice_plans = conn.execute(
        """
        SELECT COUNT(*) FROM core.fact_invoice i
        JOIN core.dim_plan p ON p.plan_sk=i.plan_sk
        WHERE NOT (p.valid_from<=i.issued_at AND (p.valid_to>i.issued_at OR p.valid_to IS NULL))
           OR p.currency<>i.currency
           OR p.plan_id IS DISTINCT FROM (
               SELECT e.plan_id FROM core.fact_subscription_event e
               WHERE e.subscription_sk=i.subscription_sk AND e.effective_at<=i.issued_at
               ORDER BY e.effective_at DESC,
                   COALESCE((substring(e.event_id FROM '-([0-9]+)$'))::int,0) DESC,
                   e.event_id DESC LIMIT 1
           )
        """
    ).fetchone()[0]
    bad_currency = conn.execute(
        """
        SELECT (SELECT COUNT(*) FROM core.fact_payment p
                JOIN core.fact_invoice i ON i.invoice_sk=p.invoice_sk
                WHERE p.currency<>i.currency)
             + (SELECT COUNT(*) FROM core.fact_refund r
                JOIN core.fact_payment p ON p.payment_sk=r.payment_sk
                WHERE r.currency<>p.currency OR
                    (r.status='succeeded' AND p.status<>'succeeded'))
        """
    ).fetchone()[0]
    final_results.append(("plan_periods", "plans", 0, bad_plans,
                          "Границы исторических версий тарифов"))
    final_results.append(("invoice_historical_plan", "invoices", 0, bad_invoice_plans,
                          "Счета связаны с правильной версией тарифа"))
    final_results.append(("money_links", None, 0, bad_currency,
                          "Валюты совпадают, успешный возврат связан с успешным платежом"))

    bad_refund_dates = conn.execute(
        """
        SELECT COUNT(*) FROM core.fact_refund r
        JOIN core.fact_payment p ON p.payment_sk=r.payment_sk
        WHERE r.refunded_at<p.paid_at
        """
    ).fetchone()[0]
    final_results.append(("refund_date", "refunds", 0, bad_refund_dates,
                          "Возврат не может быть раньше оплаты"))

    bad_refund_amounts = conn.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT p.payment_sk FROM core.fact_payment p
            JOIN core.fact_refund r ON r.payment_sk=p.payment_sk
            WHERE r.status='succeeded'
            GROUP BY p.payment_sk, p.amount
            HAVING SUM(r.amount)>p.amount
        ) AS violations
        """
    ).fetchone()[0]
    final_results.append(("refund_amount", "refunds", 0, bad_refund_amounts,
                          "Сумма успешных возвратов не превышает платеж"))

    from_day, to_day = refresh_stats["from"], refresh_stats["to_exclusive"]
    expected_invoices = conn.execute(
        """
        SELECT COUNT(*) FROM core.dim_date d JOIN core.fact_invoice i
          ON i.issued_at < ((d.calendar_date+1)::timestamp AT TIME ZONE 'UTC')
        WHERE d.calendar_date>=%s AND d.calendar_date<%s
        """, 
        (from_day, to_day)
    ).fetchone()[0]
    final_results.append(("invoice_daily_count", "invoice_daily", expected_invoices,
                          refresh_stats["invoice_daily"], "Количество снимков счетов"))

    report_date = date.fromisoformat(to_day) - timedelta(days=1)
    differences = conn.execute(
        """
        WITH day_end AS (
            SELECT ((%s::date + 1)::timestamp AT TIME ZONE 'UTC') AS t
        ), expected AS (
            SELECT i.currency, SUM(i.amount) AS invoiced,
                   SUM(COALESCE(p.amount,0)) AS paid,
                   SUM(COALESCE(r.amount,0)) AS refunded
            FROM core.fact_invoice i CROSS JOIN day_end d
            LEFT JOIN LATERAL (
                SELECT SUM(x.amount) AS amount FROM core.fact_payment x
                WHERE x.invoice_sk=i.invoice_sk AND x.status='succeeded'
                  AND x.paid_at<d.t
            ) p ON TRUE
            LEFT JOIN LATERAL (
                SELECT SUM(x.amount) AS amount FROM core.fact_refund x
                JOIN core.fact_payment payment ON payment.payment_sk=x.payment_sk
                WHERE payment.invoice_sk=i.invoice_sk AND payment.status='succeeded'
                  AND payment.paid_at<d.t AND x.status='succeeded' AND x.refunded_at<d.t
            ) r ON TRUE
            WHERE i.issued_at<d.t
            GROUP BY i.currency
        ), actual AS (
            SELECT i.currency, SUM(m.invoice_amount) AS invoiced,
                   SUM(m.paid_amount) AS paid, SUM(m.refund_amount) AS refunded
            FROM mart.invoice_daily m
            JOIN core.fact_invoice i ON i.invoice_sk=m.invoice_sk
            JOIN core.dim_date d ON d.date_key=m.date_key
            WHERE d.calendar_date=%s
            GROUP BY i.currency
        )
        SELECT COUNT(*) FROM expected e FULL JOIN actual a USING (currency)
        WHERE e.invoiced IS DISTINCT FROM a.invoiced
           OR e.paid IS DISTINCT FROM a.paid
           OR e.refunded IS DISTINCT FROM a.refunded
        """, 
        (report_date, report_date)
    ).fetchone()[0]
    final_results.append(("invoice_daily_amounts", "invoice_daily", 0, differences,
                          "Суммы core и mart.invoice_daily совпадают по валютам на последнюю дату"))

    # Ошибка приводит к rollback всей публикации. Список остается в Python.
    for name, source, expected, actual, reason in final_results:
        if expected != actual:
            raise ValueError(f"{name}: {reason}; ожидалось {expected}, получено {actual}")

def save_final_checks(conn, load_id, attempt, final_results):
    with conn.transaction():
        for name, source, expected, actual, reason in final_results:
            status = "PASS" if expected == actual else "FAIL"
            conn.execute(
                """
                INSERT INTO etl.dq_results
                    (load_id, run_id, batch_id, task_id, attempt, rule_name, rule_version,
                     source_type, period_from, period_to, severity, status, expected, actual, reason)
                SELECT load_id, run_id, batch_id, 'publish_data', %s, %s, %s, %s,
                       period_from, period_to, 'CRITICAL', %s, %s, %s, %s
                FROM etl.load_runs WHERE load_id=%s
                ON CONFLICT (load_id, task_id, attempt, rule_name) DO UPDATE SET
                    status=EXCLUDED.status, expected=EXCLUDED.expected, actual=EXCLUDED.actual,
                    reason=EXCLUDED.reason, created_at=clock_timestamp()
                """,
                (attempt, name, RULE_VERSION, source, status, Jsonb(expected), Jsonb(actual), reason, load_id),
            )


def check_period(conn, manifest, batch_id, lock=False):
    query = """SELECT last_success_until
               FROM etl.pipeline_state
               WHERE pipeline_name = %s"""

    if lock:
        query += " FOR UPDATE"

    result = conn.execute(query, ("subs_service",)).fetchone()
    if result is None:
        raise RuntimeError("Сначала выполните SQL для etl.pipeline_state")

    watermark = result[0]
    mode = manifest["mode"]
    lower = manifest["manifest_from_inclusive"]
    upper = manifest["manifest_until_exclusive"]

    if mode == "full":
        if watermark is None:
            has_core = conn.execute(
                """SELECT EXISTS (SELECT 1 FROM core.fact_invoice)
                       OR EXISTS (SELECT 1 FROM core.fact_subscription_event)"""
            ).fetchone()[0]
            if has_core:
                raise ValueError(
                    "В core уже есть данные, но watermark пуст, требуется восстановить etl.pipeline_state по опубликованному запуску"
                )
        elif batch_id != "01_full" or upper > watermark:
            raise ValueError("Повторная full-загрузка в основном потоке запрещена")

    else:
        if watermark is None:
            raise ValueError("Нельзя применять delta до успешной full-загрузки")
        if lower > watermark:
            raise ValueError(
                f"Разрыв периодов: watermark={watermark.isoformat()}, новая поставка начинается {lower.isoformat()}"
            )

    return watermark


####
def run_load(conn, batch_id=None, api_url=None, limit=5, resume_load=None, fail_after_staging=False, fail_inside_publication=False):
    if resume_load is None:
        if not batch_id:
            raise ValueError("Для новой загрузки требуется --batch")
        batch_dir = source_data_dir / batch_id
        load_id = start_load(conn, batch_id, batch_dir)
        staging_complete = False
        saved_manifest = None
        load_attempt = 1
    else:
        load_id = UUID(str(resume_load))
        with conn.transaction():
            row = conn.execute(
                """
                SELECT batch_id, source_path, status, staging_complete, manifest_data
                FROM etl.load_runs WHERE load_id=%s FOR UPDATE
                """,
                (load_id,),
            ).fetchone()
            if row is None:
                raise ValueError(f"Неизвестный load_id={load_id}")
            
            batch_id, source_path, status, staging_complete, saved_manifest = row
            batch_dir = Path(source_path)
            if status == "published":
                logger.info("event=load_already_published load_id=%s batch=%s", load_id, batch_id)
                return load_id

            if not staging_complete:
                bad_source = conn.execute(
                    "SELECT EXISTS (SELECT 1 FROM stg.source_records WHERE load_id=%s AND raw_text IS NOT NULL)",
                    (load_id,),
                ).fetchone()[0]
                if bad_source:
                    raise ValueError("Сохранен поврежденный источник: после исправления необходимо начать новую загрузку --batch")

            load_attempt = conn.execute("SELECT COALESCE(MAX(attempt), 0) + 1 FROM etl.load_events WHERE load_id=%s",
                (load_id,),
            ).fetchone()[0]
            conn.execute(
                """
                UPDATE etl.load_runs
                SET status=%s, finished_at=NULL, last_error=NULL
                WHERE load_id=%s
                """,
                ("staged" if staging_complete else "running", load_id),
            )
    
    logger.info(
        "event=load_attempt_started load_id=%s batch=%s load_attempt=%s resume=%s",
        load_id, batch_id, load_attempt, resume_load is not None,
    )
    stage = "receive"
    source_error = {}
    final_results = []

    try:
        if resume_load is not None:
            save_event(conn, load_id, "resume", "STARTED", attempt=load_attempt)
        else:
            save_event(conn, load_id, "receive", "STARTED", attempt=load_attempt)

        manifest = read_manifest(batch_dir / "manifest.json")
        if manifest["manifest_errors"]:
            raise ValueError("Некорректный manifest: " + "; ".join(manifest["manifest_errors"][:3]))
        if saved_manifest is not None and saved_manifest != manifest["manifest_data"]:
            raise ValueError("Манифест изменился: нельзя продолжать прежний load_id")

        with conn.transaction():
            conn.execute(
                """
                UPDATE etl.load_runs
                SET manifest_data=%s, mode=%s, period_from=%s, period_to=%s, manifest_complete=%s
                WHERE load_id=%s
                """,
                (
                    Jsonb(manifest["manifest_data"]), manifest["mode"],
                    manifest["manifest_from_inclusive"],
                    manifest["manifest_until_exclusive"], manifest["complete"], load_id,
                ),
            )
            check_period(conn, manifest, batch_id)

        if not staging_complete:
            csv_records = read_csv_sources(batch_dir, source_error)
            subscriptions = read_subscriptions(batch_dir, source_error)
            payments = fetch_payments(batch_id, api_url, limit=limit, source_error=source_error)
            save_event(conn, load_id, "receive", "SUCCESS", attempt=load_attempt)

            stage = "stage"
            save_event(conn, load_id, "stage", "STARTED", attempt=load_attempt)
            counts = save_staging(conn, load_id, batch_id, csv_records, subscriptions, payments)
            save_event(conn, load_id, "stage", "SUCCESS", counters=counts, attempt=load_attempt)
            if fail_after_staging:
                raise RuntimeError("Тестовый сбой после COMMIT staging")

        else:
            logger.info("event=staging_reused load_id=%s load_attempt=%s", load_id, load_attempt)
            save_event(
                conn, load_id, "stage", "SUCCESS", attempt=load_attempt,
                message="Ранее зафиксированный staging использован повторно",
            )

        stage = "validate"
        save_event(conn, load_id, "validate", "STARTED", attempt=load_attempt)
        validation = validate_staging(conn, load_id, manifest, strict=False)
        validate_status = "WARN" if validation["rejected_rows"] or validation["delivery_errors"] else "SUCCESS"
        save_event(conn, load_id, "validate", validate_status,
                   counters={**validation["counts"], "rejected_rows": validation["rejected_rows"],
                             "delivery_errors": len(validation["delivery_errors"])}, attempt=load_attempt)

        stage = "check_quality"
        records_result = check_quality(conn, load_id, "records", load_attempt)
        totals_result = check_quality(conn, load_id, "totals", load_attempt)

        stage = "quality_gate"
        approval = quality_gate(conn, load_id, records_result, totals_result)

        stage = "publish"
        save_event(conn, load_id, "publish", "STARTED", attempt=load_attempt)

        with conn.transaction():
            watermark = check_period(conn, manifest, batch_id, lock=True)
            verify_publication_approval(conn, load_id, approval)
            core_stats = apply_core(conn, validation["prepared"])
            mart_stats = refresh_marts(conn, manifest["manifest_until_exclusive"], final_results, fail_inside_publication)
            check_published_model(conn, mart_stats, core_stats, manifest["counts"], validation["prepared"], final_results)
            save_final_checks(conn, load_id, load_attempt, final_results)

            new_watermark = manifest["manifest_until_exclusive"]
            if watermark is None or new_watermark > watermark:
                conn.execute(
                    """
                    UPDATE etl.pipeline_state
                    SET last_success_until=%s, last_load_id=%s, last_batch_id=%s, updated_at=clock_timestamp()
                    WHERE pipeline_name='subs_service'
                    """,
                    (new_watermark, load_id, batch_id),
                )

            saved_counts = conn.execute("SELECT counts FROM etl.load_runs WHERE load_id=%s", (load_id,)).fetchone()[0]
            initial_counts = saved_counts.get("staging", saved_counts)
            conn.execute(
                """
                UPDATE etl.load_runs
                SET status='published', counts=%s, finished_at=clock_timestamp(), last_error=NULL
                WHERE load_id=%s""",
                (Jsonb({"staging": initial_counts, "core": core_stats, "mart": mart_stats}), load_id),
            )
            save_event(
                conn, load_id, "publish", "SUCCESS", attempt=load_attempt,
                counters={
                    "subscription_daily": mart_stats["subscription_daily"],
                    "invoice_daily": mart_stats["invoice_daily"],
                },
            )
        logger.info("event=load_complete load_id=%s load_attempt=%s", load_id, load_attempt)
        return load_id
    
    except Exception as error:
        reason = f"{stage}: {type(error).__name__}"
        if isinstance(error, (ValueError, FileNotFoundError)):
            reason += f": {str(error)[:500]}"
        with conn.transaction():
            if source_error:
                save_source_error(conn, load_id, batch_id, source_error)
            if final_results:
                save_final_checks(conn, load_id, load_attempt, final_results)
            conn.execute(
                """
                UPDATE etl.load_runs
                SET status=%s, finished_at=clock_timestamp(), last_error=%s
                WHERE load_id=%s AND status<>'published'
                """,
                ("blocked" if stage in ("validate", "quality_gate") else "failed", reason[:1000], load_id),
            )
        save_event(conn, load_id, stage, "FAILED", message=reason[:1000], attempt=load_attempt)
        logger.error( "event=load_failed load_id=%s load_attempt=%s stage=%s type=%s", load_id, load_attempt, stage, type(error).__name__,)

        raise


#основной код
def main():
    parser = argparse.ArgumentParser()
    #допустим только один вариант запуска из двух, так что закинула в группу
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--batch", help="Поставка")
    group.add_argument("--resume-load", help="Возобновить загрузку по прежнему UUID load_id")
    parser.add_argument("--limit", type=int, default=5, help="Размер страницы API: 1–50")
    #Только для воспроизводимых сценариев сбоя
    parser.add_argument("--fail-after-staging", action="store_true")
    parser.add_argument("--fail-inside-publication", action="store_true")
    args = parser.parse_args()

    configure_logging()
    load_dotenv(base_dir / ".env")
    api_url = os.getenv("PAYMENTS_API_URL", "http://127.0.0.1:8765/payments")

    try:
        with psycopg.connect(autocommit=True) as conn:
            conn.execute("SET TIME ZONE 'UTC'")
            load_id = run_load(
                conn, batch_id=args.batch, api_url=api_url,
                limit=args.limit, resume_load=args.resume_load,
                fail_after_staging=args.fail_after_staging,
                fail_inside_publication=args.fail_inside_publication,
            )
        logger.info("event=program_complete load_id=%s", load_id)
        return 0
    
    except Exception as error:
        logger.error("event=program_failed type=%s", type(error).__name__)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
