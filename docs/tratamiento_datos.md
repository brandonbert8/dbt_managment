# Tratamiento y preprocesamiento de datos — Proyecto dbt Churn (v2 técnica)

> Documento de trazabilidad exhaustiva: qué suciedad había, por qué se trata así, cómo se implementó y dónde está en el código.
> Convención: `archivo.sql L<n>: <snippet literal>`. Todas las citas son código real en `models/churn/`.
> Tablas "Antes → Después" marcadas con *Ilustrativo* usan valores ficticios solo para explicar la regla; el código citado es el real.
> Arquitectura: `Bronze 01_staging (select * RAW) → Silver 02_intermediate/int_*_clean.sql (deep_cleaning) → tests schema.yml`.

## 0. Resumen ejecutivo

El tratamiento **no es "quitar nulos"**. Es un pipeline Silver en 3 pasadas por archivo (`bronze_prepared → normalized → cleaned → select final`) que combina:

1. **Tipado seguro que no tumba el pipeline** (`try_to_*` vs `::`), 2. **Estandarización de texto bilingüe ES/EN**, 3. **Ajuste de formato en fechas** (parseo, `YYYY-MM`, secuencia, futuro, derivadas), 4. **Saneamiento numérico** (`greatest/least`, rangos físicos), 5. **Regex** (formato + stripping), 6. **Deduplicación determinista 2 niveles + CDC**, 7. **Imputación tipada o recálculo derivado** (nunca media/mediana), 8. **Reconciliación contable/técnica**, 9. **Trazabilidad DQ** (`dq_*` + `*_dq_status` + `schema.yml`).

## 1. Bronze / Staging: deliberadamente sin limpieza

Los 12 `stg_*.sql` son réplica RAW. Ejemplo:

* `stg_billing_invoices.sql L3-L6`:
```sql
-- Bronze: replica logica RAW; conserva columnas, valores, duplicados y metadatos.
-- La limpieza, el tipado y las validaciones se aplican en Silver.
select *
from {{ source('raw_churn','billing_invoices') }}
```
* `models/churn/schema.yml L4-L5` (patrón repetido L7-L35):
```yaml
- name: stg_billing_invoices
  description: Bronze RAW sin transformaciones; conserva todas las filas, columnas, valores originales y metadatos de Airbyte. La limpieza y validacion se realizan en int_billing_clean.
```

**Por qué importa:** si Bronze limpiara, se perdería auditoría. Por eso Silver hace doble pasada: primero filtra lo irrecuperable (`WHERE`), después corrige lo recuperable (`IFF/CASE/GREATEST`) y siempre deja flag DQ.

---

## 2. Fechas: ajuste de formato, validación y derivadas (subsección completa)

Es la familia más densa (74 ocurrencias). Se aplica en 6 patrones encadenados. El orden real en cada archivo es: **(a) filtrar nulos → (b) parseo seguro → (c) normalizar a YYYY-MM/month_start → (d) validar secuencia → (e) excluir futuro → (f) derivar**.

### 2.1 Parseo seguro que no aborta el modelo (`try_to_*` vs `::`)

**Problema:** las fuentes traen fechas como texto heterogéneo (`'2024-02-30'`, `'31/02/2024'`, `'2024/13/01'`, `''`, `null`, timestamps con zona). Un casteo directo `::date` abortaría todo el `dbt run`. Por eso se usa `try_to_date(to_varchar(col))` / `try_to_timestamp_ntz(col)`: lo inválido → `NULL`, y el `NULL` se gestiona después con `WHERE` o `IFF`.

| Archivo | Cita exacta | Qué protege |
|---|---|---|
| pagos | `int_payment_clean.sql L40: try_to_date(to_varchar(payment_date)) as payment_date,` | `payment_date` texto → fecha o `NULL` |
| clientes | `int_customer_clean.sql L58: try_to_date(to_varchar(registration_date)) as registration_date,` | `registration_date` heterogénea |
| suscripciones | `int_subscription_clean.sql L60-L62: try_to_date(to_varchar(contract_start_date)) as contract_start_date,` / `try_to_date(to_varchar(contract_end_date)) as contract_end_date_source,` / `try_to_date(to_varchar(plan_change_date)) as plan_change_date_source,` | triple fecha contractual; se conserva `_source` para auditar lo inválido |
| churn | `int_churn_clean.sql L30: try_to_date(to_varchar(churn_date)) as churn_date,` | `churn_date` |
| uso | `int_usage_clean.sql L30: try_to_date(to_varchar(usage_date)) as usage_date,` | `usage_date` |
| soporte | `int_support_clean.sql L14-L15: try_to_timestamp_ntz(created_at) as created_at,` / `try_to_timestamp_ntz(to_varchar(resolved_at)) as resolved_at,` + `L30-L31: try_to_timestamp_ntz(to_varchar(created_at)) as created_timestamp,` | doble pasada timestamp; `L23: from deduplicated where try_to_timestamp_ntz(created_at) is not null` exige creado válido desde Bronze |
| red | `int_network_clean.sql L13: trim(device_id) as device_id, try_to_timestamp_ntz(timestamp) as event_timestamp,` + `L26: where try_to_timestamp_ntz(timestamp) is not null` | evento de red |
| marketing | `int_marketing_clean.sql L33-L34: try_to_date(to_varchar(contact_date)) as contact_date_parsed,` / `try_to_date(to_varchar(response_date)) as response_date_source,` | contacto vs respuesta (separadas para validar secuencia después) |
| macro/metas | `int_macro_clean.sql L27: try_to_date(to_varchar(month_start)) as month_start,` / `int_retention_target_clean.sql L38: try_to_date(to_varchar(month_start)) as month_start,` + `L29: try_to_timestamp_ntz(ultima_actualizacion) as last_updated_at,` | segunda pasada de `month_start` ya construido con `to_date` |

*Ilustrativo — Antes → Después de `try_to_date(to_varchar(x))`:*

| Entrada RAW | Salida Silver | Destino posterior |
|---|---|---|
| `'2024-03-15'` | `2024-03-15` | `VALID` |
| `'2024-02-30'`, `'2024-13-01'`, `''`, `'null'` | `NULL` | `WHERE ... is not null` o `'MISSING'/'INVALID_*'` |
| `'2024-03-15 14:30:00'` (en campo date) | `2024-03-15` | normalizado |

**Contraste consciente:** cuando la columna ya viene tipada en el warehouse se usa casteo directo, no `try_*`. Ej. `int_billing_clean.sql L62: source_extracted_at::timestamp_ltz as source_extracted_at`, `int_macro_clean.sql L28-L31: ipc_index::float as ipc_source,`. Es decir: `try_*` = dato externo sucio; `::` = metadato interno confiable (Airbyte).

### 2.2 Normalización de formato a `YYYY-MM` y `month_start` (grano mensual)

**Problema:** cada dominio necesita agregación mensual comparable. Se crean dos columnas canónicas: `year_month TEXT 'YYYY-MM'` y `month_start DATE primer día del mes`.

* `int_billing_clean.sql L25: to_char(issue_date,'YYYY-MM') as year_month,` (+ `L49: year_month as billing_period,`)
* `int_payment_clean.sql L23: to_char(payment_date,'YYYY-MM') as year_month,`
* `int_usage_clean.sql L12: to_char(usage_date,'YYYY-MM') as year_month,`
* `int_support_clean.sql L16: to_char(try_to_timestamp_ntz(created_at),'YYYY-MM') as year_month,`
* `int_network_clean.sql L14: to_char(try_to_timestamp_ntz(timestamp),'YYYY-MM') as year_month,`
* `int_marketing_clean.sql L14: contact_date, response_date, to_char(contact_date,'YYYY-MM') as year_month,`
* Construcción inversa (cuando la llave es texto): `int_macro_clean.sql L10: to_date(trim(year_month) || '-01') as month_start,` y `int_retention_target_clean.sql L21: to_date(trim(year_month) || '-01') as month_start,`

**Por qué `to_char` antes del `try_to_date`:** en `bronze_prepared` se genera `year_month` con el valor crudo para no perder el grano aunque la fecha falle el parseo; en `normalized` se re-parsea con `try_to_date` y se filtra. Ver `int_usage_clean.sql L11-L12` (crudo) → `L30` (parseado) → `L99-L100` (filtro).

### 2.3 Formato estricto `YYYY-MM` con regex (solo macro y metas)

**Problema:** `year_month` manual (`'2024-1'`, `'24-01'`, `'2024/01'`, `'2024-13'`) rompería joins mensuales. Se exige regex:

* `int_macro_clean.sql L18: where regexp_like(trim(year_month),'^[0-9]{4}-(0[1-9]|1[0-2])$')`
* `int_retention_target_clean.sql L10-L11: where nullif(trim(target_id),'') is not null and regexp_like(trim(year_month),'^[0-9]{4}-(0[1-9]|1[0-2])$')`
* Verificación cruzada: `int_macro_clean.sql L45-L46: regexp_like(year_month, '^[0-9]{4}-(0[1-9]|1[0-2])$') and month_start = try_to_date(year_month || '-01') as dq_valid_year_month,` (idem `int_retention_target_clean.sql L77-L78`).

*Ilustrativo:*

| Entrada | `regexp_like` | Resultado |
|---|---|---|
| `'2024-03'` | true | pasa, `month_start=2024-03-01` |
| `'2024-3'`, `'24-03'`, `'2024/03'`, `'2024-13'` | false | filtrado en `WHERE` + `macro_dq_status='INVALID_YEAR_MONTH'` (`int_macro_clean.sql L52`) |

### 2.4 Secuencia lógica: fin < inicio → `NULL` (no se borra la fila, se anula la fecha)

**Problema:** `due_date < issue_date`, `contract_end < contract_start`, `resolved < created`, `response < contact` son imposibles. En vez de eliminar la fila (se perdería la factura/ticket), se conserva la fila y se pone la fecha fin a `NULL` + flag.

* `int_billing_clean.sql L72: iff(due_date_source is null or due_date_source >= issue_date, due_date_source, null) as due_date,` — se guarda `due_date_source` original (`L48, L100`) para auditar.
* `int_subscription_clean.sql L83-L86: iff(contract_end_date_source is null or contract_end_date_source >= contract_start_date, contract_end_date_source, null) as contract_end_date,` (idem `plan_change_date`).
* `int_support_clean.sql L57-L58: iff(resolved_timestamp_source is null or resolved_timestamp_source >= created_timestamp, resolved_timestamp_source, null) as resolved_timestamp,`
* `int_marketing_clean.sql L56-L61: iff( response_date_source is null or (response_date_source >= contact_date_parsed and response_date_source <= current_date()), response_date_source, null ) as response_date_parsed,`

Cada uno tiene su `dq_*`: `int_billing_clean.sql L121: due_date_source is null or due_date_source >= issue_date as dq_valid_dates,`, `int_subscription_clean.sql L114-L118`, `int_support_clean.sql L105-L106`, `int_marketing_clean.sql L99-L101`.

### 2.5 Exclusión de futuro (`current_date()` como corte)

**Problema:** fechas futuras (`issue_date`, `churn_date`, `payment_date` > hoy) son errores de carga o fuga temporal. Se excluyen en el `WHERE` final y se etiquetan.

| Dominio | Filtro final | Estado |
|---|---|---|
| billing | `int_billing_clean.sql L67: and issue_date <= current_date()` | `L125: when due_date_source < issue_date then 'INVALID_DUE_DATE_TO_NULL'` |
| customer | `int_customer_clean.sql L120-L122: where age between 18 and 120 and registration_date is not null and registration_date <= current_date()` | `L97-L101: when registration_date is null then 'MISSING' / when registration_date > current_date() then 'FUTURE_TO_EXCLUDE'` |
| subscription | `int_subscription_clean.sql L142-L143: where contract_start_date is not null and contract_start_date <= current_date()` | `L121: when contract_start_date > current_date() then 'FUTURE_START_EXCLUDED'` |
| churn | `int_churn_clean.sql L96-L98: where churn_date is not null and churn_date <= current_date() and (registration_date is null or churn_date >= registration_date)` | `L80: when churn_date > current_date() then 'FUTURE_CHURN_EXCLUDED'` |
| uso | `int_usage_clean.sql L99-L100: where usage_date is not null and usage_date <= current_date()` | `L85: when usage_date > current_date() then 'FUTURE_DATE_EXCLUDED'` |
| soporte | `int_support_clean.sql L129-L130: where created_timestamp is not null and created_timestamp::date <= current_date()` | `L110: when created_timestamp::date > current_date() then 'FUTURE_TICKET_EXCLUDED'` |
| red | `int_network_clean.sql L103-L104: where event_timestamp is not null and event_timestamp::date <= current_date()` | `L81: when event_timestamp::date > current_date() then 'FUTURE_EVENT_EXCLUDED'` |
| marketing | `int_marketing_clean.sql L125-L126: where contact_date_parsed is not null and contact_date_parsed <= current_date()` | `L107: when contact_date_parsed > current_date() then 'FUTURE_CONTACT_EXCLUDED'` |
| macro | `int_macro_clean.sql L67-L68: where month_start is not null and month_start <= date_trunc('month', current_date())` | `L53: when month_start > date_trunc('month', current_date()) then 'FUTURE_MONTH_EXCLUDED'` |
| pagos | (revisión, no exclusión dura) `int_payment_clean.sql L93: when payment_date > current_date() then 'FUTURE_PAYMENT_DATE_REVIEW'` | se conserva pero marcado |

### 2.6 Fechas derivadas (`datediff/dateadd` como imputación y feature)

* Tenure cliente: `int_customer_clean.sql L114: datediff(month, registration_date, current_date())::integer as customer_tenure_months,`
* Tenure contrato: `int_subscription_clean.sql L135-L136: datediff(month, contract_start_date, coalesce(contract_end_date, current_date()))::integer as tenure_months,`
* Tenure churn recalculado (corrige fuente): `int_churn_clean.sql L42-L44: iff(c.registration_date is not null and ch.churn_date >= c.registration_date, datediff(month, c.registration_date, ch.churn_date), null)::integer as tenure_months_recalculated` + `L51-L52: greatest(coalesce(tenure_months_recalculated, tenure_months_source, 0), 0)::integer as tenure_months_at_end,`
* Recencia churn: `int_churn_clean.sql L69-L75: when churn_date >= dateadd(day, -30, current_date()) then 'CHURN_0_30_DAYS' ... datediff(day, churn_date, current_date())::integer as days_since_churn,`
* Duración soporte imputada por fechas (si `resolution_minutes` es nulo): `int_support_clean.sql L60-L68: greatest(coalesce( resolution_minutes, iff(resolved_timestamp_source >= created_timestamp, datediff(minute, created_timestamp, resolved_timestamp_source), null), 0), 0)::number(18,2) as resolution_minutes_clean,`

---

## 3. Texto: orden canónico y normalización bilingüe

**Orden real en todos los cleans:** `trim` → `upper/initcap` → `nullif('')` → `coalesce('UNKNOWN'/dominio)` → `CASE ES/EN` → `LIKE taxonomía`. Alterar el orden rompería la regla (ej. `upper` antes de `trim` dejaría `'  mensual '` sin matchear).

### 3.1 `trim` + `upper`/`initcap` + `nullif`

* IDs: `int_billing_clean.sql L19-L22: trim(invoice_id) as invoice_id, / trim(customer_id) ... / trim(subscription_id) ... / trim(account_id) ...`, `int_xref_customer_clean.sql L7-L8: trim(account_id) as account_id, / trim(customer_id) as customer_id,`
* Categorías a mayúsculas: `int_subscription_clean.sql L20-L23: upper(trim(status)) as subscription_status, / upper(trim(plan_name)) ... / upper(trim(plan_tier)) ... / upper(trim(contract_type)) ...`, `int_support_clean.sql L12-L14: upper(trim(status)) as ticket_status, upper(trim(channel)) as channel, ... upper(trim(agent_team)) as agent_team,`
* Nombre propio a título: `int_marketing_clean.sql L12: trim(campaign_id) as campaign_id, initcap(trim(campaign_name)) as campaign_name,` + `L32: coalesce(nullif(initcap(trim(campaign_name)), ''), 'Unknown') as campaign_name,`
* Ciudad a título, resto a mayúsculas: `int_customer_clean.sql L22-L25: initcap(trim(city)) as city, / upper(trim(region)) as region, / trim(postal_code) as postal_code, / upper(trim(marital_status)) as marital_status,`

### 3.2 Vacío → valor de dominio (`coalesce(nullif(...))`)

* `int_billing_clean.sql L50-L51: coalesce(nullif(upper(trim(invoice_status)), ''), 'UNKNOWN') as invoice_status, / coalesce(nullif(upper(trim(currency)), ''), 'BOB') as currency,` — moneda por defecto `BOB` (negocio Bolivia), no `UNKNOWN`.
* `int_payment_clean.sql L41: coalesce(nullif(upper(trim(payment_method)), ''), 'UNKNOWN') as payment_method,` + `L49: coalesce(nullif(upper(trim(collection_action)), ''), 'NONE') as collection_action,`
* `int_customer_clean.sql L46-L48: coalesce(nullif(initcap(trim(city)), ''), 'Unknown') as city, / coalesce(nullif(upper(trim(region)), ''), 'UNKNOWN') as region, / nullif(upper(trim(postal_code)), '') as postal_code,` — postal se deja `NULL` (no se inventa geografía).
* `int_support_clean.sql L39-L43: coalesce(nullif(upper(trim(channel)), ''), 'UNKNOWN') as channel,` (idem category/priority/reason/agent_team).
* `int_network_clean.sql L34-L36: coalesce(nullif(upper(trim(event_type)), ''), 'UNKNOWN') as event_type_norm,`.
* `int_retention_target_clean.sql L36: coalesce(nullif(upper(trim(region)), ''), 'UNKNOWN') as region,` + `L44: coalesce(nullif(upper(trim(commercial_owner)), ''), 'UNASSIGNED') as commercial_owner,`.

### 3.3 Normalización bilingüe ES/EN muchos→uno (`CASE`)

* Género: `int_customer_clean.sql L40-L45: when upper(trim(gender)) in ('M','MALE','MASCULINO','HOMBRE') then 'MALE' / when upper(trim(gender)) in ('F','FEMALE','FEMENINO','MUJER') then 'FEMALE' / when upper(trim(gender)) in ('NON_BINARY','NO BINARIO','NB') then 'NON_BINARY' / else 'UNKNOWN'`
* Estado civil: `int_customer_clean.sql L49-L55: when upper(trim(marital_status)) in ('MARRIED','CASADO','CASADA') then 'MARRIED' / ... ('SINGLE','SOLTERO','SOLTERA') / ('DIVORCED','DIVORCIADO','DIVORCIADA') / ('WIDOWED','VIUDO','VIUDA')`
* Suscripción: `int_subscription_clean.sql L46-L51: when upper(trim(subscription_status)) in ('ACTIVE','ACTIVO','ENABLED') then 'ACTIVE' / when ... in ('CANCELLED','CANCELED','INACTIVE','BAJA') then 'CANCELLED' / when ... in ('SUPERSEDED','REPLACED') then 'SUPERSEDED'` + contrato `L54-L59: ('MONTH-TO-MONTH','MONTH TO MONTH','MENSUAL') → 'MONTH_TO_MONTH' / ('ONE YEAR','1 YEAR','ANNUAL') → 'ONE_YEAR' / ('TWO YEAR','2 YEAR','BIENNIAL') → 'TWO_YEAR'`
* Pagos: `int_payment_clean.sql L42-L48: ('COMPLETED','PAID','SUCCESS','SUCCEEDED') → 'COMPLETED' / ('FAILED','DECLINED','REJECTED') → 'FAILED' / ('PARTIAL','PARTIALLY_PAID') → 'PARTIAL' / ('PENDING','PROCESSING') → 'PENDING'`
* Soporte: `int_support_clean.sql L32-L38: ('CLOSED','CERRADO') → 'CLOSED' / ('RESOLVED','RESUELTO') → 'RESOLVED' / ('OPEN','ABIERTO') → 'OPEN' / ('PENDING','PENDIENTE') → 'PENDING'`

### 3.4 Clasificación libre con `LIKE` (taxonomía, no solo limpieza)

* Motivo churn: `int_churn_clean.sql L55-L66: when churn_reason_raw like '%PRICE%' or ... like '%COST%' or ... like '%EXPENS%' then 'PRICE' / when ... like '%NETWORK%' or ... like '%SIGNAL%' or ... like '%OUTAGE%' then 'NETWORK_QUALITY' / when ... like '%SUPPORT%' ... then 'CUSTOMER_SERVICE' / when ... like '%COMPET%' then 'COMPETITOR' / when ... like '%MOVE%' or ... like '%RELOCAT%' then 'RELOCATION'`
* Tickets: `int_support_clean.sql L86-L94: when category like '%CANCEL%' or reason like '%CANCEL%' or ... like '%CHURN%' then 'RETENTION_RISK' / when ... like '%BILL%' or ... like '%PAY%' then 'BILLING' / when ... like '%NETWORK%' ... then 'NETWORK' / when ... like '%TECH%' then 'TECHNICAL'`

*Ilustrativo texto Antes → Después:*

| RAW | Regla | Silver |
|---|---|---|
| `'  mensual '`, `'MENSUAL'`, `'month to month'` | `upper(trim()) + CASE` (`subscription L55`) | `MONTH_TO_MONTH` |
| `'masculino '`, `'M'`, `'hombre'` | `CASE` (`customer L41`) | `MALE` |
| `'cerrado'`, `'CLOSED '` | `CASE` (`support L33`) | `CLOSED` |
| `''`, `'   '`, `null` | `coalesce(nullif(...),'UNKNOWN')` | `UNKNOWN` (o `BOB`/`NONE`/`UNASSIGNED` según dominio) |
| `'Too expensive!!!'` | `LIKE '%EXPENS%'` (`churn L57`) | `PRICE` |

---

## 4. Numéricos: imputación, anti-negativo, capping y stripping

### 4.1 `coalesce(col,0)` + `greatest(...,0)` (negativo imposible → 0)

Primera pasada en `bronze_prepared`, segunda en `cleaned` (doble barrera intencional):

* `int_billing_clean.sql L28-L33: coalesce(base_charge,0)::number(18,2)`, `L73-L80: greatest(coalesce(base_charge, 0), 0)::number(18,2) as base_charge_clean,` (idem service/equipment/extra/tax/discount/total/previous).
* `int_usage_clean.sql L13-L22: greatest(coalesce(sms_count,0),0)::integer as sms_count,` (idem voice/internacional/upload/download/peak/offpeak/streaming/sesiones) + `L49-L58` re-saneamiento a `*_clean`.
* `int_payment_clean.sql L26-L30: coalesce(amount_due,0)::number(18,2)`, `greatest(coalesce(outstanding_balance,0),0)`, `greatest(coalesce(days_late,0),0)::integer` + `L63-L67` re-saneamiento.
* `int_support_clean.sql L19-L20: greatest(coalesce(try_to_number(to_varchar(resolution_minutes)),0),0)::number(18,2)`, `greatest(coalesce(interaction_count,0),0)::integer`.
* `int_network_clean.sql L17-L22: coalesce(latency_ms,0)::number(18,2) ... coalesce(outage_duration_seconds,0)` + `L53-L60` capping.

### 4.2 Capping `least(...,techo)` con justificación física (outliers)

No se elimina el outlier, se topa y se marca `OUTLIER_CAPPED` / `CAPPED_*`:

* `int_usage_clean.sql L53: least(greatest(coalesce(streaming_hours, 0), 0), 24)::number(18,2) as streaming_clean,` (un día tiene 24h) + `L55: least(greatest(coalesce(avg_session_minutes, 0), 0), 1440)::number(18,2) as avg_session_clean,` (1440 min/día) + `L86: when coalesce(streaming_hours, 0) > 24 or coalesce(avg_session_minutes, 0) > 1440 then 'OUTLIER_CAPPED'`.
* `int_marketing_clean.sql L62-L63: least(greatest(coalesce(discount_pct, 0), 0), 100)::number(18,4) as discount_pct_num,` (descuento 0-100%) + `least(greatest(coalesce(free_months, 0), 0), 24)::integer as free_months_num,` (tope comercial 24) + `L110-L111: when discount_pct not between 0 and 100 then 'DISCOUNT_CAPPED' / when free_months > 24 then 'FREE_MONTHS_CAPPED'`.
* `int_network_clean.sql L53-L55: least(greatest(coalesce(latency_ms, 0), 0), 10000)::number(18,2) as latency_clean,` (10s), `least(...,5000) jitter`, `least(...,100) packet_loss` + `L84: when packet_loss_pct not between 0 and 100 then 'PACKET_LOSS_CAPPED'` + `L86: when latency_ms > 2000 or jitter_ms > 1000 or outage_duration_seconds > 86400 then 'EXTREME_OUTLIER_CAPPED'`.
* `int_retention_target_clean.sql L57: least(greatest(coalesce(churn_rate_target_pct, 0), 0), 100)::number(18,4) as churn_rate_clean,` + `L87: when churn_rate_target_pct > 100 then 'CAPPED_AT_100'` + `L70: least(retained_clean, active_base_clean) as retained_customers_target_num,` + `L93: when retained_clean > active_base_clean then 'CAPPED_TO_ACTIVE_BASE'`.
* Señal con rango físico a `NULL` (no a 0, porque 0 dBm es valor válido distinto): `int_network_clean.sql L58-L59: iff(signal_strength_dbm between -150 and -10, signal_strength_dbm, null)::number(18,2) as signal_strength_clean,` + `L85: when signal_strength_dbm is null or signal_strength_dbm not between -150 and -10 then 'INVALID_SIGNAL_TO_NULL'`.
* Macro a `NULL` (no sesgar serie temporal): `int_macro_clean.sql L39-L43: iff(ipc_source > 0 and ipc_source < 10000, ipc_source, null)::float as ipc_index,` (idem desempleo 0-100, inflación -50-100, tipo cambio 0-100) + `L59: ... then 'OUTLIER_TO_NULL'`.

### 4.3 Stripping de texto numérico con regex (solo metas)

Único dominio donde el número viene con símbolos (`'$ 1.200'`, `'45%'`, `'1,000 BOB'`):

* `int_retention_target_clean.sql L22-L26: try_to_decimal(regexp_replace(arpu_target_bob,'[^0-9.-]',''),18,2) as arpu_target_bob,` / `try_to_number(regexp_replace(active_base_target,'[^0-9.-]',''))::integer` / idem budget/churn_rate/retained. `regexp_replace` deja solo dígitos/punto/guion, `try_to_*` evita abortar si queda vacío.

*Ilustrativo numérico:*

| RAW | Regla | Silver | Flag |
|---|---|---|---|
| `null` (cargo) | `coalesce→0` (`billing L28`) | `0.00` | `TOTAL_IMPUTED_ZERO` (`L127`) |
| `-15.5` (latencia) | `greatest(...,0)` (`network L53`) | `0.00` | `NEGATIVE_METRIC_CORRECTED` (`L83`) |
| `48` (streaming h) | `least(...,24)` (`usage L53`) | `24.00` | `OUTLIER_CAPPED` (`L86`) |
| `150` (descuento %) | `least(...,100)` (`marketing L62`) | `100.0000` | `DISCOUNT_CAPPED` (`L110`) |
| `'-80 dBm'` aprox. fuera de `-150..-10` | `iff(rango,null)` (`network L58`) | `NULL` | `INVALID_SIGNAL_TO_NULL` (`L85`) |
| `'$ 1.200 BOB'` | `regexp_replace+try_to_decimal` (`retention L22`) | `1200.00` | `VALID` |

---

## 5. Imputación: tabla por tipo (qué `NULL` va a qué valor y por qué)

No hay imputación estadística (medias/medianas). Hay 5 estrategias deterministas:

| Estrategia | Patrón | Citas |
|---|---|---|
| Montos/conteos → `0` | `coalesce(col,0)` + `greatest(...,0)` | `billing L28-L37/L73-L80`, `usage L13-L22`, `payment L26-L30/L63-L67`, `retention L54-L58` |
| Booleanos → `false` | `coalesce(col,false)` / `iff(coalesce(col,0)=1,true,false)` | `subscription L29-L38: iff(coalesce(phone_service,0)=1,true,false)`, `L65-L74: coalesce(has_phone_service, false)`, `support L17-L18/L44/L48`, `network L23/L44`, `marketing L15-L18/L37-L39/L44-L46`, `customer L56/L59`, `macro L15/L32` |
| Categorías → dominio | `'UNKNOWN'/'BOB'/'NONE'/'UNASSIGNED'/'Unknown'` | `billing L50-L51`, `payment L41/L49`, `customer L31/L46-L47/L57`, `marketing L32`, `retention L36/L44` |
| Derivada (recalculada) | `coalesce(nulo, cálculo)` / `datediff` / `total recalculado` | `billing L114-L120: coalesce(amount_change, total_amount_source - previous_month_amount_clean)`, `support L60-L68: coalesce(resolution_minutes, datediff(minute,...), 0)`, `churn L51: greatest(coalesce(tenure_months_recalculated, tenure_months_source, 0),0)`, `payment L80` |
| Conservadora → `NULL` | `iff(fuera_rango, null)` | `network L58-L59` señal, `macro L39-L43` indicadores, `support L69-L70: iff(satisfaction_score between 0 and 5, satisfaction_score, null)`, `customer L48: nullif(upper(trim(postal_code)), '')` |

PII: `int_customer_clean.sql L19: sha2(coalesce(msisdn,''),256) as msisdn_hash,` — el `NULL` se hashea como `''` para no perder la fila y a la vez anonimizar.

## 6. Filtros `WHERE` + deduplicación + CDC (qué filas se descartan y cuáles se quedan)

### 6.1 Filtros de entrada (irrecuperable)

* Claves vacías: `billing L9-L10`, `payment L9-L10`, `customer L10`, `subscription L9-L10`, `churn L10`, `uso L8`, `soporte L8`, `red L8`, `marketing L8`, `metas L10`, `xref L13-L14` — siempre `nullif(trim(id),'') is not null`.
* Rangos imposibles: `customer L11: and age between 18 and 120`, `billing L12: and coalesce(total_amount,0) >= 0`, `payment L11-L12: and coalesce(amount_due,0) >= 0 and coalesce(amount_paid,0) >= 0`.
* Llave mensual: `macro L18`, `retention L11` (regex).
* Borrados CDC: `support L8: ... and _ab_cdc_deleted_at is null`, `network L8-L9: and _ab_cdc_deleted_at is null` — respeta deletes de la fuente, no los resucita.

### 6.2 Deduplicación determinista 2 niveles

Nivel 1 (Bronze, qué evento gana): quedarse con la extracción Airbyte más reciente:

* `int_billing_clean.sql L13-L16: qualify row_number() over ( partition by trim(invoice_id) order by _airbyte_extracted_at desc, _airbyte_generation_id desc ) = 1`
* Idem `payment L13-L16 (trim(payment_id))`, `customer L12-L15 (trim(customer_id))`, `subscription L12-L15`, `churn L12-L15`, `uso L9`, `soporte L9`, `red L10`, `marketing L9`, `macro L19-L22`, `metas L12-L15`, `xref L15-L18: partition by trim(account_id), trim(customer_id)`.

Nivel 2 (Silver, tras normalizar): `int_billing_clean.sql L135: qualify row_number() over (partition by invoice_id order by source_extracted_at desc) = 1` (idem `payment L110-L113` con `order by source_extracted_at desc, payment_date desc nulls last`, `customer L123`, `subscription L144-L147` con `contract_start_date desc`, `churn L99`, `uso L101`, `soporte L131`, `red L105`, `marketing L127`, `macro L69`, `retention L108-L111` con `last_updated_at desc nulls last`).

**Por qué `trim` dentro del `partition`:** `' 123 '` y `'123'` son la misma factura; sin `trim` quedarían duplicadas.

## 7. Reconciliación y coherencia cruzada (la prueba de que no es solo nulos)

* Facturación recalculada: `int_billing_clean.sql L86-L90: greatest(base_charge_clean + service_charge_clean + equipment_charge_clean + extra_usage_charge_clean + tax_clean - discount_clean, 0)::number(18,2) as total_amount_recalculated` + `L112: (total_amount_source - total_amount_recalculated)::number(18,2) as amount_variance,` + `L123: abs(total_amount_source - total_amount_recalculated) <= 0.05 as dq_total_match,` + `L126: when abs(...) > 0.05 then 'TOTAL_MISMATCH_REVIEW'`.
* Pagos: `int_payment_clean.sql L80: greatest(amount_due_clean - amount_paid_clean, 0)::number(18,2) as outstanding_balance,` + `L82-L83 / L90-L91 / L97: outstanding_balance_variance / dq_outstanding_reconciles / 'BALANCE_RECALCULATED'`.
* Uso peak/offpeak: `int_usage_clean.sql L81-L82: abs((peak_clean + offpeak_clean) - download_clean) <= greatest(0.10, download_clean * 0.10) as dq_peak_offpeak_reconciles,` + `L88-L89: then 'USAGE_COMPONENT_MISMATCH'`.
* Tenure churn vs maestro cliente: `int_churn_clean.sql L42-L44 + L51-L52 + L83-L86: 'TENURE_RECALCULATED' si |fuente - recalculado| > 1`.
* Monotonía retención 30/60/90 (si retuvo a 90 tuvo que retener a 60 y 30): `int_marketing_clean.sql L66-L68: (retained_30d_source or retained_60d_source or retained_90d_source) as retained_30d, / (retained_60d_source or retained_90d_source) as retained_60d,` + `L102-L104: not (retained_90d_source and not retained_60d_source) ... as dq_retention_windows_monotonic,` + `L112-L114: then 'RETENTION_WINDOWS_CORRECTED'`.
* Tope lógico: `int_retention_target_clean.sql L70/L74-L76: least(retained_clean, active_base_clean) ... 100 * least(...) / active_base_clean as retention_rate_target_pct_num,`.
* Coherencia booleano↔conteo/edad: `int_customer_clean.sql L69-L71: greatest(coalesce(number_of_dependents, 0), 0)::integer as dependents_count, / iff(age >= 65, true, false) as senior_citizen, / iff(... > 0, true, false) as has_dependents` + `L91-L105` flags `BOOLEAN_ALIGNED_TO_COUNT/FLAG_ALIGNED_TO_AGE`.

## 8. Derivadas de negocio creadas en la limpieza (no vienen de RAW)

* `int_customer_clean.sql L106-L114: age_band ('18_24'...'65_PLUS') + datediff(month, registration_date, current_date())::integer as customer_tenure_months,`
* `int_subscription_clean.sql L129-L136: is_active_subscription / churn_candidate_flag / datediff(month, contract_start_date, coalesce(contract_end_date, current_date()))::integer as tenure_months,`
* `int_churn_clean.sql L68-L75: 'CHURNED' as customer_status, / churn_recency_window / datediff(day, churn_date, current_date())::integer as days_since_churn,`
* `int_usage_clean.sql L75-L78/L92-L93: total_data_gb / total_voice_minutes / avg_data_gb_per_session / has_service_activity,`
* `int_support_clean.sql L86-L94 classification + L117-L123 sla_status (SLA_BREACH si CRITICAL/URGENT>240, HIGH>480, >1440),`
* `int_network_clean.sql L89-L94: network_quality_band ('OUTAGE_OR_DROP'/'DEGRADED'/'WEAK_SIGNAL'/'NORMAL'),`
* `int_marketing_clean.sql L95-L97: iff(campaign_cost_num > 0, (estimated_customer_value_num - campaign_cost_num) / campaign_cost_num, null)::number(18,4) as estimated_campaign_roi, + L117-L119: has_post_outcome_data,`
* `int_retention_target_clean.sql L74-L76: retention_rate_target_pct_num,`
* `int_macro_clean.sql L44: iff(tariff_shock, 1, 0) as shock_tarifa,`

## 9. Macros y tests como parte del tratamiento

* `macros/churn_utils.sql L5-L6: case when lower(trim(to_varchar({{ column_name }}))) in ('1','true','t','yes','y','si','sí','s') then true ... else {{ default_value }} end` — único `lower(trim())` centralizado para booleanos multilingüe.
* `macros/churn_utils.sql L1-L2: coalesce(cast({{ field }} as varchar), '_dbt_null_')` en `generate_surrogate_key` — nulo-segura.
* `macros/churn_utils.sql L9-L11: {% test between ... %} select * from {{ model }} where {{ column_name }} is not null and ({{ column_name }} < {{ min_value }} or {{ column_name }} > {{ max_value }})`.
* `models/churn/schema.yml L40-L50: int_customer_clean.customer_id [not_null, unique], age [not_null, between 18-120]`; `L54-L59: dependents_dq_status accepted_values ['VALID','IMPUTED_ZERO','CORRECTED_TO_ZERO','BOOLEAN_ALIGNED_TO_COUNT']`; `L64-L72: int_churn_clean.customer_id [not_null, unique, relationships to: ref('int_customer_clean')]`; `L80-L90` subscription; `L95-L104` billing/payment `not_null+unique`; `L126-L132` satisfaction `between 0-5`.

## 10. Fichas por dominio (detalle con citas ya verificadas)

* **Billing `int_billing_clean.sql`:** filtros `L9-L12`, dedup `L13-L16`, trim `L19-L22`, `upper` `L26-L27`, `coalesce→0` `L28-L37`, `UNKNOWN/BOB` `L50-L51`, `::timestamp_ltz` `L62`, `WHERE` `L64-L67`, `IFF fechas` `L72`, `greatest` `L73-L80`, recálculo `L86-L90`, varianza/imputación derivada `L112/L114-L120`, DQ `L121-L129`, dedup2 `L135`.
* **Pagos `int_payment_clean.sql`:** filtros `L9-L12`, dedup `L13-L16`, trim/upper `L19-L25`, `coalesce/greatest` `L26-L30`, `nullif` `L31`, `try_to_date` `L40`, mapeo `L42-L48`, `NONE` `L49`, re-saneamiento `L63-L67`, saldo recalculado `L80/L82-L83`, DQ `L86-L104`, dedup `L110-L113`.
* **Clientes `int_customer_clean.sql`:** filtro edad `L10-L11`, dedup `L12-L15`, `sha2(coalesce())` `L19`, `upper/initcap` `L21-L27`, `iff(coalesce()=1)` `L26/L29`, `CASE ES/EN` `L40-L55`, `coalesce bool` `L56-L57`, `try_to_date` `L58`, `greatest/iff` `L69-L71`, DQ `L91-L105`, `age_band/tenure` `L106-L114`, filtro final `L120-L123`.
* **Suscripciones `int_subscription_clean.sql`:** filtros `L9-L11`, dedup `L12-L15`, `upper` `L20-L23`, `nullif previous_plan` `L27`, `greatest` `L28`, `iff 0/1→bool` `L29-L38`, `CASE` `L46-L59`, `try_to_date x3` `L60-L62`, `coalesce bool` `L65-L74`, `IFF secuencia` `L83-L87`, DQ/flags/tenure `L114-L136`, filtro `L142-L147`.
* **Churn `int_churn_clean.sql`:** `churned in (0,1)` `L10-L11`, dedup `L12-L15`, `upper` `L21-L22`, `greatest` `L23`, `try_to_date` `L30`, crudo `L32`, `churned=1` `L35-L36`, recálculo tenure `L42-L44/L51-L52`, `LIKE` `L55-L66`, ventanas `L69-L75`, DQ `L76-L88`, filtro `L96-L99`.
* **Uso `int_usage_clean.sql`:** filtro `L8`, dedup `L9`, `greatest` `L13-L22`, `try_to_date` `L30`, `least capping` `L53/L55`, derivadas `L75-L78`, DQ `L79-L91`, `has_service_activity` `L92-L93`, filtro `L99-L101`.
* **Soporte `int_support_clean.sql`:** CDC `L8`, dedup `L9`, `upper` `L12-L14`, `try_to_timestamp/boolean/number` `L14-L20`, exige creado `L23`, `try_to_timestamp` `L30-L31`, `CASE ES/EN` `L32-L38`, `coalesce UNKNOWN/false` `L39-L48`, `IFF` `L57-L58`, `datediff imputación` `L60-L68`, `iff rango` `L69-L70`, `LIKE` `L86-L94`, DQ/SLA `L100-L123`, filtro `L129-L131`.
* **Red `int_network_clean.sql`:** CDC `L8-L9`, dedup `L10`, `try_to_timestamp` `L13/L26`, `upper` `L15-L16`, `coalesce/try_to_boolean` `L17-L23`, `UNKNOWN` `L34-L36`, `least/greatest/iff` `L53-L60`, DQ/banda `L79-L97`, filtro `L103-L105`.
* **Marketing `int_marketing_clean.sql`:** filtro `L8`, dedup `L9`, `initcap/upper` `L11-L13`, `coalesce bool` `L15-L18`, `greatest` `L19-L22`, `nullif` `L23`, `Unknown/UNKNOWN` `L31-L36`, `try_to_date x2` `L33-L34`, `IFF respuesta` `L56-L61`, `least capping` `L62-L65`, monotonía `L66-L68`, ROI/DQ `L95-L116`, `has_post_outcome` `L117-L119`, filtro `L125-L127`.
* **Macro `int_macro_clean.sql`:** `trim/to_date/casts/iff` `L9-L16`, regex+dedup `L18-L22`, `try_to_date` `L27`, `::float/coalesce` `L28-L33`, `iff→null` `L39-L43`, `shock` `L44`, DQ `L45-L61`, filtro `L67-L69`.
* **Metas `int_retention_target_clean.sql`:** regex `L10-L11`, dedup `L12-L15`, `trim/upper` `L18-L20`, `to_date` `L21`, `try_to_decimal(regexp_replace())` `L22-L26`, `UNKNOWN/UNASSIGNED` `L36/L44`, `try_to_date/timestamp` `L38/L45`, `greatest/least` `L54-L58`, `least/tope/tasa` `L70/L74-L76`, 4 DQ `L79-L100`, filtro `L106-L111`.
* **Xref `int_xref_customer_clean.sql`:** `trim/upper` `L7-L10`, filtros `L13-L14`, dedup compuesta `L15-L18` — sin imputación/DQ (control negativo).

## 11. Matriz técnica → dónde (índice rápido)

| Técnica | Dónde (líneas) |
|---|---|
| `trim` | todos `int_*_clean` bloque `bronze_prepared` (ej. `billing L19-L22`, `xref L7-L8`) |
| `upper/initcap` | `billing L26-L27`, `customer L21-L27`, `subscription L20-L23`, `support L12-L14`, `network L15-L16`, `marketing L12-L13` |
| `try_to_date` | `payment L40`, `customer L58`, `subscription L60-L62`, `churn L30`, `uso L30`, `marketing L33-L34`, `macro L27`, `retention L38` |
| `try_to_timestamp_ntz` | `support L14-L15/L30-L31`, `network L13/L26`, `retention L29/L45` |
| `try_to_boolean/number/decimal` | `support L17-L19`, `network L23`, `retention L22-L26` |
| `regexp_like/replace` | `macro L18/L45`, `retention L11/L22-L26` |
| `to_char/to_date/date_trunc` | `billing L25`, `payment L23`, `uso L12`, `support L16`, `network L14`, `macro L10`, `retention L21` |
| `coalesce→0/false/dominio` | `billing L28-L37/L50-L51`, `payment L26-L27/L41/L49`, `customer L30-L31/L56-L57`, `subscription L29-L38/L65-L74`, `support L17-L18/L39-L48`, `marketing L15-L18/L32/L35-L46` |
| `greatest/least/iff rango` | `billing L73-L80`, `uso L13-L22/L53/L55`, `payment L28-L30/L63-L67`, `network L53-L60`, `marketing L62-L65`, `macro L39-L43`, `retention L54-L58` |
| `WHERE` | `billing L9-L12/L64-L67`, `customer L10-L11/L120-L122`, `churn L10-L11/L96-L98`, `macro L67-L68`, `soporte L8/L23`, `red L8/L26` |
| `QUALIFY row_number` | `billing L13-L16/L135`, `payment L13-L16/L110-L113`, `customer L12-L15/L123`, `subscription L12-L15/L144-L147`, `xref L15-L18` |
| `IFF secuencia→NULL` | `billing L72`, `subscription L83-L86`, `support L57-L58`, `marketing L56-L61` |
| `datediff/dateadd` | `customer L114`, `subscription L135-L136`, `churn L42-L44/L70-L75`, `support L64` |
| `CASE/LIKE` | `customer L40-L55`, `subscription L46-L59`, `payment L42-L48`, `support L32-L43/L86-L94`, `churn L55-L66` |
| `reconciliación` | `billing L86-L90/L112`, `payment L80/L82`, `uso L81-L82`, `churn L42-L52`, `marketing L66-L68`, `retention L70/L74-L76` |
| `dq/status` | `billing L121-L129`, `payment L86-L104`, `customer L91-L105`, `subscription L114-L128`, `churn L76-L88`, `uso L79-L91`, `support L100-L123`, `network L79-L97`, `marketing L98-L116`, `macro L45-L61`, `retention L79-L100` |

## 12. Qué NO se hizo

* Bronze no tipa/filtra (`select *`).
* Sin `LOWER()` en cleans (solo `macros/churn_utils.sql L6`).
* Sin regex general (solo `macro L18`, `retention L22-L26`).
* Sin medias/medianas: imputación `0/false/dominio/derivada/null-conservador`.
* `xref` sin imputación/DQ.
* Filas solo se eliminan por clave vacía, fecha nula/futura o rango imposible; lo demás se corrige y se marca (`TO_NULL/CORRECTED/CAPPED/RECALCULATED/REVIEW`).

## 13. Conclusión

El ajuste de formato (fechas §2, texto §3, números §4) + validación (filtros, secuencia, futuro, reconciliación) + trazabilidad (DQ + `schema.yml`) es lo que hace reutilizable el dato. Los nulos son solo la capa superficial: lo diferencial es **capping físico, normalización bilingüe, `try_*` anti-caída, fechas inválidas→`NULL` sin perder la fila, y recálculos auditables con `*_source` + `*_variance` + `dq_*`**.
