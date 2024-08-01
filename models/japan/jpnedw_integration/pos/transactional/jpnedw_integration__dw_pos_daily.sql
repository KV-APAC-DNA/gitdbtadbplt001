{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append',
        pre_hook = "{% if is_incremental() %}
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_aeon')}} WHERE account_key NOT IN ('AEON', 'aeon') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_csms')}} WHERE account_key NOT IN ('CSMS', 'csms') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_dnki')}} WHERE account_key NOT IN ('DNKI', 'dnki') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_sugi')}} WHERE account_key NOT IN ('SUGI', 'sugi') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_tsur')}} WHERE account_key NOT IN ('TSUR', 'tsur') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_wlca')}} WHERE account_key NOT IN ('WLCA', 'wlca') OR account_key = ' ' OR account_key = '' OR account_key IS NULL;                    
                    {% endif %}"
    )
}}

with wk_pos_daily_aeon as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_aeon')}}
),
wk_pos_daily_csms as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_csms')}}
),
wk_pos_daily_dnki as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_dnki')}}
),
wk_pos_daily_others as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_others')}}
),
wk_pos_daily_sugi as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_sugi')}}
),
wk_pos_daily_tsur as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_tsur')}}
),
wk_pos_daily_wlca as(
	select * from {{ ref('jpnwks_integration__wk_pos_daily_wlca')}}
),
aeon as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_aeon
         {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_aeon.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
csms as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_csms
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_csms.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
dnki as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_dnki
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_dnki.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
otherss as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
       CASE 
            WHEN accounting_date LIKE '%/%/%'
                then to_date(TO_CHAR(TO_DATE(trim(accounting_date), 'YYYY/MM/DD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_others
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_others.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
tsur as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_tsur
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_tsur.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
sugi as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_sugi
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_sugi.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
wlca as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        CASE 
            WHEN accounting_date LIKE '%/%/%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '________'
                THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD')
            WHEN accounting_date LIKE '%-%-%'
                THEN to_date(trim(accounting_date), 'YYYY-MM-DD')
            END AS accounting_date,
        CASE 
            WHEN quantity LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(quantity, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(quantity) AS INTEGER)
            END as quantity,
        CASE 
            WHEN amount LIKE '%(%)%'
                THEN CAST(TRIM(replace(replace(amount, '(', '-'), ')', '')) AS INTEGER)
            ELSE CAST(TRIM(amount) AS INTEGER)
            END as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        to_char(CURRENT_TIME, 'HH24:MI:SS') as upload_time
    from wk_pos_daily_wlca
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where wk_pos_daily_wlca.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
transformed as(
    select * from aeon
    union all
    select * from csms
    union all
    select * from dnki
    union all
    select * from otherss
    union all
    select * from tsur
    union all
    select * from wlca
    union all
    select * from sugi
)
select * from transformed