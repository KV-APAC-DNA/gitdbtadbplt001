{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append',
        pre_hook = ["{{build_dw_pos_daily_temp()}}","{% if is_incremental() %}
                    DELETE FROM {{ ref('jpnwks_integration__wk_pos_daily_aeon')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'AEON' );
                     delete FROM {{ ref('jpnwks_integration__wk_pos_daily_aeon')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_csms')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'CSMS' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_csms')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_dnki')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'DNKI' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_dnki')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_others')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' then to_date(TO_CHAR(TO_DATE(trim(accounting_date), 'YYYY/MM/DD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key NOT IN ('AEON', 'CSMS', 'DNKI', 'SUGI', 'TSUR', 'SUGI', 'WLCA') );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_others')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_sugi')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'SUGI' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_sugi')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_tsur')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'TSUR' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_tsur')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_wlca')}} WHERE ( TRIM(store_key_1) || TRIM(coalesce(store_key_2, '')) || TRIM(jan_code) || TRIM(product_name) || cast(( CASE  WHEN accounting_date LIKE '%/%/%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') WHEN accounting_date LIKE '________' THEN TO_DATE(to_char(to_date(trim(accounting_date), 'YYYYMMDD'), 'YYYY-MM-DD'), 'YYYY-MM-DD') WHEN accounting_date LIKE '%-%-%' THEN to_date(trim(accounting_date), 'YYYY-MM-DD') END ) AS VARCHAR) || CASE  WHEN quantity LIKE '%(%)%' THEN TRIM(replace(replace(quantity, '(', '-'), ')', '')) ELSE TRIM(quantity) END || CASE  WHEN amount LIKE '%(%)%' THEN TRIM(replace(replace(amount, '(', '-'), ')', '')) ELSE TRIM(amount) END ) IN ( SELECT DISTINCT (store_key_1 || coalesce(store_key_2, '') || jan_code || product_name || cast(accounting_date AS VARCHAR) || cast(quantity AS VARCHAR) || cast(amount AS VARCHAR)) AS str1 FROM {{ source('jpnedw_integration', 'dw_pos_daily_temp') }} WHERE account_key = 'WLCA' );
                    delete FROM {{ ref('jpnwks_integration__wk_pos_daily_wlca')}} WHERE ( store_key_1 IS NULL AND store_key_2 IS NULL AND jan_code IS NULL AND product_name IS NULL AND accounting_date IS NULL AND quantity IS NULL AND amount IS NULL ) OR ( store_key_1 = '' AND store_key_2 = '' AND jan_code = '' AND product_name = '' AND accounting_date = '' AND quantity = '' AND amount = '' );
                    {% endif %}"]
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
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_aeon A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('AEON', 'aeon')
         {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
csms as(
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_csms A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('CSMS', 'csms')
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
dnki as(
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_dnki A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('DNKI', 'dnki')
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
tsur as(
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_tsur A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('TSUR', 'tsur')
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
sugi as(
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_sugi A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('SUGI', 'sugi')
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
wlca as(
    SELECT 
        A.store_key_1 as store_key_1,
        A.store_key_2 as store_key_2,
        A.jan_code as jan_code,
        A.product_name as product_name,
        A.accounting_date as accounting_date,
        A.quantity as quantity,
        A.amount as amount,
        A.account_key as account_key,
        A.source_file_date as source_file_date,
        A.upload_dt as upload_dt,
        A.upload_time as upload_time,
        'Account_key is not valid' as issue_description
    FROM wk_pos_daily_wlca A
    WHERE A.account_key = ' '
        OR A.account_key = ''
        OR A.account_key IS NULL
        OR A.account_key NOT IN ('WLCA', 'wlca')
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and A.upload_dt > (select max(upload_dt) from {{ this }}) 
        {% endif %}
),
transformed as(
    select * from aeon
    union all
    select * from csms
    union all
    select * from dnki
    union all
    select * from tsur
    union all
    select * from wlca
    union all
    select * from sugi
)
select * from transformed