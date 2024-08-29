{{
    config
    (
        materialized = 'incremental',
        incremental_model = 'append'
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