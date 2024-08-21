{{
    config(
        materialized='incremental',
        incremental_strategy= "append"
    )
}}

with jp_pos_daily_aeon as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_aeon') }}
),
jp_pos_daily_csms as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_csms') }}
),
jp_pos_daily_dnki as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_csms') }}
),
jp_pos_daily_others as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_others') }}
),
jp_pos_daily_tsur as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_tsur') }}
),
jp_pos_daily_wlca as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_wlca') }}
),
jp_pos_daily_sugi as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_sugi') }}
),
aeon as(
    select
        TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt::VARCHAR(10) as upload_dt,
	    upload_time::varchar(8) as upload_time
    from jp_pos_daily_aeon
    
        -- this filter will only be applied on an incremental run
    where TO_DATE(jp_pos_daily_aeon.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
   
),
csms as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_csms
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_csms.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
),
dnki as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_dnki
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_dnki.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
),
otherss as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_others
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_others.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
),
tsur as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_tsur
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_tsur.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
),
sugi as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_sugi
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_sugi.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
),
wlca as(
    SELECT TRIM(store_key_1) as store_key_1,
        TRIM(store_key_2) as store_key_2,
        TRIM(jan_code) as jan_code,
        TRIM(product_name) as product_name,
        TRIM(accounting_date) as accounting_date,
        CAST(TRIM(quantity) AS INTEGER) as quantity,
        CAST(TRIM(amount) AS INTEGER) as amount,
        TRIM(account_key) as account_key,
        source_file_date as source_file_date,
        upload_dt as upload_dt,
        upload_time as upload_time
    from jp_pos_daily_wlca
        
        -- this filter will only be applied on an incremental run
        where TO_DATE(jp_pos_daily_wlca.upload_dt, 'MM-DD-YYYY') > (select max(TO_DATE(upload_dt, 'MM-DD-YYYY')) from {{this}}) 
       
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
),
final as(
    select
        store_key_1::varchar(255) as store_key_1,
        store_key_2::varchar(255) as store_key_2,
        jan_code::varchar(255) as jan_code,
        product_name::varchar(255) as product_name,
        accounting_date::varchar(10) as accounting_date,
        quantity::varchar(30) as quantity,
        amount::varchar(30) as amount,
        account_key::varchar(10) as account_key,
        source_file_date::varchar(30) as source_file_date,
        upload_dt::VARCHAR(10) as upload_dt,
	    upload_time::varchar(8) as upload_time    
    from transformed
)
select * from final


