{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['sap_prnt_cust_desc', 'article_code', 'article_desc', 'inv_date'],
        pre_hook= ["delete from {{this}} where (upper(sap_prnt_cust_desc), ltrim(article_code, '0'), article_desc, inv_date) in ( select upper(sap_prnt_cust_desc), ltrim(article_code, '0'), article_desc, inv_date from {{ ref('pcfwks_integration__wks_dstr_coles_inv') }} );"," delete from {{this}} where (upper(sap_prnt_cust_desc), ltrim(article_code, '0'), article_desc, inv_date) in ( select upper(sap_prnt_cust_desc), ltrim(article_code, '0'), article_desc, inv_date from {{ ref('pcfwks_integration__wks_dstr_woolworth_inv') }} );"]
    )
}}

with source as(
    select * from {{ ref('pcfwks_integration__wks_dstr_coles_inv') }}
),
source2 as(
    select * from {{ ref('pcfwks_integration__wks_dstr_woolworth_inv') }}
),
union1 as(
    select 
        sap_prnt_cust_key::varchar(20) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        inv_date::date as inv_date,
        article_code::varchar(50) as article_code,
        article_desc::varchar(200) as article_desc,
        soh_qty::number(16,5) as soh_qty,
        soh_price::number(16,5) as soh_price,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
),
union2 as(
    select 
        sap_prnt_cust_key::varchar(20) as sap_prnt_cust_key,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        inv_date::date as inv_date,
        article_code::varchar(50) as article_code,
        article_desc::varchar(200) as article_desc,
        soh_qty::number(16,5) as soh_qty,
        soh_price::number(16,5) as soh_price,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source2
),
final as(
    select * from union1
    union all
    select * from union2
)
select * from final