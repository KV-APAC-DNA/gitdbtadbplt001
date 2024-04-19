{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        pre_hook = "delete from {{this}} where (upper(sap_parent_customer_desc),ltrim(dstr_prod_cd, '0'),coalesce(ltrim(matl_num, '0'), 'NA'),coalesce(ltrim(ean, '0'), 'NA'),so_date) in 
            (
                select upper(sap_parent_customer_desc),ltrim(dstr_prod_cd, '0'),coalesce(ltrim(matl_num, '0'), 'NA'),coalesce(ltrim(ean, '0'), 'NA'),so_date
                from
                ( 
                    select * from {{ ref('pcfwks_integration__wks_dstr_so_api') }}
                    union all
                    select * from {{ ref('pcfwks_integration__wks_dstr_so_chs') }}
                    union all
                    select * from {{ ref('pcfwks_integration__wks_dstr_so_sigma') }}
                    union all
                    select * from {{ ref('pcfwks_integration__wks_dstr_so_symbion') }}
                )
            )
        "
    )
}}
with wks_dstr_so_api as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_so_api') }}
),
wks_dstr_so_chs as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_so_chs') }}
),
wks_dstr_so_sigma as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_so_sigma') }}
),
wks_dstr_so_symbion as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_so_symbion') }}
),
api as
(
    select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        so_date,
        so_qty,
        std_cost
    from wks_dstr_so_api
),
chs as
(
    select
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        so_date,
        so_qty,
        std_cost
    from wks_dstr_so_chs
),
sigma as
(
    select
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        so_date,
        so_qty,
        std_cost
    from wks_dstr_so_sigma
),
symbion as
(
    select
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        so_date,
        so_qty,
        std_cost
    from wks_dstr_so_symbion
),
transformed as
(
    select * from api
    union all
    select * from chs
    union all
    select * from sigma
    union all
    select * from symbion
),
final as
(
    select
        sap_parent_customer_key::varchar(12) as sap_parent_customer_key,
        sap_parent_customer_desc::varchar(50) as sap_parent_customer_desc,
        dstr_prod_cd::varchar(30) as dstr_prod_cd,
        dstr_product_desc::varchar(100) as dstr_product_desc,
        matl_num::varchar(30) as matl_num,
        ean::varchar(30) as ean,
        so_date::date as so_date,
        so_qty::number(16,4) as so_qty,
        std_cost::number(16,4) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
    from transformed
)
select * from final