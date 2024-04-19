{{
    config(
        materialized= "incremental",
        incremental_strategy= "delete+insert",
        pre_hook = ["delete from {{this}}
                    where (upper(sap_parent_customer_desc),ltrim(dstr_prod_cd, '0'),coalesce(ltrim(matl_num, '0'), 'NA'),coalesce(ltrim(ean, '0'), 'NA'),inv_date) in (
                            select upper(sap_parent_customer_desc),ltrim(dstr_prod_cd, '0'),coalesce(ltrim(matl_num, '0'), 'NA'),coalesce(ltrim(ean, '0'), 'NA'),inv_date
                            from
                            ( 
                                select * from {{ ref('pcfwks_integration__wks_dstr_inv_api') }}
                                union all
                                select * from {{ ref('pcfwks_integration__wks_dstr_inv_chs') }}
                                union all
                                select * from {{ ref('pcfwks_integration__wks_dstr_inv_sigma') }}
                                union all
                                select * from {{ ref('pcfwks_integration__wks_dstr_inv_symbion') }}
                            )
                        )",
                        "delete from {{this}} where (upper(sap_parent_customer_desc),ltrim(dstr_prod_cd, '0'),inv_date) in (select UPPER(SAP_PRNT_CUST_DESC),ltrim(prod_cd, '0'),inv_date from {{ ref('pcfwks_integration__wks_dstr_metcash_inv') }})"
]
    )
}}
with wks_dstr_inv_api as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_inv_api') }}
),
wks_dstr_inv_chs as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_inv_chs') }}
),
wks_dstr_inv_sigma as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_inv_sigma') }}
),
wks_dstr_inv_symbion as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_inv_symbion') }}
),
wks_dstr_metcash_inv as
(
    select * from {{ ref('pcfwks_integration__wks_dstr_metcash_inv') }}
),
api as
(
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        cast(inv_date as Date) as inv_date,
        inventory_qty,
        inventory_amt,
        back_order_qty,
        std_cost
    from wks_dstr_inv_api
),
chs as
(
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        inv_date,
        inventory_qty,
        inventory_amt,
        back_order_qty,
        std_cost
    from wks_dstr_inv_chs
),
sigma as
(
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        inv_date,
        inventory_qty,
        inventory_amt,
        back_order_qty,
        std_cost
    from wks_dstr_inv_sigma
),
symbion as
(
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        dstr_prod_cd,
        dstr_product_desc,
        matl_num,
        ean,
        inv_date,
        inventory_qty,
        inventory_amt,
        back_order_qty,
        std_cost,
    from wks_dstr_inv_symbion
),
combined as
(
        select * from api
        union all
        select * from chs
        union all
        select * from sigma
        union all
        select * from symbion
),
metcash as
(
    select
        sap_prnt_cust_key as sap_parent_customer_key,
        sap_prnt_cust_desc as sap_parent_customer_desc,
        prod_cd as dstr_prod_cd,
        prod_desc as dstr_product_desc,
        null as matl_num,
        null as ean,
        inv_date as inv_date,
        Inventory_qty as inventory_qty,
        Inventory_amt as inventory_amt,
        null as back_order_qty,
        null as std_cost,
    from wks_dstr_metcash_inv
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
        inv_date::date as inv_date,
        inventory_qty::number(16,4) as inventory_qty,
        inventory_amt::number(16,4) as inventory_amt,
        back_order_qty::number(16,4) as back_order_qty,
        std_cost::number(10,4) as std_cost,
        current_timestamp::timestamp_ntz(9) as crt_dttm
        from
        (
            select * from combined 
            union all
            select * from metcash
        )
)
select * from final

