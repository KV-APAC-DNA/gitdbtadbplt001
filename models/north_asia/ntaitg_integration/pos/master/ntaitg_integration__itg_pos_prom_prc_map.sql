{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        unique_key=["cust_prod_cd","barcd", "cust"],
        pre_hook = ["delete from {{this}} itg_pos_prom_prc_map using {{ ref('ntawks_integration__wks_itg_pos_promotional_price_map') }} wks_itg_pos_promotional_price_map where wks_itg_pos_promotional_price_map.barcode = itg_pos_prom_prc_map.barcd and  wks_itg_pos_promotional_price_map.customer=itg_pos_prom_prc_map.cust and  wks_itg_pos_promotional_price_map.cust_prod_cd=itg_pos_prom_prc_map.cust_prod_cd and   wks_itg_pos_promotional_price_map.chng_flg = 'U';"]
    )
}}

with source as(
    select * from {{ ref('ntawks_integration__wks_itg_pos_promotional_price_map') }} 
),
final as(
    select 
        customer::varchar(20) as cust,
        barcode::varchar(20) as barcd,
        cust_prod_cd::varchar(20) as cust_prod_cd,
        promotional_price::number(30,4) as prom_prc,
        to_date(promotion_start_date) as prom_strt_dt,
        to_date(promotion_end_date) as prom_end_dt,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm 
    from source
)
select * from final