{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["cust_prod_cd","barcd", "cust","prom_prc"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with source as(
    select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.wks_itg_pos_promotional_price_map
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