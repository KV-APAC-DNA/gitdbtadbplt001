{{
    config(
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["lakshyat_territory_name"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}
with wks_ittarget as 
(
    select * from {{ ref('indwks_integration__wks_ittarget') }}
),
final as 
(
    select 
        lakshyat_territory_name::varchar(50) as lakshyat_territory_name,
        target_variant::varchar(50) as target_variant,
        janamount::number(37,5) as janamount,
        febamount::number(37,5) as febamount,
        maramount::number(37,5) as maramount,
        apramount::number(37,5) as apramount,
        mayamount::number(37,5) as mayamount,
        junamount::number(37,5) as junamount,
        julyamount::number(37,5) as julyamount,
        augamount::number(37,5) as augamount,
        sepamount::number(37,5) as sepamount,
        octamount::number(37,5) as octamount,
        novamount::number(37,5) as novamount,
        decamount::number(37,5) as decamount,
        ytdamount::number(37,5) as ytdamount,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from wks_ittarget
)
select * from final