with itg_id_ps_msl_reference as 
(
    select * from {{ref('idnitg_integration__itg_id_ps_msl_reference')}}
),
edw_id_ps_msl_osa as
(
    select * from {{ref('idnedw_integration__edw_id_ps_msl_osa')}}
),
final as 
(
    select distinct
        msl.sku::varchar(100) as sku,
        msl.sku_variant::varchar(100) as sku_variant,
        msl.sub_brand::varchar(50) as franchise,
        msl.brand::varchar(50) as brand,
        msl.sub_brand::varchar(50) as sub_brand,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from itg_id_ps_msl_reference msl
    left join edw_id_ps_msl_osa pa on upper (msl.sku) = upper (pa.put_up_sku)
)
select * from final
