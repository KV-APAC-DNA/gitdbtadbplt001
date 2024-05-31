with itg_id_ps_visibility as 
(
    select * from {{ref('idnitg_integration__itg_id_ps_visibility')}}
),
edw_vw_ps_targets as 
(
    select * from {{ref('aspedw_integration__edw_vw_ps_targets')}}
),
final as
(
    select 
        VI.outlet_id::varchar(10) as outlet_id,
        VI.outlet_name::varchar(100) as outlet_name,
        VI.province::varchar(50) as province,
        VI.city::varchar(50) as city,
        VI.channel::varchar(50) as channel,
        VI.merchandiser_id::varchar(20) as merchandiser_id,
        VI.merchandiser_name::varchar(50) as merchandiser_name,
        VI.cust_group::varchar(50) as cust_group,
        VI.input_date::date as input_date,
        VI.day_name::varchar(20) as day_name,
        VI.franchise::varchar(50) as franchise,
        VI.product_cmp_competitor_jnj::varchar(50) as product_cmp_competitor_jnj,
        VI.number_of_facing::varchar(20) as number_of_facing,
        VI.share_of_shelf::number(38,5) as share_of_shelf,
        VI.photo_link::varchar(100) as photo_link,
        cast(ft.value as numeric(38,5)) as threshold_reference,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from itg_id_ps_visibility VI
    LEFT JOIN edw_vw_ps_targets FT
        ON UPPER (VI.FRANCHISE) = UPPER (FT.ATTRIBUTE_1)
        AND TO_CHAR (VI.INPUT_DATE,'YYYYMM') = FT.ATTRIBUTE_2
        AND UPPER (FT.CHANNEL) <> 'E-COMMERCE'
        AND UPPER(MARKET)='INDONESIA'
)
select * from final
