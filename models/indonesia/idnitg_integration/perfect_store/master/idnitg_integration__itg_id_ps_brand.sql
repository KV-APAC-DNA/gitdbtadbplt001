with source as
(
    select * from {{source('idnsdl_raw', 'sdl_mds_id_ps_brand')}}
),
final as
(
    select 
        trim(franchise)::varchar(510) as franchise,
        trim(brand)::varchar(510) as brand,
        trim(rg_brand)::varchar(510) as rg_brand,
        trim(jj_brand)::varchar(200) as jj_brand,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final
