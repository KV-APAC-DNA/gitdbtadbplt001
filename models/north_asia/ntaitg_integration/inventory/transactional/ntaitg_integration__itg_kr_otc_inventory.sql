{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= " {% if is_incremental() %}
                    delete from {{this}} where nvl(mnth_id, '#') || nvl(matl_num, '#') || nvl(distributor_cd, '#') in 
                    (
                    select distinct nvl(mnth_id, '#') || nvl(matl_num, '#') || nvl(distributor_cd, '#')
                    from {{ source('ntasdl_raw', 'sdl_kr_otc_inventory') }} where file_name not in
                    (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__null_test') }}
                    union all
                    select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__duplicate_test') }}
                    )
                                    );
                                    {% endif %}
                                    "
                    )
}}
with source as (
    select *,dense_rank() over(partition by nvl(mnth_id, '#'),nvl(matl_num, '#'),nvl(distributor_cd, '#') 
    order by file_name desc
    ) rnk from {{ source('ntasdl_raw', 'sdl_kr_otc_inventory') }} where file_name not in
     (select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__null_test') }}
      union all
      select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_otc_inventory__duplicate_test') }}
     ) qualify rnk = 1

),
final as 
(
    SELECT 
        mnth_id::varchar(20) as mnth_id,
        matl_num::varchar(50) as matl_num,
        brand::varchar(255) as brand,
        product_name::varchar(255) as product_name,
        distributor_cd::varchar(50) as distributor_cd,
        cast(replace(nvl(unit_price, '0'), ',', '') as numeric(20, 4)) as unit_price,
        cast( replace(nvl(inv_qty, '0'), ',', '') as numeric(20, 4)) as inv_qty,
        cast(replace(replace(inv_amt, '', '0'), ',', '') as numeric(20, 4)) as inv_amt,
        filename::varchar(255) as filename,
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    FROM source
)
select * from final
