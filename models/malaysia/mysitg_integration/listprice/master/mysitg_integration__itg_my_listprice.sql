with source as(
    select * from {{ source('myssdl_raw', 'sdl_my_listprice') }}
),
final as(
    select 
       plant::varchar(4) as plant,
       cnty::varchar(4) as cnty,
       item_cd::varchar(20) as item_cd,
       item_desc::varchar(100) as item_desc,
       to_date(valid_from,'DD-MM-YYYY')::varchar(10) as valid_from,
       to_date(valid_to,'DD-MM-YYYY')::varchar(10) as valid_to,
       cast(rate as numeric(16,4))::numeric(20,4) as rate,
       currency::varchar(4) as currency,
       cast(field9 as numeric(16,4))::numeric(16,4) as field9,
       uom::varchar(6) as uom,
       replace(yearmo,'/','')::varchar(6) as yearmo,
       null::varchar(6) as jj_mnth_id,
       cdl_dttm::varchar(255) as cdl_dttm,
       curr_dt::timestamp_ntz(9) as crtd_dttm,
      current_timestamp::timestamp_ntz(9) as updt_dttm,
      NULL::varchar(20) as snapshot_dt
from source
)

select * from final