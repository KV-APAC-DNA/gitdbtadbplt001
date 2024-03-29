with source as 
(
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_gt_gts_ratio') }}
),

final as
(
    select
    distributor::varchar(255) as distributor,
	"from month"::varchar(10) as from_month,
	"to month"::varchar(10) as to_month,
	percentage::number(18,4) as percentage,
	null::number(14,0) as run_id,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm
    from source
)
select * from final