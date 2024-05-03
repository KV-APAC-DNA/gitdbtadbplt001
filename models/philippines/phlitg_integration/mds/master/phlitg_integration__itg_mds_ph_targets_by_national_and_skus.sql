{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['year'],
        pre_hook= "delete from {{this}} where (nvl(cast(year as numeric(31)), 9999)) in (select distinct nvl(year, 9999) from {{ source('phlsdl_raw', 'sdl_mds_ph_targets_by_national_and_skus') }});"
    )
}}

with sdl_mds_ph_targets_by_national_and_skus as 
(
    select * from {{ source('phlsdl_raw', 'sdl_mds_ph_targets_by_national_and_skus') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
trans as 
(
        select cast(year as varchar) as year,
            decode(
                mnth_long,
                'JANUARY',
                '01',
                'FEBRUARY',
                '02',
                'MARCH',
                '03',
                'APRIL',
                '04',
                'MAY',
                '05',
                'JUNE',
                '06',
                'JULY',
                '07',
                'AUGUST',
                '08',
                'SEPTEMBER',
                '09',
                'OCTOBER',
                '10',
                'NOVEMBER',
                '11',
                'DECEMBER',
                '12'
            ) AS month,
            master_code as master_cd,
            sbu,
            item_code as item_cd,
            description as item_desc,
            brand,
            decode(
                mnth_long,
                'JANUARY',
                jan_qty,
                'FEBRUARY',
                feb_qty,
                'MARCH',
                mar_qty,
                'APRIL',
                apr_qty,
                'MAY',
                MAY_qty,
                'JUNE',
                jun_qty,
                'JULY',
                jul_qty,
                'AUGUST',
                aug_qty,
                'SEPTEMBER',
                sep_qty,
                'OCTOBER',
                sep_qty,
                'NOVEMBER',
                sep_qty,
                'DECEMBER',
                sep_qty
            ) as target_qty,
            decode(
                mnth_long,
                'JANUARY',
                jan_value,
                'FEBRUARY',
                feb_value,
                'MARCH',
                mar_value,
                'APRIL',
                apr_value,
                'MAY',
                MAY_value,
                'JUNE',
                jun_value,
                'JULY',
                jul_value,
                'AUGUST',
                aug_value,
                'SEPTEMBER',
                sep_value,
                'OCTOBER',
                oct_value,
                'NOVEMBER',
                nov_value,
                'DECEMBER',
                dec_value
            ) as target_value
        from sdl_mds_ph_targets_by_national_and_skus as t1,
            (
                select distinct mnth_long
                from edw_vw_os_time_dim
            ) as t2
),
final as
(
    select 
    year::varchar(10) as year,
	month::varchar(10) as month,
	master_cd::varchar(100) as master_cd,
	sbu::varchar(100) as sbu,
	item_cd::varchar(50) as item_cd,
	item_desc::varchar(255) as item_desc,
	brand::varchar(255) as brand,
	target_qty::number(31,7) as target_qty,
	target_value::number(31,7) as target_value,
    current_timestamp()::timestamp_ntz(9) as crtd_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
from trans
)
select * from final