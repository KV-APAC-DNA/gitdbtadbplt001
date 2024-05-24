with sdl_id_ps_planogram as(
    select * from {{ source('idnsdl_raw', 'sdl_id_ps_planogram') }}
),
union1 as(
select itg.outlet_id,
	itg.outlet_custom_code,
	itg.outlet_name,
	itg.province,
	itg.city,
	itg.channel,
	itg.merchandiser_id,
	itg.merchandiser_name,
	itg.cust_group,
	itg.address,
	itg.jnj_year,
	itg.jnj_month,
	itg.jnj_week,
	itg.day_name,
	itg.input_date,
	itg.franchise,
	itg.photo_link,
	itg.file_name,
	itg.crt_dttm
from {{this}} itg
where (
		itg.outlet_id,
		itg.merchandiser_id,
		itg.input_date,
		upper(itg.franchise),
		coalesce(itg.photo_link, 'NA')
		) not in (
		select distinct trim(sdl.outlet_id),
			trim(sdl.merchandiser_id),
			to_date(trim(sdl.input_date)),
			upper(trim(sdl.franchise)),
			coalesce(trim(sdl.photo_link), 'NA')
		from sdl_id_ps_planogram sdl
		)
),
union2 as(
select trim(sdl.outlet_id) outlet_id,
	trim(sdl.outlet_custom_code) outlet_custom_code,
	trim(sdl.outlet_name) outlet_name,
	trim(sdl.province) province,
	trim(sdl.city) city,
	trim(sdl.channel) channel,
	trim(sdl.merchandiser_id) merchandiser_id,
	trim(sdl.merchandiser_name) merchandiser_name,
	trim(sdl.cust_group) cust_group,
	trim(sdl.address) address,
	trim(sdl.jnj_year) jnj_year,
	trim(sdl.jnj_month) jnj_month,
	trim(sdl.jnj_week) jnj_week,
	trim(sdl.day_name) day_name,
	to_date(trim(sdl.input_date)) as input_date,
	trim(sdl.franchise) franchise,
	trim(sdl.photo_link) photo_link,
	sdl.file_name,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
from sdl_id_ps_planogram sdl
where (
		trim(sdl.outlet_id),
		trim(sdl.merchandiser_id),
		to_date(trim(sdl.input_date)),
		upper(trim(sdl.franchise)),
		coalesce(trim(sdl.photo_link), 'NA')
		) not in (
		select distinct itg.outlet_id,
			itg.merchandiser_id,
			itg.input_date,
			upper(itg.franchise),
			coalesce(itg.photo_link, 'NA')
		from {{this}} itg
		)
),
union3 as(
select trim(sdl.outlet_id) outlet_id,
	trim(sdl.outlet_custom_code) outlet_custom_code,
	trim(sdl.outlet_name) outlet_name,
	trim(sdl.province) province,
	trim(sdl.city) city,
	trim(sdl.channel) channel,
	trim(sdl.merchandiser_id) merchandiser_id,
	trim(sdl.merchandiser_name) merchandiser_name,
	trim(sdl.cust_group) cust_group,
	trim(sdl.address) address,
	trim(sdl.jnj_year) jnj_year,
	trim(sdl.jnj_month) jnj_month,
	trim(sdl.jnj_week) jnj_week,
	trim(sdl.day_name) day_name,
	to_date(trim(sdl.input_date)) as input_date,
	trim(sdl.franchise) franchise,
	trim(sdl.photo_link) photo_link,
	sdl.file_name,
	current_timestamp()::timestamp_ntz(9) as crt_dttm
from sdl_id_ps_planogram sdl
where (
		trim(sdl.outlet_id),
		trim(sdl.merchandiser_id),
		to_date(trim(sdl.input_date)),
		upper(trim(sdl.franchise)),
		coalesce(trim(sdl.photo_link), 'NA')
		) in (
		select distinct itg.outlet_id,
			itg.merchandiser_id,
			itg.input_date,
			upper(itg.franchise),
			coalesce(itg.photo_link, 'NA')
		from {{this}} itg
		)
),
transformed as(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
),
final as(
    select
        outlet_id::varchar(10) as outlet_id,
        outlet_custom_code::varchar(10) as outlet_custom_code,
        outlet_name::varchar(100) as outlet_name,
        province::varchar(50) as province,
        city::varchar(50) as city,
        channel::varchar(50) as channel,
        merchandiser_id::varchar(20) as merchandiser_id,
        merchandiser_name::varchar(50) as merchandiser_name,
        cust_group::varchar(50) as cust_group,
        address::varchar(255) as address,
        jnj_year::varchar(4) as jnj_year,
        jnj_month::varchar(2) as jnj_month,
        jnj_week::varchar(5) as jnj_week,
        day_name::varchar(20) as day_name,
        to_date(input_date) as input_date,
        franchise::varchar(50) as franchise,
        photo_link::varchar(100) as photo_link,
        file_name::varchar(100) as file_name,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from transformed
)
select * from final