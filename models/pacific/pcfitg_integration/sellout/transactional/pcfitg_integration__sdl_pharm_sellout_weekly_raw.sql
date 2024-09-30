with
sdl_pharm_sellout_probe as
(
    select * from {{ source('pcfsdl_raw', 'sdl_pharm_sellout_probe') }}
    where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_pharm_sellout_probe__duplicate_test')}}
    )
),
sdl_pharm_sellout_outlet as
(
    select * from {{ source('pcfsdl_raw', 'sdl_pharm_sellout_outlet') }}
    where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_pharm_sellout_outlet__duplicate_test')}}
    )
),
sdl_pharm_sellout_product as
(
    select * from {{ source('pcfsdl_raw', 'sdl_pharm_sellout_product') }}
    where filename not in (
        select distinct file_name from {{source('pcfwks_integration','TRATBL_sdl_pharm_sellout_product__duplicate_test')}}
    )
),
outlet as 
(
    select 
            outletnumber,
            name
    from (
            select 
                outletnumber,
                name,
                row_number() over ( partition by outletnumber order by outletnumber) as "row_number"
            from sdl_pharm_sellout_outlet
        )
    where "row_number" = 1
    order by 1,2
),
trans as
(
select 
    probe.outletnumber,
    outlet.name,
    probe.pfc,
    product.pack_long_desc,
    (
        substring(probe.weekendingdate, 7, 2) || substring (probe.weekendingdate, 5, 2) || substring (probe.weekendingdate, 1, 4)
    ) as week_ending_date,
    probe.units,
    case
        when probe.units != 0 then trunc(probe.value / probe.units)
        else 0
    end derived_price,
    probe.units * derived_price as amount,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    null as run_id
from sdl_pharm_sellout_probe probe
    left join outlet on probe.outletnumber = outlet.outletnumber
    left join sdl_pharm_sellout_product product on probe.pfc = product.pfc
),
final as
(
select 
    outletnumber::varchar(10) as outlet_number,
	name::varchar(30) as name,
	pfc::varchar(10) as pfc,
	pack_long_desc::varchar(100) as pack_long_desc,
	week_ending_date::varchar(8) as week_ending_date,
	units::number(18,0) as units,
	derived_price::number(18,6) as derived_price,
	amount::number(18,6) as amount,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	run_id::number(14,0) as run_id
from trans
)
select * from final