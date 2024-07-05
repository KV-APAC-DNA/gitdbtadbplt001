{{
    config
    (
        materializede="incremental",
        incremental_strategy="delete+insert",
        unique_key=["distrcode","salesmancode","motherskucode"]
    )
}}
with sdl_xdm_salesmanskulinemapping as
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_salesmanskulinemapping') }}
),
final as
(
    select 
    cmpcode::varchar(10) as cmpcode,
	distrcode::varchar(50) as distrcode,
	distrbrcode::varchar(50) as distrbrcode,
	salesmancode::varchar(50) as salesmancode,
	skuline::varchar(50) as skuline,
	skuhierlevelcode::number(18,0) as skuhierlevelcode,
	skuhiervaluecode::varchar(50) as skuhiervaluecode,
	moddt::timestamp_ntz(9) as moddt,
	createddt::timestamp_ntz(9) as createddt,
	motherskucode::varchar(50) as motherskucode,
	filename::varchar(100) as filename,
	run_id::varchar(50) as run_id,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
    convert_timezone('UTC', 'Asia/Kolkata', current_timestamp())::timestamp_ntz(9) as updt_dttm
    from
    (select *, row_number() over (partition by distrcode,salesmancode,motherskucode order by createddt desc)  rn 
    from sdl_xdm_salesmanskulinemapping
    )  final 
where rn=1
    {% if is_incremental() %}
    --this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
    {% endif %}
)
select * from final