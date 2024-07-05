with source as
(
    select * from {{ source('jpndclsdl_raw', 'sdl_mds_jp_dcl_mt_h_product') }}
),

final as
(
    select
		code::varchar(500) as "ci-code",
		"happy bag flag_code"::varchar(500) as "happy bag flag",
		"outlet flag_code"::varchar(500) as "outlet flag",
		"family sale flag_code"::varchar(500) as "family sale flag",
		flag01::varchar(500) as flag01,
		flag02::varchar(500) as flag02,
		flag03::varchar(500) as flag03,
		flag04::varchar(500) as flag04,
		flag05::varchar(500) as flag05,
		flag06::varchar(500) as flag06,
		flag07::varchar(500) as flag07,
		flag08::varchar(500) as flag08,
		flag09::varchar(500) as flag09,
		flag10::varchar(500) as flag10,
		description::varchar(800) as description
    from source
)

select * from final