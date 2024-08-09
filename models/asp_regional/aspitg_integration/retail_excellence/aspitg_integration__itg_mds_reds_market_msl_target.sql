with
source as (

    select * from {{ source('aspsdl_raw', 'sdl_mds_mds_reds_market_msl_target') }}

),

final as
(
  select
    NAME::VARCHAR(500) as NAME,
	CODE ::VARCHAR(500) as CODE,
	START_MONTH_NAME::VARCHAR(500) as START_MONTH_NAME,
	START_YEAR_CODE::VARCHAR(500) as START_YEAR_CODE,
	END_MONTH_NAME :: VARCHAR(500) END_MONTH_NAME,
	END_YEAR_CODE :: VARCHAR(500) as END_YEAR_CODE,
	MARKET :: VARCHAR(200) as MARKET,
	BRAND_CODE :: VARCHAR(500) as BRAND_CODE,
	MDP_TARGET :: NUMBER(28,0) as MDP_TARGET,
	"%_GROWTH" :: NUMBER(28,2) as "%_GROWTH",
	LASTCHGDATETIME :: TIMESTAMP_NTZ(9) as LASTCHGDATETIME,
	VALIDATIONSTATUS :: VARCHAR(500) as VALIDATIONSTATUS
  from source
)
select * from final



