with 
itg_ittarget as 
(
    select * from {{ ref('inditg_integration__itg_ittarget') }}
),
itg_territory as 
(
    select * from {{ source('inditg_integration', 'itg_territory') }}
),
itg_businesscalender as 
(
    select * from {{ ref('inditg_integration__itg_businesscalender') }}
),
trans as 
(
    SELECT 
coalesce(b.Territorycode,'-1') as Territory_Code,
coalesce(a.lakshyat_territory_name,'Unknown') as Territory_Name,
'2016' as Year,
CASE WHEN  m.Month='January'  THEN '1'
     WHEN  m.Month='February' THEN '2'
     WHEN  m.Month='March'    THEN '3'
     WHEN  m.Month='April'    THEN '4'
     WHEN  m.Month='May'      THEN '5'
     WHEN  m.Month='June'     THEN '6'
     WHEN  m.Month='July'     THEN '7'
     WHEN  m.Month='August'   THEN '8'
     WHEN  m.Month='September' THEN '9'
     WHEN  m.Month='October'  THEN '10'
     WHEN  m.Month='November' THEN '11'
     WHEN  m.Month='December' THEN '12'
     ELSE 0 end as month,
CASE WHEN  m.Month='January'  THEN janamount
     WHEN  m.Month='February' THEN febamount
     WHEN  m.Month='March'    THEN maramount
     WHEN  m.Month='April'    THEN apramount
     WHEN  m.Month='May'      THEN mayamount
     WHEN  m.Month='June'     THEN junamount
     WHEN  m.Month='July'     THEN julyamount
     WHEN  m.Month='August'   THEN augamount
     WHEN  m.Month='September' THEN sepamount
     WHEN  m.Month='October'  THEN octamount
     WHEN  m.Month='November' THEN novamount
     WHEN  m.Month='December' THEN decamount
     ELSE 0
     END AS Target,
current_timestamp()::timestamp_ntz(9) as CRT_DTTM,
current_timestamp()::timestamp_ntz(9) as UPDT_DTTM
FROM itg_ittarget a 
LEFT OUTER JOIN  itg_territory b on a.lakshyat_territory_name=b.territoryname
cross join(select distinct month from itg_businesscalender where year>=2014) m
-- cross join(select * from in_log.table_last_load_date where src_table_name='itg_ittarget' ) p
),
final as 
(
    select
    territory_code::number(18,0) as territory_code,
	territory_name::varchar(50) as territory_name,
	year::number(18,0) as year,
	month::number(18,0) as month,
	target::number(37,5) as target,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final