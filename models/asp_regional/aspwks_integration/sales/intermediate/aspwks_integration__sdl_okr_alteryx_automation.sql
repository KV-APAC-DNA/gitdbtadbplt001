with wks_okr_alteryx_automation as(
    select * from {{ source('aspsdl_raw', 'sdl_okr_alteryx_automation') }}
),
edw_okr_brand_map as(
    select * from {{ source('aspedw_integration', 'edw_okr_brand_map') }}
),
final as(
    select case when kpi = 'CM' then 'Contribution_margin' else kpi end as kpi,
       datatype,
       case when cluster = 'MA' then 'Metropolitan Asia'
            when cluster = 'ANZ' then 'Pacific'
            when cluster = 'One CHINA' then 'China'
            when cluster = 'ONE JP' then 'Japan'
            when cluster = 'South Asia' then 'Southern Asia'
            else cluster end as cluster ,
       case when market = 'China Personal Care' then 'China PC'
            when market = 'China Selfcare' then 'China Self'
            when market = 'Japan JJKK' then 'JJKK'
			when market = 'DCL Japan' then 'Japan DCL'
			when market = 'Korea' then 'South Korea'
            else market end as market,
       case when (fact.brand is not null or fact.brand <> '') then br_map.segment else fact.segment end as segment ,
       case when fact.brand = 'Ci Labo' then 'Dr. Ci: Labo'
            else fact.brand end as brand,
  yearmonth,
  year,
  quarter,
  actuals,
  target,
  target_type,
filename,
run_id ,
crt_dttm
from wks_okr_alteryx_automation fact
left join edw_okr_brand_map br_map on upper(case when fact.brand = 'Ci Labo' then 'Dr. Ci: Labo'
            else fact.brand end) = upper(br_map.brand) and (fact.brand  is not null or fact.brand <> '')
)
select *  from final





