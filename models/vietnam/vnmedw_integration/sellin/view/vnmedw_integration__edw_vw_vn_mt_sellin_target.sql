with itg_vn_mt_sellin_target as
(
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_target') }}
),
jnj as 
(
select 
    itg_vn_mt_sellin_target.mtd_code as cust_code
	,case 
		when (
				(len((itg_vn_mt_sellin_target.mtd_code)::text) = 6)
				and ((itg_vn_mt_sellin_target.mtd_code)::text like '6%'::character varying)::text
				)
			then 'JNJ'::character varying
		else null::character varying
		end as data_type
	,itg_vn_mt_sellin_target.target as "target"
	,itg_vn_mt_sellin_target.sellin_cycle
	,itg_vn_mt_sellin_target.sellin_year
	,itg_vn_mt_sellin_target.visit
from itg_vn_mt_sellin_target
where (
		(itg_vn_mt_sellin_target.mti_code is null)
		and (itg_vn_mt_sellin_target.mtd_code is not null)
		)
),
dksh as 
(
select 
    itg_vn_mt_sellin_target.mti_code as cust_code
	,'DKSH' as data_type
	,itg_vn_mt_sellin_target.target as "target"
	,itg_vn_mt_sellin_target.sellin_cycle
	,itg_vn_mt_sellin_target.sellin_year
	,itg_vn_mt_sellin_target.visit
from itg_vn_mt_sellin_target
where (
		(itg_vn_mt_sellin_target.mtd_code is null)
		and (itg_vn_mt_sellin_target.mti_code is not null)
		)
),
final as
(
    select * from jnj 
    union all
    select * from dksh
)

select * from final