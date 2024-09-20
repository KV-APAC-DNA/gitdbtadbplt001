with source as(
    select * from {{ ref('jpnedw_integration__vw_copa_plan_bp') }}
),
transformed as(
    SELECT 
    year_445 as "year_445",
	half_445 as "half_445",
	quarter_445 as "quarter_445",
	month_445 as "month_445",
	acct_hier_shrt_desc as "acct_hier_shrt_desc",
	acct_hier_desc as "acct_hier_desc",
	acct_num as "acct_num",
	category as "category",
	amt_obj_crcy as "amt_obj_crcy"
    FROM source
)
select * from transformed