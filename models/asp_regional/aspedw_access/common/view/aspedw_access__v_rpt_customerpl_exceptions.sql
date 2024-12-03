with edw_rpt_customerpl_exceptions as (
    select * from {{ ref('aspedw_integration__edw_rpt_customerpl_exceptions') }}
),
final as (
    select
			error_category as "error_category",
            year  as "year",
			period  as "period",
            market as "market",
			"customer code"  as "customer code",
			"material code"  as "material code",
            profit_centr as "profit_centr",
            error_desc as "error_desc",
            corrective_action as "corrective_action"
    from edw_rpt_customerpl_exceptions
)
select * from final