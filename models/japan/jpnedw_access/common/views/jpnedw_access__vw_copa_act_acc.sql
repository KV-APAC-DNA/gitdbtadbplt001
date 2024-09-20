with source as(
    select * from {{ ref('jpnedw_integration__vw_copa_act_acc') }}
),
transformed as(
    SELECT 
    year_445 as "year_445",
	month_445 as "month_445",
	half_445 as "half_445",
	quarter_445 as "quarter_445",
	bravo_account_cd as "bravo_account_cd",
	jcp_amt as "jcp_amt"
    FROM source
)
select * from transformed