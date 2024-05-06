with edw_sales_org_dim as (
    select * from {{ ref('aspedw_integration__edw_sales_org_dim') }}
),
final as (
    select
			clnt  as "clnt",
			sls_org  as "sls_org",
			sls_org_nm  as "sls_org_nm",
			stats_crncy  as "stats_crncy",
			sls_org_co_cd  as "sls_org_co_cd",
			cust_num_intco_bill  as "cust_num_intco_bill",
			ctry_key  as "ctry_key",
			crncy_key as "crncy_key",
			fisc_yr_vrnt as "fisc_yr_vrnt",
			crt_dttm  as "crt_dttm",
			updt_dttm as "updt_dttm"
    from edw_sales_org_dim
)
select * from final