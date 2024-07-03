with edw_copa_trans_fact as (
    select * from {{ ref('aspedw_integration__edw_copa_trans_fact') }}
),
itg_query_parameters as
(
    select * from {{ source('inditg_integration', 'itg_query_parameters') }}
),
transformed as
(
    SELECT
    matl_num,
    chrt_acct,
    acct_num,
    dstr_chnl,
    ctry_key,
    caln_yr_mo,
    fisc_yr,
    fisc_yr_per,
    acct_hier_desc,
    acct_hier_shrt_desc,
    co_cd,
    SUM(amt_obj_crncy) AS amt_obj_crncy,
    SUM(qty) AS qty FROM edw_copa_trans_fact WHERE co_cd IN ('8080', '9860')
    AND acct_hier_shrt_desc <> 'NTS'
    AND LEFT(caln_yr_mo, 4) >= EXTRACT(YEAR FROM SYSDATE) - (
      SELECT CAST(PARAMETER_VALUE AS INTEGER) AS PARAMETER_VALUE
      FROM ITG_QUERY_PARAMETERS
      WHERE UPPER(COUNTRY_CODE) = 'IN'
        AND UPPER(PARAMETER_TYPE) = 'DATA_RETENTION_PERIOD'
        AND UPPER(PARAMETER_NAME) = 'IN_FIN_SIM_DATA_RETENTION_PERIOD'
      )
    --Taking only >=2021
    GROUP BY matl_num,
    chrt_acct,
    acct_num,
    dstr_chnl,
    ctry_key,
    caln_yr_mo,
    fisc_yr,
    fisc_yr_per,
    acct_hier_desc,
    acct_hier_shrt_desc,
    co_cd
),
final as
(
    select
        matl_num::varchar(18) as matl_num,
	    chrt_acct::varchar(4) as chrt_acct,
	    acct_num::varchar(10) as acct_num,
	    dstr_chnl::varchar(2) as dstr_chnl,
	    ctry_key::varchar(3) as ctry_key,
	    caln_yr_mo::number(18,0) as caln_yr_mo,
	    fisc_yr::number(18,0) as fisc_yr,
	    fisc_yr_per::number(18,0) as fisc_yr_per,
	    acct_hier_desc::varchar(100) as acct_hier_desc,
	    acct_hier_shrt_desc::varchar(100) as acct_hier_shrt_desc,
	    co_cd::varchar(4) as co_cd,
	    amt_obj_crncy::number(38,5) as amt_obj_crncy,
	    qty::number(38,5) as qty
        
    from transformed
)
select * from final