with EDW_VW_TH_MARKET_SHARE_DISTRIBUTION as 
 (select * from {{ ref('thaedw_integration__edw_vw_th_market_share_distribution') }}
),
final as (
SELECT 
cntry_cd::varchar(2) as cntry_cd,
cntry_nm::varchar(8) as cntry_nm,
chnl::varchar(11) as chnl,
mnfctrer::varchar(17) as mnfctrer,
yr_mnth::number(18,0) as yr_mnth,
measure::varchar(100) as measure,
category::varchar(256) as category,
value::number(20,5) as value
FROM EDW_VW_TH_MARKET_SHARE_DISTRIBUTION)
select * from final