with edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
transformed as(
select a.*,b.*,'Japan DCL' as reporting_market,'One DCL' as reporting_cluster from edw_company_dim a,(select 'Dr Ci Labo' as mega_brnd_desc) b where ctry_group='Japan'
union all
select a.*,b.*,'China Selfcare DCL' as reporting_market,'One DCL' as reporting_cluster  from edw_company_dim a,(select 'Dr Ci Labo' as mega_brnd_desc) b where ctry_group='China Selfcare'
union all
select a.*,b.*,'China Personal Care DCL' as reporting_market,'One DCL' as reporting_cluster  from edw_company_dim a,(select 'Dr Ci Labo' as mega_brnd_desc) b where ctry_group='China Personal Care'
union all
select a.*,b.*,'Travel Retail' as reporting_market,'One DCL' as reporting_cluster from edw_company_dim a,(select 'Dr Ci Labo' as mega_brnd_desc) b where ctry_group='APSC Regional'
union all
select a.*,b.*,'China Jupiter' as reporting_market,"cluster" as reporting_cluster  from edw_company_dim a,(select 'Jupiter (PH to CH)' as mega_brnd_desc) b where ctry_group='China Selfcare' 
union all
select a.*,b.*,ctry_group as reporting_market,'APSC Direct' as reporting_cluster  from edw_company_dim a,(select 'NA' as mega_brnd_desc) b where ctry_group='APSC Regional' 
union all
select a.*,b.*,ctry_group as reporting_market,"cluster" as reporting_cluster  from edw_company_dim a,(select 'NA' as mega_brnd_desc) b where ctry_group <> 'APSC Regional' 
)
select * from transformed