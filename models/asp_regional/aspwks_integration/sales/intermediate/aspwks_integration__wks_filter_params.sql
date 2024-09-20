with itg_mds_ap_ecom_oneview_config as(
    select * from {{ ref('aspitg_integration__itg_mds_ap_ecom_oneview_config') }}
),
edw_calendar_dim as(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_company_dim as(
    select * from {{ ref('aspedw_integration__edw_company_dim') }}
),
ctry_key as (
    select dataset, filter_value
    from itg_mds_ap_ecom_oneview_config
    where column_name = 'ctry_key'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' )
),
re as(
    select dataset, case when dataset = 'Japan (SAP)' then null else filter_value end filter_value
    from itg_mds_ap_ecom_oneview_config
    where column_name = 'retail_env'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' )
),
acct_hier_gts as(
    select dataset, filter_value
    from itg_mds_ap_ecom_oneview_config
    where column_name = 'acct_hier_shrt_desc'
    and filter_value = 'GTS'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' ) 
),
acct_hier_nts as(
    select dataset, filter_value
    from itg_mds_ap_ecom_oneview_config
    where column_name = 'acct_hier_shrt_desc'
    and filter_value = 'NTS'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' ) 
),
fisc_yr as(
    select dataset, (select fisc_yr 
        from edw_calendar_dim 
        where to_date(cal_day) = to_date(current_timestamp())) - filter_value as filter_value 
    from itg_mds_ap_ecom_oneview_config

    where column_name = 'fisc_yr'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)' ,'Singapore (SAP)','Japan (SAP)','Indonesia (SAP)')

),
clust as(
    select dataset, filter_value  
    from itg_mds_ap_ecom_oneview_config 
    where column_name = 'cluster'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' )
),
customer as(
    select dataset, filter_value  
    from itg_mds_ap_ecom_oneview_config 
    where column_name = 'cust_num'
    and dataset  in ('Philippines (SAP)','Thailand (SAP)','Malaysia (SAP)','Hong Kong (SAP)',
                        'Taiwan (SAP)','Korea (SAP)','Indonesia (SAP)','Singapore (SAP)','Japan (SAP)','Indonesia (SAP)' )
),
filters as(
    select ctry_key.dataset, ctry_key.filter_value ctry_key, re.filter_value retail_env
                    , acct_hier_gts.filter_value gts
                    , acct_hier_nts.filter_value nts, fisc_yr.filter_value min_year
                    , clust.filter_value as cluster_filter, customer.filter_value as cust_filter
    from ctry_key, re, acct_hier_gts, acct_hier_nts, fisc_yr, clust ,customer                
    where ctry_key. dataset = re.dataset  
        and ctry_key. dataset = acct_hier_gts.dataset  
        and ctry_key. dataset = acct_hier_nts.dataset  
        and ctry_key. dataset = fisc_yr.dataset  
        and clust.dataset (+) = ctry_key. dataset 
        and  customer.dataset(+) = ctry_key. dataset 
),
transformed as(
    Select  ctry_group, cy."cluster", cy.ctry_key , cy.co_cd, filters.retail_env, filters.gts, filters.nts, filters.min_year, 
            filters.cluster_filter, filters.cust_filter 
    From filters 
        , edw_company_dim cy  
        WHERE  cy.ctry_key = filters.ctry_key
        AND  cy."cluster"  = nvl(filters.cluster_filter, cy."cluster")
)
select * from transformed