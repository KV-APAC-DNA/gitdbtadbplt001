with source as 
(
    select * from {{ ref('indedw_integration__edw_rpt_sss_scorecard') }}
)
select
    table_rn as "table_rn",
    country as "country",
    region as "region",
    zone as "zone",
    territory as "territory",
    channel as "channel",
    retail_environment as "retail_environment",
    salesman_name as "salesman_name",
    salesman_code as "salesman_code",
    distributor_code as "distributor_code",
    distributor_name as "distributor_name",
    store_code as "store_code",
    store_name as "store_name",
    program_type as "program_type",
    franchise as "franchise",
    kpi as "kpi",
    quarter as "quarter",
    year as "year",
    source_actual_value as "source_actual_value",
    source_target_value as "source_target_value",
    actual_value as "actual_value",
    target_value as "target_value",
    compliance as "compliance",
    weight as "weight",
    kpi_score as "kpi_score",
    kpi_achievement as "kpi_achievement",
    max_potential_brand_score as "max_potential_brand_score",
    achievement_nr as "achievement_nr",
    prod_hier_l1 as "prod_hier_l1",
    prod_hier_l2 as "prod_hier_l2",
    prod_hier_l3 as "prod_hier_l3",
    prod_hier_l4 as "prod_hier_l4",
    prod_hier_l5 as "prod_hier_l5",
    prod_hier_l6 as "prod_hier_l6",
    prod_hier_l7 as "prod_hier_l7",
    prod_hier_l8 as "prod_hier_l8",
    prod_hier_l9 as "prod_hier_l9",
    actual_value_msl_l9 as "actual_value_msl_l9",
    target_value_msl_l9 as "target_value_msl_l9",
    rtruniquecode as "rtruniquecode",
    promotor_store as "promotor_store",
    crt_dtt as "crt_dtt",
    updt_dttm as "updt_dttm",
    rebase_score as "rebase_score"
from source
