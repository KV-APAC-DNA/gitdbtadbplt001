{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}

with edw_demand_forecast_snapshot as(
    select * from {{ ref('pcfedw_integration__edw_demand_forecast_snapshot_temp') }}
),
vw_dmnd_frcst_customer_dim as(
    select * from {{ ref('pcfedw_integration__edw_demand_frcst_customer_dim') }}
),
vw_material_dim as(
    select * from {{ ref('pcfedw_integration__material_dim_tbl') }}
),
vw_apo_parent_child_dim as(
    select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),
mstrcd as(
    select distinct
        master_code,
        parent_matl_desc
    from vw_apo_parent_child_dim
    where
    to_char(cmp_id) = '7470'
    union all
    select distinct
        to_char(master_code),
        parent_matl_desc
    from vw_apo_parent_child_dim
    where
    not master_code in (
        select distinct
        to_char(master_code)
        from vw_apo_parent_child_dim
        where
        to_char(cmp_id) = '7470'
    )
),

transformed as
(
    select * from mstrcd
)

select * from transformed
