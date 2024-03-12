with edw_vw_th_sellout_sales_foc_fact as(
 select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ source('aspitg_integration','edw_vw_os_time_dim') }}
),
edw_vw_th_sellout_sales_foc_fact as (
    select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
itg_th_dstrbtr_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }}
),
itg_th_dstrbtr_customer_dim_snapshot as (
    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim_snapshot') }}
),
itg_th_dstrbtr_material_dim as (
   select * from {{ ref('thaitg_integration__itg_th_dstrbtr_material_dim') }} 
),
itg_th_gt_dstrbtr_control as (
    select * from {{ ref('thaitg_integration__itg_th_gt_dstrbtr_control') }}
),
itg_th_gt_target_sales_re as (
    select * from {{ ref('thaitg_integration__itg_th_gt_target_sales_re') }}
),
itg_th_target_sales as (
    select * from {{ ref('thaitg_integration__itg_th_target_sales') }}
),
edw_vw_th_gt_msl_distribution as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_msl_distribution
),
edw_vw_th_gt_route as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_route
),
edw_vw_th_gt_sales_order as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_sales_order
),
edw_vw_th_gt_schedule as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_schedule
),
edw_vw_th_gt_visit as (
    select * from dev_dna_core.snenav01_workspace.thaedw_integration__edw_vw_th_gt_visit
)








