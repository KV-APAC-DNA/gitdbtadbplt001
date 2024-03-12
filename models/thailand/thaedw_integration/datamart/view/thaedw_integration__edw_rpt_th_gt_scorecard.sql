with edw_vw_th_sellout_sales_foc_fact as(
 select * from {{ ref('thaedw_integration__edw_vw_th_sellout_sales_foc_fact') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
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
(
    select * from DEV_DNA_CORE.SNENAV01_WORKSPACE.THAEDW_INTEGRATION__EDW_VW_TH_GT_MSL_DISTRIBUTION
)








