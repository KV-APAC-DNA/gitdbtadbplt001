{{
    config(
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}
with edw_id_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_id_rpt_retail_excellence_details') }}
),
edw_my_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_my_rpt_retail_excellence_details') }}
),
edw_ph_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_ph_rpt_retail_excellence_details') }}
),
edw_sg_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_sg_rpt_retail_excellence_details') }}
),
edw_kr_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_kr_rpt_retail_excellence_details') }}
),
edw_th_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_th_rpt_retail_excellence_details') }}
),
edw_in_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_in_rpt_retail_excellence_details') }}
),
edw_anz_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_anz_rpt_retail_excellence_details') }}
),
edw_jp_rpt_retail_excellence_details as (
    select * from {{ ref('aspedw_integration__edw_jp_rpt_retail_excellence_details') }}
),
edw_rpt_retail_excellence_details as 
(

SELECT * FROM edw_id_rpt_retail_excellence_details UNION all
SELECT * FROM edw_my_rpt_retail_excellence_details UNION all
SELECT * FROM edw_ph_rpt_retail_excellence_details UNION all
SELECT * FROM edw_kr_rpt_retail_excellence_details UNION all
SELECT * FROM edw_sg_rpt_retail_excellence_details UNION all
SELECT * FROM edw_th_rpt_retail_excellence_details UNION all
SELECT * FROM edw_in_rpt_retail_excellence_details UNION all
SELECT * FROM edw_anz_rpt_retail_excellence_details UNION all
SELECT * FROM edw_jp_rpt_retail_excellence_details 

)
select * from edw_rpt_retail_excellence_details