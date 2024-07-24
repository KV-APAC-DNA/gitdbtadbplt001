with edw_id_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_id_rpt_retail_excellence_summary') }}
),
edw_my_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_my_rpt_retail_excellence_summary') }}
),
edw_ph_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_ph_rpt_retail_excellence_summary') }}
),
edw_sg_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_sg_rpt_retail_excellence_summary') }}
),
edw_kr_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_kr_rpt_retail_excellence_summary') }}
),
edw_th_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_th_rpt_retail_excellence_summary') }}
),
edw_in_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_in_rpt_retail_excellence_summary') }}
),
edw_anz_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_anz_rpt_retail_excellence_summary') }}
),
edw_jp_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_jp_rpt_retail_excellence_summary') }}
),
edw_cnsc_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_cnsc_rpt_retail_excellence_summary') }}
),
edw_cnpc_rpt_retail_excellence_summary as (
    select * from {{ ref('aspedw_integration__edw_cnpc_rpt_retail_excellence_summary') }}
),

edw_rpt_retail_excellence_summary as (

SELECT * FROM edw_id_rpt_retail_excellence_summary UNION
SELECT * FROM edw_my_rpt_retail_excellence_summary UNION
SELECT * FROM edw_ph_rpt_retail_excellence_summary UNION
SELECT * FROM edw_kr_rpt_retail_excellence_summary UNION
SELECT * FROM edw_sg_rpt_retail_excellence_summary UNION
SELECT * FROM edw_th_rpt_retail_excellence_summary UNION
SELECT * FROM edw_in_rpt_retail_excellence_summary UNION
SELECT * FROM edw_anz_rpt_retail_excellence_summary UNION
SELECT * FROM edw_jp_rpt_retail_excellence_summary UNION
SELECT * FROM edw_cnsc_rpt_retail_excellence_summary UNION
SELECT * FROM edw_cnpc_rpt_retail_excellence_summary
)
select * from edw_rpt_retail_excellence_summary