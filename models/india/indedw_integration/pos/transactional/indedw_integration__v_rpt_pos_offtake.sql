{{
    config(
        materialized='view'
    )
}}

with v_rpt_ka_sales as
(
    select * from {{ ref('indedw_integration__v_rpt_ka_sales') }}
),
edw_vw_pos_offtake as
(
    select * from {{ ref('indedw_integration__edw_vw_pos_offtake') }}
),
itg_query_parameters as
(
    select * from {{ source('aspitg_integration', 'itg_query_parameters') }}
),
final as
(
    SELECT 
    txn.store_cd, 
    txn.article_cd, 
    txn.period, 
    txn.fisc_year, 
    txn.fisc_month AS fis_month, 
    trim((txn.level):: text) AS "level", 
    sum(txn.sls_qty) AS sls_qty, 
    sum(txn.sls_val_lcy) AS sls_val_lcy, 
    txn.account_name, 
    txn.store_name, 
    txn."region", 
    txn.zone, 
    txn.re, 
    txn.promotor, 
    txn.mother_sku_name, 
    txn.sap_cd, 
    txn.brand_name, 
    txn.franchise_name, 
    txn.internal_category, 
    txn.internal_subcategory, 
    txn.external_category, 
    txn.external_subcategory, 
    sum(txn.promos) AS promos, 
    txn.file_upload_date, 
    txn.article_name, 
    txn.product_name, 
    txn.product_category_name, 
    txn.variant_name, 
    txn.quarter, 
    sum((kac.totalsalesnrconfirmed / kac.totalqtyconfirmed)) AS nr, 
    sum(((kac.totalsalesnrconfirmed / kac.totalqtyconfirmed) * txn.sls_qty)) AS achivement_nr 
    FROM 
    ((SELECT edw_vw_pos_offtake.store_cd, 
            edw_vw_pos_offtake.article_cd, 
            edw_vw_pos_offtake.period, 
            edw_vw_pos_offtake.fisc_year, 
            edw_vw_pos_offtake.fisc_month, 
            edw_vw_pos_offtake.level, 
            edw_vw_pos_offtake.sls_qty, 
            edw_vw_pos_offtake.sls_val_lcy, 
            edw_vw_pos_offtake.account_name, 
            edw_vw_pos_offtake.store_name, 
            edw_vw_pos_offtake."region", 
            edw_vw_pos_offtake.zone, 
            edw_vw_pos_offtake.re, 
            edw_vw_pos_offtake.promotor, 
            edw_vw_pos_offtake.mother_sku_name, 
            edw_vw_pos_offtake.sap_cd, 
            edw_vw_pos_offtake.brand_name, 
            edw_vw_pos_offtake.franchise_name, 
            edw_vw_pos_offtake.internal_category, 
            edw_vw_pos_offtake.internal_subcategory, 
            edw_vw_pos_offtake.external_category, 
            edw_vw_pos_offtake.external_subcategory, 
            edw_vw_pos_offtake.promos, 
            edw_vw_pos_offtake.file_upload_date, 
            edw_vw_pos_offtake.article_name, 
            edw_vw_pos_offtake.product_name, 
            edw_vw_pos_offtake.product_category_name, 
            edw_vw_pos_offtake.variant_name, 
            edw_vw_pos_offtake.quarter 
        FROM edw_vw_pos_offtake) txn 
        LEFT JOIN (SELECT v_rpt_ka_sales.parent_name, 
            v_rpt_ka_sales.product_code, 
            max(v_rpt_ka_sales.day) AS "day", 
            sum(COALESCE( v_rpt_ka_sales.totalsalesnrconfirmed, 0.0)) AS totalsalesnrconfirmed, 
            sum(COALESCE(((v_rpt_ka_sales.totalqtyconfirmed):: numeric):: numeric(18, 0),0.0)) AS totalqtyconfirmed 
        FROM 
            v_rpt_ka_sales 
        WHERE 
            (v_rpt_ka_sales.totalqtyconfirmed > 0) 
        GROUP BY 
            v_rpt_ka_sales.parent_name, 
            v_rpt_ka_sales.product_code
        ) kac ON ((((upper((txn.account_name):: text) = upper((kac.parent_name):: text)) 
            AND (ltrim((txn.sap_cd):: text,('0' :: character varying):: text) = ltrim((kac.product_code):: text,('0' :: character varying):: text))) 
            AND ((replace(((txn.file_upload_date):: character varying):: text,('-' :: character varying):: text,('' :: character varying):: text)):: integer > kac."day")
        ))) 
    WHERE (txn.fisc_year >= (((date_part(year,(to_date(current_timestamp():: date)):: date) - ((SELECT ( itg_query_parameters.parameter_value):: integer AS parameter_value FROM itg_query_parameters WHERE ((upper((itg_query_parameters.country_code):: text) = ('IN' :: character varying):: text) AND (upper((itg_query_parameters.parameter_name):: text) = ('INDIA_KAM_POS_OFFTAKE_TDE_DATA_RETENTION_YEARS' :: character varying):: text)))):: double precision)):: character varying):: text) 
    GROUP BY 
    txn.store_cd, 
    txn.article_cd, 
    txn.period, 
    txn.fisc_year, 
    txn.fisc_month, 
    txn.level, 
    txn.account_name, 
    txn.store_name, 
    txn."region", 
    txn.zone, 
    txn.re, 
    txn.promotor, 
    txn.mother_sku_name, 
    txn.sap_cd, 
    txn.brand_name, 
    txn.franchise_name, 
    txn.internal_category, 
    txn.internal_subcategory, 
    txn.external_category, 
    txn.external_subcategory, 
    txn.file_upload_date, 
    txn.article_name, 
    txn.product_name, 
    txn.product_category_name, 
    txn.variant_name, 
    txn.quarter
)
select * from final