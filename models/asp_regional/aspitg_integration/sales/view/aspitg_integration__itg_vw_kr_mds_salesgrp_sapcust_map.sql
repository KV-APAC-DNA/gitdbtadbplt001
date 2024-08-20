{{
    config(
        materialized='view'
    )
}}

with itg_kr_sales_store_map as
(
    select * from {{ ref('ntaitg_integration__itg_kr_sales_store_map') }}
),
v_edw_customer_sales_dim as
(
    select * from {{ ref('aspedw_integration__v_edw_customer_sales_dim') }}
),
edw_customer_base_dim as
(
    select * from {{ ref('aspedw_integration__edw_customer_base_dim') }}
),
final as
(
    SELECT distinct (LTRIM((slsdim.cust_num)::TEXT,'0'::TEXT))::CHARACTER VARYING AS cust_num,
        cusbase.cust_nm,  
        slsdim.sls_ofc,
        strmap.sls_ofc_desc,
        strmap.channel,
        strmap.store_type,
        strmap.sales_group_code,
        strmap.sales_grp_nm,
        strmap.customer_segmentation_code,
        strmap.customer_segmentation_level_2_code
    FROM (itg_kr_sales_store_map strmap join
        v_edw_customer_sales_dim slsdim
        ON ( ( ( (strmap.sls_ofc)::TEXT = (slsdim.sls_ofc)::TEXT)
    AND ( (strmap.sales_group_code)::TEXT = (slsdim.sls_grp)::TEXT))))
    LEFT JOIN (select distinct cust_num,cust_nm from edw_customer_base_dim) cusbase on slsdim.cust_num = cusbase.cust_num 
    where slsdim.sls_org = '320S'
)
select * from final