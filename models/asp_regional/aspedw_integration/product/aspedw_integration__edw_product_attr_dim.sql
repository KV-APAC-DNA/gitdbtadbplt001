
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= [" {% if is_incremental() %}
                    delete
                    from {{this}} edw_product_attr_dim using {{ ref('aspwks_integration__wks_edw_product_attr_dim') }} wks_edw_product_attr_dim
                    where wks_edw_product_attr_dim.ean = edw_product_attr_dim.ean
                    and   wks_edw_product_attr_dim.cntry = edw_product_attr_dim.cntry
                    and   wks_edw_product_attr_dim.chng_flg = 'U';
                    {% endif %}
                    ",
                    "{% if is_incremental() %}
                    delete
                    from {{this}} edw_product_attr_dim using {{ ref('aspwks_integration__wks_edw_product_attr_dim_2') }} wks_edw_product_attr_dim
                    where wks_edw_product_attr_dim.ean = edw_product_attr_dim.ean
                    and   wks_edw_product_attr_dim.cntry = edw_product_attr_dim.cntry
                    and   wks_edw_product_attr_dim.chng_flg = 'U';
                    {% endif %}
                    "]
    )
}}
with wks_edw_product_attr_dim as (
    select * from {{ ref('aspwks_integration__wks_edw_product_attr_dim') }}
),
wks_edw_product_attr_dim_1 as (
    select * from {{ ref('aspwks_integration__wks_edw_product_attr_dim_2') }}
),
wks_1 as (
( SELECT SRC.aw_remote_key,
       SRC.awrefs_prod_remotekey,
       SRC.awrefs_buss_unit,
       SRC.sap_matl_num,
       SRC.cntry,
        SRC.ean,
       SRC.prod_hier_l1,
       SRC.prod_hier_l2,
       SRC.prod_hier_l3,
       SRC.prod_hier_l4,
       SRC.prod_hier_l5,
       SRC.prod_hier_l6,
       SRC.prod_hier_l7,
       SRC.prod_hier_l8,
       SRC.prod_hier_l9,
       CASE WHEN CHNG_FLG = 'I' THEN current_timestamp()::timestamp_ntz(9) ELSE TGT_CRT_DTTM END  AS CRT_DTTM ,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS UPDT_DTTM,
       SRC.lcl_prod_nm
       FROM 
	   (select a.*, ROW_NUMBER() over (PARTITION BY aw_remote_key,cntry
		order by aw_remote_key desc) as d_rnk from wks_edw_product_attr_dim
         a) SRC where SRC.d_rnk=1
		)
),
wks_2 as 
( SELECT SRC.aw_remote_key,
       SRC.awrefs_prod_remotekey,
       SRC.awrefs_buss_unit,
       SRC.sap_matl_num,
       SRC.cntry,
        SRC.ean,
       SRC.prod_hier_l1,
       SRC.prod_hier_l2,
       SRC.prod_hier_l3,
       SRC.prod_hier_l4,
       SRC.prod_hier_l5,
       SRC.prod_hier_l6,
       SRC.prod_hier_l7,
       SRC.prod_hier_l8,
       SRC.prod_hier_l9,
       CASE WHEN CHNG_FLG = 'I' THEN current_timestamp()::timestamp_ntz(9) ELSE TGT_CRT_DTTM END  AS CRT_DTTM ,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) AS UPDT_DTTM,
       SRC.lcl_prod_nm
       FROM 
	   (select a.*, ROW_NUMBER() over (PARTITION BY aw_remote_key,cntry
		order by aw_remote_key desc) as d_rnk from wks_edw_product_attr_dim_1 a) SRC where SRC.d_rnk=1

),
final as (
    select * from wks_1
    union all
    select * from wks_2
)
select * from final