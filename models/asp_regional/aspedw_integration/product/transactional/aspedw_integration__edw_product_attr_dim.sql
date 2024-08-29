
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
                    "],
        post_hook="
        ------------Update the lcl_prod_nm for non_korea region--------------------------
            UPDATE {{this}}
            SET lcl_prod_nm = A.prod_nm
            FROM {{ source('aspitg_integration', 'itg_mysls_prod_trnl') }} a, {{this}} b where REPLACE (A.aw_rmte_key, ' ', '') = REPLACE (B.aw_remote_key, ' ', '')
                AND UPPER (SUBSTRING (A.lang, 4, 5)) = B.cntry
            and UPPER (SUBSTRING (A.lang, 4, 5)) <> 'KR';
            ------------Update the lcl_prod_nm for korea region--------------------------
            UPDATE {{this}}
            SET lcl_prod_nm = A.prod_nm
            from (
                    select distinct aw_rmte_key,
                        A.lang,
                        A.prod_nm
                    FROM {{ source('aspitg_integration', 'itg_mysls_prod_trnl') }} a
                    where REPLACE (A.aw_rmte_key, ' ', '') || UPPER (SUBSTRING (A.lang, 4, 5)) not in (
                            select REPLACE(REPLACE(barcode, '-', ''), ' ', '') || cntry_cd
                            from {{ ref('aspedw_integration__edw_vw_pop6_products') }}
                            union
                            select distinct ean || cntry
                            from {{ ref('aspwks_integration__wks_edw_product_attr_dim') }}
                        )
                ) a, {{this}} b where REPLACE (A.aw_rmte_key, ' ', '') = REPLACE (B.aw_remote_key, ' ', '')
                AND UPPER (SUBSTRING (A.lang, 4, 5)) = B.cntry
            and UPPER (SUBSTRING (A.lang, 4, 5)) = 'KR';"
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
transformed as (
    select * from wks_1
    union all
    select * from wks_2
),
final as (
select 
aw_remote_key::varchar(100) as aw_remote_key,
awrefs_prod_remotekey::varchar(100) as awrefs_prod_remotekey,
awrefs_buss_unit::varchar(100) as awrefs_buss_unit,
sap_matl_num::varchar(100) as sap_matl_num,
cntry::varchar(10) as cntry,
ean::varchar(20) as ean,
prod_hier_l1::varchar(500) as prod_hier_l1,
prod_hier_l2::varchar(500) as prod_hier_l2,
prod_hier_l3::varchar(500) as prod_hier_l3,
prod_hier_l4::varchar(500) as prod_hier_l4,
prod_hier_l5::varchar(500) as prod_hier_l5,
prod_hier_l6::varchar(500) as prod_hier_l6,
prod_hier_l7::varchar(500) as prod_hier_l7,
prod_hier_l8::varchar(500) as prod_hier_l8,
prod_hier_l9::varchar(500) as prod_hier_l9,
crt_dttm::timestamp_ntz(9) as crt_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
lcl_prod_nm::varchar(100) as lcl_prod_nm,
from transformed
)
select * from final