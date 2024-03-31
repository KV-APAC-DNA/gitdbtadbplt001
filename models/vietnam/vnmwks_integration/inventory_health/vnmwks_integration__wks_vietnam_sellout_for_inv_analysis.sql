with edw_vw_vn_sellout_sales_fact as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_sellout_sales_fact') }}
),
edw_vw_vn_customer_dim as (
    select * from {{ ref('vnmedw_integration__edw_vw_vn_customer_dim') }}
),
itg_mds_vn_gt_gts_ratio as (
    select * from {{ ref('vnmitg_integration__itg_mds_vn_gt_gts_ratio') }}
),
itg_query_parameters as (
    select * from {{ source('sgpitg_integration','itg_query_parameters') }}
),
itg_vn_mt_sellin_dksh as (
    select * from {{ ref('vnmitg_integration__itg_vn_mt_sellin_dksh') }}
),
sdl_mds_vn_distributor_products as (
    select * from {{ source('vnmsdl_raw','sdl_mds_vn_distributor_products') }}
),
wks_dksh_unmapped as (
    select * from {{ ref('vnmwks_integration__wks_dksh_unmapped') }}
),
edw_list_price as (
    select * from {{ ref('aspedw_integration__edw_list_price') }}
),
itg_parameter_reg_inventory as (
    select * from {{ ref('vnmitg_integration__itg_parameter_reg_inventory') }}
),
edw_gch_producthierarchy as (
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_material_sales_dim as (
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
mat_prod as (
    SELECT 
        DISTINCT EMD.matl_num AS SAP_MATL_NUM,
        EMD.pka_product_key as pka_product_key,
        EMD.pka_size_desc as pka_size_desc,
        EGPH.GCPH_BRAND AS GPH_PROD_BRND,
        EGPH.GCPH_VARIANT AS GPH_PROD_VRNT,
        EGPH.GCPH_CATEGORY AS GPH_PROD_CTGRY,
        EGPH.GCPH_SEGMENT AS GPH_PROD_SGMNT
    FROM
        (
            Select *
            from edw_material_dim
        ) EMD,
        EDW_GCH_PRODUCTHIERARCHY EGPH
    WHERE LTRIM(EMD.MATL_NUM, '0') = ltrim(EGPH.MATERIALNUMBER(+), 0)
        AND EMD.PROD_HIER_CD <> ''
        AND LTRIM(EMD.MATL_NUM, '0') IN (
            SELECT DISTINCT LTRIM(MATL_NUM, '0')
            FROM edw_material_sales_dim
            WHERE sls_org in ('260S')
        )
),
trans_so as (
    SELECT COALESCE(evvcd.sap_prnt_cust_key,'NA') AS sap_prnt_cust_key,
        ltrim(evvssf.sap_matl_num::text, '0'::text)::character varying AS sku,
        evvssf.bill_date,
        evvssf.grs_trd_sls + evvssf.ret_val SO_VALUE
    FROM edw_vw_vn_sellout_sales_fact evvssf
        LEFT JOIN (
            (
                SELECT DISTINCT edw_vw_vn_customer_dim.sap_cust_id,
                    edw_vw_vn_customer_dim.sap_prnt_cust_key,
                    edw_vw_vn_customer_dim.sap_prnt_cust_desc
                FROM edw_vw_vn_customer_dim
            ) evvcd
            LEFT JOIN itg_mds_vn_gt_gts_ratio ra ON upper(
                COALESCE(ra.distributor, '#'::character varying)::text
            ) = upper(
                COALESCE(evvcd.sap_prnt_cust_desc, '#'::character varying)::text
            )
        ) ON ltrim(evvcd.sap_cust_id::text, '0'::text) = ltrim(evvssf.soldto_code::text, '0'::text)
),
trans_si as (
    SELECT cust.sap_prnt_cust_key,
        ltrim(T1.matl_id::text, '0'::character varying::text)::character varying AS sku,
        T1.bill_date,
        T1.SO_VALUE
    FROM (
            SELECT LTRIM(
                    qp.sap_sold_to_code::TEXT,
                    '0'::CHARACTER VARYING::TEXT
                )::CHARACTER VARYING AS soldto_code,
                smdp.jnj_sap_code AS matl_id,
                TO_DATE(
                    (
                        (
                            (
                                "substring" (a.invoice_date::TEXT, 0, 4) || '-'::CHARACTER VARYING::TEXT
                            ) || "substring" (a.invoice_date::TEXT, 5, 2)
                        ) || '-'::CHARACTER VARYING::TEXT
                    ) || "substring" (a.invoice_date::TEXT, 7, 2),
                    'YYYY-MM-DD'::CHARACTER VARYING::TEXT
                ) AS bill_date,
                a.qty_exclude_foc::NUMERIC::NUMERIC(18, 0) * lp.amount AS SO_VALUE
            FROM (
                    SELECT itg_query_parameters.parameter_value AS sap_sold_to_code
                    FROM itg_query_parameters
                    WHERE itg_query_parameters.country_code::TEXT = 'VN'::CHARACTER VARYING::TEXT
                        AND itg_query_parameters.parameter_name::TEXT = 'vn_dksh_soldto_code'::CHARACTER VARYING::TEXT
                ) qp,
                itg_vn_mt_sellin_dksh a
                LEFT JOIN (
                    (
                        SELECT sdl_mds_vn_distributor_products.jnj_sap_code::CHARACTER VARYING AS jnj_sap_code,
                            sdl_mds_vn_distributor_products.code
                        FROM sdl_mds_vn_distributor_products
                        WHERE NOT (
                                sdl_mds_vn_distributor_products.code IN (
                                    SELECT DISTINCT wks_dksh_unmapped.product_id
                                    FROM wks_dksh_unmapped
                                )
                            )
                            AND sdl_mds_vn_distributor_products.jnj_sap_code IS NOT NULL
                        UNION ALL
                        SELECT DISTINCT wks_dksh_unmapped.jnj_sap_code,
                            wks_dksh_unmapped.product_id
                        FROM wks_dksh_unmapped
                    ) smdp
                    LEFT JOIN (
                        SELECT lp.material,
                            lp.list_price,
                            b.parameter_value,
                            lp.list_price * b.parameter_value::NUMERIC(10, 4) AS amount
                        FROM (
                                SELECT LTRIM(
                                        edw_list_price.material::TEXT,
                                        0::CHARACTER VARYING::TEXT
                                    ) AS material,
                                    edw_list_price.amount AS list_price,
                                    row_number() OVER (
                                        PARTITION BY LTRIM(
                                            edw_list_price.material::TEXT,
                                            0::CHARACTER VARYING::TEXT
                                        )
                                        ORDER BY TO_DATE(
                                                edw_list_price.valid_to::TEXT,
                                                'YYYYMMDD'::CHARACTER VARYING::TEXT
                                            ) DESC,
                                            TO_DATE(
                                                edw_list_price.dt_from::TEXT,
                                                'YYYYMMDD'::CHARACTER VARYING::TEXT
                                            ) DESC
                                    ) AS rn
                                FROM edw_list_price
                                WHERE edw_list_price.sls_org::TEXT = '260S'::CHARACTER VARYING::TEXT
                            ) lp,
                            itg_parameter_reg_inventory b
                        WHERE lp.rn = 1
                            AND b.country_name::TEXT = 'VT'::CHARACTER VARYING::TEXT
                            AND b.parameter_name::TEXT = 'VT_DKSH'::CHARACTER VARYING::TEXT
                    ) lp ON smdp.jnj_sap_code::TEXT = lp.material
                ) ON LTRIM (a.productid::TEXT, 0::CHARACTER VARYING::TEXT) = LTRIM (smdp.code::TEXT, 0::CHARACTER VARYING::TEXT)
        ) T1
        LEFT JOIN (
            SELECT DISTINCT edw_vw_vn_customer_dim.sap_cust_id,
                edw_vw_vn_customer_dim.sap_cust_nm,
                edw_vw_vn_customer_dim.sap_sls_org,
                edw_vw_vn_customer_dim.sap_prnt_cust_key,
                edw_vw_vn_customer_dim.sap_prnt_cust_desc
            FROM edw_vw_vn_customer_dim
        ) cust WHERE T1.soldto_code::TEXT = LTRIM (
            cust.sap_cust_id(+)::TEXT,
            0::CHARACTER VARYING::TEXT
        )
),
trans_siso as (
    SELECT COALESCE(NULLIF(sku, ''), 'NA') AS MATL_NUM,
            bill_date,
            SO_VALUE,
            sap_prnt_cust_key
    FROM 
    (
        select * from trans_so
        union all
        select * from trans_si
    )
    where SO_VALUE>0
),
final as (
    SELECT 
        min(bill_date) as min_date,
        TRIM(NVL(NULLIF(T4.GPH_PROD_BRND, ''), 'NA')) AS BRAND,
        TRIM(NVL(NULLIF(T4.GPH_PROD_VRNT, ''), 'NA')) AS VARIANT,
        TRIM(NVL(NULLIF(T4.GPH_PROD_SGMNT, ''), 'NA')) AS SEGMENT,
        TRIM(NVL(NULLIF(T4.GPH_PROD_CTGRY, ''), 'NA')) AS PROD_CATEGORY,
        TRIM(NVL(NULLIF(T4.pka_size_desc, ''), 'NA')) AS pka_size_desc,
        TRIM(NVL(NULLIF(T4.pka_product_key, ''), 'NA')) AS pka_product_key,
        NVL(sap_prnt_cust_key, 'NA') AS SAP_PARENT_CUSTOMER_KEY
    FROM trans_siso A, mat_prod T4
    WHERE LTRIM(T4.SAP_MATL_NUM(+), '0') = A.MATL_NUM
    AND left(A.bill_date, 4) > (DATE_PART(YEAR, to_date(current_timestamp())) -6)
    GROUP BY GPH_PROD_BRND,
    GPH_PROD_VRNT,
    GPH_PROD_SGMNT,
    GPH_PROD_CTGRY,
    pka_size_desc,
    pka_product_key,
    sap_parent_customer_key
)
select * from final