with 
itg_id_pos_carrefour_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_carrefour_sellout') }}
),
itg_id_pos_diamond_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_diamond_sellout') }}
),
edw_distributor_group_dim as 
(
  select * from {{ ref('idnedw_integration__edw_distributor_group_dim') }}
),
itg_mds_id_pos_cust_prod_mapping as 
(
  select * from {{ ref('idnitg_integration__itg_mds_id_pos_cust_prod_mapping') }}
),
itg_id_pos_idm_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_idm_sellout') }}
),
itg_id_pos_igr_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_igr_sellout') }}
),
itg_id_pos_midi_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_midi_sellout') }}
),
itg_id_pos_sat_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_sat_sellout') }}
),
edw_product_dim as 
(
  select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
itg_id_pos_superindo_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_superindo_sellout') }}
),
itg_id_pos_watson_sellout as 
(
  select * from {{ ref('idnitg_integration__itg_id_pos_watson_sellout') }}
),
itg_id_pos_idm_sellout_qty as 
(
    select * from  itg_id_pos_idm_sellout where trim((itg_id_pos_idm_sellout.type)::text) = ('QTY'::character varying)::text
),
itg_id_pos_idm_sellout_idr as 
(
    select * from  itg_id_pos_idm_sellout where trim((itg_id_pos_idm_sellout.type)::text) = ('IDR'::character varying)::text
),
itg_id_pos_igr_sellout_qty as 
(
    select * from itg_id_pos_igr_sellout where trim((itg_id_pos_igr_sellout.type)::text) = ('QTY'::character varying)::text
),
itg_id_pos_igr_sellout_idr as 
(
    select * from itg_id_pos_igr_sellout where trim((itg_id_pos_igr_sellout.type)::text) = ('IDR'::character varying)::text
),
itg_id_pos_midi_sellout_qty as 
(
    select * from itg_id_pos_midi_sellout where trim((itg_id_pos_midi_sellout.type)::text) = ('QTY'::character varying)::text
),
itg_id_pos_midi_sellout_idr as 
(
    select * from itg_id_pos_midi_sellout where trim((itg_id_pos_midi_sellout.type)::text) = ('VALUE'::character varying)::text
),
itg_id_pos_sat_sellout_qty as 
(
    select * from itg_id_pos_sat_sellout where trim((itg_id_pos_sat_sellout.type)::text) = ('QTY'::character varying)::text
),
itg_id_pos_sat_sellout_idr as 
(
    select * from itg_id_pos_sat_sellout where trim((itg_id_pos_sat_sellout.type)::text) = ('VALUE'::character varying)::text
),
final as 
(
    select * from
(
    (
        (
            (
                (
                    (
                        SELECT 'ID' AS sap_cntry_cd,
                            'Indonesia' AS sap_cntry_nm,
                            'Sellout' AS dataset,
                            COALESCE(
                                dist.dstrbtr_grp_cd,
                                'CARREFOUR'::character varying
                            ) AS dstrbtr_grp_cd,
                            ("substring"((c4.yearmonth)::text, 1, 4))::character varying AS "year",
                            c4.yearmonth,
                            NULL AS customer_brnch_code,
                            NULL AS customer_brnch_name,
                            NULL AS customer_store_code,
                            NULL AS customer_store_name,
                            c4.fdesc AS customer_franchise,
                            NULL AS customer_brand,
                            NULL AS customer_product_code,
                            NULL AS customer_product_desc,
                            NULL AS jj_sap_prod_id,
                            NULL AS brand,
                            NULL AS brand2,
                            NULL AS sku_sales_cube,
                            NULL AS customer_product_range,
                            NULL AS customer_product_group,
                            NULL AS customer_store_class,
                            NULL AS customer_store_channel,
                            c4.sales_qty,
                            c4.net_sales AS sales_value,
                            NULL AS service_level,
                            NULL AS sales_order,
                            c4.share,
                            NULL AS store_stock_qty,
                            NULL AS store_stock_value,
                            NULL AS branch_stock_qty,
                            NULL AS branch_stock_value,
                            NULL AS stock_uom,
                            NULL AS stock_days,
                            current_timestamp()::timestamp_ntz(9) AS crtd_dttm
                        FROM (
                                itg_id_pos_carrefour_sellout c4
                                LEFT JOIN edw_distributor_group_dim dist ON (
                                    (
                                        (c4.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                                    )
                                )
                            )
                        WHERE (
                                (
                                    (c4.pos_cust)::text = ('CARREFOUR'::character varying)::text
                                )
                                AND (
                                    (c4.scc_name)::text = ('JOHNSON&JOHNSON'::character varying)::text
                                )
                            )
                        UNION ALL
                        SELECT 'ID' AS sap_cntry_cd,
                            'Indonesia' AS sap_cntry_nm,
                            'Sellout' AS dataset,
                            COALESCE(
                                dist.dstrbtr_grp_cd,
                                'DIAMOND'::character varying
                            ) AS dstrbtr_grp_cd,
                            ("substring"((dmd.yearmonth)::text, 1, 4))::character varying AS "year",
                            dmd.yearmonth,
                            dmd.branch AS customer_brnch_code,
                            dmd.branch AS customer_brnch_name,
                            NULL AS customer_store_code,
                            NULL AS customer_store_name,
                            NULL AS customer_franchise,
                            NULL AS customer_brand,
                            (
                                trim("substring"((dmd.nama_barang)::text, 1, 8))
                            )::character varying AS customer_product_code,
                            dmd.nama_barang AS customer_product_desc,
                            mds.jj_sap_prod_id,
                            mds.brand,
                            mds.brand2,
                            mds.sku_sales_cube,
                            NULL AS customer_product_range,
                            NULL AS customer_product_group,
                            NULL AS customer_store_class,
                            NULL AS customer_store_channel,
                            dmd.qty AS sales_qty,
                            dmd.sales AS sales_value,
                            NULL AS service_level,
                            NULL AS sales_order,
                            NULL AS "share",
                            NULL AS store_stock_qty,
                            NULL AS store_stock_value,
                            NULL AS branch_stock_qty,
                            NULL AS branch_stock_value,
                            NULL AS stock_uom,
                            NULL AS stock_days,
                            current_timestamp()::timestamp_ntz(9) AS crtd_dttm
                        FROM (
                                (
                                    itg_id_pos_diamond_sellout dmd
                                    LEFT JOIN edw_distributor_group_dim dist ON (
                                        (
                                            (dmd.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                                        )
                                    )
                                )
                                LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                                    (
                                        (
                                            trim("substring"((dmd.nama_barang)::text, 1, 8)) = (mds.plu_sku_desc)::text
                                        )
                                        AND (
                                            upper((mds.account)::text) = upper(('Diamond'::character varying)::text)
                                        )
                                    )
                                )
                            )
                        WHERE (
                                (dmd.pos_cust)::text = ('DIAMOND'::character varying)::text
                            )
                    )
                    UNION ALL
                    SELECT 'ID' AS sap_cntry_cd,
                        'Indonesia' AS sap_cntry_nm,
                        'Sellout' AS dataset,
                        COALESCE(
                            dist.dstrbtr_grp_cd,
                            'INDOMARET'::character varying
                        ) AS dstrbtr_grp_cd,
                        ("substring"((idm.yearmonth)::text, 1, 4))::character varying AS "year",
                        idm.yearmonth,
                        idm.branch AS customer_brnch_code,
                        idm.branch AS customer_brnch_name,
                        NULL AS customer_store_code,
                        NULL AS customer_store_name,
                        NULL AS customer_franchise,
                        NULL AS customer_brand,
                        idm.plu AS customer_product_code,
                        (
                            (
                                (
                                    (idm.plu)::text || (' '::character varying)::text
                                ) || (idm.description)::text
                            )
                        )::character varying AS customer_product_desc,
                        mds.jj_sap_prod_id,
                        mds.brand,
                        mds.brand2,
                        mds.sku_sales_cube,
                        NULL AS customer_product_range,
                        NULL AS customer_product_group,
                        NULL AS customer_store_class,
                        NULL AS customer_store_channel,
                        qty."values" as sales_qty,
                        idr."values" as sales_value,
                        NULL AS service_level,
                        NULL AS sales_order,
                        NULL AS "share",
                        NULL AS store_stock_qty,
                        NULL AS store_stock_value,
                        NULL AS branch_stock_qty,
                        NULL AS branch_stock_value,
                        NULL AS stock_uom,
                        NULL AS stock_days,
                        current_timestamp()::timestamp_ntz(9) AS crtd_dttm
                    FROM (
                            (
                                itg_id_pos_idm_sellout idm
                                LEFT JOIN edw_distributor_group_dim dist ON (
                                    (
                                        (idm.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                                    )
                                )
                            )
                            LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                                (
                                    ((idm.plu)::text = (mds.plu_sku_desc)::text)
                                    AND (
                                        upper((mds.account)::text) = upper(('Indomaret'::character varying)::text)
                                    )
                                )
                            )
                            left join itg_id_pos_idm_sellout_qty as qty
                            on (
                                    (
                                        (
                                            (
                                                (
                                                    (qty.type)::text = (idm.type)::text
                                                )
                                            )
                                            AND (
                                                (qty.plu)::text = (idm.plu)::text
                                            )
                                        )
                                        AND (
                                            (qty.branch)::text = (idm.branch)::text
                                        )
                                    )
                                    AND (
                                        (qty.yearmonth)::text = (idm.yearmonth)::text
                                    )
                                )
                            left join itg_id_pos_idm_sellout_idr as idr
                            on (
                                    (
                                        (
                                            (
                                                (
                                                    (idr.type)::text = (idm.type)::text
                                                )
                                            )
                                            AND (
                                                (idr.plu)::text = (idm.plu)::text
                                            )
                                        )
                                        AND (
                                            (idr.branch)::text = (idm.branch)::text
                                        )
                                    )
                                    AND (
                                        (idr.yearmonth)::text = (idm.yearmonth)::text
                                    )
                                )
                        ) 
                    WHERE (
                            (idm.pos_cust)::text = ('INDOMARET'::character varying)::text
                        )
                )
                UNION ALL
                SELECT 'ID' AS sap_cntry_cd,
                    'Indonesia' AS sap_cntry_nm,
                    'Sellout' AS dataset,
                    COALESCE(
                        dist.dstrbtr_grp_cd,
                        'INDOGROSIR'::character varying
                    ) AS dstrbtr_grp_cd,
                    ("substring"((igr.yearmonth)::text, 1, 4))::character varying AS "year",
                    igr.yearmonth,
                    igr.branch AS customer_brnch_code,
                    igr.branch AS customer_brnch_name,
                    NULL AS customer_store_code,
                    NULL AS customer_store_name,
                    NULL AS customer_franchise,
                    NULL AS customer_brand,
                    NULL AS customer_product_code,
                    igr.description AS customer_product_desc,
                    mds.jj_sap_prod_id,
                    mds.brand,
                    mds.brand2,
                    mds.sku_sales_cube,
                    NULL AS customer_product_range,
                    NULL AS customer_product_group,
                    NULL AS customer_store_class,
                    NULL AS customer_store_channel,
                    qty."values" AS sales_qty,
                    idr."values" AS sales_value,
                    NULL AS service_level,
                    NULL AS sales_order,
                    NULL AS "share",
                    NULL AS store_stock_qty,
                    NULL AS store_stock_value,
                    NULL AS branch_stock_qty,
                    NULL AS branch_stock_value,
                    NULL AS stock_uom,
                    NULL AS stock_days,
                    current_timestamp()::timestamp_ntz(9) AS crtd_dttm
                FROM (
                        (
                            itg_id_pos_igr_sellout igr
                            LEFT JOIN edw_distributor_group_dim dist ON (
                                (
                                    (igr.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                                )
                            )
                        )
                        LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                            (
                                (
                                    (igr.description)::text = (mds.plu_sku_desc)::text
                                )
                                AND (
                                    upper((mds.account)::text) = upper(('Indogrosir'::character varying)::text)
                                )
                            )
                        )
                        left join itg_id_pos_igr_sellout_qty  as qty on 
                            (
                                (
                                    (
                                        (
                                            
                                            (
                                                (qty.type)::text = (igr.type)::text
                                            )
                                        )
                                        AND (
                                            (qty.description)::text = (igr.description)::text
                                        )
                                    )
                                    AND (
                                        (qty.branch)::text = (igr.branch)::text
                                    )
                                )
                                AND (
                                    (qty.yearmonth)::text = (igr.yearmonth)::text
                                )
                            )
                            left join itg_id_pos_igr_sellout_idr as idr on 
                            (
                                (
                                    (
                                        (
                                            
                                            (
                                                (idr.type)::text = (igr.type)::text
                                            )
                                        )
                                        AND (
                                            (idr.description)::text = (igr.description)::text
                                        )
                                    )
                                    AND (
                                        (idr.branch)::text = (igr.branch)::text
                                    )
                                )
                                AND (
                                    (idr.yearmonth)::text = (igr.yearmonth)::text
                                )
                            )
                    )
                WHERE (
                        (igr.pos_cust)::text = ('INDOGROSIR'::character varying)::text
                    )
            )
            UNION ALL
            SELECT 'ID' AS sap_cntry_cd,
                'Indonesia' AS sap_cntry_nm,
                'Sellout' AS dataset,
                COALESCE(
                    dist.dstrbtr_grp_cd,
                    'ALFAMIDI'::character varying
                ) AS dstrbtr_grp_cd,
                ("substring"((midi.yearmonth)::text, 1, 4))::character varying AS "year",
                midi.yearmonth,
                midi.branch AS customer_brnch_code,
                midi.branch AS customer_brnch_name,
                NULL AS customer_store_code,
                NULL AS customer_store_name,
                NULL AS customer_franchise,
                NULL AS customer_brand,
                midi.plu AS customer_product_code,
                midi.description AS customer_product_desc,
                mds.jj_sap_prod_id,
                mds.brand,
                mds.brand2,
                mds.sku_sales_cube,
                NULL AS customer_product_range,
                NULL AS customer_product_group,
                NULL AS customer_store_class,
                NULL AS customer_store_channel,
                qty."values" as sales_qty,
                idr."values" as sales_value,
                NULL AS service_level,
                NULL AS sales_order,
                NULL AS "share",
                NULL AS store_stock_qty,
                NULL AS store_stock_value,
                NULL AS branch_stock_qty,
                NULL AS branch_stock_value,
                NULL AS stock_uom,
                NULL AS stock_days,
                current_timestamp()::timestamp_ntz(9) AS crtd_dttm
            FROM (
                    (
                        itg_id_pos_midi_sellout midi
                        LEFT JOIN edw_distributor_group_dim dist ON (
                            (
                                (midi.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                            )
                        )
                    )
                    LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                        (
                            ((midi.plu)::text = (mds.plu_sku_desc)::text)
                            AND (
                                upper((mds.account)::text) = upper(('Alfamidi'::character varying)::text)
                            )
                        )
                    )
                    left join itg_id_pos_midi_sellout_qty as qty on  (
                            (
                                (
                                    (
                                        
                                         (
                                            (qty.plu)::text = (midi.plu)::text
                                        )
                                    )
                                    AND (
                                        trim((qty.type)::text) = trim((midi.type)::text)
                                    )
                                )
                                AND (
                                    (qty.branch)::text = (midi.branch)::text
                                )
                            )
                            AND (
                                (qty.yearmonth)::text = (midi.yearmonth)::text
                            )
                        )

                        left join itg_id_pos_midi_sellout_idr as idr on (
                            (
                                (
                                    (
                                        (
                                            (idr.plu)::text = (midi.plu)::text
                                        )
                                    )
                                    AND (
                                        trim((idr.type)::text) = trim((midi.type)::text)
                                    )
                                )
                                AND (
                                    (idr.branch)::text = (midi.branch)::text
                                )
                            )
                            AND (
                                (idr.yearmonth)::text = (midi.yearmonth)::text
                            )
                        )
                )
            WHERE (
                    (midi.pos_cust)::text = ('ALFAMIDI'::character varying)::text
                )
        )
        UNION ALL
        SELECT 'ID' AS sap_cntry_cd,
            'Indonesia' AS sap_cntry_nm,
            'Sellout' AS dataset,
            COALESCE(dist.dstrbtr_grp_cd, 'ALFA'::character varying) AS dstrbtr_grp_cd,
            ("substring"((sat.yearmonth)::text, 1, 4))::character varying AS "year",
            sat.yearmonth,
            sat.branch AS customer_brnch_code,
            sat.branch AS customer_brnch_name,
            NULL AS customer_store_code,
            NULL AS customer_store_name,
            NULL AS customer_franchise,
            NULL AS customer_brand,
            sat.plu AS customer_product_code,
            sat.description AS customer_product_desc,
            mds.jj_sap_prod_id,
            mds.brand,
            mds.brand2,
            mds.sku_sales_cube,
            NULL AS customer_product_range,
            NULL AS customer_product_group,
            NULL AS customer_store_class,
            NULL AS customer_store_channel,
            qty."values" AS sales_qty,
            idr."values" AS sales_value,
            NULL AS service_level,
            NULL AS sales_order,
            NULL AS "share",
            NULL AS store_stock_qty,
            NULL AS store_stock_value,
            NULL AS branch_stock_qty,
            NULL AS branch_stock_value,
            NULL AS stock_uom,
            NULL AS stock_days,
            current_timestamp()::timestamp_ntz(9) AS crtd_dttm
        FROM  itg_id_pos_sat_sellout sat
                    LEFT JOIN edw_distributor_group_dim dist ON (
                        (
                            (sat.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                        )
                    )
                
                LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                    (
                        ((sat.plu)::text = (mds.plu_sku_desc)::text)
                        AND (
                            upper((mds.account)::text) = upper(('ALFA'::character varying)::text)
                        )
                    )
                )
                left join itg_id_pos_sat_sellout_qty as qty on (
                        (
                            (
                                (
                                    (
                                        (qty.type)::text = (sat.type)::text
                                    )
                                )
                                AND (
                                    (qty.plu)::text = (sat.plu)::text
                                )
                            )
                            AND (
                                (qty.branch)::text = (sat.branch)::text
                            )
                        )
                        AND (
                            (qty.yearmonth)::text = (sat.yearmonth)::text
                        )
                    )

                    left join itg_id_pos_sat_sellout as idr on (
                        (
                            (
                                (
                                     (
                                        (idr.plu)::text = (sat.plu)::text
                                    )
                                )
                                AND (
                                    (idr.type)::text = (sat.type)::text
                                )
                            )
                            AND (
                                (idr.branch)::text = (sat.branch)::text
                            )
                        )
                        AND (
                            (idr.yearmonth)::text = (sat.yearmonth)::text
                        )
                    )
            
        WHERE (
                (sat.pos_cust)::text = ('ALFA'::character varying)::text
            )
    )
    UNION ALL
    SELECT 'ID' AS sap_cntry_cd,
        'Indonesia' AS sap_cntry_nm,
        'Sellout' AS dataset,
        COALESCE(
            dist.dstrbtr_grp_cd,
            'SUPER INDO'::character varying
        ) AS dstrbtr_grp_cd,
        ("substring"((indo.yearmonth)::text, 1, 4))::character varying AS "year",
        indo.yearmonth,
        indo.region AS customer_brnch_code,
        indo.region AS customer_brnch_name,
        NULL AS customer_store_code,
        NULL AS customer_store_name,
        indo.grp AS customer_franchise,
        NULL AS customer_brand,
        ("substring"((indo.product)::text, 1, 7))::character varying AS customer_product_code,
        indo.product AS customer_product_desc,
        mds.jj_sap_prod_id,
        mds.brand,
        mds.brand2,
        mds.sku_sales_cube,
        NULL AS customer_product_range,
        NULL AS customer_product_group,
        NULL AS customer_store_class,
        NULL AS customer_store_channel,
        indo.mon_supply AS sales_qty,
        indo.mon_supply * edw_product_dim.price AS sales_value,
        indo.mon_sales_percent AS service_level,
        indo.mon_order AS sales_order,
        NULL AS "share",
        NULL AS store_stock_qty,
        NULL AS store_stock_value,
        NULL AS branch_stock_qty,
        NULL AS branch_stock_value,
        NULL AS stock_uom,
        NULL AS stock_days,
        current_timestamp()::timestamp_ntz(9) AS crtd_dttm
    FROM (
            (
                itg_id_pos_superindo_sellout indo
                LEFT JOIN edw_distributor_group_dim dist ON (
                    (
                        (indo.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                    )
                )
            )
            LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                (
                    (
                        "substring"((indo.product)::text, 1, 7) = (mds.plu_sku_desc)::text
                    )
                    AND (
                        upper((mds.account)::text) = upper(('Super Indo'::character varying)::text)
                    )
                )
            )
            left join  edw_product_dim on (
                    (edw_product_dim.jj_sap_prod_id)::text = (mds.jj_sap_prod_id)::text
                )
        )
    WHERE (
            (indo.pos_cust)::text = ('SUPER INDO'::character varying)::text
        )
)
UNION ALL
SELECT 'ID' AS sap_cntry_cd,
    'Indonesia' AS sap_cntry_nm,
    'Sellout' AS dataset,
    COALESCE(dist.dstrbtr_grp_cd, 'WATSON'::character varying) AS dstrbtr_grp_cd,
    ("substring"((wat.yearmonth)::text, 1, 4))::character varying AS "year",
    wat.yearmonth,
    NULL AS customer_brnch_code,
    NULL AS customer_brnch_name,
    wat.str_code AS customer_store_code,
    wat.str_name AS customer_store_name,
    (((wat.dept_idnt)::text || (wat.dept_desc)::text))::character varying AS customer_franchise,
    wat.brand AS customer_brand,
    wat.prdt_code AS customer_product_code,
    wat.prdt_desc AS customer_product_desc,
    mds.jj_sap_prod_id,
    mds.brand,
    mds.brand2,
    mds.sku_sales_cube,
    wat.range_desc AS customer_product_range,
    (((wat.division)::text || (wat.div_desc)::text))::character varying AS customer_product_group,
    wat.str_class AS customer_store_class,
    wat.str_format AS customer_store_channel,
    wat.sale_qty AS sales_qty,
    wat.net_sale AS sales_value,
    NULL AS service_level,
    NULL AS sales_order,
    NULL AS "share",
    NULL AS store_stock_qty,
    NULL AS store_stock_value,
    NULL AS branch_stock_qty,
    NULL AS branch_stock_value,
    NULL AS stock_uom,
    NULL AS stock_days,
    current_timestamp()::timestamp_ntz(9) AS crtd_dttm
FROM (
        (
            itg_id_pos_watson_sellout wat
            LEFT JOIN edw_distributor_group_dim dist ON (
                (
                    (wat.pos_cust)::text = (dist.dstrbtr_grp_cd)::text
                )
            )
        )
        LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
            (
                ((wat.prdt_code)::text = (mds.plu_sku_desc)::text)
                AND (
                    upper((mds.account)::text) = upper(('Watson'::character varying)::text)
                )
            )
        )
    )
WHERE (
        (wat.pos_cust)::text = ('WATSON'::character varying)::text
    )
)
select * from final