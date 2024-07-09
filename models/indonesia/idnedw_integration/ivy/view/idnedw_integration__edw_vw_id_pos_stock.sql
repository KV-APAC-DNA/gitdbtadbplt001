with
edw_distributor_group_dim as 
(
    select * from {{ ref('idnedw_integration__edw_distributor_group_dim') }}
),
itg_id_pos_midi_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_midi_stock') }}
),
itg_id_pos_superindo_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_superindo_stock') }}
),
itg_id_pos_matahari_otc_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_matahari_otc_stock') }}
),
itg_id_pos_matahari_beauty_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_matahari_beauty_stock') }}
),
itg_id_pos_watson_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_watson_stock') }}
),
itg_id_pos_guardian_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_guardian_stock') }}
),
itg_id_pos_carrefour_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_carrefour_stock') }}
),
itg_id_pos_matahari_jb_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_matahari_jb_stock') }}
),
itg_id_pos_igr_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_igr_stock') }}
),
edw_product_dim as 
(
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
itg_mds_id_pos_cust_prod_mapping as 
(
    select * from {{ ref('idnitg_integration__itg_mds_id_pos_cust_prod_mapping') }}
),
itg_id_pos_idm_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_idm_stock') }}
),
itg_id_pos_sat_stock as 
(
    select * from {{ ref('idnitg_integration__itg_id_pos_sat_stock') }}
),
temp_product_dim as 
(
    select * from edw_product_dim
    where jj_sap_prod_id is not null
),
 carrefour as 
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'CARREFOUR'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (c4.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         c4.yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         c4.dep_desc AS customer_franchise,
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
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         NULL AS store_stock_qty,
         NULL AS store_stock_value,
         c4.stock_qty AS branch_stock_qty,
         c4.stock_amt AS branch_stock_value,
         NULL AS stock_uom,
         c4.stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM 
         (
             itg_id_pos_carrefour_stock c4
             LEFT JOIN edw_distributor_group_dim dist ON (
                 (
                     (c4.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                 )
             )
         )
     WHERE 
         (
             (c4.pos_cust)::TEXT = ('CARREFOUR'::CHARACTER VARYING)::TEXT
         )
 ),
 guardian as 
 (
     SELECT 'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'GUARDIAN'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (sir.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         sir.yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         sir.category AS customer_franchise,
         NULL AS customer_brand,
         sir.article AS customer_product_code,
         sir.article_desc AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         sir.soh_stores AS store_stock_qty,
         (sir.soh_stores * z.price) AS store_stock_value,
         sir.soh_dc AS branch_stock_qty,
         (sir.soh_dc * z.price) AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_guardian_stock sir
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (sir.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (((sir.article)::TEXT = (mds.plu_sku_desc)::TEXT))
         )
         left join temp_product_dim as z on (z.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT
     WHERE (
             (sir.pos_cust)::TEXT = ('Guardian'::CHARACTER VARYING)::TEXT
         )
 ),
 indomaret as 
 (
     SELECT 'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'INDOMARET'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (idm.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         idm.yearmonth,
         idm.branch AS customer_brnch_code,
         idm.branch AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         NULL AS customer_franchise,
         NULL AS customer_brand,
         idm.item AS customer_product_code,
         idm.item AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         idm.store_qty AS store_stock_qty,
         (idm.store_qty * a.price) AS store_stock_value,
         idm.dc_qty AS branch_stock_qty,
         (idm.dc_qty * a.price) AS branch_stock_value,
         idm.units AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_idm_stock idm
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (idm.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     ((idm.item)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Indomaret'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
         left join temp_product_dim as a on (a.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT
     WHERE (
             (idm.pos_cust)::TEXT = ('INDOMARET'::CHARACTER VARYING)::TEXT
         )
 ),
 indogrosir as
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'INDOGROSIR'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (igr.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         igr.yearmonth,
         igr.branch AS customer_brnch_code,
         igr.branch AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         NULL AS customer_franchise,
         NULL AS customer_brand,
         igr.item AS customer_product_code,
         igr.item AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         NULL AS store_stock_qty,
         NULL AS store_stock_value,
         igr.qty AS branch_stock_qty,
         (igr.qty * a.price) AS branch_stock_value,
         igr.unit AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_igr_stock igr
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (igr.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     ((igr.item)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Indogrosir'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
         left join temp_product_dim as a on (a.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT
     WHERE (
             (igr.pos_cust)::TEXT = ('INDOGROSIR'::CHARACTER VARYING)::TEXT
         )
 ),
 matahari_beauty as 
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'MATAHARI'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 ("mb".yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         "mb".yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         'BEAUTY' AS customer_franchise,
         NULL AS customer_brand,
         "mb".sku AS customer_product_code,
         "mb".sku_desc AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         NULL AS store_stock_qty,
         NULL AS store_stock_value,
         "mb".year_qty AS branch_stock_qty,
         "mb".retail_values AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM 
         (
             (
                 itg_id_pos_matahari_beauty_stock "mb"
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         ("mb".pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     (("mb".sku)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Matahari'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
     WHERE (
             ("mb".pos_cust)::TEXT = ('MATAHARI'::CHARACTER VARYING)::TEXT
         )
 ),
 matahari_baby as 
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'MATAHARI'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (mjb.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         mjb.yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         'BABY' AS customer_franchise,
         NULL AS customer_brand,
         mjb.sku AS customer_product_code,
         mjb.sku_desc AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         NULL AS store_stock_qty,
         NULL AS store_stock_value,
         mjb.qty AS branch_stock_qty,
         mjb.retail_values AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_matahari_jb_stock mjb
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (mjb.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     ((mjb.sku)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Matahari'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
     WHERE (
             (mjb.pos_cust)::TEXT = ('MATAHARI'::CHARACTER VARYING)::TEXT
         )
 ),
 matahari_otc as
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'MATAHARI'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (mtc.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         mtc.yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         mtc.loc AS customer_store_code,
         mtc.store_name AS customer_store_name,
         'OTC' AS customer_franchise,
         NULL AS customer_brand,
         mtc.item AS customer_product_code,
         mtc.item_desc AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         MTC.type AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         mtc.soh AS store_stock_qty,
         (mtc.soh * a.price) AS store_stock_value,
         NULL AS branch_stock_qty,
         NULL AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_matahari_otc_stock mtc
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (mtc.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     ((mtc.item)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Matahari'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
         left join temp_product_dim as a on (a.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT
     WHERE (
             (mtc.pos_cust)::TEXT = ('MATAHARI'::CHARACTER VARYING)::TEXT
         )
 ),
 itg_id_pos_midi_stock_store_qty as 
 (
     SELECT *
     FROM itg_id_pos_midi_stock where 
 rtrim((itg_id_pos_midi_stock.type)::TEXT) = ('QTY'::CHARACTER VARYING)::TEXT
 and rtrim((itg_id_pos_midi_stock.store_dc)::TEXT) = ('STORE STOCK'::CHARACTER VARYING)::TEXT
 ),
 itg_id_pos_midi_stock_store_values as 
 (
     SELECT *
     FROM itg_id_pos_midi_stock where 
 rtrim((itg_id_pos_midi_stock.type)::TEXT) = ('VALUE'::CHARACTER VARYING)::TEXT
 and rtrim((itg_id_pos_midi_stock.store_dc)::TEXT) = ('STORE STOCK'::CHARACTER VARYING)::TEXT
 ),
 itg_id_pos_midi_stock_store_dcqty as 
 (
     SELECT *
     FROM itg_id_pos_midi_stock where 
 rtrim((itg_id_pos_midi_stock.type)::TEXT) = ('QTY'::CHARACTER VARYING)::TEXT
 and rtrim((itg_id_pos_midi_stock.store_dc)::TEXT) = ('DC STOCK'::CHARACTER VARYING)::TEXT
 ),
 itg_id_pos_midi_stock_store_dcvalue as 
 (
     SELECT *
     FROM itg_id_pos_midi_stock where 
 rtrim((itg_id_pos_midi_stock.type)::TEXT) = ('VALUE'::CHARACTER VARYING)::TEXT
 and rtrim((itg_id_pos_midi_stock.store_dc)::TEXT) = ('DC STOCK'::CHARACTER VARYING)::TEXT
 ),
 alfamidi as 
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'ALFAMIDI'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (midi.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
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
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         p."values" AS store_stock_qty,
         q."values" AS store_stock_value,
         r."values" AS branch_stock_qty,
         s."values" AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz AS crtd_dttm
     FROM (
             (
                 itg_id_pos_midi_stock midi
                 LEFT JOIN edw_distributor_group_dim dist ON (
                     (
                         (midi.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                     )
                 )
             )
             LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                 (
                     ((midi.plu)::TEXT = (mds.plu_sku_desc)::TEXT)
                     AND (
                         (MDS.account)::TEXT = ('Alfamidi'::CHARACTER VARYING)::TEXT
                     )
                 )
             )
         )
         left join itg_id_pos_midi_stock_store_qty as p
             on 
             (p.type)::TEXT = (midi.type)::TEXT
             and 
             (p.plu)::TEXT = (midi.plu)::TEXT
             and 
             (p.branch)::TEXT = (midi.branch)::TEXT
             and 
             (p.store_dc)::TEXT = (midi.store_dc)::TEXT 
             and 
             (p.yearmonth)::TEXT = (midi.yearmonth)::TEXT
         left join itg_id_pos_midi_stock_store_values as q
             on 
             (q.type)::TEXT = (midi.type)::TEXT
             and 
             (q.plu)::TEXT = (midi.plu)::TEXT
             and 
             (q.branch)::TEXT = (midi.branch)::TEXT
             and 
             (q.store_dc)::TEXT = (midi.store_dc)::TEXT 
             and 
             (q.yearmonth)::TEXT = (midi.yearmonth)::TEXT
             left join itg_id_pos_midi_stock_store_dcvalue as r on 
             (r.type)::TEXT = (midi.type)::TEXT
             and 
             (r.plu)::TEXT = (midi.plu)::TEXT
             and 
             (r.branch)::TEXT = (midi.branch)::TEXT
             and 
             (r.store_dc)::TEXT = (midi.store_dc)::TEXT 
             and 
             (r.yearmonth)::TEXT = (midi.yearmonth)::TEXT

             left join itg_id_pos_midi_stock_store_dcqty as s on 
             (s.type)::TEXT = (midi.type)::TEXT
             and 
             (s.plu)::TEXT = (midi.plu)::TEXT
             and 
             (s.branch)::TEXT = (midi.branch)::TEXT
             and 
             (s.store_dc)::TEXT = (midi.store_dc)::TEXT 
             and 
             (s.yearmonth)::TEXT = (midi.yearmonth)::TEXT
     WHERE (
             (midi.pos_cust)::TEXT = ('ALFAMIDI'::CHARACTER VARYING)::TEXT
         )
 ),

itg_id_pos_sat_stock_store_qty as 
(
    SELECT *
    FROM itg_id_pos_sat_stock where 
rtrim((itg_id_pos_sat_stock.type)::TEXT) = ('QTY'::CHARACTER VARYING)::TEXT
and rtrim((itg_id_pos_sat_stock.store_dc)::TEXT) = ('STORE STOCK'::CHARACTER VARYING)::TEXT
),
itg_id_pos_sat_stock_store_values as 
(
    SELECT *
    FROM itg_id_pos_sat_stock where 
rtrim((itg_id_pos_sat_stock.type)::TEXT) = ('VALUE'::CHARACTER VARYING)::TEXT
and rtrim((itg_id_pos_sat_stock.store_dc)::TEXT) = ('STORE STOCK'::CHARACTER VARYING)::TEXT
),
itg_id_pos_sat_stock_store_dcqty as 
(
    SELECT *
    FROM itg_id_pos_sat_stock where 
rtrim((itg_id_pos_sat_stock.type)::TEXT) = ('QTY'::CHARACTER VARYING)::TEXT
and rtrim((itg_id_pos_sat_stock.store_dc)::TEXT) = ('DC STOCK'::CHARACTER VARYING)::TEXT
),
itg_id_pos_sat_stock_store_dcvalue as 
(
    SELECT *
    FROM itg_id_pos_sat_stock where 
rtrim((itg_id_pos_sat_stock.type)::TEXT) = ('VALUE'::CHARACTER VARYING)::TEXT
and rtrim((itg_id_pos_sat_stock.store_dc)::TEXT) = ('DC STOCK'::CHARACTER VARYING)::TEXT
),
alfa as 
(
    SELECT 
        'ID' AS sap_cntry_cd,
        'Indonesia' AS sap_cntry_nm,
        'Stock' AS dataset,
        COALESCE(dist.dstrbtr_grp_cd, 'ALFA'::CHARACTER VARYING) AS dstrbtr_grp_cd,
        (
            "substring" (
                (sat.yearmonth)::TEXT,
                1,
                4
            )
        )::CHARACTER VARYING AS "year",
        sat.yearmonth,
        (rtrim((sat.branch)::TEXT))::CHARACTER VARYING AS customer_brnch_code,
        (rtrim((sat.branch)::TEXT))::CHARACTER VARYING AS customer_brnch_name,
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
        NULL AS sales_qty,
        NULL AS sales_value,
        NULL AS service_level,
        NULL AS sales_order,
        NULL AS "share",
        e."values" AS store_stock_qty,
        f."values" AS store_stock_value,
        g."values" AS branch_stock_qty,
        h."values" AS branch_stock_value,
        NULL AS stock_uom,
        NULL AS stock_days,
        current_timestamp()::timestamp_ntz AS crtd_dttm
    FROM (
            
                itg_id_pos_sat_stock sat
                LEFT JOIN edw_distributor_group_dim dist ON (
                    (
                        (sat.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
                    )
                )
            
            LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
                (
                    ((sat.plu)::TEXT = (mds.plu_sku_desc)::TEXT)
                    AND (
                        (MDS.account)::TEXT = ('ALFA'::CHARACTER VARYING)::TEXT
                    )
                )
            )
        )
        left join itg_id_pos_sat_stock_store_qty as e
            on 
            (e.type)::TEXT = (sat.type)::TEXT
            and 
            (e.plu)::TEXT = (sat.plu)::TEXT
            and 
            (e.branch)::TEXT = (sat.branch)::TEXT
            and 
            (e.store_dc)::TEXT = (sat.store_dc)::TEXT 
            and 
            (e.yearmonth)::TEXT = (sat.yearmonth)::TEXT
        left join itg_id_pos_sat_stock_store_values as f
            on 
            (f.type)::TEXT = (sat.type)::TEXT
            and 
            (f.plu)::TEXT = (sat.plu)::TEXT
            and 
            (f.branch)::TEXT = (sat.branch)::TEXT
            and 
            (f.store_dc)::TEXT = (sat.store_dc)::TEXT 
            and 
            (f.yearmonth)::TEXT = (sat.yearmonth)::TEXT
        left join itg_id_pos_sat_stock_store_dcvalue as g on 
            (g.type)::TEXT = (sat.type)::TEXT
            and 
            (g.plu)::TEXT = (sat.plu)::TEXT
            and 
            (g.branch)::TEXT = (sat.branch)::TEXT
            and 
            (g.store_dc)::TEXT = (sat.store_dc)::TEXT 
            and 
            (g.yearmonth)::TEXT = (sat.yearmonth)::TEXT

        left join itg_id_pos_sat_stock_store_dcqty as h on 
            (h.type)::TEXT = (sat.type)::TEXT
            and 
            (h.plu)::TEXT = (sat.plu)::TEXT
            and 
            (h.branch)::TEXT = (sat.branch)::TEXT
            and 
            (h.store_dc)::TEXT = (sat.store_dc)::TEXT 
            and 
            (h.yearmonth)::TEXT = (sat.yearmonth)::TEXT
    WHERE (
            (sat.pos_cust)::TEXT = ('ALFA'::CHARACTER VARYING)::TEXT
        )
),
 super_indo as 
 (
     SELECT 
         'ID' AS sap_cntry_cd,
         'Indonesia' AS sap_cntry_nm,
         'Stock' AS dataset,
         COALESCE(
             dist.dstrbtr_grp_cd,
             'SUPER INDO'::CHARACTER VARYING
         ) AS dstrbtr_grp_cd,
         (
             "substring" (
                 (ind.yearmonth)::TEXT,
                 1,
                 4
             )
         )::CHARACTER VARYING AS "year",
         ind.yearmonth,
         NULL AS customer_brnch_code,
         NULL AS customer_brnch_name,
         NULL AS customer_store_code,
         NULL AS customer_store_name,
         NULL AS customer_franchise,
         NULL AS customer_brand,
         ind.code AS customer_product_code,
         ind.description AS customer_product_desc,
         mds.jj_sap_prod_id,
         mds.brand,
         mds.brand2,
         mds.sku_sales_cube,
         NULL AS customer_product_range,
         NULL AS customer_product_group,
         NULL AS customer_store_class,
         NULL AS customer_store_channel,
         NULL AS sales_qty,
         NULL AS sales_value,
         NULL AS service_level,
         NULL AS sales_order,
         NULL AS "share",
         ind.stock_all_stores_qty AS store_stock_qty,
         ind.stock_all_stores_qty * pd.price AS store_stock_value,
         ind.stock_dc_regular_qty AS branch_stock_qty,
         ind.stock_dc_regular_qty * pd.price AS branch_stock_value,
         NULL AS stock_uom,
         NULL AS stock_days,
         current_timestamp()::timestamp_ntz(9) AS crtd_dttm
     FROM itg_id_pos_superindo_stock ind
         LEFT JOIN edw_distributor_group_dim dist ON (
             (
                 (ind.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT
             )
         )
            
         LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
             (
                 ((ind.code)::TEXT = (mds.plu_sku_desc)::TEXT)
                 AND (
                     (MDS.account)::TEXT = ('Super Indo'::CHARACTER VARYING)::TEXT
                 )
             )
         )
         left join temp_product_dim pd on (pd.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT
        
     WHERE (
             (ind.pos_cust)::TEXT = ('SUPER INDO'::CHARACTER VARYING)::TEXT
         )
 ),
 watson as 
 (
         SELECT 'ID' AS sap_cntry_cd
         ,'Indonesia' AS sap_cntry_nm
         ,'Stock' AS dataset
         ,COALESCE(dist.dstrbtr_grp_cd, 'WATSON'::CHARACTER VARYING) AS dstrbtr_grp_cd
         ,(
             "substring" (
                 (wat.yearmonth)::TEXT
                 ,1
                 ,4
                 )
             )::CHARACTER VARYING AS "year"
         ,wat.yearmonth
         ,NULL AS customer_brnch_code
         ,NULL AS customer_brnch_name
         ,wat.store AS customer_store_code
         ,NULL AS customer_store_name
         ,(((wat.dept)::TEXT || (wat.dept_name)::TEXT))::CHARACTER VARYING AS customer_franchise
         ,wat.item_brand AS customer_brand
         ,wat.item AS customer_product_code
         ,wat.item_name AS customer_product_desc
         ,mds.jj_sap_prod_id
         ,mds.brand
         ,mds.brand2
         ,mds.sku_sales_cube
         ,NULL AS customer_product_range
         ,(((wat.grp)::TEXT || (wat.group_name)::TEXT))::CHARACTER VARYING AS customer_product_group
         ,NULL AS customer_store_class
         ,NULL AS customer_store_channel
         ,NULL AS sales_qty
         ,NULL AS sales_value
         ,NULL AS service_level
         ,NULL AS sales_order
         ,NULL AS "share"
         ,wat."values" AS store_stock_qty
         ,wat."values" * pd.price AS store_stock_value
         ,NULL AS branch_stock_qty
         ,NULL AS branch_stock_value
         ,NULL AS stock_uom
         ,NULL AS stock_days
         ,current_timestamp()::timestamp_ntz(9) AS crtd_dttm
     FROM itg_id_pos_watson_stock wat 
         LEFT JOIN edw_distributor_group_dim dist ON (((wat.pos_cust)::TEXT = (dist.dstrbtr_grp_cd)::TEXT))
         LEFT JOIN itg_mds_id_pos_cust_prod_mapping mds ON (
             (
                 ((wat.item)::TEXT = (mds.plu_sku_desc)::TEXT)
                 AND ((MDS.account)::TEXT = ('Watson'::CHARACTER VARYING)::TEXT)
                 )
             )
         left join temp_product_dim pd on ((pd.jj_sap_prod_id)::TEXT = (mds.jj_sap_prod_id)::TEXT)
        
     WHERE ((wat.pos_cust)::TEXT = ('WATSON'::CHARACTER VARYING)::TEXT)
 ),
final as 
(    
     select * from carrefour
     union all
     select * from guardian
     union all
     select * from indomaret
     union all
     select * from indogrosir
     union all
     select * from matahari_beauty
     union all
     select * from matahari_baby
     union all
     select * from matahari_otc
     union all
     select * from alfamidi
     union all
    select * from alfa
     union all
     select * from super_indo
     union all
     select * from watson
)
select * from final