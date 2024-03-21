with itg_th_dms_inventory_fact as (
    select * from {{ ref('thaitg_integration__itg_th_dms_inventory_fact') }}
),
itg_th_dstrbtr_customer_dim as (
    select * from {{ ref('thaitg_integration__itg_th_dstrbtr_customer_dim') }}
),
itg_th_dtsitemmaster as (
    select * from {{ ref('thaitg_integration__itg_th_dtsitemmaster') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_vw_th_material_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_material_dim') }}
),
edw_vw_th_dstrbtr_customer_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_dstrbtr_customer_dim') }}
),
edw_vw_th_customer_dim as (
    select * from {{ ref('thaedw_integration__edw_vw_th_customer_dim') }}
),
edw_material_dim as (
    select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
inventory as (
    SELECT 
        inv_f.typ,
        inv_f.cntry_cd,
        inv_f.cntry_nm,
        inv_f.bill_date,
        inv_f.dstrbtr_grp_cd,
        inv_f.dstrbtr_nm,
        inv_f.dstrbtr_matl_num,
        inv_f.warehse_cd,
        inv_f.pka_root_code,
        inv_f.warehse_grp,
        inv_f.batch_expiry_date,
        inv_f.inv_qty,
        inv_f.inv_amt,
        inv_f.sls_prc2,
        inv_f.unit_per_case,
        inv_f.cases,
        inv_f.eaches,
        inv_f.sls_amt,
        round(
            (
                (
                    (
                        ((inv_f.cases)::numeric)::numeric(18, 0) * inv_f.sls_amt
                    ) * ((inv_f.unit_per_case)::numeric)::numeric(18, 0)
                ) + (inv_f.eaches * inv_f.sls_amt)
            ),
            2
        ) AS total_bht
    FROM (
            SELECT inv.typ,
                inv.cntry_cd,
                inv.cntry_nm,
                inv.bill_date,
                inv.dstrbtr_grp_cd,
                inv.dstrbtr_nm,
                inv.dstrbtr_matl_num,
                inv.warehse_cd,
                inv.pka_root_code,
                inv.warehse_grp,
                inv.batch_expiry_date,
                inv.inv_qty,
                inv.inv_amt,
                inv.sls_prc2,
                inv.unit_per_case,
                CASE
                    WHEN (
                        ((inv.unit_per_case)::numeric)::numeric(18, 0) < inv.inv_qty
                    ) THEN (
                        "substring"(
                            (
                                (
                                    (
                                        round(
                                            (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                            2
                                        ) / ((inv.unit_per_case)::numeric)::numeric(18, 0)
                                    )
                                )::character varying
                            )::text,
                            0,
                            charindex(
                                ('.'::character varying)::text,
                                (
                                    (
                                        (
                                            round(
                                                (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                                2
                                            ) / ((inv.unit_per_case)::numeric)::numeric(18, 0)
                                        )
                                    )::character varying
                                )::text
                            )
                        )
                    )::integer
                    WHEN (
                        ((inv.unit_per_case)::numeric)::numeric(18, 0) >= inv.inv_qty
                    ) THEN CASE
                        WHEN (
                            ((inv.unit_per_case)::numeric)::numeric(18, 0) > round(
                                (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                2
                            )
                        ) THEN 0
                        ELSE (
                            "substring"(
                                (
                                    (
                                        (
                                            round(
                                                (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                                2
                                            ) / ((inv.unit_per_case)::numeric)::numeric(18, 0)
                                        )
                                    )::character varying
                                )::text,
                                0,
                                charindex(
                                    ('.'::character varying)::text,
                                    (
                                        (
                                            (
                                                round(
                                                    (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                                    2
                                                ) / ((inv.unit_per_case)::numeric)::numeric(18, 0)
                                            )
                                        )::character varying
                                    )::text
                                )
                            )
                        )::integer
                    END
                    ELSE NULL::integer
                END AS cases,
                CASE
                    WHEN (
                        ((inv.unit_per_case)::numeric)::numeric(18, 0) < inv.inv_qty
                    ) THEN (
                        round(
                            (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                            2
                        ) % ((inv.unit_per_case)::numeric)::numeric(18, 0)
                    )
                    WHEN (
                        ((inv.unit_per_case)::numeric)::numeric(18, 0) >= inv.inv_qty
                    ) THEN CASE
                        WHEN (
                            ((inv.unit_per_case)::numeric)::numeric(18, 0) > round(
                                (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                2
                            )
                        ) THEN round(
                            (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                            2
                        )
                        ELSE (
                            round(
                                (inv.inv_qty * ((12)::numeric)::numeric(18, 0)),
                                2
                            ) % ((inv.unit_per_case)::numeric)::numeric(18, 0)
                        )
                    END
                    ELSE (NULL::numeric)::numeric(18, 0)
                END AS eaches,
                (inv.sls_prc2 / ((12)::numeric)::numeric(18, 0)) AS sls_amt
            FROM (
                    SELECT 'Inventory'::character varying AS typ,
                        'TH'::character varying AS cntry_cd,
                        'Thailand'::character varying AS cntry_nm,
                        inventory.inv_dt AS bill_date,
                        inventory.dstrbtr_grp_cd,
                        inventory.dstrbtr_matl_num,
                        inventory.warehse_cd,
                        mtldim.pka_root_code,
                        cdim.dstrbtr_nm,
                        CASE
                            WHEN (
                                "substring"((inventory.warehse_cd)::text, 2, 1) = ('7'::character varying)::text
                            ) THEN 'Damage Goods'::character varying
                            WHEN (
                                (inventory.warehse_cd)::text = ('V902'::character varying)::text
                            ) THEN 'Damage Goods'::character varying
                            WHEN (
                                (
                                    "substring"((inventory.warehse_cd)::text, 2, 1) <> ('7'::character varying)::text
                                )
                                OR (
                                    (inventory.warehse_cd)::text <> ('V902'::character varying)::text
                                )
                            ) THEN 'Normal Goods'::character varying
                            ELSE NULL::character varying
                        END AS warehse_grp,
                        inventory.batch_expiry_date,
                        inventory.inv_qty,
                        inventory.inv_amt,
                        itg_material.sls_prc2,
                        itg_material.unit_per_case
                    FROM (
                            (
                                (
                                    (
                                        SELECT itg_th_dms_inventory_fact.whcode AS warehse_cd,
                                            itg_th_dms_inventory_fact.distributorid AS dstrbtr_grp_cd,
                                            itg_th_dms_inventory_fact.productcode AS dstrbtr_matl_num,
                                            itg_th_dms_inventory_fact.recdate AS inv_dt,
                                            itg_th_dms_inventory_fact.expirydate AS batch_expiry_date,
                                            sum(itg_th_dms_inventory_fact.qty) AS inv_qty,
                                            sum(itg_th_dms_inventory_fact.amount) AS inv_amt
                                        FROM itg_th_dms_inventory_fact
                                        WHERE (
                                                (
                                                    itg_th_dms_inventory_fact.qty > (((0)::numeric)::numeric(18, 0))::numeric(19, 6)
                                                )
                                                AND (
                                                    itg_th_dms_inventory_fact.distributorid IN (
                                                        SELECT DISTINCT itg_th_dms_inventory_fact.distributorid
                                                        FROM itg_th_dms_inventory_fact
                                                        WHERE (itg_th_dms_inventory_fact.expirydate IS NOT NULL)
                                                    )
                                                )
                                            )
                                        GROUP BY itg_th_dms_inventory_fact.whcode,
                                            itg_th_dms_inventory_fact.distributorid,
                                            itg_th_dms_inventory_fact.productcode,
                                            itg_th_dms_inventory_fact.recdate,
                                            itg_th_dms_inventory_fact.expirydate
                                    ) inventory
                                    LEFT JOIN edw_material_dim mtldim ON (
                                        (
                                            (inventory.dstrbtr_matl_num)::text = (
                                                (
                                                    ltrim(
                                                        (mtldim.matl_num)::text,
                                                        ((0)::character varying)::text
                                                    )
                                                )::character varying
                                            )::text
                                        )
                                    )
                                )
                                LEFT JOIN (
                                    SELECT derived_table1.dstrbtr_id,
                                        derived_table1.dstrbtr_nm,
                                        derived_table1.rn
                                    FROM (
                                            SELECT itg_th_dstrbtr_customer_dim.dstrbtr_id,
                                                itg_th_dstrbtr_customer_dim.dstrbtr_nm,
                                                row_number() OVER(
                                                    PARTITION BY itg_th_dstrbtr_customer_dim.dstrbtr_id
                                                    ORDER BY itg_th_dstrbtr_customer_dim.dstrbtr_nm
                                                ) AS rn
                                            FROM itg_th_dstrbtr_customer_dim
                                        ) derived_table1
                                    WHERE (derived_table1.rn = 1)
                                ) cdim ON (
                                    (
                                        (inventory.dstrbtr_grp_cd)::text = (cdim.dstrbtr_id)::text
                                    )
                                )
                            )
                            LEFT JOIN (
                                SELECT DISTINCT itg_th_dtsitemmaster.item_cd,
                                    itg_th_dtsitemmaster.sls_prc2,
                                    itg_th_dtsitemmaster.unit_per_case
                                FROM itg_th_dtsitemmaster
                            ) itg_material ON (
                                (
                                    (inventory.dstrbtr_matl_num)::text = (itg_material.item_cd)::text
                                )
                            )
                        )
                ) inv
        ) inv_f
),
cust as (
    SELECT 
        sellout_cust.dstrbtr_grp_cd,
        sellin_cust.sap_cust_id,
        sellin_cust.sap_cust_nm,
        sellin_cust.sap_sls_org,
        sellin_cust.sap_cmp_id,
        sellin_cust.sap_cntry_cd,
        sellin_cust.sap_cntry_nm,
        sellin_cust.sap_addr,
        sellin_cust.sap_region,
        sellin_cust.sap_state_cd,
        sellin_cust.sap_city,
        sellin_cust.sap_post_cd,
        sellin_cust.sap_chnl_cd,
        sellin_cust.sap_chnl_desc,
        sellin_cust.sap_sls_office_cd,
        sellin_cust.sap_sls_office_desc,
        sellin_cust.sap_sls_grp_cd,
        sellin_cust.sap_sls_grp_desc,
        sellin_cust.sap_prnt_cust_key,
        sellin_cust.sap_prnt_cust_desc,
        sellin_cust.sap_cust_chnl_key,
        sellin_cust.sap_cust_chnl_desc,
        sellin_cust.sap_cust_sub_chnl_key,
        sellin_cust.sap_sub_chnl_desc,
        sellin_cust.sap_go_to_mdl_key,
        sellin_cust.sap_go_to_mdl_desc,
        sellin_cust.sap_bnr_key,
        sellin_cust.sap_bnr_desc,
        sellin_cust.sap_bnr_frmt_key,
        sellin_cust.sap_bnr_frmt_desc,
        sellin_cust.retail_env
    FROM (
            (
                SELECT DISTINCT edw_vw_th_dstrbtr_customer_dim.dstrbtr_grp_cd,
                    edw_vw_th_dstrbtr_customer_dim.sap_soldto_code
                FROM edw_vw_th_dstrbtr_customer_dim
                WHERE (
                        (edw_vw_th_dstrbtr_customer_dim.cntry_cd)::text = ('TH'::character varying)::text
                    )
            ) sellout_cust
            JOIN edw_vw_th_customer_dim sellin_cust ON (
                (
                    (sellout_cust.sap_soldto_code)::text = (sellin_cust.sap_cust_id)::text
                )
            )
        )
),
matl as (
    SELECT DISTINCT 
        edw_vw_th_material_dim.cntry_key,
        edw_vw_th_material_dim.sap_matl_num,
        edw_vw_th_material_dim.sap_mat_desc,
        edw_vw_th_material_dim.gph_region,
        edw_vw_th_material_dim.gph_prod_frnchse,
        edw_vw_th_material_dim.gph_prod_brnd,
        edw_vw_th_material_dim.gph_prod_vrnt,
        edw_vw_th_material_dim.gph_prod_sgmnt,
        edw_vw_th_material_dim.gph_prod_put_up_desc,
        edw_vw_th_material_dim.gph_prod_sub_brnd AS prod_sub_brand,
        edw_vw_th_material_dim.gph_prod_subsgmnt AS prod_subsegment,
        edw_vw_th_material_dim.gph_prod_ctgry AS prod_category,
        edw_vw_th_material_dim.gph_prod_subctgry AS prod_subcategory
    FROM edw_vw_th_material_dim
    WHERE (
            (edw_vw_th_material_dim.cntry_key)::text = ('TH'::character varying)::text
        )
),
final as (
    SELECT 
        inventory.typ,
        inventory.cntry_cd,
        inventory.cntry_nm,
        inventory.bill_date,
        inventory.dstrbtr_grp_cd,
        inventory.dstrbtr_nm,
        inventory.dstrbtr_matl_num,
        inventory.warehse_cd,
        inventory.warehse_grp,
        inventory.batch_expiry_date,
        inventory.pka_root_code AS root_code,
        inventory.inv_qty,
        inventory.inv_amt,
        inventory.sls_prc2,
        inventory.unit_per_case,
        inventory.cases,
        inventory.eaches,
        inventory.sls_amt,
        inventory.total_bht,
        "time"."year",
        "time".qrtr,
        "time".mnth_id,
        "time".mnth_no,
        "time".wk,
        "time".mnth_wk_no,
        "time".cal_date,
        matl.sap_mat_desc AS sku_description,
        matl.gph_prod_frnchse AS franchise,
        CASE
            WHEN (
                "left"((inventory.dstrbtr_matl_num)::text, 1) = ((9)::character varying)::text
            ) THEN 'FOC'::character varying
            WHEN (
                "left"((inventory.dstrbtr_matl_num)::text, 2) = ((99)::character varying)::text
            ) THEN 'FOC'::character varying
            WHEN (
                "left"((inventory.dstrbtr_matl_num)::text, 2) = ((89)::character varying)::text
            ) THEN 'PREMIUM'::character varying
            ELSE matl.gph_prod_brnd
        END AS brand,
        matl.gph_prod_vrnt AS variant,
        matl.gph_prod_sgmnt AS segment,
        matl.gph_prod_put_up_desc AS put_up_description,
        matl.prod_sub_brand,
        matl.prod_subsegment,
        matl.prod_category,
        matl.prod_subcategory,
        cust.sap_cust_id,
        cust.sap_cust_nm,
        cust.sap_sls_org,
        cust.sap_cmp_id,
        cust.sap_cntry_cd,
        cust.sap_cntry_nm,
        cust.sap_addr,
        cust.sap_region,
        cust.sap_state_cd,
        cust.sap_city,
        cust.sap_post_cd,
        cust.sap_chnl_cd,
        cust.sap_chnl_desc,
        cust.sap_sls_office_cd,
        cust.sap_sls_office_desc,
        cust.sap_sls_grp_cd,
        cust.sap_sls_grp_desc,
        cust.sap_prnt_cust_key,
        cust.sap_prnt_cust_desc,
        cust.sap_cust_chnl_key,
        cust.sap_cust_chnl_desc,
        cust.sap_cust_sub_chnl_key,
        cust.sap_sub_chnl_desc,
        cust.sap_go_to_mdl_key,
        cust.sap_go_to_mdl_desc,
        cust.sap_bnr_key,
        cust.sap_bnr_desc,
        cust.sap_bnr_frmt_key,
        cust.sap_bnr_frmt_desc,
        cust.retail_env
    FROM (
            (
                (
                    inventory
                    RIGHT JOIN (
                        SELECT DISTINCT edw_vw_os_time_dim."year",
                            edw_vw_os_time_dim.qrtr,
                            edw_vw_os_time_dim.mnth_id,
                            edw_vw_os_time_dim.mnth_no,
                            edw_vw_os_time_dim.wk,
                            edw_vw_os_time_dim.mnth_wk_no,
                            edw_vw_os_time_dim.cal_date
                        FROM edw_vw_os_time_dim edw_vw_os_time_dim
                        WHERE (
                                (
                                    edw_vw_os_time_dim."year" > (
                                        "date_part"(
                                            year,
                                            (current_timestamp::character varying)::timestamp without time zone
                                        ) - 3
                                    )
                                )
                                OR (
                                    edw_vw_os_time_dim."year" > (
                                        "date_part"(
                                            year,
                                            (current_timestamp::character varying)::timestamp without time zone
                                        ) - 3
                                    )
                                )
                            )
                    ) "time" ON (
                        (
                            inventory.bill_date = ("time".cal_date)::timestamp without time zone
                        )
                    )
                )
                LEFT JOIN matl ON (
                    (
                        upper((inventory.dstrbtr_matl_num)::text) = upper(
                            ltrim(
                                (matl.sap_matl_num)::text,
                                ('0'::character varying)::text
                            )
                        )
                    )
                )
            )
            LEFT JOIN cust ON (
                (
                    (cust.dstrbtr_grp_cd)::text = (inventory.dstrbtr_grp_cd)::text
                )
            )
        )
)
select * from final