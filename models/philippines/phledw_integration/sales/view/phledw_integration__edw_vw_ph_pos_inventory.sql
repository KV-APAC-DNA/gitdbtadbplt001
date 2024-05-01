with
itg_mds_ph_ref_pos_primary_sold_to as 
(
    select * from snaposeitg_integration.itg_mds_ph_ref_pos_primary_sold_to
),
itg_ph_as_watsons_inventory as 
(
    select * from snaposeitg_integration.itg_ph_as_watsons_inventory
),
itg_mds_ph_pos_product as 
(
    select * from snaposeitg_integration.itg_mds_ph_pos_product
),
final as
(
SELECT 
    inv.year,
    inv.mnth_no,
    inv.inv_week,
    NULL AS inv_date,
    COALESCE(cust.primary_soldto, 'NA'::character varying) AS cust_cd,
    inv.item_cd,
    inv.item_desc,
    ltrim((mat.sap_item_cd)::text, '0'::text) AS sap_matl_num,
    inv.total_units AS end_stock_qty,
    inv.total_cost AS end_stock_val
FROM (
        SELECT DISTINCT itg_mds_ph_ref_pos_primary_sold_to.primary_soldto
        FROM itg_mds_ph_ref_pos_primary_sold_to
        WHERE (
                (
                    upper(
                        (itg_mds_ph_ref_pos_primary_sold_to.cust_cd)::text
                    ) = 'WAT'::text
                )
                AND (
                    (itg_mds_ph_ref_pos_primary_sold_to.active)::text = 'Y'::text
                )
            )
    ) cust,
    (
        (
            SELECT itg_ph_as_watsons_inventory.year,
                itg_ph_as_watsons_inventory.mnth_no,
                itg_ph_as_watsons_inventory.inv_week,
                itg_ph_as_watsons_inventory.item_cd,
                itg_ph_as_watsons_inventory.item_desc,
                itg_ph_as_watsons_inventory.total_units,
                itg_ph_as_watsons_inventory.total_cost,
                itg_ph_as_watsons_inventory.avg_sales_cost,
                itg_ph_as_watsons_inventory.wks_sup,
                itg_ph_as_watsons_inventory.remarks,
                itg_ph_as_watsons_inventory.br_ol_pl_ex,
                itg_ph_as_watsons_inventory.group_name,
                itg_ph_as_watsons_inventory.dept_name,
                itg_ph_as_watsons_inventory.class_name,
                itg_ph_as_watsons_inventory.sub_class_name,
                itg_ph_as_watsons_inventory.br_ol_pl_ex_subclass,
                itg_ph_as_watsons_inventory.subclass,
                itg_ph_as_watsons_inventory.catman,
                itg_ph_as_watsons_inventory.item_status,
                itg_ph_as_watsons_inventory.item_class,
                itg_ph_as_watsons_inventory.hold_reason_code,
                itg_ph_as_watsons_inventory.site_code,
                itg_ph_as_watsons_inventory.site_name,
                itg_ph_as_watsons_inventory.gwp,
                itg_ph_as_watsons_inventory.ret_non_ret,
                itg_ph_as_watsons_inventory.good_bad_13_wks,
                itg_ph_as_watsons_inventory.filename,
                itg_ph_as_watsons_inventory.crtd_dttm
            FROM itg_ph_as_watsons_inventory
            WHERE (
                    upper((itg_ph_as_watsons_inventory.group_name)::text) <> 'HEALTH AND FITNESS'::text
                )
        ) inv
        LEFT JOIN (
            SELECT itg_mds_ph_pos_product.code,
                itg_mds_ph_pos_product.mnth_id,
                itg_mds_ph_pos_product.item_cd,
                itg_mds_ph_pos_product.bar_cd,
                itg_mds_ph_pos_product.item_nm,
                itg_mds_ph_pos_product.sap_item_cd,
                itg_mds_ph_pos_product.sap_item_desc,
                itg_mds_ph_pos_product.parent_cust_cd,
                itg_mds_ph_pos_product.parent_cust_nm,
                itg_mds_ph_pos_product.jnj_item_desc,
                itg_mds_ph_pos_product.jnj_matl_cse_barcode,
                itg_mds_ph_pos_product.jnj_matl_pc_barcode,
                itg_mds_ph_pos_product.early_bk_period,
                itg_mds_ph_pos_product.cust_conv_factor,
                itg_mds_ph_pos_product.cust_item_prc,
                itg_mds_ph_pos_product.jnj_matl_shipper_barcode,
                itg_mds_ph_pos_product.jnj_matl_consumer_barcode,
                itg_mds_ph_pos_product.jnj_pc_per_cust_unit,
                itg_mds_ph_pos_product.computed_price_per_unit,
                itg_mds_ph_pos_product.jj_price_per_unit,
                itg_mds_ph_pos_product.cust_sku_grp,
                itg_mds_ph_pos_product.uom,
                itg_mds_ph_pos_product.jnj_pc_per_cse,
                itg_mds_ph_pos_product.lst_period,
                itg_mds_ph_pos_product.cust_cd,
                itg_mds_ph_pos_product.cust_cd2,
                itg_mds_ph_pos_product.last_chg_datetime,
                itg_mds_ph_pos_product.effective_from,
                itg_mds_ph_pos_product.effective_to,
                itg_mds_ph_pos_product.active,
                itg_mds_ph_pos_product.crtd_dttm,
                itg_mds_ph_pos_product.updt_dttm
            FROM itg_mds_ph_pos_product
            WHERE (
                    (
                        upper((itg_mds_ph_pos_product.cust_cd)::text) = 'WAT'::text
                    )
                    AND (
                        (itg_mds_ph_pos_product.active)::text = 'Y'::text
                    )
                )
        ) mat ON (
            (
                (
                    ((inv.year)::text || (inv.mnth_no)::text) = (mat.mnth_id)::text
                )
                AND ((inv.item_cd)::text = (mat.item_cd)::text)
            )
        )
    )
)
select * from final