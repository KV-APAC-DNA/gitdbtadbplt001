with 
itg_kr_ecom_dstr_inventory as 
(
    select * from {{ ref('ntaitg_integration__itg_kr_ecom_dstr_inventory') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
a as (
SELECT inv.dstr_cd,
    inv.matl_num,
    inv.ean,
    inv.brand_name,
    inv.sku_name,
    inv.inv_date,
    inv.inventory_qty,
    inv.crt_dttm
FROM itg_kr_ecom_dstr_inventory inv
WHERE (
        CASE
            WHEN ((inv.matl_num)::text = ''::text) THEN NULL::character varying
            ELSE inv.matl_num
        END IS NOT NULL
    )),

b as (
SELECT derived_table1.dstr_cd,
    derived_table1.msd_matl AS matl_num,
    derived_table1.ean,
    derived_table1.brand_name,
    derived_table1.sku_name,
    derived_table1.inv_date,
    derived_table1.inventory_qty,
    derived_table1.crt_dttm
FROM (
        SELECT inv.dstr_cd,
            inv.matl_num,
            inv.ean,
            inv.brand_name,
            inv.sku_name,
            inv.inv_date,
            inv.inventory_qty,
            inv.crt_dttm,
            msd.matl_num AS msd_matl,
            row_number() OVER(
                PARTITION BY msd.ean_num
                ORDER BY msd.matl_num
            ) AS rn
        FROM itg_kr_ecom_dstr_inventory inv,
            edw_material_sales_dim msd
        WHERE (
                (
                    CASE
                        WHEN ((inv.matl_num)::text = ''::text) THEN NULL::character varying
                        ELSE inv.matl_num
                    END IS NULL
                )
                AND ((inv.ean)::text = (msd.ean_num)::text)
            )
    ) derived_table1
WHERE (derived_table1.rn = 1)),
final as 
(
select * from a 
union all
select * from b
)select * from final