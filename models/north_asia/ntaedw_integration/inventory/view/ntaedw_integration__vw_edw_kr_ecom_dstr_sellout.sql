with 
itg_kr_ecom_dstr_sellout as 
(
    select * from {{ ref('ntaitg_integration__itg_kr_ecom_dstr_sellout') }}
),
edw_material_sales_dim as 
(
    select * from {{ ref('aspedw_integration__edw_material_sales_dim') }}
),
a as (
SELECT so.dstr_cd,
    so.matl_num,
    so.ean,
    so.brand_name,
    so.sku_name,
    so.so_date,
    so.so_qty,
    so.crt_dttm
FROM itg_kr_ecom_dstr_sellout so
WHERE (
        CASE
            WHEN ((so.matl_num)::text = ''::text) THEN NULL::character varying
            ELSE so.matl_num
        END IS NOT NULL
    )),

b as (
SELECT derived_table1.dstr_cd,
    derived_table1.msd_matl AS matl_num,
    derived_table1.ean,
    derived_table1.brand_name,
    derived_table1.sku_name,
    derived_table1.so_date,
    derived_table1.so_qty,
    derived_table1.crt_dttm
FROM (
        SELECT so.dstr_cd,
            so.matl_num,
            so.ean,
            so.brand_name,
            so.sku_name,
            so.so_date,
            so.so_qty,
            so.crt_dttm,
            msd.matl_num AS msd_matl,
            row_number() OVER(
                PARTITION BY msd.ean_num
                ORDER BY msd.matl_num
            ) AS rn
        FROM itg_kr_ecom_dstr_sellout so,
             edw_material_sales_dim msd
        WHERE (
                (
                    (
                        CASE
                            WHEN ((so.matl_num)::text = ''::text) THEN NULL::character varying
                            ELSE so.matl_num
                        END
                    )::text = NULL::text
                )
                AND ((so.ean)::text = (msd.ean_num)::text)
            )
    ) derived_table1
WHERE (derived_table1.rn = 1)),
final as 
(
select * from a 
union all
select * from b
)select * from final