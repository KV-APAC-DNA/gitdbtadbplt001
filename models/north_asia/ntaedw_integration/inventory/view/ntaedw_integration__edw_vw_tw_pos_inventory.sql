with itg_parameter_reg_inventory as(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
edw_vw_os_time_dim as(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
itg_tw_as_watsons_inventory as(
    select * from {{ ref('ntaitg_integration__itg_tw_as_watsons_inventory') }}
),
itg_pos_cust_prod_cd_ean_map as(
    select * from {{ ref('ntaitg_integration__itg_pos_cust_prod_cd_ean_map') }}
),
final as
(   
    SELECT 
        inv.year,
        td.mnth_id,
        inv.week_no AS inv_week,
        null AS inv_date,
        null AS cust_cd,
        inv.item_cd,
        inv.item_desc,
        ean_map.barcd AS ean_num,
        inv.total_stock_qty AS end_stock_qty,
        inv.total_stock_value AS end_stock_val
    FROM
        (
            SELECT edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.wk
            FROM edw_vw_os_time_dim
            GROUP BY edw_vw_os_time_dim."year",
                edw_vw_os_time_dim.mnth_id,
                edw_vw_os_time_dim.wk
        ) td,
        itg_tw_as_watsons_inventory inv
        LEFT JOIN
        (
            SELECT itg_pos_cust_prod_cd_ean_map.cust_nm,
                itg_pos_cust_prod_cd_ean_map.cust_prod_cd,
                min((itg_pos_cust_prod_cd_ean_map.barcd)::text) AS barcd
            FROM itg_pos_cust_prod_cd_ean_map
            WHERE (
                    upper((itg_pos_cust_prod_cd_ean_map.cust_nm)::text) = (
                        (
                            SELECT DISTINCT itg_parameter_reg_inventory.parameter_value
                            FROM itg_parameter_reg_inventory
                            WHERE (
                                    (
                                        (itg_parameter_reg_inventory.country_name)::text = 'TAIWAN'::text
                                    )
                                    AND (
                                        (itg_parameter_reg_inventory.parameter_name)::text = 'offtake_mapping_table'::text
                                    )
                                )
                        )
                    )::text
                )
            GROUP BY itg_pos_cust_prod_cd_ean_map.cust_prod_cd,
                itg_pos_cust_prod_cd_ean_map.cust_nm
        ) ean_map ON
        (
            (
                (
                    (ean_map.cust_prod_cd)::text = (inv.item_cd)::text
                )
            )
        )
    WHERE
        (
            (
                ltrim((inv.week_no)::text, '0'::text) = ltrim((td.wk)::text, '0'::text)
            )
            AND ((inv.year)::text = (td."year")::text)
        )
)
select * from final