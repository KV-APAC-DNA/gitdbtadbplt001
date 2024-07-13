with wks_skurecom_mi_target_tmp2 as
(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_target_tmp2') }}
),
edw_product_dim as
(
    select * from {{ ref('indedw_integration__edw_product_dim') }}
),
final as(
        SELECT tmp.*, pd.mothersku_code
        FROM wks_skurecom_mi_target_tmp2 tmp
        LEFT JOIN (SELECT mothersku_name, mothersku_code
                FROM edw_product_dim
                WHERE NVL(delete_flag,'XYZ') <> 'N'
                GROUP BY 1,2) pd
            ON UPPER(tmp.mothersku_name) = UPPER(pd.mothersku_name)
)
select * from final