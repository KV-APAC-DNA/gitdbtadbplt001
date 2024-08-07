with wks_vietnam_base as (
    select * from {{ ref('vnmwks_integration__wks_vietnam_base') }}
),
edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
all_months as (
    SELECT DISTINCT sap_parent_customer_key,
            sap_parent_customer_desc,
            matl_num,
            mnth_id
        FROM (
                SELECT DISTINCT sap_parent_customer_key,
                    sap_parent_customer_desc,
                    matl_num
                FROM wks_vietnam_base
            ) a,
            (
                SELECT DISTINCT "year",
                    mnth_id
                FROM edw_vw_os_time_dim
                WHERE "year" >(DATE_PART(YEAR, current_timestamp()::date) -6)
            ) b
    
),
final as (
    SELECT all_months.sap_parent_customer_key,
    all_months.sap_parent_customer_desc,
    all_months.matl_num,
    all_months.mnth_id AS month,
    SUM(b.so_qty) AS so_qty,
    SUM(b.so_value) AS so_value,
    SUM(b.inv_qty) AS inv_qty,
    SUM(b.inv_value) AS inv_value,
    SUM(b.sell_in_qty) AS sell_in_qty,
    SUM(b.sell_in_value) AS sell_in_value
    from all_months, wks_vietnam_base b
    WHERE all_months.sap_parent_customer_key = b.sap_parent_customer_key(+)
    AND all_months.sap_parent_customer_desc = b.sap_parent_customer_desc(+)
    AND all_months.matl_num = b.matl_num(+)
    AND mnth_id = month(+)
    GROUP BY all_months.sap_parent_customer_key,
    all_months.sap_parent_customer_desc,
    all_months.matl_num,
    all_months.mnth_id
)
select * from final