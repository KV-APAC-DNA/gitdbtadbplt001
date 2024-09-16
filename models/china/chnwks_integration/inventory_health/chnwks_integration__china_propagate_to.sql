with wks_china_base_detail_inv as
(
    select * from {{ ref('chnwks_integration__wks_china_base_detail_inv') }}
),
itg_parameter_reg_inventory as
(
    select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
edw_vw_os_time_dim as
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
final as
(
    Select 
    country_name, 
    sap_prnt_cust_key, 
    sap_bnr_key, 
    bu, 
    sold_to, 
    month, 
    so_qty, 
    so_value, 
    inv_qty, 
    inv_value, 
    case when month > (
        SELECT 
        -- mnth_id,
        third_month 
        FROM 
        (
            SELECT 
            mnth_id, 
            LAG(mnth_id, 2) OVER (
                ORDER BY 
                mnth_id
            ) third_month 
            FROM 
            (
                SELECT 
                DISTINCT "year", 
                mnth_id 
                FROM 
                EDW_VW_OS_TIME_DIM 
                WHERE 
                mnth_id <= (
                    SELECT 
                    DISTINCT MNTH_ID 
                    FROM 
                    EDW_VW_OS_TIME_DIM 
                    WHERE 
                    cal_date = to_date(
                        convert_timezone('Asia/Singapore', current_timestamp())
                    )
                )
            )
        ) 
        WHERE 
        mnth_id = (
            SELECT 
            DISTINCT MNTH_ID 
            FROM 
            EDW_VW_OS_TIME_DIM 
            WHERE 
            cal_date = to_date(
                convert_timezone('Asia/Singapore', current_timestamp())
            )
        )
    ) then case when sap_prnt_cust_key in (
        Select 
        parameter_value 
        from 
        itg_parameter_reg_inventory 
        where 
        country_name = 'China' 
        and parameter_name = 'parent_customer'
    ) 
    and (
        nvl(so_value, 0) = 0 
        or nvl(inv_value, 0) = 0
    ) then 'Y' when sap_prnt_cust_key not in(
        Select 
        parameter_value 
        from 
        itg_parameter_reg_inventory 
        where 
        country_name = 'China' 
        and parameter_name = 'parent_customer'
    ) 
    and (
        so_value is null 
        or inv_value is null
    ) then 'Y' else 'N' end else 'N' end as propagate_flag, 
    max(month) over(
        partition by country_name, sold_to, 
        sap_prnt_cust_key, sap_bnr_key, 
        bu
    ) latest_month 
    from 
    (
        Select 
        country_name, 
        sold_to, 
        sap_prnt_cust_key, 
        sap_bnr_key, 
        bu, 
        month, 
        sum(so_qty) so_qty, 
        sum(so_value) so_value, 
        sum(inv_qty) inv_qty, 
        sum(inv_value) inv_value 
        from 
        wks_china_base_detail_inv 
        where 
        month <= (
            SELECT 
            DISTINCT cal_MNTH_ID 
            FROM 
            EDW_VW_OS_TIME_DIM 
            where 
            cal_date = to_date(
                convert_timezone ('Asia/Singapore', current_timestamp())
            )
        ) 
        group by 
        country_name, 
        sold_to, 
        sap_prnt_cust_key, 
        sap_bnr_key, 
        bu, 
        month)
)
select * from final