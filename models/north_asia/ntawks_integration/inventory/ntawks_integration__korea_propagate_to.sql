with edw_vw_os_time_dim as (
    select *
    from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_korea_base_detail as (
    select *
    from {{ ref('ntawks_integration__wks_korea_base_detail') }}
),
final as (
    Select sap_parent_customer_key,
        month,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        case
            when month > (
                SELECT 
                    third_month
                FROM (
                        SELECT mnth_id,
                            LAG(mnth_id, 2) OVER (
                                ORDER BY mnth_id
                            ) third_month
                        FROM (
                                SELECT DISTINCT "year" as year,
                                    mnth_id
                                FROM edw_vw_os_time_dim
                                WHERE mnth_id <= (
                                        SELECT DISTINCT MNTH_ID
                                        FROM edw_vw_os_time_dim
                                        WHERE cal_date = to_date(current_timestamp())
                                    )
                            )
                    )
                WHERE mnth_id = (
                        SELECT DISTINCT MNTH_ID
                        FROM edw_vw_os_time_dim
                        WHERE cal_date = to_date(current_timestamp())
                    )
            )
            and (
                nvl(so_value, 0) = 0
                or nvl(inv_value, 0) = 0
            ) then 'Y'
            else 'N'
        end as propagate_flag,
        max(month) over(partition by sap_parent_customer_key) latest_month
    from (
            Select sap_parent_customer_key,
                month,
                sum(so_qty) so_qty,
                sum(so_value) so_value,
                sum(inv_qty) inv_qty,
                sum(inv_value) inv_value
            from wks_korea_base_detail
            where replicated_flag = 'N'
            group by sap_parent_customer_key,
                month
        )
)
select *
from final