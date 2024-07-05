with edw_vw_os_time_dim as (
        select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
wks_taiwan_base_detail as (
    select * from {{ ref('ntawks_integration__wks_taiwan_base_detail') }}
),
final as 
(   
    Select 
        sap_parent_customer_key,
        sap_parent_customer_desc,
        bnr_key,
        bnr_desc,
        month,
        so_qty,
        so_value,
        inv_qty,
        inv_value,
        case
            when month >
            (
                SELECT -- mnth_id,
                    third_month
                FROM (
                        select mnth_id,
                            lag(mnth_id, 2) over (
                                order by mnth_id
                            ) third_month
                        from (
                                select distinct "year",
                                    mnth_id
                                from edw_vw_os_time_dim
                                where mnth_id <= (
                                        select distinct mnth_id
                                        from edw_vw_os_time_dim
                                        where cal_date = to_date(current_timestamp())
                                    )
                            )
                    )
                WHERE mnth_id = (
                        select distinct mnth_id
                        from edw_vw_os_time_dim
                        where cal_date = to_date(current_timestamp())
                    )
            )
            and 
            (
                nvl(so_value, 0) = 0
                or nvl(inv_value, 0) = 0
            ) then 'Y'
            else 'N'
        end as propagate_flag,
        max(month) over(partition by sap_parent_customer_key) latest_month
    from 
        (
            Select sap_parent_customer_key,
                sap_parent_customer_desc,
                bnr_key,
                bnr_desc,
                month,
                sum(so_qty) so_qty,
                sum(so_value) so_value,
                sum(inv_qty) inv_qty,
                sum(inv_value) inv_value
            from wks_taiwan_base_detail
            where --sap_parent_customer_desc in ('COLES','WOOLWORTHS','METCASH','SYMBION','CENTRAL HEALTHCARE SERVICES PTY LTD','API','SIGMA') 
                --replicated_flag = 'N'
                month <= (
                    select distinct cal_mnth_id
                    from edw_vw_os_time_dim
                    where cal_date = to_date(current_timestamp())
                )
            group by sap_parent_customer_key,
                sap_parent_customer_desc,
                bnr_key,
                bnr_desc,
                month
        )
)
select * from final