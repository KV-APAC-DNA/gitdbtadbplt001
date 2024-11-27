with edw_rpt_sales_details as (
    select * from {{ ref('indedw_integration__edw_rpt_sales_details') }}

),

claimable as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_claimable') }}
),

scheme_vs_sales_rtl_dim as (
    select * from {{ ref('indwks_integration__wks_scheme_vs_sales_rtl_dim') }}
),

final as (
    select *
    from (
        select
            rtruniquecode,
            latest_customer_code,
            retailer_name,
            customer_name,
            region_name,
            zone_name,
            territory_name,
            channel_name,
            class_desc,
            retailer_category_name,
            retailer_channel_1,
            retailer_channel_2,
            retailer_channel_3,
            row_number()
                over (partition by rtruniquecode order by invoice_date desc)
                as rn
        from edw_rpt_sales_details
        where
            rtruniquecode in (
                select distinct rtruniquecode
                from claimable
            )
            and rtruniquecode not in (
                select distinct rtruniquecode from scheme_vs_sales_rtl_dim
            )
    )
    where rn = 1
)

select * from final
