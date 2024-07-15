with source as (
    select * from {{ ref('ntaedw_integration__edw_rpt_tw_sales_incentive') }}
),
final as (
    select
        source_type as "source_type",
        cntry_cd as "cntry_cd",
        crncy_cd as "crncy_cd",
        to_crncy as "to_crncy",
        psr_code as "psr_code",
        psr_name as "psr_name",
        year as "year",
        qrtr as "qrtr",
        mnth_id as "mnth_id",
        report_to as "report_to",
        reportto_name as "reportto_name",
        reverse as "reverse",
        monthly_actual as "monthly_actual",
        monthly_target as "monthly_target",
        monthly_achievement as "monthly_achievement",
        monthly_incentive_amount as "monthly_incentive_amount",
        quarterly_actual as "quarterly_actual",
        quarterly_target as "quarterly_target",
        quarterly_achievement as "quarterly_achievement",
        quarterly_incentive_amount as "quarterly_incentive_amount",
        crt_dttm as "crt_dttm"
    from source
)
select * from final