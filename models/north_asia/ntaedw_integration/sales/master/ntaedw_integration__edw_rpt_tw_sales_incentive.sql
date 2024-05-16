with source as(
    select * from DEV_DNA_CORE.SNAPNTAedw_INTEGRATION.edw_v_rpt_tw_sales_incentive
),
final as(
    select 
        source_type::varchar(15) as source_type,
        cntry_cd::varchar(5) as cntry_cd,
        crncy_cd::varchar(10) as crncy_cd,
        to_crncy::varchar(5) as to_crncy,
        coalesce(psr_code, 'Not Available')::varchar(100) AS psr_code,
        psr_name::varchar(255) as psr_name,
        year::number(18,0) as year,
        qrtr::varchar(14) as qrtr,
        mnth_id::number(18,0) as mnth_id,
        report_to::varchar(500) as report_to,
        reportto_name::varchar(500) as reportto_name,
        reverse::varchar(500) as reverse,
        monthly_actual::number(38,4) as monthly_actual,
        monthly_target::number(38,4) as monthly_target,
        coalesce(monthly_achievement, 0)::float AS monthly_achievement,
        coalesce(monthly_incentive_amount, 0)::number(38,4) AS monthly_incentive_amount,
        quarterly_actual::number(38,4) as quarterly_actual,
        quarterly_target::number(38,4) as quarterly_target,
        coalesce(quarterly_achievement, 0)::float AS quarterly_achievement,
        coalesce(quarterly_incentive_amount, 0)::number(38,4) AS quarterly_incentive_amount,
        current_timestamp()::timestamp_ntz(9) AS crt_dttm 
    from source
)
select * from final
