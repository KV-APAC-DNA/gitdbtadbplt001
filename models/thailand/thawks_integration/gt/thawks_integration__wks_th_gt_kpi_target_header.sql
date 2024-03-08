with source as (
    select * from {{ source('thaitg_integration', 'wks_th_gt_kpi_target_header') }}),
final as (
select 
    cntry_cd::varchar(5) as cntry_cd,
    jan::varchar(2) as jan,
    feb::varchar(2) as feb,
    mar::varchar(2) as mar,
    apr::varchar(2) as apr,
    may::varchar(2) as may,
    jun::varchar(2) as jun,
    jul::varchar(2) as jul,
    aug::varchar(2) as aug,
    sep::varchar(2) as sep,
    oct::varchar(2) as oct,
    nov::varchar(2) as nov,
    dec::varchar(2) as dec
from source
)
select * from final
    
