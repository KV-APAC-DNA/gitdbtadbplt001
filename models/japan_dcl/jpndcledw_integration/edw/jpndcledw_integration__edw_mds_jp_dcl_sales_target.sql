with source as
(
    --select * from {{ source('jpdclsdl_raw', 'sdl_mds_jp_dcl_targets')  }}
    select * from {{ source('jpdclsdl_raw', 'SDL_MDS_JP_DCL_Targets_ADFTemp')  }}
    
),
final as (
    SELECT fiscal_year_code || month as month_id,  
case channel_code
    when 'Tsuhan-Web' then 'Web'
    when 'Tsuhan-Call' then '通販'
    when 'Tsuhan-Store' then '直営・百貨店'
    when '3r Party EC' then 'E-Commerce'
    when 'WholeSale' then 'Wholesaler'
end channel_code, kpi_code, target FROM (
SELECT fiscal_year_code,kpi_code, channel_code, kpitype_code, month, target from source
unpivot (target for month in ("01","02","03","04","05","06","07","08","09","10","11","12"))
where fiscal_year_code = extract (year from current_date))
)
select * from final
