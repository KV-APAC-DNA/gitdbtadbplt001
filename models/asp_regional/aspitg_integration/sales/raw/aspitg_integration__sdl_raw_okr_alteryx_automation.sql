with sdl_okr_alteryx_automation as(
    select *, dense_rank() over (partition by (upper(kpi), 
    upper(datatype) , coalesce (upper(cluster) , 'NA') ,
     coalesce (upper(market) , 'NA'),coalesce (upper(segment) , 'NA'),
     coalesce (upper(brand) , 'NA') , coalesce (upper(yearmonth) , 'NA') , year , 
    coalesce (quarter , 9999) ,  coalesce (upper(target_type) , 'NA') ) 
     order by filename desc) rn 
    from {{ source('aspsdl_raw', 'sdl_okr_alteryx_automation') }}
    where filename not in (
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_okr_alteryx_automation____null_test')}}
        union all
        select distinct file_name from {{source('aspwks_integration','TRATBL_sdl_okr_alteryx_automation__duplicate_test')}}
        )    
    qualify rn=1
),
final as(
  select kpi,
  datatype,
  CLUSTER,
  market,
  segment,
  brand,
  yearmonth,
  year,
  quarter,
  actuals,
  target,
  target_type,
  filename,
  run_id,
  crt_dttm from sdl_okr_alteryx_automation)
  select * from final