--Import CTE

with source as
( select * 
   from {{ ref('wks_edw_perfect_store_kpi_wt_mnth_cname') }}
   ),

--Logical CTE

--Final CTE
final as
(
select country, customername, scheduledmonth, sum(chnl_wt) total_weight, 
       case when sum(chnl_wt) = 1 
             then 1 
            when sum(chnl_wt) <= 0
             then 0
            else 
                1/sum(chnl_wt) 
            end as calc_weight
 from  source
group by country, customername, scheduledmonth
)

select * from final