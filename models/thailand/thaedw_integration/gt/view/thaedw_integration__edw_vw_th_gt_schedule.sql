with source as
(
    select * from snaposeitg_integration.itg_th_gt_schedule
),
final as
(
    select 
        cntry_cd,
        crncy_cd,
        employeeid as sales_rep_id,
        routeid as route_id,
        schedule_date,
        approved,
        saleunit as distributor_id
    from source
)
select * from final