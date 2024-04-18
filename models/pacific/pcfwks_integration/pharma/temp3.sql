with final as (
    select temp1.* from {{ ref('temp1') }} temp1  inner join {{ ref('temp2') }} temp2
     on temp1.delvry_dt = temp2.delvry_dt
)
select * from final