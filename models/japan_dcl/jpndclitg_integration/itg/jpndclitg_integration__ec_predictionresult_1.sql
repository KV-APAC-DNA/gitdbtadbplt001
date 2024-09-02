with source as (
    select * from {{source('jpdclsdl_raw','ec_predictionresult')}}
),
final as (
select * from source
)
select * from final