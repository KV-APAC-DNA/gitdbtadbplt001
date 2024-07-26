with source as (
    select * from DEV_DNA_LOAD.SNAPJPDCLSDL_RAW.EC_PREDICTIONRESULT
),
final as (
select * from source
)
select * from final