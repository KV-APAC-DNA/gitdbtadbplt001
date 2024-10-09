with edw_ph_pos_sellout_target as 
(

    select * from {{ ref('phledw_integration__edw_ph_pos_sellout_targets') }}
),
transformed as 
(
    select 
    * 
    from edw_ph_pos_sellout_target
),
final as 
(
    select 
    *
    from transformed
)
select * from final