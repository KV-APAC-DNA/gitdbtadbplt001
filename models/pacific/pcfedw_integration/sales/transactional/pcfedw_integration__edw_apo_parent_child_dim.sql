with apo_parent_child_dim 
(
    select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
)
transformed as 
(
    select * from apo_parent_child_dim
)
select * from transformed
