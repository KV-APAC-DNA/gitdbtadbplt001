with edw_tb_my_curr_prev_sellin_fact as(
    select * from {{ ref('mysedw_integration__edw_tb_my_curr_prev_sellin_fact') }}
)


select * from edw_tb_my_curr_prev_sellin_fact