--Import CTEs
with edw_tb_my_sellin_analysis_part1 as (
    select * from {{ ref('mysedw_integration__edw_tb_my_sellin_analysis_part1') }}
),
edw_tb_my_sellin_analysis_part2 as (
    select * from {{ ref('mysedw_integration__edw_tb_my_sellin_analysis_part2') }}
)

select * from edw_tb_my_sellin_analysis_part1
union all
select * from edw_tb_my_sellin_analysis_part2