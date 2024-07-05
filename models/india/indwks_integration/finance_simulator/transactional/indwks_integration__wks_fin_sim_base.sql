with wks_fin_sim_base_temp1 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp1') }}
),
wks_fin_sim_base_temp2 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp2') }}
),
wks_fin_sim_base_temp3 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp3') }}
),
wks_fin_sim_base_temp4 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp4') }}
),
wks_fin_sim_base_temp5 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp5') }}
),
wks_fin_sim_base_temp6 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp6') }}
),
wks_fin_sim_base_temp7 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp7') }}
),
wks_fin_sim_base_temp8 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp8') }}
),
wks_fin_sim_base_temp9 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp9') }}
),
wks_fin_sim_base_temp10 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp10') }}
),
wks_fin_sim_base_temp11 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp11') }}
),
wks_fin_sim_base_temp12 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp12') }}
),
wks_fin_sim_base_temp13 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp13') }}
),
wks_fin_sim_base_temp14 as (
    select * from {{ ref('indwks_integration__wks_fin_sim_base_temp14') }}
),
final_combined AS (
    SELECT * FROM wks_fin_sim_base_temp1
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp2
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp3
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp4
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp5
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp6
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp7
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp8
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp9
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp10
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp11
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp12
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp13
    UNION ALL
    SELECT * FROM wks_fin_sim_base_temp14
)
SELECT * FROM final_combined