with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_mds_in_sv_kapf_month_end_dates') }}
),
final as 
(
    select
        year::number(18,0) AS year,
        CASE 
            WHEN month = 'January'
                THEN 01
            WHEN month = 'February'
                THEN 02
            WHEN month = 'March'
                THEN 03
            WHEN month = 'April'
                THEN 04
            WHEN month = 'May'
                THEN 05
            WHEN month = 'June'
                THEN 06
            WHEN month = 'July'
                THEN 07
            WHEN month = 'August'
                THEN 08
            WHEN month = 'September'
                THEN 09
            WHEN month = 'October'
                THEN 10
            WHEN month = 'November'
                THEN 11
            WHEN month = 'December'
                THEN 12
            END::number(18,0) AS month,
        key_account_month_end::timestamp_ntz(9) AS key_account_month_end,
        pathfinder_month_end::timestamp_ntz(9) AS pathfinder_month_end,
        lastchgdatetime::timestamp_ntz(9) AS mds_lastchgdatetime,
        current_timestamp()::timestamp_ntz(9) AS updtddatetime,
        NULL::number(14,0) as run_id
    from source
)
select * from final