with kr_comm_point_para
AS
(
    select * from {{ source('jpdcledw_integration', 'kr_comm_point_para') }}
),
cim01kokya as
(
    select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
numbers_table as
(
    SELECT row_number() OVER (order by kokyano) AS numbers_column,
            (SELECT 365 * ((date_part('year',convert_timezone('Asia/Tokyo',current_timestamp())) -2020) + 1)) AS days,
            to_date(('20200101'::CHARACTER VARYING)::TEXT, ('YYYYMMDD'::CHARACTER VARYING)::TEXT)::timestamp_ntz(9) AS start_date
    FROM cim01kokya
),
derived_table1 as
(
    SELECT dateadd(day,numbers_table.numbers_column,numbers_table.start_date ) AS calendar
    FROM numbers_table
    WHERE (numbers_table.numbers_column <= numbers_table.days)
),
final as
(
    SELECT DISTINCT to_char((derived_table1.calendar)::timestamp_ntz(9), ('YYYYMM'::CHARACTER VARYING)::TEXT) AS yymm,
                    (SELECT kr_comm_point_para.source_file_date
                        FROM kr_comm_point_para
                    ) AS db_refresh_date
    FROM derived_table1
)
select * from final



