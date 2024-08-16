{{
    config(
        materialized= "incremental",
        incremental_strategy= "append"
          )
}}


WITH mykokya AS (
    SELECT * FROM {{ source('jpdclsdl_raw', 'mykokya') }}
),

mykokya_param AS (
    SELECT *
    FROM {{ ref('jpndclitg_integration__mykokya_param') }}
),

transform AS (
    SELECT
        b.file_id,
        a.customer_no,
        a.filename,
        NULL
    FROM (
        SELECT
            customer_no,
            NULL,
            (
                SELECT max(filename)
                FROM mykokya
                WHERE filename IS NOT NULL
            ) AS filename
        FROM mykokya
    ) AS a
    INNER JOIN (
        SELECT
            filename,
            max(file_id) AS file_id
        FROM mykokya_param
        GROUP BY filename
    ) AS b ON a.filename = b.filename
),

final AS (
    SELECT
        file_id::NUMBER(18, 0) AS file_id,
        customer_no::VARCHAR(20) AS customer_no,
        filename::VARCHAR(100) AS filename,
        NULL::VARCHAR(30) AS source_file_date,
        to_char(to_date(current_timestamp()::TIMESTAMP_NTZ(9)),'MM-DD-YYYY')::varchar(10) AS upload_dt,
        to_char(current_timestamp()::TIMESTAMP_NTZ(9),'HH:MI:ss')::varchar(8) AS upload_time
        
    FROM transform
)

SELECT * FROM final
