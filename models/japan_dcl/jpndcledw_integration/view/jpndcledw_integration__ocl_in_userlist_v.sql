with dm_mykokya as (
 select * from {{ ref('jpndcledw_integration__dm_mykokya') }}
),

ocl_exclude_file_id as (
select * from {{ ref('jpndcledw_integration__ocl_exclude_file_id') }}
),

a AS (
        SELECT max(((((dm_mykokya.upload_dt))::CHARACTER VARYING)::TEXT || (dm_mykokya.upload_time)::TEXT)) AS max_time
        FROM dm_mykokya
        )
,

final as (
SELECT dm_mykokya.file_id,
    dm_mykokya.filename,
    (dm_mykokya.kokyano)::BIGINT AS diusrid
FROM dm_mykokya
WHERE (
        (
            (lower((dm_mykokya.purpose_type)::TEXT) = ('outcall'::CHARACTER VARYING)::TEXT)
            AND (
                dm_mykokya.file_id IN (
                    SELECT DISTINCT dm_mykokya.file_id
                    FROM dm_mykokya,
                        a
                    WHERE (
                            (a.max_time = ((((dm_mykokya.upload_dt))::CHARACTER VARYING)::TEXT || (dm_mykokya.upload_time)::TEXT))
                            AND (lower((dm_mykokya.purpose_type)::TEXT) = ('outcall'::CHARACTER VARYING)::TEXT)
                            )
                    )
                )
            )
        AND (
            NOT (
                dm_mykokya.file_id IN (
                    SELECT ocl_exclude_file_id.file_id
                    FROM ocl_exclude_file_id
                    )
                )
            )
        )
    )

select * from final