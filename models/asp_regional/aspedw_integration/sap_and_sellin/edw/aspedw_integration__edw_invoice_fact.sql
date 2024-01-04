with source as(
    select * from {{ ref('aspwks_integration__wks_edw_invc_fact') }}
),
final as(
SELECT
    A.*,
    CURRENT_TIMESTAMP() AS crt_dttm,
    CURRENT_TIMESTAMP() AS upd_dttm
  FROM source AS A
)

select * from final