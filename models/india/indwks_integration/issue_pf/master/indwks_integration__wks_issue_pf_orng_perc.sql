with source as
(
    select * from {{ ref('indwks_integration__wks_issue_pf_ret_dim') }}
),
final as
(
    SELECT ret.*,
    CASE WHEN mnth_id = 202305 THEN 40.00 ELSE 50.00 END AS orange_percentage
    FROM source ret
)
select * from final