

with source as
(
    select * from {{ source('idnwks_integration', 'wks_itg_all_distributor_stock') }}
),

final as
(
    SELECT DISTINCT JJ_MNTH_ID,
        DSTRBTR_GRP_CD,
        JJ_SAP_DSTRBTR_ID,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as CRT_DTTM
    FROM source
)
select * from final