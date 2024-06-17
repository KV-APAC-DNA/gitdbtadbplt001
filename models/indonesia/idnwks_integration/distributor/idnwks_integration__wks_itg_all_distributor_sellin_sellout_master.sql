with source as
(
    select * from {{ source('idnwks_integration', 'wks_itg_all_distributor_sellin_sellout_fact') }}
),

final as
(
    SELECT distinct jj_mnth_id,
        dstrbtr_grp_cd,
        jj_sap_dstrbtr_id,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crt_dttm
    FROM source
)
select * from final