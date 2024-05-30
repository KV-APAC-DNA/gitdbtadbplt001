{{
    config
    (
        pre_hook = "{{ build_edw_p_load_indonesia_fact_pre_req_sellin_sellout_fact() }}"
    )
}}

with source as
(
    select * from {{ source('idnwks_integration', 'wks_itg_all_distributor_sellin_sellout_fact') }}
),

final as
(
    SELECT DISTINCT JJ_MNTH_ID,
        DSTRBTR_GRP_CD,
        JJ_SAP_DSTRBTR_ID,
        SYSDATE() as CRT_DTTM
    FROM source
)
select * from final