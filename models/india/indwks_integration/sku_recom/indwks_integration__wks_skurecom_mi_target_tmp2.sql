with wks_skurecom_mi_target_tmp1 as
(
    select * from {{ ref('indwks_integration__wks_skurecom_mi_target_tmp1') }}
),
edw_msl_spike_mi_msku_list as
(
    select * from {{ ref('indedw_integration__edw_msl_spike_mi_msku_list') }}
),
final as(
        SELECT tmp.*, msku.mothersku_name AS mothersku_name
        FROM wks_skurecom_mi_target_tmp1 tmp
        INNER JOIN edw_msl_spike_mi_msku_list msku
        ON UPPER(tmp.region_name) = UPPER(msku.region_name)
        AND UPPER(tmp.zone_name) = UPPER(msku.zone_name)
        AND fisc_yr::text||'Q'||qtr::text = msku.period
)
select * from final
