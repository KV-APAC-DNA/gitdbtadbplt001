{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook =" {% if is_incremental() %}
                    delete from {{this}} itg_ecc_standard_cost_history USING {{ ref('aspwks_integration__wks_itg_ecc_standard_cost_history') }} WKS_itg_ecc_standard_cost_history
                    WHERE WKS_itg_ecc_standard_cost_history.mandt=itg_ecc_standard_cost_history.mandt
                    AND WKS_itg_ecc_standard_cost_history.matnr=itg_ecc_standard_cost_history.matnr
                    AND WKS_itg_ecc_standard_cost_history.bwkey=itg_ecc_standard_cost_history.bwkey
                    AND WKS_itg_ecc_standard_cost_history.bwtar=itg_ecc_standard_cost_history.bwtar
                    AND WKS_itg_ecc_standard_cost_history.lfgja=itg_ecc_standard_cost_history.lfgja
                    AND WKS_itg_ecc_standard_cost_history.lfmon=itg_ecc_standard_cost_history.lfmon
                    AND WKS_itg_ecc_standard_cost_history.CHNG_FLG='U';
                    {% endif %}"
    )
}}

with wks_itg_ecc_standard_cost_history as
(
    select * from {{ ref('aspwks_integration__wks_itg_ecc_standard_cost_history') }}
),
final as
(
    SELECT mandt,
            matnr,
            bwkey,
            bwtar,
            lfgja,
            lfmon,
            lbkum,
            salk3,
            vprsv,
            verpr,
            stprs,
            peinh,
            bklas,
            salkv,
            vksal,
    CASE WHEN CHNG_FLG = 'I' THEN convert_timezone('Asia/Singapore',current_timestamp()) ELSE TGT_CRT_DTTM END  AS CRT_DTTM ,
        convert_timezone('Asia/Singapore',current_timestamp()) AS UPDT_DTTM
        FROM wks_itg_ecc_standard_cost_history
)
select mandt::varchar(3) as mandt,
    matnr::varchar(18) as matnr,
    bwkey::varchar(4) as bwkey,
    bwtar::varchar(10) as bwtar,
    lfgja::number(4,0) as lfgja,
    lfmon::number(2,0) as lfmon,
    lbkum::number(13,3) as lbkum,
    salk3::number(13,2) as salk3,
    vprsv::varchar(1) as vprsv,
    verpr::number(11,2) as verpr,
    stprs::number(11,2) as stprs,
    peinh::number(5,0) as peinh,
    bklas::varchar(4) as bklas,
    salkv::number(13,2) as salkv,
    vksal::number(13,2) as vksal,
    crt_dttm::timestamp_ntz(9) as crt_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
 from final