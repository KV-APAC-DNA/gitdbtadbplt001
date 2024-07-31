{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['act_flg', 'jan_cd', 'acnt_cd', 'promo_cd' ]
    )
}}

WITH source
AS (
    SELECT *
    FROM {{ source('jpnsdl_raw', 'tsp_datamart_promo_amt') }}
    ),
final
AS (
    SELECT promo_cd::VARCHAR(10) AS promo_cd,
        jan_cd::VARCHAR(18) AS jan_cd,
        tsp_acnt_cd::VARCHAR(10) AS tsp_acnt_cd,
        acnt_cd::VARCHAR(6) AS acnt_cd,
        cstctr_cd::VARCHAR(4) AS cstctr_cd,
        payee_cd::VARCHAR(10) AS payee_cd,
        act_flg::VARCHAR(10) AS act_flg,
        amt::number(11, 0) AS amt,
        promo_status_cd::VARCHAR(2) AS promo_status_cd,
        STATUS::VARCHAR(2) AS STATUS,
        chn_cd::VARCHAR(5) AS chn_cd,
        apply_begin_dt::timestamp_ntz(9) AS apply_begin_dt,
        apply_end_dt::timestamp_ntz(9) AS apply_end_dt,
        promo_direct_flg::VARCHAR(2) AS promo_direct_flg
    FROM source
    )
SELECT *
FROM final