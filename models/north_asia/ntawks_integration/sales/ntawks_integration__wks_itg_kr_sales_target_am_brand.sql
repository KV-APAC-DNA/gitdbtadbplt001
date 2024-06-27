with source as (
    select * from {{ source('ntasdl_raw','sdl_kr_sales_target_am_brand') }}
),
itg_kr_sales_target_am_brand_temp as 
(
    select * from {{ source('ntaitg_integration','itg_kr_sales_target_am_brand_temp') }}
),
final as 
(
    SELECT 
        brand,
        account_manager,
        year,
        jan_trgt_amt,
        feb_trgt_amt,
        mar_trgt_amt,
        apr_trgt_amt,
        may_trgt_amt,
        jun_trgt_amt,
        jul_trgt_amt,
        aug_trgt_amt,
        sep_trgt_amt,
        oct_trgt_amt,
        nov_trgt_amt,
        dec_trgt_amt,
        country_code,
        TGT.CRT_DTTM AS TGT_CRT_DTTM,
        UPDT_DTTM,
        CASE
            WHEN TGT.CRT_DTTM IS NULL THEN 'I'
            ELSE 'U'
        END AS CHNG_FLG
    FROM source SRC
        LEFT OUTER JOIN 
        (
            SELECT brnd,
                acct_mgr,
                yr,
                ctry_cd,
                CRT_DTTM
            FROM itg_kr_sales_target_am_brand_temp
        ) TGT ON Upper(SRC.brand) = Upper(TGT.brnd)
        AND Upper(SRC.account_manager) = Upper(TGT.acct_mgr)
        AND SRC.year = TGT.yr
        AND SRC.country_code = TGT.ctry_cd
)
select * from final