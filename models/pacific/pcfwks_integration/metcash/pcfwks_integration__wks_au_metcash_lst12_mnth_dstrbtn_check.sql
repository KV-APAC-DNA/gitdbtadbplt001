with wks_au_metcash_monthly as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_monthly') }}
),
wks_au_metcash_prod_acct_date_comb as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_prod_acct_date_comb') }}
    where delvry_dt >(select distinct dateadd(month,-24,wapm.delvry_dt)
                  from wks_au_metcash_monthly wapm,
                       (select max(delvry_dt) delvry_dt
                        from wks_au_metcash_monthly) maxd
                  where wapm.delvry_dt = maxd.delvry_dt)
) ,
wks_au_metcash_dstrbtn_date_range as (
    select * from {{ ref('pcfwks_integration__wks_au_metcash_dstrbtn_date_range') }}
),
Last12Months AS (
    SELECT 
        c.DELVRY_DT,
        ACCT_KEY,
        PROD_KEY,
        COUNT(CASE WHEN UNIT_QTY <> 0 THEN 1 END) OVER (PARTITION BY PROD_KEY ORDER BY c.DELVRY_DT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS PROD_LST12_MNTH_QTY,
        COUNT(CASE WHEN UNIT_QTY <> 0 THEN 1 END) OVER (PARTITION BY ACCT_KEY ORDER BY c.DELVRY_DT ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ACCT_LST12_MNTH_QTY
    FROM WKS_AU_METCASH_MONTHLY C
    JOIN WKS_AU_METCASH_DSTRBTN_DATE_RANGE B
        ON C.DELVRY_DT > B.LST12_MNTH_DELVRY_DT
        AND C.DELVRY_DT <= B.DELVRY_DT
) 
SELECT 
    A.DELVRY_DT,
    A.ACCT_KEY,
    A.PROD_KEY,
    B.PROD_LST12_MNTH_QTY,
    B.ACCT_LST12_MNTH_QTY
FROM WKS_AU_METCASH_PROD_ACCT_DATE_COMB A
JOIN Last12Months B
    ON A.DELVRY_DT = B.DELVRY_DT
    AND A.PROD_KEY = B.PROD_KEY
    AND A.ACCT_KEY = B.ACCT_KEY
