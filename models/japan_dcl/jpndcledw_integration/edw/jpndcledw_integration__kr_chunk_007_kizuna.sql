WITH teikikeiyaku_data_mart_uni
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.teikikeiyaku_data_mart_uni
  ),
kr_chunk_005_kizuna
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.kr_chunk_005_kizuna
  ),
transformed
AS (
  SELECT
         TEI.C_DIUSRID KOKYANO
        --BGN-MOD 20210630 D.YAMASHITA ***変更23225(QlikViewの「顧客_定期契約離脱年月」が不正)****
        --    ,MAX(TEI.C_DSDELEVERYYM) CONTRACT_ENDYM
            ,MAX(SUBSTRING(TEI.KAIYAKUBI,1,6)) CONTRACT_ENDYM
            ,NULL INSERTED_BY
            ,NULL UPDATED_BY
        --END-MOD 20210630 D.YAMASHITA ***変更23225(QlikViewの「顧客_定期契約離脱年月」が不正)****
        FROM   teikikeiyaku_data_mart_uni TEI
            LEFT JOIN kr_chunk_005_kizuna CHUNK_005 --現在定期
                    ON CHUNK_005.KOKYANO = TEI.C_DIUSRID
        WHERE  CHUNK_005.KOKYANO IS NULL --現在定期でない
        AND  TEI.C_DSDELEVERYYM <= TO_CHAR(CURRENT_TIMESTAMP()::DATE, 'YYYYMM')
            --Timezone conversion from UTC 20220211
        -- TEI.C_DSDELEVERYYM <= TO_CHAR(SYSDATE,'YYYYMM') --将来定期のような人を除外
        GROUP BY TEI.C_DIUSRID
  ),
final
AS (
  SELECT 
            kokyano::varchar(30) as kokyano,
            contract_endym::varchar(9) as contract_endym,
            current_timestamp()::timestamp_ntz(9) AS inserted_date,
            inserted_by::varchar(100) as inserted_by,
            current_timestamp()::timestamp_ntz(9) AS updated_date,
            updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final