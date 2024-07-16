with edw_hcp360_ventasys_hcp_dim_snapshot as
(
  select * from dev_dna_core.snapindedw_integration.edw_hcp360_ventasys_hcp_dim_snapshot
),
edw_vw_hcp360_date_dim as 
(
  select * from dev_dna_core.snapindedw_integration.edw_vw_hcp360_date_dim
),
transformed as
(
    SELECT 
    cal_mnth_id,
    hcp_id,
    valid_to,
    is_active
    FROM (
  SELECT 
    C.CAL_MNTH_ID,
    H.VALID_FROM,
    H.VALID_TO,
    H.HCP_ID,
    IS_ACTIVE,
    CAL_DATE_ID
  FROM edw_hcp360_ventasys_hcp_dim_snapshot H,
    EDW_VW_HCP360_DATE_DIM C
  WHERE C.CAL_DATE BETWEEN H.VALID_FROM
    AND H.VALID_TO
    AND CAL_DATE >= ('2019-01-01')
    AND CAL_MNTH_ID <= TO_CHAR(current_timestamp()::timestamp_ntz(9), 'YYYYMM')
    AND SUBSTRING(CAL_DATE_ID, 7, 8) = '05'
  UNION
  SELECT 
    CAL_MNTH_ID,
    VALID_FROM,
    VALID_TO,
    HCP_ID,
    IS_ACTIVE,
    date_id
  FROM (
    SELECT 
      C.CAL_MNTH_ID,
      H.VALID_FROM,
      H.VALID_TO,
      H.HCP_ID,
      IS_ACTIVE,
      min(CAL_DATE_ID) AS date_id,
      row_number() OVER 
      (PARTITION BY HCP_ID ORDER BY cal_mnth_id) AS rn
    FROM edw_hcp360_ventasys_hcp_dim_snapshot H,
      EDW_VW_HCP360_DATE_DIM C
    WHERE C.CAL_DATE BETWEEN H.VALID_FROM
      AND H.VALID_TO
      AND CAL_DATE >= ('2019-01-01')
      AND CAL_MNTH_ID <= TO_CHAR(current_timestamp()::timestamp_ntz(9), 'YYYYMM')
    GROUP BY 1,
      2,
      3,
      4,
      5
    )
  WHERE rn = 1
    AND SUBSTRING(DATE_ID, 7, 8) > '05'
        )
),
final as
(
    select
        cal_mnth_id::number(18,0) as cal_mnth_id,
        hcp_id::varchar(20) as hcp_id,
        valid_to::timestamp_ntz(9) as valid_to,
        is_active::varchar(10) as is_active
    from transformed
)
select * from final
