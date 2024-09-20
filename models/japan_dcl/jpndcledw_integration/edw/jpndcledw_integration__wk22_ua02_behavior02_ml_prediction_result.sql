with cc_predictionresult as (
select * from {{ ref('jpndclitg_integration__cc_predictionresult') }}
),
ec_predictionresult as (
select * from {{ ref('jpndclitg_integration__ec_predictionresult') }}
),
clustermapping as (
select * from {{ source('jpdclitg_integration', 'clustermapping') }}
),
cim01kokya as (
select * from {{ ref('jpndcledw_integration__cim01kokya') }}
),
vc100_predictionresult as (
select * from {{ ref('jpndclitg_integration__vc100_predictionresult') }}
),
acgel_predictionresult as (
select * from {{ ref('jpndclitg_integration__acgel_predictionresult') }}
),
cluster_predictionresult as (
select * from {{ ref('jpndclitg_integration__cluster_predictionresult') }}
),
last_file_ecpropensity AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_ecpropensity_file_dt 
  from 
    EC_PredictionResult
), 
ecpropensity AS (
  SELECT 
    DISTINCT customer_id, 
    LAST_VALUE(ecpropensity) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) as ecpropensity 
  FROM 
    EC_PredictionResult ec 
    inner join last_file_ecpropensity on to_char(ec.inserted_date, 'YYYYMM') = last_file_ecpropensity.last_ecpropensity_file_dt
), 
last_file_ccpropensity AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_ccpropensity_file_dt 
  from 
    CC_PredictionResult
), 
ccpropensity AS (
  SELECT 
    DISTINCT customer_id, 
    LAST_VALUE(ccpropensity) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) AS ccpropensity 
  FROM 
    CC_PredictionResult cc 
    inner join last_file_ccpropensity on to_char(cc.inserted_date, 'YYYYMM') = last_file_ccpropensity.last_ccpropensity_file_dt
), 
last_file_acgelpropensity AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_acgelpropensity_file_dt 
  from 
    ACGEL_PredictionResult
), 
acgelpropensity AS (
  SELECT 
    DISTINCT customer_id, 
    LAST_VALUE(acgelpropensity) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) AS acgelpropensity 
  FROM 
    ACGEL_PredictionResult ac 
    inner join last_file_acgelpropensity on to_char(ac.inserted_date, 'YYYYMM') = last_file_acgelpropensity.last_acgelpropensity_file_dt
), 
last_file_vc100propensity AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_vc100propensity_file_dt 
  from 
    VC100_PredictionResult
), 
vc100propensity AS (
  SELECT 
    DISTINCT customer_id, 
    LAST_VALUE(vc100propensity) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) AS vc100propensity 
  FROM 
    VC100_PredictionResult vc 
    inner join last_file_vc100propensity on to_char(vc.inserted_date, 'YYYYMM') = last_file_vc100propensity.last_vc100propensity_file_dt
), 
last_file_cluster5_cd AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_cluster5_cd_file_dt 
  from 
    Cluster_PredictionResult
), 
cluster5_cd AS (
  SELECT 
    DISTINCT customer_id, 
    LAST_VALUE(cluster5_cd) OVER (
      PARTITION BY customer_id 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) AS cluster5_cd 
  FROM 
    Cluster_PredictionResult cpr 
    inner join last_file_cluster5_cd on to_char(cpr.inserted_date, 'YYYYMM') = last_file_cluster5_cd.last_cluster5_cd_file_dt
), 
last_file_cluster5_nm AS (
  select 
    max(
      to_char(inserted_date, 'YYYYMM')
    ) last_cluster5_nm_file_dt 
  from 
    clustermapping
), 
cluster5_nm AS (
  SELECT 
    DISTINCT cluster5_cd, 
    LAST_VALUE(cluster5_nm) OVER (
      PARTITION BY cluster5_cd 
      ORDER BY 
        inserted_date ROWS BETWEEN unbounded preceding 
        AND unbounded following
    ) AS cluster5_nm 
  FROM 
    clustermapping cm 
    inner join last_file_cluster5_nm on to_char(cm.inserted_date, 'YYYYMM') = last_file_cluster5_nm.last_cluster5_nm_file_dt
), 
transformed as (
SELECT 
  ck.kokyano AS customer_no, 
  a.ecpropensity, 
  b.ccpropensity, 
  c.acgelpropensity, 
  d.vc100propensity, 
  e.cluster5_cd, 
  f.cluster5_nm 
FROM 
  cim01kokya ck 
  LEFT JOIN ecpropensity a on ck.kokyano = NVL(
    LPAD(a.customer_id, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN ccpropensity b on ck.kokyano = NVL(
    LPAD(b.customer_id, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN acgelpropensity c on ck.kokyano = NVL(
    LPAD(c.customer_id, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN vc100propensity d on ck.kokyano = NVL(
    LPAD(d.customer_id, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN cluster5_cd e on ck.kokyano = NVL(
    LPAD(e.customer_id, 10, '0'), 
    '0000000000'
  ) 
  LEFT JOIN cluster5_nm f on e.cluster5_cd = f.cluster5_cd 
WHERE 
  ck.testusrflg = '通常ユーザ'
),
final as (
select
customer_no::varchar(60) as customer_no,
ecpropensity::varchar(60) as ecpropensity,
ccpropensity::varchar(60) as ccpropensity,
acgelpropensity::varchar(60) as acgelpropensity,
vc100propensity::varchar(60) as vc100propensity,
cluster5_cd::number(18,0) as cluster5_cd,
cluster5_nm::varchar(300) as cluster5_nm 
from transformed
)
select * from final
