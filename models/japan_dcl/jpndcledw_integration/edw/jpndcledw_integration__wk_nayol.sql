{{
    config
    (   
        materialized = 'incremental',
        incremental_logic = 'append',
        pre_hook = "{{build_wk_nayol_temp()}}"
    )
}}

WITH cil02nayol
AS (
  SELECT *
  FROM {{ ref('jpndcledw_integration__cil02nayol') }}
  ),

transformed
AS (
  SELECT nayosesakino,
    kokyakuno,
    lastdate,
    '0' as shori_sts
  FROM cil02nayol
  WHERE NOT EXISTS (
      SELECT 'X'
      FROM {{this}} wk_nayo
      WHERE wk_nayo.nayosesakino = nayosesakino
        AND wk_nayo.kokyano = kokyakuno
      )
  ) ,

final
AS (
  SELECT 
    nayosesakino::VARCHAR(15) AS nayosesakino,
    kokyakuno::VARCHAR(15) AS kokyano,
    lastdate::VARCHAR(12) AS insertdate,
    shori_sts::VARCHAR(1) AS shori_sts,
    null::timestamp_ntz(9) AS inserted_date,
    null::VARCHAR(100) AS inserted_by,
    null::timestamp_ntz(9) AS updated_date,
    null::VARCHAR(100) AS updated_by
  FROM transformed
  )
SELECT *
FROM final