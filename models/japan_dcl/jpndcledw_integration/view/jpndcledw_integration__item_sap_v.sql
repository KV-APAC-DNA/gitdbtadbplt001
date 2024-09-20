with edw_material_dim as (
select * from {{ ref('aspedw_integration__edw_material_dim') }}
),
final as (
SELECT 
  DISTINCT base.matl_num AS itemcode_sap, 
  base.old_matl_num AS itemcode, 
  base.prmry_upc_cd AS jancode, 
  base.size_dims_txt AS bar_cd2 
FROM 
  (
    SELECT 
      base.matl_num, 
      base.old_matl_num, 
      base.prmry_upc_cd, 
      base.size_dims_txt, 
      row_number() OVER(
        PARTITION BY base.old_matl_num 
        ORDER BY 
          base.matl_num DESC
      ) AS rn 
    FROM 
      edw_material_dim base
  ) base 
WHERE 
  (
    (base.rn = 1) 
    AND (base.old_matl_num IS NOT NULL)
  )
)
select * from final
