{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = ['ph_cd']
    )
}}

WITH edw_material_dim AS
(
    SELECT * FROM {{ ref('aspedw_integration__edw_material_dim') }}
),
source as 
(
    SELECT 
    distinct prodh1 as ph_cd,
        '1' AS ph_lvl,
        prodh1_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product'
    union
    SELECT 
    distinct prodh2 as ph_cd,
        '2' AS ph_lvl,
        prodh2_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product' 
    Union
    SELECT distinct prodh3 as ph_cd,
        '3' AS ph_lvl,
        prodh3_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product' and trim(prodh3) 
    not in (SELECT distinct trim(prodh2) FROM edw_material_dim where matl_type_desc = 'Finished product')
    Union 
    SELECT distinct prodh4 as ph_cd,
        '4' AS ph_lvl,
        prodh4_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product' and trim(prodh4)
    not in (SELECT distinct trim(prodh3) FROM edw_material_dim where matl_type_desc = 'Finished product')
    Union 
    SELECT distinct prodh5 as ph_cd,
        '5' AS ph_lvl,
        prodh5_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product' and trim(prodh5)
    not in (SELECT distinct trim(prodh4) FROM edw_material_dim where matl_type_desc = 'Finished product')
    UNION 
    SELECT distinct prodh6 as ph_cd,
        '6' AS ph_lvl,
        prodh6_txtmd as ph_nm
    FROM edw_material_dim
    where matl_type_desc = 'Finished product' and trim(prodh6)
    not in (SELECT distinct trim(prodh5) FROM edw_material_dim where matl_type_desc = 'Finished product')
),
final AS
(
    SELECT
        current_timestamp()::TIMESTAMP_NTZ(9) AS create_dt,
        'Administrator'::VARCHAR(256) AS create_user,
        current_timestamp()::TIMESTAMP_NTZ(9)  AS update_dt,
        'Administrator'::VARCHAR(256) AS update_user,
        ph_cd::VARCHAR(256) AS ph_cd,
        ph_lvl::VARCHAR(256) AS ph_lvl,
        ph_nm::VARCHAR(256) AS ph_nm
    FROM source
)

SELECT * FROM final