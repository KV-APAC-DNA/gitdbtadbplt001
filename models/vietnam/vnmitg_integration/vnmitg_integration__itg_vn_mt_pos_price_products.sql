with sdl_mds_vn_price_products as (
  select * from DEV_DNA_LOAD.vnmSDL_RAW.SDL_MDS_VN_PRICE_PRODUCTS
), 
wks AS (
  SELECT 
    --- case 1.a: PK present in ITG, record is updated, insert the old record from ITG keeping active as 'N'
    POS.id, 
    POS.muid, 
    POS.versionname, 
    POS.versionnumber, 
    POS.version_id, 
    POS.versionflag, 
    POS.name, 
    POS.code, 
    POS.changetrackingmask, 
    POS.jnj_sap_code, 
    POS.franchise, 
    POS.brand, 
    POS.sku, 
    POS.bar_code, 
    POS.pc_per_case, 
    POS.price, 
    POS.product_id_concung, 
    POS.enterdatetime, 
    POS.enterusername, 
    POS.enterversionnumber, 
    POS.lastchgdatetime, 
    POS.lastchgusername, 
    POS.lastchgversionnumber, 
    POS.validationstatus, 
    POS.effective_from, 
    CASE WHEN POS.effective_to IS NULL THEN dateadd (
      DAY, 
      -1, 
      current_timestamp()
    ) ELSE POS.effective_to END AS effective_to, 
    'N' AS active, 
    POS.run_id, 
    POS.crtd_dttm, 
    POS.updt_dttm 
  FROM 
    (
      SELECT 
        itg.*, 
        ROW_NUMBER() OVER (
          PARTITION BY sdl.bar_code, 
          sdl.product_id_concung 
          order by 
            sdl.lastchgdatetime
        ) AS rn 
      FROM 
        sdl_mds_vn_price_products sdl, 
        {{this}} itg 
      WHERE 
        sdl.lastchgdatetime != itg.lastchgdatetime 
        AND sdl.bar_code = itg.bar_code 
        AND coalesce(sdl.product_id_concung, '0')= coalesce(itg.product_id_concung, '0')
    ) POS 
  WHERE 
    POS.rn = 1 
  UNION ALL 
  SELECT 
    --- case 1.b: PK is present in ITG, record is updated, insert new record from SDL keeping active as 'Y'
    POS.id, 
    POS.muid, 
    POS.versionname, 
    POS.versionnumber, 
    POS.version_id, 
    POS.versionflag, 
    POS.name, 
    POS.code, 
    POS.changetrackingmask, 
    POS.jnj_sap_code, 
    POS.franchise, 
    POS.brand, 
    POS.sku, 
    POS.bar_code, 
    POS.pc_per_case, 
    POS.price, 
    POS.product_id_concung, 
    POS.enterdatetime, 
    POS.enterusername, 
    POS.enterversionnumber, 
    POS.lastchgdatetime, 
    POS.lastchgusername, 
    POS.lastchgversionnumber, 
    POS.validationstatus, 
    current_timestamp() AS effective_from, 
    NULL AS effective_to, 
    'Y' AS active, 
    NULL, 
    POS.enterdatetime, 
    --- taking from SDL enterdatetime
    current_timestamp() AS updt_dttm 
  FROM 
    (
      SELECT 
        sdl.*, 
        ROW_NUMBER() OVER (
          PARTITION BY sdl.bar_code, 
          sdl.product_id_concung 
          order by 
            sdl.lastchgdatetime
        ) AS rn 
      FROM 
        sdl_mds_vn_price_products sdl, 
        {{this}} itg 
      WHERE 
        sdl.lastchgdatetime != itg.lastchgdatetime 
        AND sdl.bar_code = itg.bar_code 
        AND coalesce(sdl.product_id_concung, '0')= coalesce(itg.product_id_concung, '0') 
        AND itg.active = 'Y'
    ) POS 
  WHERE 
    POS.rn = 1 
  UNION ALL 
  SELECT 
    --- case 2: PK present in ITG and active = 'Y', record is not updated in SDL, hence insert SDL record in ITG keeping active 'Y'
    POS.id, 
    POS.muid, 
    POS.versionname, 
    POS.versionnumber, 
    POS.version_id, 
    POS.versionflag, 
    POS.name, 
    POS.code, 
    POS.changetrackingmask, 
    POS.jnj_sap_code, 
    POS.franchise, 
    POS.brand, 
    POS.sku, 
    POS.bar_code, 
    POS.pc_per_case, 
    POS.price, 
    POS.product_id_concung, 
    POS.enterdatetime, 
    POS.enterusername, 
    POS.enterversionnumber, 
    POS.lastchgdatetime, 
    POS.lastchgusername, 
    POS.lastchgversionnumber, 
    POS.validationstatus, 
    POS.effective_from, 
    NULL AS effective_to, 
    'Y' AS active, 
    NULL, 
    POS.crtd_dttm, 
    current_timestamp() AS updt_dttm 
  FROM 
    (
      SELECT 
        sdl.*, 
        itg.effective_from, 
        itg.crtd_dttm, 
        ROW_NUMBER() OVER (
          PARTITION BY sdl.bar_code, 
          sdl.product_id_concung 
          order by 
            sdl.lastchgdatetime
        ) AS rn 
      FROM 
        sdl_mds_vn_price_products sdl, 
        {{this}} itg 
      WHERE 
        sdl.lastchgdatetime = itg.lastchgdatetime 
        AND sdl.bar_code = itg.bar_code 
        AND coalesce(sdl.product_id_concung, '0')= coalesce(itg.product_id_concung, '0') 
        AND itg.active = 'Y'
    ) POS 
  WHERE 
    POS.rn = 1 
  UNION ALL 
  SELECT 
    --- case 3: PK present in ITG and active = 'N', record is not updated in SDL, hence insert ITG record in ITG keeping active 'N'
    POS.id, 
    POS.muid, 
    POS.versionname, 
    POS.versionnumber, 
    POS.version_id, 
    POS.versionflag, 
    POS.name, 
    POS.code, 
    POS.changetrackingmask, 
    POS.jnj_sap_code, 
    POS.franchise, 
    POS.brand, 
    POS.sku, 
    POS.bar_code, 
    POS.pc_per_case, 
    POS.price, 
    POS.product_id_concung, 
    POS.enterdatetime, 
    POS.enterusername, 
    POS.enterversionnumber, 
    POS.lastchgdatetime, 
    POS.lastchgusername, 
    POS.lastchgversionnumber, 
    POS.validationstatus, 
    POS.effective_from, 
    POS.effective_to, 
    POS.active, 
    NULL, 
    POS.crtd_dttm, 
    POS.updt_dttm 
  FROM 
    (
      SELECT 
        itg.*, 
        ROW_NUMBER() OVER (
          PARTITION BY sdl.bar_code, 
          sdl.product_id_concung 
          order by 
            sdl.lastchgdatetime
        ) AS rn 
      FROM 
        sdl_mds_vn_price_products sdl, 
        {{this}} itg 
      WHERE 
        sdl.lastchgdatetime = itg.lastchgdatetime 
        AND sdl.bar_code = itg.bar_code 
        AND coalesce(sdl.product_id_concung, '0')= coalesce(itg.product_id_concung, '0') 
        AND itg.active = 'N'
    ) POS 
  WHERE 
    POS.rn = 1 
  UNION ALL 
  SELECT 
    --- case 4: PK not present in ITG, insert the whole new record in ITG from SDL keeping active 'Y'
    POS.id, 
    POS.muid, 
    POS.versionname, 
    POS.versionnumber, 
    POS.version_id, 
    POS.versionflag, 
    POS.name, 
    POS.code, 
    POS.changetrackingmask, 
    POS.jnj_sap_code, 
    POS.franchise, 
    POS.brand, 
    POS.sku, 
    POS.bar_code, 
    POS.pc_per_case, 
    POS.price, 
    POS.product_id_concung, 
    POS.enterdatetime, 
    POS.enterusername, 
    POS.enterversionnumber, 
    POS.lastchgdatetime, 
    POS.lastchgusername, 
    POS.lastchgversionnumber, 
    POS.validationstatus, 
    current_timestamp() AS effective_from, 
    NULL AS effective_to, 
    'Y' AS active, 
    NULL, 
    POS.enterdatetime, 
    -- taking from SDL enterdatetime
    current_timestamp() AS updt_dttm 
  FROM 
    (
      SELECT 
        sdl.*, 
        ROW_NUMBER() OVER (
          PARTITION BY sdl.bar_code, 
          sdl.product_id_concung 
          order by 
            sdl.lastchgdatetime
        ) AS rn 
      FROM 
        sdl_mds_vn_price_products sdl 
      WHERE 
        (
          nvl(sdl.bar_code, '#'), 
          nvl(sdl.product_id_concung, '#')
        ) NOT IN (
          SELECT 
            nvl(bar_code, '#') as bar_code, 
            nvl(product_id_concung, '#') as product_id_concung 
          FROM 
            {{this}}
        )
    ) pos 
  WHERE 
    POS.rn = 1
) 
SELECT 
  * 
FROM 
  wks 
UNION ALL 
SELECT 
  * 
FROM 
  {{this}} pos 
WHERE 
  (
    nvl(pos.bar_code, '#'), 
    nvl(pos.product_id_concung, '#')
  ) NOT IN (
    SELECT 
      nvl(bar_code, '#'), 
      nvl(product_id_concung, '#') 
    FROM 
      wks
  )
