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
) ,
transformed as (
SELECT  * FROM wks 
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
),
final as (
select 
id::number(18,0) as id,
muid::varchar(36) as muid,
versionname::varchar(100) as versionname,
versionnumber::number(18,0) as versionnumber,
version_id::number(18,0) as version_id,
versionflag::varchar(100) as versionflag,
name::varchar(500) as name,
code::varchar(500) as code,
changetrackingmask::number(18,0) as changetrackingmask,
jnj_sap_code::number(31,0) as jnj_sap_code,
franchise::varchar(60) as franchise,
brand::varchar(200) as brand,
sku::varchar(500) as sku,
bar_code::varchar(200) as bar_code,
pc_per_case::number(31,2) as pc_per_case,
price::number(31,5) as price,
product_id_concung::varchar(200) as product_id_concung,
enterdatetime::timestamp_ntz(9) as enterdatetime,
enterusername::varchar(200) as enterusername,
enterversionnumber::number(18,0) as enterversionnumber,
lastchgdatetime::timestamp_ntz(9) as lastchgdatetime,
lastchgusername::varchar(200) as lastchgusername,
lastchgversionnumber::number(18,0) as lastchgversionnumber,
validationstatus::varchar(500) as validationstatus,
effective_from::timestamp_ntz(9) as effective_from,
effective_to::timestamp_ntz(9) as effective_to,
active::varchar(2) as active,
run_id::number(14,0) as run_id,
crtd_dttm::timestamp_ntz(9) as crtd_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm
from transformed
)
select * from final