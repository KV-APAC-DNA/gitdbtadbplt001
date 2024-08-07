with tm09nendai_ikou as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM09NENDAI_IKOU
),
final as (
SELECT 
  tm09nendai_ikou.nendaicode, 
  tm09nendai_ikou.nenreifrom, 
  tm09nendai_ikou.nenreito, 
  tm09nendai_ikou.nendainame 
FROM 
  tm09nendai_ikou tm09nendai_ikou
)
select * from final