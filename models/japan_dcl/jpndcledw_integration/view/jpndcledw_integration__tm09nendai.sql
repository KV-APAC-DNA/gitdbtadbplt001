with tm09nendai_ikou as (
select * from {{ source('jpdcledw_integration', 'tm09nendai_ikou') }}
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