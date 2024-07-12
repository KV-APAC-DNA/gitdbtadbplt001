with WK_RANKDM_HANRO as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.WK_RANKDM_HANRO
),
------add reference from sourav's model 
final as (
SELECT 
diusrid::number(10,0) as diusrid,
dm_tuhan::varchar(40) as dm_tuhan,
dm_tenpo::varchar(40) as dm_tenpo,
kingaku_sum::number(10,0) as kingaku_sum,
hanro_bddm::varchar(40) as hanro_bddm,
hanro_rank::varchar(40) as hanro_rank,
forced_tuhan_dm_send_flg::varchar(4) as forced_tuhan_dm_send_flg
FROM WK_RANKDM_HANRO
)
select * from final