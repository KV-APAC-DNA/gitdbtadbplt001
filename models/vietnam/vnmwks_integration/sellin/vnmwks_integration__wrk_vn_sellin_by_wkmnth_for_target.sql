with itg_vn_distributor_sap_sold_to_mapping as
(
    select * from {{ source('vnmitg_integration', 'itg_vn_distributor_sap_sold_to_mapping') }}
),
itg_vn_dms_distributor_dim as
(
    select * from snaposeitg_integration.itg_vn_dms_distributor_dim
),
edw_vw_vn_billing_fact as
(
    select * from snaposeedw_integration.edw_vw_vn_billing_fact
),
wrk_vn_mnth_week as
(
    select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
mapp as
(
    select distinct territory_dist,
        sap_ship_to_code
    from itg_vn_distributor_sap_sold_to_mapping,itg_vn_dms_distributor_dim
    WHERE dstrbtr_id = distributor_id
),
final as
(
    SELECT 
        territory_dist::varchar(100) as territory_dist,
        w.mnth_id::varchar(23) as mnth_id,
        mnth_wk_no::number(38,0) as mnth_wk_no,
        sum(bill_fact.net_amt)::number(38,4) as amount
    FROM mapp,
        edw_vw_vn_billing_fact bill_fact,
        wrk_vn_mnth_week w
    WHERE bill_fact.ship_to = mapp.sap_ship_to_code
        and bill_fact.bill_type in ('ZF2V', 'ZSML')
        and bill_fact.bill_dt between try_to_date(w.frst_day,'yyyymmdd') and try_to_date(w.lst_day,'yyyymmdd')
    group by 
        territory_dist,
        w.mnth_id,
        mnth_wk_no
)
select * from final