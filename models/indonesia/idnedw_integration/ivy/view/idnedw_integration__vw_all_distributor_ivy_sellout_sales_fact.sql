with itg_all_ivy_distributor_sellout_sales_fact as
(
    select * from {{ ref('idnitg_integration__itg_all_ivy_distributor_sellout_sales_fact') }}
),
itg_all_distributor_sellout_sales_fact as
(
    select * from {{ ref('idnitg_integration__itg_all_distributor_sellout_sales_fact') }}
),
final as
(
    SELECT src.trans_key
        ,src.bill_doc
        ,src.bill_dt
        ,src.order_id
        ,src.jj_mnth_id
        ,src.jj_wk
        ,src.dstrbtr_grp_cd
        ,src.dstrbtr_id
        ,src.jj_sap_dstrbtr_id
        ,src.dstrbtr_cust_id
        ,src.dstrbtr_prod_id
        ,src.jj_sap_prod_id
        ,src.dstrbtn_chnl
        ,src.grp_outlet
        ,src.dstrbtr_slsmn_id
        ,src.sls_qty
        ,src.grs_val
        ,src.jj_net_val
        ,src.trd_dscnt
        ,src.dstrbtr_net_val
        ,src.rtrn_qty
        ,src.rtrn_val
        ,src.crtd_dttm
        ,src.updt_dttm
    FROM itg_all_ivy_distributor_sellout_sales_fact src

    UNION ALL

    SELECT src.trans_key
        ,src.bill_doc
        ,src.bill_dt
        ,NULL AS order_id
        ,src.jj_mnth_id
        ,src.jj_wk
        ,src.dstrbtr_grp_cd
        ,src.dstrbtr_id
        ,src.jj_sap_dstrbtr_id
        ,src.dstrbtr_cust_id
        ,src.dstrbtr_prod_id
        ,src.jj_sap_prod_id
        ,src.dstrbtn_chnl
        ,src.grp_outlet
        ,src.dstrbtr_slsmn_id
        ,src.sls_qty
        ,src.grs_val
        ,src.jj_net_val
        ,src.trd_dscnt
        ,src.dstrbtr_net_val
        ,src.rtrn_qty
        ,src.rtrn_val
        ,src.crtd_dttm
        ,src.updt_dttm
    FROM itg_all_distributor_sellout_sales_fact src
    WHERE (
            (src.dstrbtr_grp_cd)::TEXT = 'DNR'::TEXT
            OR (src.dstrbtr_grp_cd)::TEXT = 'CSA'::TEXT
            OR (src.dstrbtr_grp_cd)::TEXT = 'PON'::TEXT
            OR (src.dstrbtr_grp_cd)::TEXT = 'SAS'::TEXT
            OR (src.dstrbtr_grp_cd)::TEXT = 'RFS'::TEXT
            OR (src.dstrbtr_grp_cd)::TEXT = 'SNC'::TEXT
            OR (
                (src.dstrbtr_grp_cd)::TEXT = 'ADT'::TEXT
                AND (src.jj_sap_dstrbtr_id)::TEXT = '117089'::TEXT
                )
            OR (
                (src.dstrbtr_grp_cd)::TEXT = 'SDN'::TEXT
                AND (src.jj_sap_dstrbtr_id)::TEXT = '116193'::TEXT
                )
            )
        AND (src.jj_mnth_id)::TEXT <= '201904'::TEXT
)
select * from final