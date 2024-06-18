with v_intrm_invc_trans as 
(
    select * from ntaedw_integration.v_intrm_invc_trans
),
edw_calendar_dim as 
(
    select * from snapaspedw_integration.edw_calendar_dim
),
final as
(   
    SELECT 
        a.fisc_yr,
        a.fisc_mo,
        a.channel,
        sum(a.net_bill_val) AS tot_invc_val,
        sum(a.bill_qty_pc) AS tot_invc_qty,
        a.country,
        a.currency,
        a.from_crncy,
        a.to_crncy,
        a.ex_rt_typ,
        a.ex_rt,
        a.catg,
        a.brnd_nm,
        a.sls_grp,
        a.sls_grp_desc,
        a.sls_ofc,
        a.sls_ofc_desc,
        a.store_typ,
        a.cust_num,
        a.edw_cust_nm,
        a.country_cd,
        (
            SELECT edw_calendar_dim.fisc_per
            FROM edw_calendar_dim
            WHERE (
                    edw_calendar_dim.cal_day = convert_timezone('UTC', current_timestamp())::date
                )
        ) AS current_fisc_per
    FROM 
        (
            SELECT 
                "substring"(
                    ((edw_invc_trans_fact.fisc_yr)::character varying)::text,
                    1,
                    4
                ) AS fisc_yr,
                "substring"(
                    ((edw_invc_trans_fact.fisc_yr)::character varying)::text,
                    6,
                    8
                ) AS fisc_mo,
                edw_invc_trans_fact.channel,
                edw_invc_trans_fact.net_bill_val,
                edw_invc_trans_fact.bill_qty_pc,
                edw_invc_trans_fact.ctry_key AS country,
                edw_invc_trans_fact.curr_key AS currency,
                edw_invc_trans_fact.from_crncy,
                edw_invc_trans_fact.to_crncy,
                edw_invc_trans_fact.ex_rt_typ,
                edw_invc_trans_fact.ex_rt,
                edw_invc_trans_fact.prod_hier_l3 AS catg,
                edw_invc_trans_fact.prod_hier_l4 AS brnd_nm,
                edw_invc_trans_fact.sls_grp,
                edw_invc_trans_fact.sls_grp_desc,
                edw_invc_trans_fact.sls_ofc,
                edw_invc_trans_fact.sls_ofc_desc,
                edw_invc_trans_fact.store_typ,
                edw_invc_trans_fact.cust_num,
                edw_invc_trans_fact.edw_cust_nm,
                edw_invc_trans_fact.ctry_cd AS country_cd
            FROM v_intrm_invc_trans edw_invc_trans_fact
            WHERE (
                    ((edw_invc_trans_fact.fisc_yr)::character varying)::text >= (
                        (
                            (
                                (
                                    (
                                        (
                                            date_part(
                                                year,
                                                convert_timezone('UTC', current_timestamp())::timestamp without time zone
                                            ) - 4
                                        )
                                    )::character varying
                                )::text || ((0)::character varying)::text
                            ) || ((0)::character varying)::text
                        ) || ((1)::character varying)::text
                    )
                )
        ) a
    GROUP BY a.fisc_yr,
        a.fisc_mo,
        a.channel,
        a.country,
        a.currency,
        a.from_crncy,
        a.to_crncy,
        a.ex_rt_typ,
        a.ex_rt,
        a.catg,
        a.brnd_nm,
        a.sls_grp,
        a.sls_grp_desc,
        a.sls_ofc,
        a.sls_ofc_desc,
        a.store_typ,
        a.cust_num,
        a.edw_cust_nm,
        a.country_cd
)
select * from final