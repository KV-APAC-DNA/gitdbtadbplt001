with itg_vn_dms_kpi as(
    select * from {{ ref('vnmitg_integration__itg_vn_dms_kpi') }}
),
wrk_vn_mnth_week as(
    select * from {{ ref('vnmwks_integration__wrk_vn_mnth_week') }}
),
wrk_vn_sellout_by_wkmnth_for_target as(
    select * from {{ ref('vnmwks_integration__wrk_vn_sellout_by_wkmnth_for_target') }}
),
t2 as(
        SELECT DISTINCT
        p_2,
        dstrbtr_id,
        saleman_code,
        i.cycle,
        i.target_value,
        w.mnth_wk_no
    FROM itg_vn_dms_kpi AS i, wrk_vn_mnth_week AS w
    WHERE
        kpi_type = 'Sellout' AND i.cycle = w.mnth_id
),
transformed as(
    SELECT
        p_2,
        t2.dstrbtr_id,
        t2.saleman_code,
        t2.cycle AS target_cyc,
        t2.mnth_wk_no AS target_wk,
        t2.target_value,
        st2.mnth_id AS sales_mnth,
        st2.mnth_wk_no AS sales_wk,
        st2.amount AS amt_by_wk
    FROM  t2, wrk_vn_sellout_by_wkmnth_for_target AS st2
    WHERE
    st2.MNTH_ID(+) = t2.p_2
    AND st2.DSTRBTR_ID(+) = t2.dstrbtr_id
    AND st2.SALESREP_ID(+) = t2.saleman_code
    AND st2.MNTH_WK_NO(+) = t2.mnth_wk_no
)
select * from transformed