with edw_px_master_fact as 
(
    select * from snappcfedw_integration.edw_px_master_fact
),
edw_px_gl_trans_lkp as
(
    select * from {{ ref('pcfedw_integration__edw_px_gl_trans_lkp') }}
),
vw_customer_dim as
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
edw_time_dim as
(
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_px_forecast_fact as
(
    select * from {{ ref('pcfedw_integration__edw_px_forecast_fact') }}
),
edw_px_listprice as
(
    select * from {{ ref('pcfedw_integration__edw_px_listprice') }}
),
dly_sls_cust_attrb_lkp as
(
    select * from {{ ref('pcfedw_integration__dly_sls_cust_attrb_lkp') }}
),
vw_jjbr_curr_exch_dim as
(
    select * from {{ ref('pcfedw_integration__vw_jjbr_curr_exch_dim') }}
),
vw_sap_std_cost as
(
    select * from {{ ref('pcfedw_integration__vw_sap_std_cost') }}
),
edw_px_term_plan_ext as
(
    select * from {{ ref('pcfedw_integration__edw_px_term_plan_ext') }}
),
edw_account_dim as
(
    select * from {{ ref('aspedw_integration__edw_account_dim') }}
),
union_1 as
(  
    select 
        pac_source_type,
        pac_subsource_type,
        cmp_id,
        cust_no,
        px_jj_mnth,
        px_cal_mnth,
        matl_id,
        gl_trans_desc,
        sap_accnt,
        sap_accnt_nm,
        px_measure,
        px_bucket,
        key_measure,
        ciw_category,
        ciw_account_group,
        local_ccy,
        committed_spend as px_base_measure,
        0 as px_qty,
        0 as px_gts,
        0 as px_rtrns,
        0 as px_gts_less_rtrns,
        (
            case
                when PX_BUCKET = 'Efficiency' then committed_spend
                else 0
            end
        ) as PX_EFF_VAL,
        (
            case
                when PX_BUCKET = 'Joint Growth Fund: Shopper Indirect' then committed_spend
                else 0
            end
        ) as PX_JGF_SI_VAL,
        (
            case
                when PX_BUCKET = 'Payment Terms' then committed_spend
                else 0
            end
        ) as PX_PMT_TERMS_VAL,
        (
            case
                when PX_BUCKET = 'Data and Insights' then committed_spend
                else 0
            end
        ) as PX_DATAINS_VAL,
        (
            case
                when PX_BUCKET = 'Expenses and Adjustments' then committed_spend
                else 0
            end
        ) as PX_EXP_ADJ_VAL,
        (
            case
                when PX_BUCKET = 'Joint Growth Fund: Shopper Direct' then committed_spend
                else 0
            end
        ) as PX_JGF_SD_VAL,
        (
            case
                when PX_BUCKET IN (
                    'Efficiency',
                    'Joint Growth Fund: Shopper Indirect',
                    'Joint Growth Fund: Shopper Direct',
                    'Payment Terms',
                    'Data and Insights',
                    'Expenses and Adjustments'
                ) then committed_spend
                else 0
            end
        ) as px_ciw_tot,
        0 as px_cogs,
        case_qty,
        tot_planspend,
        tot_paid,
        committed_spend,
        tp_category
    from
    (
        select pac_source_type,
            pac_subsource_type,
            cmp_id,
            cust_no,
            px_jj_mnth,
            px_cal_mnth,
            matl_id,
            gl_trans_desc,
            sap_accnt,
            sap_accnt_nm,
            px_measure,
            px_bucket,
            key_measure,
            ciw_category,
            ciw_account_group,
            local_ccy,
            sum(case_qty) case_qty,
            sum(tot_planspend) tot_planspend,
            sum(tot_paid) tot_paid,
            sum(committed_spend) committed_spend,
            tp_category
        FROM 
        (
            SELECT 'PROMAX' as PAC_SOURCE_TYPE,
                'PX_MASTER' as pac_subsource_type,
                vcd.cmp_id,
                pmf.cust_id as cust_no,
                pmf.p_startdate as px_date,
                etd.jj_mnth_id as px_jj_mnth,
                null as px_cal_mnth,
                pmf.matl_id,
                pgtm.gl_trans as gl_trans_desc,
                pgtm.sap_account as sap_accnt,
                pgtm.acct_nm as sap_accnt_nm,
                pgtm.promax_measure as px_measure,
                pgtm.promax_bucket as px_bucket,
                null as key_measure,
                pgtm.promax_bucket as ciw_category,
                null as ciw_account_group,
                vcd.curr_cd as local_ccy,
                pmf.case_quantity as case_qty,
                pmf.planspend_total as tot_planspend,
                pmf.paid_total as tot_paid,
                pmf.committed_spend,
                -- the committed spend logic is now implemented  in edw_px_master_fact table population.
                pgtm.tp_category
            from edw_px_master_fact pmf,
                (
                    select a.row_id,
                        a.gl_trans,
                        a.sap_account,
                        b.acct_nm,
                        a.promax_measure,
                        a.promax_bucket,
                        a.tp_category
                    from edw_px_gl_trans_lkp a,
                        (
                            select distinct acct_num,
                                acct_nm
                            from edw_account_dim
                            where bravo_acct_l1 <> ''
                        ) b
                    where ltrim(b.acct_num, '0') = a.sap_account
                ) pgtm,
                vw_customer_dim vcd,
                edw_time_dim etd
            where pmf.gltt_rowid = pgtm.row_id (+)
                and pmf.cust_id = ltrim(vcd.cust_no (+), '0')
                and (pmf.promotionforecastweek)::date = (etd.cal_date)::date
        )
        group by --updated joins based on promotionforecastweek
            pac_source_type,
            pac_subsource_type,
            cmp_id,
            cust_no,
            px_jj_mnth,
            px_cal_mnth,
            matl_id,
            gl_trans_desc,
            sap_accnt,
            sap_accnt_nm,
            px_measure,
            px_bucket,
            key_measure,
            ciw_category,
            ciw_account_group,
            local_ccy,
            tp_category
    )
),
union_2 as
(
    select a.pac_source_type,
        a.pac_subsource_type,
        a.cmp_id,
        a.cust_no,
        a.px_jj_mnth,
        a.px_cal_mnth,
        a.matl_id,
        null as gl_trans_desc,
        null as sap_accnt,
        null as sap_accnt_nm,
        null as px_measure,
        null as px_bucket,
        null as key_measure,
        'Gross Trade Sales' as ciw_category,
        null as ciw_account_group,
        a.local_ccy,
        0 as px_base_measure,
        a.px_qty,
        a.px_gts,
        a.px_rtrns,
        (a.px_gts - a.px_rtrns) as px_gts_less_rtrns,
        0 as px_eff_val,
        0 as px_jgf_si_val,
        0 as px_pmt_terms_val,
        0 as px_datains_val,
        0 as px_exp_adj_val,
        0 as px_jgf_sd_val,
        0 as px_ciw_tot,
        a.px_qty *(nvl (b.std_cost_aud, 0) *(1 / d.exch_rate::double precision)) as px_cogs,
        0 as case_qty,
        0 as tot_planspend,
        0 as tot_paid,
        0 as committed_spend,
        null as tp_category
    from 
        (
            select pac_source_type,
                pac_subsource_type,
                cmp_id,
                cust_no,
                px_jj_mnth,
                px_cal_mnth,
                matl_id,
                gl_trans_desc,
                sap_accnt,
                sap_accnt_nm,
                px_measure,
                px_bucket,
                local_ccy,
                sum(px_qty) as px_qty,
                sum(px_gts) as px_gts,
                sum(px_rtrns) as px_rtrns,
                sum(px_gp) as px_gp
            from
                (
                    select 'PROMAX' as PAC_SOURCE_TYPE,
                        'PX_FORECAST' as pac_subsource_type,
                        epf.cmp_id,
                        epf.ac_attribute as cust_no,
                        epf.est_date as px_date,
                        epf.jj_mnth_id as px_jj_mnth,
                        null as px_cal_mnth,
                        epf.sku_stockcode as matl_id,
                        null as gl_trans_desc,
                        null as sap_accnt,
                        null as sap_accnt_nm,
                        null as px_measure,
                        null as px_bucket,
                        epf.curr_cd as local_ccy,
                        epf.est_estimate as px_qty,
                        (epf.est_estimate * epl.lp_price)::double precision as px_gts,
                        0 as px_rtrns,
                        epf.est_estimate * epl.lp_price as px_gp
                    from 
                    (
                        select a.ac_code,
                            a.ac_attribute,
                            a.sku_stockcode,
                            a.sku_attribute,
                            a.sku_profitcentre,
                            a.est_date,
                            c.jj_mnth_id,
                            a.est_normal,
                            a.est_promotional,
                            a.est_estimate,
                            b.cmp_id,
                            b.curr_cd,
                            c.jj_wk as week_no
                        from edw_px_forecast_fact a,
                            vw_customer_dim b,
                            edw_time_dim c
                        where a.ac_attribute = ltrim(b.cust_no (+), '0')
                            and to_date(a.est_date) = to_date(c.cal_date)
                    ) epf,
                        -- edw_time_dim etd,	
                    (
                        /* old code 
                            
                            select distinct lp.sku_stockcode,
                            
                            lkp.cmp_id,
                            
                            lp.lp_price
                            
                            from edw_px_listprice lp,
                            
                            (select distinct cmp_id,
                            
                            sls_org
                            
                            from dly_sls_cust_attrb_lkp) lkp
                            
                            where lp.sales_org = lkp.sls_org
                            
                            and   to_date(lp.lp_startdate) <= to_date(sysdate)
                            
                            and   (to_date(lp.lp_stopdate) > to_date(sysdate) or to_date(lp.lp_stopdate) is  null) */
                        select distinct eff_jj_month,
                            sku_stockcode,
                            cmp_id,
                            lp_price,
                            week_no
                        from (
                                select distinct time.cal_date as eff_date,
                                    time.jj_mnth_id as eff_jj_month,
                                    time.jj_wk as week_no,
                                    lp.sku_stockcode,
                                    lkp.cmp_id,
                                    lp.lp_price,
                                    (
                                        row_number() over (
                                            partition by lkp.cmp_id,
                                            lp.sku_stockcode,
                                            time.jj_mnth_id,
                                            time.jj_wk
                                            order by time.cal_date desc
                                        )
                                    ) as rank
                                from edw_px_listprice lp,
                                    (
                                        select distinct cmp_id,
                                            sls_org
                                        from dly_sls_cust_attrb_lkp
                                    ) lkp,
                                    (
                                        select jj_mnth_id,
                                            cal_date,
                                            jj_wk
                                        from edw_time_dim
                                    ) as time
                                where lp.sales_org = lkp.sls_org
                                    and to_date(time.cal_date) between to_date(lp.lp_startdate) and to_date(lp.lp_stopdate)
                            )
                        where rank = 1
                    ) epl,
                    vw_jjbr_curr_exch_dim curr
                    where --to_date(epf.est_date) = to_date(etd.cal_date)
                    --and   
                    epf.sku_stockcode = epl.sku_stockcode (+)
                    and epf.jj_mnth_id = epl.eff_jj_month (+) ---join condition added
                    and epf.week_no = epl.week_no (+)
                    and epf.cmp_id = epl.cmp_id (+)
                    and curr.to_ccy = 'AUD'
                    and epf.jj_mnth_id = curr.jj_mnth_id
                    and epf.curr_cd = curr.from_ccy
                )
            group by pac_source_type,
                pac_subsource_type,
                cmp_id,
                cust_no,
                px_jj_mnth,
                px_cal_mnth,
                matl_id,
                gl_trans_desc,
                sap_accnt,
                sap_accnt_nm,
                px_measure,
                px_bucket,
                local_ccy
        ) a,
        vw_sap_std_cost b,
        (
            select cmp_id,
                cust_id,
                matl_id,
                time_period,
                sum(px_term_proj_amt) as terms_val
            from edw_px_term_plan_ext
            group by cmp_id,
                cust_id,
                matl_id,
                time_period
        ) c,
        vw_jjbr_curr_exch_dim d
    where ltrim(b.matnr (+), '0') = a.matl_id
        and b.cmp_no (+) = a.cmp_id
        and a.cmp_id = c.cmp_id (+)
        and a.cust_no = c.cust_id (+)
        and a.matl_id = c.matl_id (+)
        and a.px_jj_mnth = c.time_period (+)
        and d.to_ccy = 'AUD'
        and a.px_jj_mnth = d.jj_mnth_id
        and a.local_ccy = d.from_ccy
),
union_3 as
(
    select 
        a.pac_source_type,
        a.pac_subsource_type,
        a.cmp_id,
        a.cust_no,
        a.px_jj_mnth,
        a.px_cal_mnth,
        a.matl_id,
        c.gl_trans_desc as gl_trans_desc,
        c.sap_accnt,
        c.sap_accnt_nm,
        c.px_measure,
        c.px_bucket,
        null as key_measure,
        c.px_bucket as ciw_category,
        null as ciw_account_group,
        c.local_ccy,
        c.px_base_measure,
        0 as px_qty,
        0 as px_gts,
        0 as px_rtrns,
        0 as px_gts_less_rtrns,
        c.px_eff_val as px_eff_val,
        c.px_jgf_si_val as px_jgf_si_val,
        c.px_pmt_terms_val as px_pmt_terms_val,
        c.px_datains_val as px_datains_val,
        c.px_exp_adj_val as px_exp_adj_val,
        c.px_jgf_sd_val as px_jgf_sd_val,
        c.px_ciw_tot as px_ciw_tot,
        0 as px_cogs,
        0 as case_qty,
        0 as tot_planspend,
        0 as tot_paid,
        c.px_base_measure as committed_spend,
        c.tp_category
    from 
        (
            select pac_source_type,
                pac_subsource_type,
                cmp_id,
                cust_no,
                px_jj_mnth,
                px_cal_mnth,
                matl_id,
                --gl_trans_desc,
                --sap_accnt,
                --sap_accnt_nm,
                --px_measure,
                --px_bucket,
                local_ccy,
                0 as px_base_measure,
                0 as px_qty,
                0 as px_gts,
                0 as px_rtrns,
                0 as px_gp
            from 
                (
                    SELECT 'PROMAX' as PAC_SOURCE_TYPE,
                        'PX_TERMS' as pac_subsource_type,
                        epf.cmp_id,
                        epf.ac_attribute as cust_no,
                        epf.est_date as px_date,
                        epf.jj_mnth_id as px_jj_mnth,
                        null as px_cal_mnth,
                        epf.sku_stockcode as matl_id,
                        null as gl_trans_desc,
                        null as sap_accnt,
                        null as sap_accnt_nm,
                        null as px_measure,
                        null as px_bucket,
                        epf.curr_cd as local_ccy,
                        0 as px_qty,
                        0 as px_gts,
                        0 as px_rtrns,
                        0 as px_gp
                    from 
                        (
                            select a.ac_code,
                                a.ac_attribute,
                                a.sku_stockcode,
                                a.sku_attribute,
                                a.sku_profitcentre,
                                a.est_date,
                                c.jj_mnth_id,
                                a.est_normal,
                                a.est_promotional,
                                a.est_estimate,
                                b.cmp_id,
                                b.curr_cd
                            from edw_px_forecast_fact a,
                                vw_customer_dim b,
                                edw_time_dim c
                            where a.ac_attribute = ltrim(b.cust_no (+), '0')
                                and to_date(a.est_date) = to_date(c.cal_date)
                        ) epf,
                        --edw_time_dim etd,	
                        (
                            /*select distinct lp.sku_stockcode,
                             
                             lkp.cmp_id,
                             
                             lp.lp_price
                             
                             from edw_px_listprice lp,
                             
                             (select distinct cmp_id,
                             
                             sls_org
                             
                             from dly_sls_cust_attrb_lkp) lkp
                             
                             where lp.sales_org = lkp.sls_org
                             
                             and   to_date(lp.lp_startdate) <= to_date(sysdate)
                             
                             and   (to_date(lp.lp_stopdate) > to_date(sysdate) or to_date(lp.lp_stopdate) is  null)*/
                            -- old code
                            select distinct eff_jj_month,
                                sku_stockcode,
                                cmp_id,
                                lp_price
                            from 
                                (
                                    select distinct time.cal_date as eff_date,
                                        time.jj_mnth_id as eff_jj_month,
                                        lp.sku_stockcode,
                                        lkp.cmp_id,
                                        lp.lp_price,
                                        (
                                            row_number() over (partition by lkp.cmp_id,lp.sku_stockcode,time.jj_mnth_id order by time.cal_date desc
                                            )
                                        ) as rank
                                    from edw_px_listprice lp,
                                        (
                                            select distinct cmp_id,
                                                sls_org
                                            from dly_sls_cust_attrb_lkp
                                        ) lkp,
                                        (
                                            select jj_mnth_id,
                                                cal_date
                                            from edw_time_dim
                                        ) as time
                                    where lp.sales_org = lkp.sls_org
                                        and to_date(time.cal_date) between to_date(lp.lp_startdate) and to_date(lp.lp_stopdate)
                                )
                            where rank = 1
                        ) epl,
                        vw_jjbr_curr_exch_dim curr
                    where --to_date(epf.est_date) = to_date(etd.cal_date)
                        --and   
                        epf.sku_stockcode = epl.sku_stockcode (+)
                        and epf.cmp_id = epl.cmp_id (+)
                        and epf.jj_mnth_id = epl.eff_jj_month (+) --newly included
                        and curr.to_ccy = 'AUD'
                        and epf.jj_mnth_id = curr.jj_mnth_id
                        and epf.curr_cd = curr.from_ccy
                )
            group by pac_source_type,
                pac_subsource_type,
                cmp_id,
                cust_no,
                px_jj_mnth,
                px_cal_mnth,
                matl_id,
                gl_trans_desc,
                sap_accnt,
                sap_accnt_nm,
                px_measure,
                px_bucket,
                local_ccy
        ) a,
        vw_sap_std_cost b,
        (
            select cmp_id,
                cust_id,
                matl_id,
                time_period,
                gl_trans as gl_trans_desc,
                sap_account as sap_accnt,
                acct_nm as sap_accnt_nm,
                promax_measure as px_measure,
                promax_bucket as px_bucket,
                local_ccy,
                sum(px_term_proj_amt) as px_base_measure,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Efficiency' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_EFF_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Joint Growth Fund: Shopper Indirect' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_JGF_SI_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Payment Terms' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_PMT_TERMS_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Data and Insights' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_DATAINS_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Expenses and Adjustments' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_EXP_ADJ_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET = 'Joint Growth Fund: Shopper Direct' then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_JGF_SD_VAL,
                (
                    case
                        when PGTM.PROMAX_BUCKET IN (
                            'Efficiency',
                            'Joint Growth Fund: Shopper Indirect',
                            'Joint Growth Fund: Shopper Direct',
                            'Payment Terms',
                            'Data and Insights',
                            'Expenses and Adjustments'
                        ) then PX_TERM_PROJ_AMT
                        else 0
                    end
                ) as PX_CIW_TOT,
                PGTM.tp_category
            FROM 
                (
                    SELECT CMP_ID,
                        CUST_ID,
                        MATL_ID,
                        TIME_PERIOD,
                        GLTT_ROWID,
                        DECODE(
                            CMP_ID,
                            '7470',
                            'AUD',
                            '8361',
                            'NZD'
                        ) as local_ccy,
                        sum(px_term_proj_amt) as px_term_proj_amt
                    from edw_px_term_plan_ext
                    group by cmp_id,
                        cust_id,
                        matl_id,
                        time_period,
                        local_ccy,
                        gltt_rowid
                ) ptp,
                (
                    select a.row_id,
                        a.gl_trans,
                        a.sap_account,
                        b.acct_nm,
                        a.promax_measure,
                        a.promax_bucket,
                        a.tp_category
                    from edw_px_gl_trans_lkp a,
                        (
                            select distinct acct_num,
                                acct_nm
                            from edw_account_dim
                            where bravo_acct_l1 <> ''
                        ) b
                    where ltrim(b.acct_num, '0') = a.sap_account
                ) pgtm
            where ptp.gltt_rowid = pgtm.row_id (+)
            group by cmp_id,
                cust_id,
                matl_id,
                time_period,
                gl_trans,
                sap_accnt,
                sap_accnt_nm,
                local_ccy,
                promax_measure,
                promax_bucket,
                px_term_proj_amt,
                tp_category
        ) c,
        vw_jjbr_curr_exch_dim d
    where ltrim(b.matnr (+), '0') = a.matl_id
        and b.cmp_no (+) = a.cmp_id
        and a.cmp_id = c.cmp_id
        and a.cust_no = c.cust_id
        and a.matl_id = c.matl_id
        and a.px_jj_mnth = c.time_period (+)
        AND D.TO_CCY = 'AUD'
        and a.px_jj_mnth = d.jj_mnth_id
        and a.local_ccy = d.from_ccy
),
final as
(
    select
        pac_source_type::varchar(6) as pac_source_type,
        pac_subsource_type::varchar(11) as pac_subsource_type,
        cmp_id::varchar(20) as cmp_id,
        cust_no::varchar(50) as cust_no,
        px_jj_mnth::number(18,0) as px_jj_mnth,
        px_cal_mnth::varchar(1) as px_cal_mnth,
        matl_id::varchar(18) as matl_id,
        gl_trans_desc::varchar(40) as gl_trans_desc,
        sap_accnt::varchar(40) as sap_accnt,
        sap_accnt_nm::varchar(100) as sap_accnt_nm,
        px_measure::varchar(40) as px_measure,
        px_bucket::varchar(40) as px_bucket,
        key_measure::varchar(40) as key_measure,
        ciw_category::varchar(40) as ciw_category,
        ciw_account_group::varchar(40) as ciw_account_group,
        local_ccy::varchar(5) as local_ccy,
        px_base_measure::float as px_base_measure,
        px_qty::number(38,0) as px_qty,
        px_gts::float as px_gts,
        px_rtrns::number(38,0) as px_rtrns,
        px_gts_less_rtrns::float as px_gts_less_rtrns,
        px_eff_val::float as px_eff_val,
        px_jgf_si_val::float as px_jgf_si_val,
        px_pmt_terms_val::float as px_pmt_terms_val,
        px_datains_val::float as px_datains_val,
        px_exp_adj_val::float as px_exp_adj_val,
        px_jgf_sd_val::float as px_jgf_sd_val,
        px_ciw_tot::float as px_ciw_tot,
        px_cogs::number(38,8) as px_cogs,
        case_qty::number(38,0) as case_qty,
        tot_planspend::float as tot_planspend,
        tot_paid::float as tot_paid,
        committed_spend::float as committed_spend,
        tp_category::varchar(20) as tp_category  
    from
    (
        select * from union_1
        union all
        select * from union_2
        union all
        select * from union_3
    )
)
select * from final