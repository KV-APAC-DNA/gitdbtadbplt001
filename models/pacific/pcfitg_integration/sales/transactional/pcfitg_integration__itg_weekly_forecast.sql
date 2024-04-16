{{
    config(
        materialized='incremental',
        incremental_strategy= 'append'
    )
}}

with source as(
    select * from DEV_DNA_LOAD.SNAPPCFSDL_RAW.SDL_WEEKLY_FORECAST
),
final as(
    select 
        to_timestamp(create_dt, 'YYYYMMDD HH24:MI:SS') as create_dt,
        calmonth::number(6,0) as calmonth,
        calquart1::number(1,0) as calquart1,
        calweek::number(6,0) as calweek,
        calyear::number(4,0) as calyear,
        fiscvarnt::varchar(2) as fiscvarnt,
        fiscper::number(7,0) as fiscper,
        fiscper3::number(3,0) as fiscper3,
        salesorg::varchar(4) as salesorg,
        co_cd::varchar(4) as co_cd,
        distr_chan::varchar(2) as distr_chan,
        mat_plant::varchar(18) as mat_plant,
        version::varchar(3) as version,
        vtype::number(3,0) as vtype,
        to_date(jnj_fiscal_week,'YYYYMMDD') as jnj_fiscal_week,
        base_uom::varchar(3) as base_uom,
        doc_currcy::varchar(5) as doc_currcy,
        to_date(snap_dt,'YYYYMMDD') as snap_dt,
        snap_wk::number(6,0) as snap_wk,
        jnj_fiscal_yr::number(6,0) as jnj_fiscal_yr,
        plant::varchar(4) as plant,
        prod_mnr::varchar(18) as prod_mnr,
        opr_grp::varchar(18) as opr_grp,
        fran_grp::varchar(18) as fran_grp,
        franchise::varchar(18) as franchise,
        prod_fran::varchar(18) as prod_fran,
        prod_mjr::varchar(18) as prod_mjr,
        b1_mega_brnd::varchar(3) as b1_mega_brnd,
        b2_brnd::varchar(3) as b2_brnd,
        b3_base_prod::varchar(3) as b3_base_prod,
        b4_variant::varchar(3) as b4_variant,
        b5_put_up::varchar(3) as b5_put_up,
        material::varchar(18) as material,
        mercia_reference::varchar(5) as mercia_reference,
        mat_sales::varchar(18) as mat_sales,
        snap_per::number(7,0) as snap_per,
        fiscyear::number(4,0) as fiscyear,
       	case when substr(ablcma,-1) = '-' then substr(ablcma, 1, length(ablcma)-1)::number(17,3) * -1
        else ablcma::number(17,3) end as ablcma,
        case when substr(base_fc,-1) = '-' then substr(base_fc, 1, length(base_fc)-1)::number(17,3) * -1
        else base_fc::number(17,3) end as base_fc,
        case when substr(cnblefct,-1) = '-' then substr(cnblefct, 1, length(cnblefct)-1)::number(17,3) * -1
        else cnblefct::number(17,3) end as cnblefct,
        case when substr(tot_for_val,-1) = '-' then substr(tot_for_val, 1, length(tot_for_val)-1)::number(17,3) * -1
        else tot_for_val::number(17,3) end as tot_for_val,
        case when substr(cor_dmd_hist,-1) = '-' then substr(cor_dmd_hist, 1, length(cor_dmd_hist)-1)::number(17,3) * -1
        else cor_dmd_hist::number(17,3) end as cor_dmd_hist,
        case when substr(cpfr_fcst,-1) = '-' then substr(cpfr_fcst, 1, length(cpfr_fcst)-1)::number(17,3) * -1
        else cpfr_fcst::number(17,3) end as cpfr_fcst,
        case when substr(dpndnt_his,-1) = '-' then substr(dpndnt_his, 1, length(dpndnt_his)-1)::number(17,3) * -1
        else dpndnt_his::number(17,3) end as dpndnt_his,
        case when substr(dmd_his,-1) = '-' then substr(dmd_his, 1, length(dmd_his)-1)::number(17,3) * -1
        else dmd_his::number(17,3) end as dmd_his,
        case when substr(his_ship,-1) = '-' then substr(his_ship, 1, length(his_ship)-1)::number(17,3) * -1
        else his_ship::number(17,3) end as his_ship,
        case when substr(mape001,-1) = '-' then substr(mape001, 1, length(mape001)-1)::number(17,3) * -1
        else mape001::number(17,3) end as mape001,
        case when substr(mape002,-1) = '-' then substr(mape002, 1, length(mape002)-1)::number(17,3) * -1
        else mape002::number(17,3) end as mape002,
        case when substr(mape003,-1) = '-' then substr(mape003, 1, length(mape003)-1)::number(17,3) * -1
        else mape003::number(17,3) end as mape003,
        case when substr(mape004,-1) = '-' then substr(mape004, 1, length(mape004)-1)::number(17,3) * -1
        else mape004::number(17,3) end as mape004,
        case when substr(mape005,-1) = '-' then substr(mape005, 1, length(mape005)-1)::number(17,3) * -1
        else mape005::number(17,3) end as mape005,
        case when substr(non_af_fcst,-1) = '-' then substr(non_af_fcst, 1, length(non_af_fcst)-1)::number(17,3) * -1
        else non_af_fcst::number(17,3) end as non_af_fcst,
        case when substr(othmisi,-1) = '-' then substr(othmisi, 1, length(othmisi)-1)::number(17,3) * -1
        else othmisi::number(17,3) end as othmisi,
        case when substr(price_chng,-1) = '-' then substr(price_chng, 1, length(price_chng)-1)::number(17,3) * -1
        else price_chng::number(17,3) end as price_chng,
        case when substr(promo,-1) = '-' then substr(promo, 1, length(promo)-1)::number(17,3) * -1
        else promo::number(17,3) end as promo,
        case when substr(fin_prop_fct,-1) = '-' then substr(fin_prop_fct, 1, length(fin_prop_fct)-1)::number(17,3) * -1
        else fin_prop_fct::number(17,3) end as fin_prop_fct,
        case when substr(fin_prop_fac_tbd,-1) = '-' then substr(fin_prop_fac_tbd, 1, length(fin_prop_fac_tbd)-1)::number(17,3) * -1
        else fin_prop_fac_tbd::number(17,3) end as fin_prop_fac_tbd,
        case when substr(prop_fact_tbd,-1) = '-' then substr(prop_fact_tbd, 1, length(prop_fact_tbd)-1)::number(17,3) * -1
        else prop_fact_tbd::number(17,3) end as prop_fact_tbd,
        case when substr(sam_free_fct,-1) = '-' then substr(sam_free_fct, 1, length(sam_free_fct)-1)::number(17,3) * -1
        else sam_free_fct::number(17,3) end as sam_free_fct,
        case when substr(tot_mk_sls_events,-1) = '-' then substr(tot_mk_sls_events, 1, length(tot_mk_sls_events)-1)::number(17,3) * -1
        else tot_mk_sls_events::number(17,3) end as tot_mk_sls_events,
        case when substr(tot_forecast,-1) = '-' then substr(tot_forecast, 1, length(tot_forecast)-1)::number(17,3) * -1
        else tot_forecast::number(17,3) end as tot_forecast,
        case when substr(tot_bas_fct,-1) = '-' then substr(tot_bas_fct, 1, length(tot_bas_fct)-1)::number(17,3) * -1
        else tot_bas_fct::number(17,3) end as tot_bas_fct,
        case when substr(net_fct,-1) = '-' then substr(net_fct, 1, length(net_fct)-1)::number(17,3) * -1
        else net_fct::number(17,3) end as net_fct,
        case when substr(trd_invc,-1) = '-' then substr(trd_invc, 1, length(trd_invc)-1)::number(17,3) * -1
        else trd_invc::number(17,3) end as trd_invc,
        case when substr(unit_prc_sku,-1) = '-' then substr(unit_prc_sku, 1, length(unit_prc_sku)-1)::number(17,3) * -1
        else unit_prc_sku::number(17,3) end as unit_prc_sku,
        case when substr(aux02,-1) = '-' then substr(aux02, 1, length(aux02)-1)::number(17,3) * -1
        else aux02::number(17,3) end as aux02,
        case when substr(prop_fact,-1) = '-' then substr(prop_fact, 1, length(prop_fact)-1)::number(17,3) * -1
        else prop_fact::number(17,3) end as prop_fact,
        case when substr(fix_prop_fact,-1) = '-' then substr(fix_prop_fact, 1, length(fix_prop_fact)-1)::number(17,3) * -1
        else fix_prop_fact::number(17,3) end as fix_prop_fact,
        case when substr(his_sal_ord,-1) = '-' then substr(his_sal_ord, 1, length(his_sal_ord)-1)::number(17,3) * -1
        else his_sal_ord::number(17,3) end as his_sal_ord,
        case when substr(apo_cor_hist,-1) = '-' then substr(apo_cor_hist, 1, length(apo_cor_hist)-1)::number(17,3) * -1
        else apo_cor_hist::number(17,3) end as apo_cor_hist,
        case when substr(prd_disc,-1) = '-' then substr(prd_disc, 1, length(prd_disc)-1)::number(17,3) * -1
        else prd_disc::number(17,3) end as prd_disc,
        case when substr(final_stat_fc,-1) = '-' then substr(final_stat_fc, 1, length(final_stat_fc)-1)::number(17,3) * -1
        else final_stat_fc::number(17,3) end as final_stat_fc,
        case when substr(sncfcst,-1) = '-' then substr(sncfcst, 1, length(sncfcst)-1)::number(17,3) * -1
        else sncfcst::number(17,3) end as sncfcst,
        case when substr(totnoafc,-1) = '-' then substr(totnoafc, 1, length(totnoafc)-1)::number(17,3) * -1
        else totnoafc::number(17,3) end as totnoafc
    from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.create_dt > (select max(create_dt) from {{ this }}) 
     {% endif %}
)
select * from final