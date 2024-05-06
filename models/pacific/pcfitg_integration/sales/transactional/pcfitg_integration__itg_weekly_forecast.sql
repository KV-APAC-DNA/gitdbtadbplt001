{{
    config(
        materialized='incremental',
        incremental_strategy= 'append',
        post_hook="{{sap_transaction_processed_files('BWA_WEEKLY_FORECAST','vw_stg_sdl_weekly_forecast','itg_weekly_forecast')}}"
    )
}}

with source as(
    select * from {{ ref('pcfitg_integration__vw_stg_sdl_weekly_forecast') }}
),
sap_transactional_processed_files as (
    select * from {{ source('aspwks_integration', 'sap_transactional_processed_files') }}
),
final as(
    select
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
       	ablcma::number(17,3) as ablcma,
        base_fc::number(17,3) as base_fc,
        cnblefct::number(17,3) as cnblefct,
        tot_for_val::number(17,3) as tot_for_val,
        cor_dmd_hist::number(17,3) as cor_dmd_hist,
        cpfr_fcst::number(17,3) as cpfr_fcst,
        dpndnt_his::number(17,3) as dpndnt_his,
        dmd_his::number(17,3) as dmd_his,
        his_ship::number(17,3) as his_ship,
        mape001::number(17,3) as mape001,
        mape002::number(17,3) as mape002,
        mape003::number(17,3) as mape003,
        mape004::number(17,3) as mape004,
        mape005::number(17,3) as mape005,
        non_af_fcst::number(17,3) as non_af_fcst,
        othmisi::number(17,3) as othmisi,
        price_chng::number(17,3) as price_chng,
        promo::number(17,3) as promo,
        fin_prop_fct::number(17,3) as fin_prop_fct,
        fin_prop_fac_tbd::number(17,3) as fin_prop_fac_tbd,
        prop_fact_tbd::number(17,3) as prop_fact_tbd,
        sam_free_fct::number(17,3) as sam_free_fct,
        tot_mk_sls_events::number(17,3) as tot_mk_sls_events,
        tot_forecast::number(17,3) as tot_forecast,
        tot_bas_fct::number(17,3) as tot_bas_fct,
        net_fct::number(17,3) as net_fct,
        trd_invc::number(17,3) as trd_invc,
        unit_prc_sku::number(17,3) as unit_prc_sku,
        aux02::number(17,3) as aux02,
        prop_fact::number(17,3) as prop_fact,
        fix_prop_fact::number(17,3) as fix_prop_fact,
        his_sal_ord::number(17,3) as his_sal_ord,
        apo_cor_hist::number(17,3) as apo_cor_hist,
        prd_disc::number(17,3) as prd_disc,
        final_stat_fc::number(17,3) as final_stat_fc,
        sncfcst::number(17,3) as sncfcst,
        totnoafc::number(17,3) as totnoafc,
        file_name::varchar(255) as file_name,
        create_dt::timestamp_ntz(9) as create_dt
    from source
    where not exists (
    select 
        act_file_name 
    from sap_transactional_processed_files 
    where target_table_name='itg_weekly_forecast' and sap_transactional_processed_files.act_file_name=source.file_name
  )

)
select * from final