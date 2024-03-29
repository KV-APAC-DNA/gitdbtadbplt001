{{
  config(
    materialized="incremental",
    incremental_strategy="merge",
    unique_key=["cust_num_1"],
    merge_exclude_columns=["crt_dttm"]
  )
}}
with
source as
(
  select * from {{ ref('aspwks_integration__wks_itg_cust_base') }}
),

trans as
(
select
        mandt as clnt,
        kunnr as cust_num_1,
        adrnr as addr,
        anred as title,
        aufsd as cntrl_ordr_blk_cust,
        bahne as exp_train_stn,
        bahns as train_stn,
        bbbnr as intnl_loc_num1,
        bbsnr as intnl_loc_num2,
        begru as auth_grp,
        brsch as indstr_key,
        bubkz as chk_dig_intnl_loc_num,
        datlt as data_comm_line_no,
        erdat as dt_on_rec_crt,
        ernam as nm_prsn_crt_obj,
        exabl as unld_pt_exist_ind,
        faksd as cent_bill_blk_cust,
        fiskn as acct_num_mstr_rec_fisc_addr,
        knazk as wrk_num_cal,
        knrza as acct_num_alt_pyr,
        konzs as grp_key,
        ktokd as cust_acct_grp,
        kukla as cust_clsn,
        land1 as ctry_key,
        lifnr as acct_num_vend,
        lifsd as cent_delv_blk_cust,
        locco as city_coordnt,
        loevm as cent_del_fl_mstr_rec,
        name1 as nm_1,
        name2 as nm_2,
        name3 as nm_3,
        name4 as nm_4,
        niels as nlsn_id,
        ort01 as city,
        ort02 as dstrc,
        pfach as po_box,
        pstl2 as po_box_pstl_cd,
        pstlz as pstl_cd,
        regio as rgn,
        counc as cnty_cd,
        cityc as city_cd,
        rpmkr as fcst_chnl,
        sortl as srt_fld,
        sperr as cent_pstng_blk,
        spras as lang_key,
        stcd1 as tax_num_1,
        stcd2 as tax_num_2,
        stkza as busn_ptnr_subj_eqlztn_tax_ind,
        stkzu as liab_for_vat,
        stras as hs_num_and_street,
        telbx as telebox_num,
        telf1 as fst_phn_num,
        telf2 as sec_phn_num,
        telfx as fax_num,
        teltx as teletex_num,
        telx1 as telex_num,
        lzone as trspn_zn_goods_delv,
        xcpdk as is_acct_one_time_acct_ind,
        xzemp as alt_payee_doc_allw_ind,
        vbund as co_id_trad_ptnr,
        stceg as vat_regs_num,
        dear1 as cmpt_ind,
        dear2 as sls_ptnr_ind,
        dear3 as sls_prosp_ind,
        dear4 as for_cust_type4_ind,
        dear5 as id_dflt_sold_to_prty,
        dear6 as in_cnsmr,
        gform as legal_sts,
        bran1 as indstr_cd_1,
        bran2 as indstr_cd_2,
        bran3 as indstr_cd_3,
        bran4 as indstr_cd_4,
        bran5 as indstr_cd_5,
        ekont as init_cntct,
        umsat as annl_sls_1,
        umjah as yr_for_sls_gvn,
        uwaer as crncy_sls_fig,
        jmzah as yr_num_emp,
        jmjah as yr_for_num_emp_gvn,
        katr1 as attr_1,
        katr2 as attr_2,
        katr3 as attr_3,
        katr4 as attr_4,
        katr5 as attr_5,
        katr6 as attr_6,
        katr7 as attr_7,
        katr8 as attr_8,
        katr9 as attr_9,
        katr10 as attr_10,
        stkzn as ntrl_prsn,
        umsa1 as annl_sls_2,
        txjcd as tax_juris,
        mcod1 as srch_term_match_cd_srch1,
        mcod2 as srch_term_match_cd_srch2,
        mcod3 as srch_term_match_cd_srch3,
        periv as fisc_yr_vrnt,
        abrvw as usg_ind,
        inspbydebi as insp_carr_out_by_cust,
        inspatdebi as insp_delv_note_after_outb_delv,
        ktocd as ref_acct_grp_one_time_acct,
        pfort as po_box_city,
        werks as plnt,
        dtams as data_med_exch_ind,
        dtaws as instr_key_data_med_exch,
        duefl as sts_data_tfr_subsq_rlse,
        hzuor as asgnmt_hier,
        sperz as pmt_blk,
        etikg as cust_grp,
        civve as id_mn_non_mil_use,
        milve as id_mn_mil_use,
        kdkg1 as cust_cond_grp_1,
        kdkg2 as cust_cond_grp_2,
        kdkg3 as cust_cond_grp_3,
        kdkg4 as cust_cond_grp_4,
        kdkg5 as cust_cond_grp_5,
        xknza as alt_pyr_acct_num_ind,
        fityp as tax_type,
        stcdt as tax_num_type,
        stcd3 as tax_num3,
        stcd4 as tax_num4,
        xicms as cust_is_icms_expt,
        xxipi as cust_is_ipi_expt,
        xsubt as cust_grp_tax_subst,
        cfopc as cust_cfop_cat,
        txlw1 as tax_law_icms,
        txlw2 as tax_law_ipi,
        ccc01 as biochem_wf_ind,
        ccc02 as nuclr_np_ind,
        ccc03 as natl_scty_ind,
        ccc04 as missile_tech_ind,
        cassd as cent_sls_blk_cust,
        knurl as unifm_rsrs_lctr,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
),

final as(
    select
        clnt::varchar(150) as clnt,
        cust_num_1::varchar(10) as cust_num_1,
        addr::varchar(150) as addr,
        title::varchar(150) as title,
        cntrl_ordr_blk_cust::varchar(150) as cntrl_ordr_blk_cust,
        exp_train_stn::varchar(150) as exp_train_stn,
        train_stn::varchar(150) as train_stn,
        intnl_loc_num1::number(18,0) as intnl_loc_num1,
        intnl_loc_num2::number(18,0) as intnl_loc_num2,
        auth_grp::varchar(150) as auth_grp,
        indstr_key::varchar(150) as indstr_key,
        chk_dig_intnl_loc_num::number(18,0) as chk_dig_intnl_loc_num,
        data_comm_line_no::varchar(150) as data_comm_line_no,
        dt_on_rec_crt::date as dt_on_rec_crt,
        nm_prsn_crt_obj::varchar(150) as nm_prsn_crt_obj,
        unld_pt_exist_ind::varchar(150) as unld_pt_exist_ind,
        cent_bill_blk_cust::varchar(150) as cent_bill_blk_cust,
        acct_num_mstr_rec_fisc_addr::varchar(150) as acct_num_mstr_rec_fisc_addr,
        wrk_num_cal::varchar(150) as wrk_num_cal,
        acct_num_alt_pyr::varchar(150) as acct_num_alt_pyr,
        grp_key::varchar(150) as grp_key,
        cust_acct_grp::varchar(150) as cust_acct_grp,
        cust_clsn::varchar(150) as cust_clsn,
        ctry_key::varchar(150) as ctry_key,
        acct_num_vend::varchar(150) as acct_num_vend,
        cent_delv_blk_cust::varchar(150) as cent_delv_blk_cust,
        city_coordnt::varchar(150) as city_coordnt,
        cent_del_fl_mstr_rec::varchar(150) as cent_del_fl_mstr_rec,
        nm_1::varchar(150) as nm_1,
        nm_2::varchar(150) as nm_2,
        nm_3::varchar(150) as nm_3,
        nm_4::varchar(150) as nm_4,
        nlsn_id::varchar(150) as nlsn_id,
        city::varchar(150) as city,
        dstrc::varchar(150) as dstrc,
        po_box::varchar(150) as po_box,
        po_box_pstl_cd::varchar(150) as po_box_pstl_cd,
        pstl_cd::varchar(150) as pstl_cd,
        rgn::varchar(150) as rgn,
        cnty_cd::varchar(150) as cnty_cd,
        city_cd::varchar(150) as city_cd,
        fcst_chnl::varchar(150) as fcst_chnl,
        srt_fld::varchar(150) as srt_fld,
        cent_pstng_blk::varchar(150) as cent_pstng_blk,
        lang_key::varchar(150) as lang_key,
        tax_num_1::varchar(150) as tax_num_1,
        tax_num_2::varchar(150) as tax_num_2,
        busn_ptnr_subj_eqlztn_tax_ind::varchar(150) as busn_ptnr_subj_eqlztn_tax_ind,
        liab_for_vat::varchar(150) as liab_for_vat,
        hs_num_and_street::varchar(150) as hs_num_and_street,
        telebox_num::varchar(150) as telebox_num,
        fst_phn_num::varchar(150) as fst_phn_num,
        sec_phn_num::varchar(150) as sec_phn_num,
        fax_num::varchar(150) as fax_num,
        teletex_num::varchar(150) as teletex_num,
        telex_num::varchar(150) as telex_num,
        trspn_zn_goods_delv::varchar(150) as trspn_zn_goods_delv,
        is_acct_one_time_acct_ind::varchar(150) as is_acct_one_time_acct_ind,
        alt_payee_doc_allw_ind::varchar(150) as alt_payee_doc_allw_ind,
        co_id_trad_ptnr::varchar(150) as co_id_trad_ptnr,
        vat_regs_num::varchar(150) as vat_regs_num,
        cmpt_ind::varchar(150) as cmpt_ind,
        sls_ptnr_ind::varchar(150) as sls_ptnr_ind,
        sls_prosp_ind::varchar(150) as sls_prosp_ind,
        for_cust_type4_ind::varchar(150) as for_cust_type4_ind,
        id_dflt_sold_to_prty::varchar(150) as id_dflt_sold_to_prty,
        in_cnsmr::varchar(150) as in_cnsmr,
        legal_sts::varchar(150) as legal_sts,
        indstr_cd_1::varchar(150) as indstr_cd_1,
        indstr_cd_2::varchar(150) as indstr_cd_2,
        indstr_cd_3::varchar(150) as indstr_cd_3,
        indstr_cd_4::varchar(150) as indstr_cd_4,
        indstr_cd_5::varchar(150) as indstr_cd_5,
        init_cntct::varchar(150) as init_cntct,
        annl_sls_1::varchar(150) as annl_sls_1,
        yr_for_sls_gvn::number(18,0) as yr_for_sls_gvn,
        crncy_sls_fig::varchar(150) as crncy_sls_fig,
        yr_num_emp::number(18,0) as yr_num_emp,
        yr_for_num_emp_gvn::number(18,0) as yr_for_num_emp_gvn,
        attr_1::varchar(150) as attr_1,
        attr_2::varchar(150) as attr_2,
        attr_3::varchar(150) as attr_3,
        attr_4::varchar(150) as attr_4,
        attr_5::varchar(150) as attr_5,
        attr_6::varchar(150) as attr_6,
        attr_7::varchar(150) as attr_7,
        attr_8::varchar(150) as attr_8,
        attr_9::varchar(150) as attr_9,
        attr_10::varchar(150) as attr_10,
        ntrl_prsn::varchar(150) as ntrl_prsn,
        annl_sls_2::varchar(150) as annl_sls_2,
        tax_juris::varchar(150) as tax_juris,
        srch_term_match_cd_srch1::varchar(150) as srch_term_match_cd_srch1,
        srch_term_match_cd_srch2::varchar(150) as srch_term_match_cd_srch2,
        srch_term_match_cd_srch3::varchar(150) as srch_term_match_cd_srch3,
        fisc_yr_vrnt::varchar(150) as fisc_yr_vrnt,
        usg_ind::varchar(150) as usg_ind,
        insp_carr_out_by_cust::varchar(150) as insp_carr_out_by_cust,
        insp_delv_note_after_outb_delv::varchar(150) as insp_delv_note_after_outb_delv,
        ref_acct_grp_one_time_acct::varchar(150) as ref_acct_grp_one_time_acct,
        po_box_city::varchar(150) as po_box_city,
        plnt::varchar(150) as plnt,
        data_med_exch_ind::varchar(150) as data_med_exch_ind,
        instr_key_data_med_exch::varchar(150) as instr_key_data_med_exch,
        sts_data_tfr_subsq_rlse::varchar(150) as sts_data_tfr_subsq_rlse,
        asgnmt_hier::number(18,0) as asgnmt_hier,
        pmt_blk::varchar(150) as pmt_blk,
        cust_grp::varchar(150) as cust_grp,
        id_mn_non_mil_use::varchar(150) as id_mn_non_mil_use,
        id_mn_mil_use::varchar(150) as id_mn_mil_use,
        cust_cond_grp_1::varchar(150) as cust_cond_grp_1,
        cust_cond_grp_2::varchar(150) as cust_cond_grp_2,
        cust_cond_grp_3::varchar(150) as cust_cond_grp_3,
        cust_cond_grp_4::varchar(150) as cust_cond_grp_4,
        cust_cond_grp_5::varchar(150) as cust_cond_grp_5,
        alt_pyr_acct_num_ind::varchar(150) as alt_pyr_acct_num_ind,
        tax_type::varchar(150) as tax_type,
        tax_num_type::varchar(150) as tax_num_type,
        tax_num3::varchar(150) as tax_num3,
        tax_num4::varchar(150) as tax_num4,
        cust_is_icms_expt::varchar(150) as cust_is_icms_expt,
        cust_is_ipi_expt::varchar(150) as cust_is_ipi_expt,
        cust_grp_tax_subst::varchar(150) as cust_grp_tax_subst,
        cust_cfop_cat::varchar(150) as cust_cfop_cat,
        tax_law_icms::varchar(150) as tax_law_icms,
        tax_law_ipi::varchar(150) as tax_law_ipi,
        biochem_wf_ind::varchar(150) as biochem_wf_ind,
        nuclr_np_ind::varchar(150) as nuclr_np_ind,
        natl_scty_ind::varchar(150) as natl_scty_ind,
        missile_tech_ind::varchar(150) as missile_tech_ind,
        cent_sls_blk_cust::varchar(150) as cent_sls_blk_cust,
        unifm_rsrs_lctr::varchar(132) as unifm_rsrs_lctr,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final
