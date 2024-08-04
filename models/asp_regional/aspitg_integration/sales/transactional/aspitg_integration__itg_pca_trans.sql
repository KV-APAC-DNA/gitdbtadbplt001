{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        DELETE FROM {{this}} as itg_pca_trans
        USING DEV_DNA_CORE.ASPWKS_INTEGRATION.WKS_ITG_PCA_TRANS as WKS_itg_pca_trans
        WHERE itg_pca_trans.request_number=WKS_itg_pca_trans.request_number
        AND itg_pca_trans.data_packet=WKS_itg_pca_trans.data_packet
        AND itg_pca_trans.data_record=WKS_itg_pca_trans.data_record
        AND WKS_itg_pca_trans.CHNG_FLG='U';
        {% endif %}"
    )
}}
with wks_itg_pca_trans as
(
    select * from DEV_DNA_CORE.ASPWKS_INTEGRATION.WKS_ITG_PCA_TRANS
),
trans as
(
    select
       request_number as REQUEST_NUMBER,
       data_packet as DATA_PACKET,
       data_record as DATA_RECORD,
       fiscper as FISC_YR_PER,
       fiscvarnt as FISC_YR_VRNT,
       fiscyear as FISC_YR,
       fiscper3 as PSTNG_PER,
       currency as CRNCY,
       unit as UOM,
       account as ACCT_NUM,
       chrt_accts as CHRT_OF_ACCT,
       comp_code as CO_CD,
       co_area as CNTL_AREA,
       curtype as CRNCY_TYPE,
       part_prctr as PTNR_PRFT_CTR,
       profit_ctr as PRFT_CTR,
       version as VERS,
       deprarea as DEPR_AREA,
       orig_pca as ORIG_OBJ_TYPE,
       pcompany as TRAD_PTNR,
       pcomp_code as PTNR_CO_CD,
       porig_pca as PARTOROBTYPE_PCA,
       scope as OBJ_CLS,
       move_type as MVMT_TYPE,
       balance as CUM_BAL,
       credit as TOT_CR_POSTGS,
       debit as TOT_DR_POSTGS,
       quantity as QTY,
       zmovper as MVMT_FOR_PER,
       valuation as VALUT_VIEW,
       version_ra as RA_VERS,
       vtype as VAL_TYPE,
       func_area as FUNC_AREA,
       pfunc_area as PTNR_F_AREA,
       plant as PLNT,
       rep_matl as REP_MATL,
       zpc_activ as PCA_ACTV,
       CASE WHEN CHNG_FLG = 'I' THEN current_timestamp() ELSE TGT_CRT_DTTM END  AS CRT_DTTM ,
       current_timestamp()::timestamp_ntz(9) AS UPDT_DTTM
    from wks_itg_pca_trans
),
final as
(
    select
    request_number::varchar(100) as request_number,
	data_packet::varchar(50) as data_packet,
	data_record::varchar(100) as data_record,
	fisc_yr_per::number(38,0) as fisc_yr_per,
	fisc_yr_vrnt::varchar(2) as fisc_yr_vrnt,
	fisc_yr::number(38,0) as fisc_yr,
	pstng_per::number(38,0) as pstng_per,
	crncy::varchar(5) as crncy,
	uom::varchar(4) as uom,
	acct_num::varchar(10) as acct_num,
	chrt_of_acct::varchar(4) as chrt_of_acct,
	co_cd::varchar(4) as co_cd,
	cntl_area::varchar(4) as cntl_area,
	crncy_type::varchar(2) as crncy_type,
	ptnr_prft_ctr::varchar(10) as ptnr_prft_ctr,
	prft_ctr::varchar(10) as prft_ctr,
	vers::varchar(3) as vers,
	depr_area:: number(38,0) as depr_area,
	orig_obj_type:: number(38,0) as orig_obj_type,
	trad_ptnr::varchar(6) as trad_ptnr,
	ptnr_co_cd::varchar(4) as ptnr_co_cd,
	partorobtype_pca::number(38,0) as partorobtype_pca,
	obj_cls::varchar(2) as obj_cls,
	mvmt_type::varchar(3) as mvmt_type,
	cum_bal::number(20,2) as cum_bal,
	tot_cr_postgs::number(20,2) as tot_cr_postgs,
	tot_dr_postgs::number(20,2) as tot_dr_postgs,
	qty::number(20,3) as qty,
	mvmt_for_per::number(20,2) as mvmt_for_per,
	valut_view::number(38,0) as valut_view,
	ra_vers::varchar(3) as ra_vers,
	val_type::number(38,0) as val_type,
	func_area::varchar(16) as func_area,
	ptnr_f_area::varchar(16) as ptnr_f_area,
	plnt::varchar(4) as plnt,
	rep_matl::varchar(18) as rep_matl,
	pca_actv::varchar(4) as pca_actv,
	crt_dttm::timestamp_ntz(9) as crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm
    from trans
)
select * from final