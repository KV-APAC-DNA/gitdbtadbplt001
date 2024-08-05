{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook = " {% if is_incremental() %}
        DELETE FROM {{this}} as itg_pca_trans
        USING {{ ref('aspwks_integration__wks_itg_pca_trans') }} as WKS_itg_pca_trans
        WHERE itg_pca_trans.request_number=WKS_itg_pca_trans.request_number
        AND itg_pca_trans.data_packet=WKS_itg_pca_trans.data_packet
        AND itg_pca_trans.data_record=WKS_itg_pca_trans.data_record
        AND WKS_itg_pca_trans.CHNG_FLG='U';
        {% endif %}"
    )
}}
with wks_itg_pca_trans as
(
    select * from {{ ref('aspwks_integration__wks_itg_pca_trans') }}
),
trans as
(
    select
       request_number as request_number,
       data_packet as data_packet,
       data_record as data_record,
       fiscper as fisc_yr_per,
       fiscvarnt as fisc_yr_vrnt,
       fiscyear as fisc_yr,
       fiscper3 as pstng_per,
       currency as crncy,
       unit as uom,
       account as acct_num,
       chrt_accts as chrt_of_acct,
       comp_code as co_cd,
       co_area as cntl_area,
       curtype as crncy_type,
       part_prctr as ptnr_prft_ctr,
       profit_ctr as prft_ctr,
       version as vers,
       deprarea as depr_area,
       orig_pca as orig_obj_type,
       pcompany as trad_ptnr,
       pcomp_code as ptnr_co_cd,
       porig_pca as partorobtype_pca,
       scope as obj_cls,
       move_type as mvmt_type,
       balance as cum_bal,
       credit as tot_cr_postgs,
       debit as tot_dr_postgs,
       quantity as qty,
       zmovper as mvmt_for_per,
       valuation as valut_view,
       version_ra as ra_vers,
       vtype as val_type,
       func_area as func_area,
       pfunc_area as ptnr_f_area,
       plant as plnt,
       rep_matl as rep_matl,
       zpc_activ as pca_actv,
       CASE WHEN CHNG_FLG = 'I' THEN current_timestamp() ELSE tgt_crt_dttm end  as crt_dttm,
       current_timestamp()::timestamp_ntz(9) AS updt_dttm
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