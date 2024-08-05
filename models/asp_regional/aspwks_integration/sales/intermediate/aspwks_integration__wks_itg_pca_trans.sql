with sdl_sap_bw_pca_actuals as
(
    select * from {{source('aspsdl_raw', 'sdl_sap_bw_pca_actuals')}}
),
itg_pca_trans as
(
    select * from {{source('aspitg_integration', 'itg_pca_trans')}}
),
trans as
(
    SELECT 
       src.request_number,
       src.data_packet,
       src.data_record,
       fiscper,
       fiscvarnt,
       fiscyear,
       fiscper3,
       currency,
       unit,
       account,
       chrt_accts,
       comp_code,
       co_area,
       curtype,
       part_prctr,
       profit_ctr,
       version,
       deprarea,
       orig_pca,
       pcompany,
       pcomp_code,
       porig_pca,
       scope,
       move_type,
       balance,
       credit,
       debit,
       quantity,
       zmovper,
       valuation,
       version_ra,
       vtype,
       func_area,
       pfunc_area,
       plant,
       rep_matl,
       zpc_activ,
       TGT.CRT_DTTM AS TGT_CRT_DTTM,
       UPDT_DTTM,
          CASE WHEN 
       TGT.CRT_DTTM IS NULL
       THEN 'I' ELSE 'U' END
       AS CHNG_FLG
       FROM sdl_sap_bw_pca_actuals SRC
    LEFT OUTER JOIN (SELECT request_number, data_packet, data_record, CRT_DTTM FROM itg_pca_trans) TGT
        ON SRC.request_number=TGT.request_number
        AND SRC.data_packet=TGT.data_packet
        AND SRC.data_record=TGT.data_record
),
final as
(
   select
    request_number::varchar(100) as request_number,
	data_packet::varchar(50) as data_packet,
	data_record::varchar(100) as data_record,
	fiscper::number(38,0) as fiscper,
	fiscvarnt::varchar(2) as fiscvarnt,
	fiscyear::number(38,0) as fiscyear,
	fiscper3::number(38,0) as fiscper3,
	currency::varchar(5) as currency,
	unit::varchar(4) as unit,
	account::varchar(10) as account,
	chrt_accts::varchar(4) as chrt_accts,
	comp_code::varchar(4) as comp_code,
	co_area::varchar(4) as co_area,
	curtype::varchar(2) as curtype,
	part_prctr::varchar(10) as part_prctr,
	profit_ctr::varchar(10) as profit_ctr,
	version::varchar(3) as version,
	deprarea::number(38,0) as deprarea,
	orig_pca::number(38,0) as orig_pca,
	pcompany::varchar(6) as pcompany,
	pcomp_code::varchar(4) as pcomp_code,
	porig_pca::number(38,0) as porig_pca,
	scope::varchar(2) as scope,
	move_type::varchar(3) as move_type,
	balance::number(20,2) as balance,
	credit::number(20,2) as credit,
	debit::number(20,2) as debit,
	quantity::number(20,3) as quantity,
	zmovper::number(20,2) as zmovper,
	valuation::number(38,0) as valuation,
	version_ra::varchar(3) as version_ra,
	vtype::number(38,0) as vtype,
	func_area::varchar(16) as func_area,
	pfunc_area::varchar(16) as pfunc_area,
	plant::varchar(4) as plant,
	rep_matl::varchar(18) as rep_matl,
	zpc_activ::varchar(4) as zpc_activ,
	tgt_crt_dttm::timestamp_ntz(9) as tgt_crt_dttm,
	updt_dttm::timestamp_ntz(9) as updt_dttm,
	chng_flg::varchar(1) as chng_flg
    from trans
)
select * from final