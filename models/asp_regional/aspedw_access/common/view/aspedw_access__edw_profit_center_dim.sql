with edw_profit_center_dim as (
    select * from {{ ref('aspedw_integration__edw_profit_center_dim') }}
),
final as (
    select
        lang_key as "lang_key",
	    cntl_area  as "cntl_area ",
	    prft_ctr  as "prft_ctr ",
	    vld_to_dt  as "vld_to_dt ",
	    vld_from_dt  as "vld_from_dt ",
	    shrt_desc  as "shrt_desc ",
	    med_desc  as "med_desc ",
	    prsn_resp  as "prsn_resp ",
	    crncy_key  as "crncy_key ",
	    crt_dttm  as "crt_dttm ",
	    updt_dttm  as "updt_dttm ",
	    need_stat_shrt_desc  as "need_stat_shrt_desc ",
	    strng_hold_shrt_desc  as "strng_hold_shrt_desc ",
	    rflt  as "rflt "
 from edw_profit_center_dim
)
select * from final