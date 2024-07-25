with dw_so_planet_err as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err') }}
),

consistency_error_2 as (
    select * from {{ ref('jpnwks_integration__consistency_error_2') }}
),

temp_tbl as (
    select * from {{ source('jpnwks_integration', 'temp_tbl') }}
),

dw_so_planet_err_cd_2 as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err_cd_2') }}
),

dw_so_planet_err_cd as (
    select * from {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
),

temp_table_3 as (
    select * from {{ ref('jpnwks_integration__temp_table_3') }}
),


insert1 as (
select uy.jcp_rec_seq,
    uy.id,
    uy.rcv_dt,
    uy.test_flag,
    uy.bgn_sndr_cd,
    uy.ws_cd,
    uy.rtl_type,
    uy.rtl_cd,
    uy.trade_type,
    uy.shp_date,
    uy.shp_num,
    uy.trade_cd,
    uy.dep_cd,
    uy.chg_cd,
    uy.person_in_charge,
    uy.person_name,
    uy.rtl_name,
    uy.rtl_ho_cd,
    uy.rtl_address_cd,
    uy.data_type,
    uy.opt_fld,
    uy.item_nm,
    uy.item_cd_typ,
    uy.item_cd,
    uy.qty,
    uy.qty_type,
    uy.price,
    uy.price_type,
    uy.bgn_sndr_cd_gln,
    uy.rcv_cd_gln,
    uy.ws_cd_gln,
    uy.shp_ws_cd,
    uy.shp_ws_cd_gln,
    uy.rep_name_kanji,
    uy.rep_info,
    uy.trade_cd_gln,
    uy.rtl_cd_gln,
    uy.rtl_name_kanji,
    uy.rtl_ho_cd_gln,
    uy.item_cd_gtin,
    uy.item_nm_kanji,
    uy.unt_prc,
    uy.net_prc,
    uy.sales_chan_type,
    uy.jcp_create_date
from (
    select su.jcp_rec_seq,
        su.id,
        su.rcv_dt,
        su.test_flag,
        su.bgn_sndr_cd,
        su.ws_cd,
        su.rtl_type,
        su.rtl_cd,
        su.trade_type,
        su.shp_date,
        su.shp_num,
        su.trade_cd,
        su.dep_cd,
        su.chg_cd,
        su.person_in_charge,
        su.person_name,
        su.rtl_name,
        su.rtl_ho_cd,
        su.rtl_address_cd,
        su.data_type,
        su.opt_fld,
        su.item_nm,
        su.item_cd_typ,
        su.item_cd,
        su.qty,
        su.qty_type,
        su.price,
        su.price_type,
        su.bgn_sndr_cd_gln,
        su.rcv_cd_gln,
        su.ws_cd_gln,
        su.shp_ws_cd,
        su.shp_ws_cd_gln,
        su.rep_name_kanji,
        su.rep_info,
        su.trade_cd_gln,
        su.rtl_cd_gln,
        su.rtl_name_kanji,
        su.rtl_ho_cd_gln,
        su.item_cd_gtin,
        su.item_nm_kanji,
        su.unt_prc,
        su.net_prc,
        su.sales_chan_type,
        su.jcp_create_date,
        row_number() over (
            partition by su.jcp_rec_seq order by tmp.priority
            ) as rnk
    from dw_so_planet_err su
    inner join consistency_error_2 r on r.jcp_rec_seq = su.jcp_rec_seq
    inner join temp_tbl tmp on r.exec_flag = tmp.exec_flag
    where r.exec_flag = 'MANUAL'
        and su.export_flag = 0
        and su.jcp_rec_seq not in (
            select jcp_rec_seq
            from dw_so_planet_err_cd_2
            where exec_flag in ('DELETE')
                and export_flag = 0
            )
        and su.jcp_rec_seq not in (
            select jcp_rec_seq
            from dw_so_planet_err_cd
            where error_cd = 'NRTL'
            )
    ) uy
where uy.rnk = 1
)
, 
insert2 as (

select uy.jcp_rec_seq,
    uy.id,
    uy.rcv_dt,
    uy.test_flag,
    uy.bgn_sndr_cd,
    uy.ws_cd,
    uy.rtl_type,
    uy.rtl_cd,
    uy.trade_type,
    uy.shp_date,
    uy.shp_num,
    uy.trade_cd,
    uy.dep_cd,
    uy.chg_cd,
    uy.person_in_charge,
    uy.person_name,
    uy.rtl_name,
    uy.rtl_ho_cd,
    uy.rtl_address_cd,
    uy.data_type,
    uy.opt_fld,
    uy.item_nm,
    uy.item_cd_typ,
    uy.item_cd,
    uy.qty,
    uy.qty_type,
    uy.price,
    uy.price_type,
    uy.bgn_sndr_cd_gln,
    uy.rcv_cd_gln,
    uy.ws_cd_gln,
    uy.shp_ws_cd,
    uy.shp_ws_cd_gln,
    uy.rep_name_kanji,
    uy.rep_info,
    uy.trade_cd_gln,
    uy.rtl_cd_gln,
    uy.rtl_name_kanji,
    uy.rtl_ho_cd_gln,
    uy.item_cd_gtin,
    uy.item_nm_kanji,
    uy.unt_prc,
    uy.net_prc,
    uy.sales_chan_type,
    uy.jcp_create_date
from temp_table_3 uy
where uy.jcp_rec_seq in (
        select distinct prev_rec_seq
        from temp_table_3
        where prev_rec_seq is not null
        )
    or uy.jcp_rec_seq is null
order by id,
    jcp_rec_seq nulls last,
    item_cd
    ),
    

result as (
select * from insert1
union all 
select * from insert2
)
,
final as (
select 
    jcp_rec_seq::number(10,0) as jcp_rec_seq,
	id::number(10,0) as id,
	rcv_dt::varchar(256) as rcv_dt,
	test_flag::varchar(256) as test_flag,
	bgn_sndr_cd::varchar(256) as bgn_sndr_cd,
	ws_cd::varchar(256) as ws_cd,
	rtl_type::varchar(256) as rtl_type,
	rtl_cd::varchar(256) as rtl_cd,
	trade_type::varchar(256) as trade_type,
	shp_date::varchar(256) as shp_date,
	shp_num::varchar(256) as shp_num,
	trade_cd::varchar(256) as trade_cd,
	dep_cd::varchar(256) as dep_cd,
	chg_cd::varchar(256) as chg_cd,
	person_in_charge::varchar(256) as person_in_charge,
	person_name::varchar(256) as person_name,
	null::varchar(256) as column_17,
	rtl_name::varchar(256) as rtl_name,
	rtl_ho_cd::varchar(256) as rtl_ho_cd,
	rtl_address_cd::varchar(256) as rtl_address_cd,
	data_type::varchar(256) as data_type,
	opt_fld::varchar(256) as opt_fld,
	item_nm::varchar(256) as item_nm,
	item_cd_typ::varchar(256) as item_cd_typ,
	item_cd::varchar(256) as item_cd,
	qty::varchar(256) as qty,
	qty_type::varchar(256) as qty_type,
	price::varchar(256) as price,
	price_type::varchar(256) as price_type,
	bgn_sndr_cd_gln::varchar(256) as bgn_sndr_cd_gln,
	rcv_cd_gln::varchar(256) as rcv_cd_gln,
	ws_cd_gln::varchar(256) as ws_cd_gln,
	shp_ws_cd::varchar(256) as shp_ws_cd,
	shp_ws_cd_gln::varchar(256) as shp_ws_cd_gln,
	rep_name_kanji::varchar(256) as rep_name_kanji,
	rep_info::varchar(256) as rep_info,
	trade_cd_gln::varchar(256) as trade_cd_gln,
	rtl_cd_gln::varchar(256) as rtl_cd_gln,
	rtl_name_kanji::varchar(256) as rtl_name_kanji,
	rtl_ho_cd_gln::varchar(256) as rtl_ho_cd_gln,
	item_cd_gtin::varchar(256) as item_cd_gtin,
	item_nm_kanji::varchar(256) as item_nm_kanji,
	unt_prc::varchar(256) as unt_prc,
	net_prc::varchar(256) as net_prc,
	sales_chan_type::varchar(256) as sales_chan_type,
	current_timestamp()::timestamp_ntz(9) as jcp_create_date
from result
)

select * from final