with edi_item_m
as (
    select *
    from {{ ref('jpnedw_integration__edi_item_m') }}
    ),
wk_so_planet_no_dup
as (
    select *
    from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
    ),
customer_cd
as (
    select *
    from {{ ref('jpnwks_integration__customer_cd') }}
    ),
store_v_str_cd
as (
    select *
    from {{ ref('jpnwks_integration__store_v_str_cd') }}
    ),
item_m_stg
as (
    select *
    from {{ ref('jpnwks_integration__item_m_stg') }}
    ),
store_v_price
as (
    select *
    from {{ ref('snapjpnwks_integration__store_v_price') }}
    ),
store_v_qty_mod
as (
    select *
    from {{ ref('jpnwks_integration__store_v_qty_mod') }}
    ),
planet_no_dup_unt_prc
as (
    select *
    from {{ ref('jpnwks_integration__planet_no_dup_unt_prc') }}
    ),
planet_no_dup_net_prc
as (
    select *
    from {{ ref('jpnwks_integration__planet_no_dup_net_prc') }}
    ),
consistency_error_2
as (
    select *
    from {{ ref('jpnwks_integration__consistency_error_2') }}
    ),
dw_so_planet_err_cd
as (
    select *
    from {{ ref('jpnitg_integration__dw_so_planet_err_cd') }}
    ),
dw_so_planet_err
as (
    select *
    from {{ ref('jpnitg_integration__dw_so_planet_err') }}
    ),
wk_so_planet_modified
as (
    select *
    from {{ ref('jpnwks_integration__wk_so_planet_modified') }}
    ),
insert1
as (
    select distinct a.jcp_rec_seq,
        a.id,
        to_date(a.rcv_dt, 'YYMMDD') rcv_dt,
        a.test_flag,
        a.bgn_sndr_cd,
        a.ws_cd,
        a.rtl_type,
        a.rtl_cd,
        a.trade_type,
        to_date(a.shp_date, 'YYMMDD') shp_date,
        a.shp_num,
        a.trade_cd,
        a.dep_cd,
        a.chg_cd,
        a.person_in_charge,
        a.person_name,
        a.rtl_name,
        a.rtl_ho_cd,
        a.rtl_address_cd,
        a.data_type,
        a.opt_fld,
        a.item_nm,
        a.item_cd_typ,
        a.item_cd,
        a.qty,
        a.qty_type,
        a.price,
        a.price_type,
        a.bgn_sndr_cd_gln,
        a.rcv_cd_gln,
        a.ws_cd_gln,
        a.shp_ws_cd,
        a.shp_ws_cd_gln,
        a.rep_name_kanji,
        a.rep_info,
        a.trade_cd_gln,
        a.rtl_cd_gln,
        a.rtl_name_kanji,
        a.rtl_ho_cd_gln,
        a.item_cd_gtin,
        a.item_nm_kanji,
        case 
            when a.unt_prc = ''
                then 0
            else cast(a.unt_prc as integer)
            end unt_prc,
        cast(a.net_prc as integer) net_prc,
        a.sales_chan_type as sales_chan_type,
        a.jcp_create_date as jcp_create_date,
        round((cast(a.qty as integer) / cast(i.pc as integer)), 3) as jcp_add_qty_c,
        cast(a.qty as integer) * cast(i.unt_prc as integer) as jcp_add_c_price,
        b.cust_cd as jcp_add_shp_to_cd,
        c.v_str_cd as jcp_add_str_cd,
        d.v_item_cd_jc as jcp_add_item_cd_jc,
        round(cast(f.v_qty_mod as integer) / cast(i.pc as integer), 3) as jcp_mod_qty,
        d.v_item_cd as jcp_mod_item_cd,
        cast(e.v_price as integer) as jcp_mod_price,
        case 
            when g.v_chgnum = ''
                then 0
            else cast(g.v_chgnum as integer)
            end jcp_mod_unt_price,
        case 
            when h.v_chgnum = ''
                then 0
            else cast(h.v_chgnum as integer)
            end jcp_mod_net_price,
        case 
            when a.price_type = 'T'
                then (cast(a.qty as integer) * abs(cast(g.unt_prc as integer)))
            else cast(h.net_prc as integer)
            end jcp_net_price
    from edi_item_m i,
        wk_so_planet_no_dup a,
        customer_cd b,
        store_v_str_cd c,
        item_m_stg d,
        store_v_price e,
        store_v_qty_mod f,
        planet_no_dup_unt_prc g,
        planet_no_dup_net_prc h
    where i.jan_cd_so = a.item_cd
        and a.qty is not null
        and a.jcp_rec_seq = b.jcp_rec_seq
        and b.cust_cd is not null
        and a.jcp_rec_seq = c.jcp_rec_seq
        and a.jcp_rec_seq = d.jcp_rec_seq
        and a.jcp_rec_seq = e.jcp_rec_seq
        and a.jcp_rec_seq = f.jcp_rec_seq
        and a.jcp_rec_seq = g.jcp_rec_seq
        and a.jcp_rec_seq = h.jcp_rec_seq
        and a.jcp_rec_seq not in (
            select jcp_rec_seq
            from consistency_error_2
            )
        and a.jcp_rec_seq not in (
            select jcp_rec_seq
            from dw_so_planet_err_cd
            where error_cd = 'NRTL'
            )
        and a.jcp_rec_seq not in (
            select jcp_rec_seq
            from dw_so_planet_err
            where export_flag = '0'
            )
    ),
insert2
as (
    select distinct a.jcp_rec_seq,
        a.id,
        to_date(a.rcv_dt, 'YYMMDD') rcv_dt,
        a.test_flag,
        a.bgn_sndr_cd,
        a.ws_cd,
        a.rtl_type,
        a.rtl_cd,
        a.trade_type,
        to_date(a.shp_date, 'YYMMDD') shp_date,
        a.shp_num,
        a.trade_cd,
        a.dep_cd,
        a.chg_cd,
        a.person_in_charge,
        a.person_name,
        a.rtl_name,
        a.rtl_ho_cd,
        a.rtl_address_cd,
        a.data_type,
        a.opt_fld,
        a.item_nm,
        a.item_cd_typ,
        a.item_cd,
        a.qty,
        a.qty_type,
        a.price,
        a.price_type,
        a.bgn_sndr_cd_gln,
        a.rcv_cd_gln,
        a.ws_cd_gln,
        a.shp_ws_cd,
        a.shp_ws_cd_gln,
        a.rep_name_kanji,
        a.rep_info,
        a.trade_cd_gln,
        a.rtl_cd_gln,
        a.rtl_name_kanji,
        a.rtl_ho_cd_gln,
        a.item_cd_gtin,
        a.item_nm_kanji,
        case 
            when a.unt_prc = ''
                then 0
            else cast(a.unt_prc as integer)
            end unt_prc,
        cast(a.net_prc as integer) net_prc,
        a.sales_chan_type as sales_chan_type,
        a.jcp_create_date as jcp_create_date,
        round((cast(a.qty as integer) / cast(i.pc as integer)), 3) as jcp_add_qty_c,
        cast(a.qty as integer) * cast(i.unt_prc as integer) as jcp_add_c_price,
        b.cust_cd as jcp_add_shp_to_cd,
        c.v_str_cd as jcp_add_str_cd,
        d.v_item_cd_jc as jcp_add_item_cd_jc,
        round(cast(f.v_qty_mod as integer) / cast(i.pc as integer), 3) as jcp_mod_qty,
        d.v_item_cd as jcp_mod_item_cd,
        cast(e.v_price as integer) as jcp_mod_price,
        case 
            when g.v_chgnum = ''
                then 0
            else cast(g.v_chgnum as integer)
            end jcp_mod_unt_price,
        case 
            when h.v_chgnum = ''
                then 0
            else cast(h.v_chgnum as integer)
            end jcp_mod_net_price,
        case 
            when a.price_type = 'T'
                then (cast(a.qty as integer) * abs(cast(g.unt_prc as integer)))
            else cast(h.net_prc as integer)
            end v_jc_netjcp_net_price_prc
    from edi_item_m i,
        wk_so_planet_modified a,
        customer_cd b,
        store_v_str_cd c,
        item_m_stg d,
        store_v_price e,
        store_v_qty_mod f,
        planet_no_dup_unt_prc g,
        planet_no_dup_net_prc h
    where i.jan_cd_so = a.item_cd
        and a.qty is not null
        and a.jcp_rec_seq = b.jcp_rec_seq
        and b.cust_cd is not null
        and a.jcp_rec_seq = c.jcp_rec_seq
        and a.jcp_rec_seq = d.jcp_rec_seq
        and a.jcp_rec_seq = e.jcp_rec_seq
        and a.jcp_rec_seq = f.jcp_rec_seq
        and a.jcp_rec_seq = g.jcp_rec_seq
        and a.jcp_rec_seq = h.jcp_rec_seq
        and a.jcp_rec_seq in (
            select jcp_rec_seq
            from consistency_error_2
            )
        and a.jcp_rec_seq not in (
            select jcp_rec_seq
            from dw_so_planet_err_cd
            where error_cd = 'NRTL'
            )
    ),
result
as (
    select *
    from insert1
    
    union all
    
    select *
    from insert2
    ),
final
as (
    select jcp_rec_seq::number(10, 0) as jcp_rec_seq,
        id::number(10, 0) as id,
        rcv_dt::date as rcv_dt,
        test_flag::varchar(256) as test_flag,
        bgn_sndr_cd::varchar(256) as bgn_sndr_cd,
        ws_cd::varchar(256) as ws_cd,
        rtl_type::varchar(256) as rtl_type,
        rtl_cd::varchar(256) as rtl_cd,
        trade_type::varchar(256) as trade_type,
        shp_date::date as shp_date,
        shp_num::varchar(256) as shp_num,
        trade_cd::varchar(256) as trade_cd,
        dep_cd::varchar(256) as dep_cd,
        chg_cd::varchar(256) as chg_cd,
        person_in_charge::varchar(256) as person_in_charge,
        person_name::varchar(256) as person_name,
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
        unt_prc::number(18, 0) as unt_prc,
        net_prc::number(18, 0) as net_prc,
        sales_chan_type::varchar(256) as sales_chan_type,
        current_timestamp()::timestamp_ntz(9) as jcp_create_date,
        jcp_add_qty_c::number(18, 3) as jcp_add_qty_c,
        jcp_add_c_price::number(18, 3) as jcp_add_c_price,
        jcp_add_shp_to_cd::varchar(256) as jcp_add_shp_to_cd,
        jcp_add_str_cd::varchar(256) as jcp_add_str_cd,
        jcp_add_item_cd_jc::varchar(256) as jcp_add_item_cd_jc,
        jcp_mod_qty::number(18, 3) as jcp_mod_qty,
        jcp_mod_item_cd::varchar(256) as jcp_mod_item_cd,
        jcp_mod_price::number(18, 3) as jcp_mod_price,
        jcp_mod_unt_price::number(18, 3) as jcp_mod_unt_price,
        jcp_mod_net_price::number(18, 3) as jcp_mod_net_price,
        jcp_net_price::number(18, 3) as jcp_net_price
    from result
    )


select *
from final
