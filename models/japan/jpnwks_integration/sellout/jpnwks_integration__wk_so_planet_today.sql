{{
    config
    (
        post_hook = "
                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }} 
                    SET MAX_VALUE=(select max(JCP_REC_SEQ) from {{this}});
                    "
    )
}}
with edi_sell_out_planet as (
    select * from {{ source('jpnsdl_raw', 'edi_sell_out_planet') }}
),

mt_constant_seq as (
    select * from {{ ref('jpnedw_integration__mt_constant_seq') }}
)
,

-- mt_cnst as (
--     select 
--         trim(edi_sell_out_planet.id) as id,
--         trim(mt_constant_seq.max_value - (row_number() over(order by edi_sell_out_planet.id desc)-1)) as sequence_no
--     from edi_sell_out_planet,mt_constant_seq
-- )
mt_cnst as (
    select 
        trim(edi_sell_out_planet.id) as id,
        trim(mt_constant_seq.max_value + row_number() over(order by edi_sell_out_planet.id desc)) as sequence_no
    from edi_sell_out_planet, mt_constant_seq
)
,
final as (
select 
    trim(sequence_no)::number(10,0) as jcp_rec_seq,
	trim(s.id)::number(10,0) as id,
    trim(rcv_dt)::varchar(256) as rcv_dt,
	trim(test_flag)::varchar(256) as test_flag,
	trim(bgn_sndr_cd)::varchar(256) as bgn_sndr_cd,
	trim(ws_cd)::varchar(256) as ws_cd,
	trim(rtl_type)::varchar(256) as rtl_type,
	trim(rtl_cd)::varchar(256) as rtl_cd,
	trim(trade_type)::varchar(256) as trade_type,
	trim(shp_date)::varchar(256) as shp_date,
	trim(shp_num)::varchar(256) as shp_num,
	trim(trade_cd)::varchar(256) as trade_cd,
	trim(dep_cd)::varchar(256) as dep_cd ,
	trim(chg_cd)::varchar(256) as chg_cd,
	trim(person_in_charge)::varchar(256) as person_in_charge,
	trim(person_name)::varchar(256) as person_name,
	trim(rtl_name)::varchar(256) as rtl_name,
	trim(rtl_ho_cd)::varchar(256) as rtl_ho_cd,
	trim(rtl_address_cd)::varchar(256) as rtl_address_cd,
	trim(data_type)::varchar(256) as data_type,
	trim(opt_fld)::varchar(256) as opt_fld,
	trim(item_nm)::varchar(256) as item_nm,
	trim(item_cd_typ)::varchar(256) as item_cd_typ,
	trim(item_cd)::varchar(256) as item_cd,
	trim(qty)::varchar(256) as qty,
    trim(qty_type)::varchar(256) as qty_type,
	trim(price)::varchar(256) as price,
    trim(price_type)::varchar(256) as price_type,
	trim(bgn_sndr_cd_gln)::varchar(256) as bgn_sndr_cd_gln,
	trim(rcv_cd_gln)::varchar(256) as rcv_cd_gln,
	trim(ws_cd_gln)::varchar(256) as ws_cd_gln,
	trim(shp_ws_cd)::varchar(256) as shp_ws_cd,
	trim(shp_ws_cd_gln)::varchar(256) as shp_ws_cd_gln,
	trim(rep_name_kanji)::varchar(256) as rep_name_kanji,
	trim(rep_info)::varchar(256) as rep_info,
	trim(trade_cd_gln)::varchar(256) as trade_cd_gln,
	trim(rtl_cd_gln)::varchar(256) as rtl_cd_gln,
	trim(rtl_name_kanji)::varchar(256) as rtl_name_kanji,
	trim(rtl_ho_cd_gln)::varchar(256) as rtl_ho_cd_gln,
	trim(item_cd_gtin)::varchar(256) as item_cd_gtin,
	trim(item_nm_kanji)::varchar(256) as item_nm_kanji,
	trim(unt_prc)::varchar(256) as unt_prc,
	trim(net_prc)::varchar(256) as net_prc,
	trim(sales_chan_type)::varchar(256) as sales_chan_type,
	current_timestamp()::timestamp_ntz(9) as jcp_create_date
from edi_sell_out_planet s
join mt_cnst m on m.id=s.id
)

select * from final
