with out_dly as (
    select * from dev_dna_core.jpnedw_integration.dw_so_sell_out_dly
),
item_m as (
    select * from dev_dna_core.snapjpnedw_integration.edi_item_m
),
store_m as (
    select * from dev_dna_core.snapjpnedw_integration.edi_store_m
),

chn_m as (
    select * from dev_dna_core.snapjpnedw_integration.edi_chn_m
),
frnch_m as (
    select * from dev_dna_core.jpnedw_integration.edi_frnch_m
),

so as (
        select edi.item_cd as item_cd,
            a.jcp_str_cd as str_cd,
            sum(a.qty) as pc
        from out_dly a
        inner join item_m edi on a.item_cd = edi.jan_cd_so
        where to_date(a.shp_date) >= to_date(dateadd(month, -6, current_timestamp()))
        group by edi.item_cd,
            a.jcp_str_cd
        having sum(a.qty) > 0
        ),
ls as (
        select nvl(al1.str_cd, concat (
                    'D',
                    al2.chn_cd
                    )) as str_cd,
            al1.cmmn_nm_knj as cmmn_nm_knj,
            al1.adrs_knj1 as adrs_knj1,
            al1.tel_no as tel_no
        from store_m al1,
            chn_m al2
        where al1.chn_cd(+) = al2.chn_cd
            and al2.rank in ('KA', 'P1', 'P2', 'P3', 'P4')
            and al2.sgmt <> 'D'
        ),

al11 as (
            select *
            from frnch_m
            where ph_lvl = '5'
),
li as (
        select al1.item_cd as item_cd,
            al1.iten_nm_kn as item_nm_kn,
            al11.ph_cd as mjr_prod_cd,
            al11.ph_nm as mjr_prod_nm
        from item_m al1, al11
        where al11.ph_cd(+) = substring(al1.sub_frnch, 1, 14)
        ),

result as (
        select li.mjr_prod_cd,
            li.mjr_prod_nm,
            li.item_cd,
            li.item_nm_kn,
            so.str_cd,
            ls.cmmn_nm_knj,
            ls.adrs_knj1,
            ls.tel_no,
            sum(so.pc) as pc
        from so,
            ls,
            li
        where so.str_cd = ls.str_cd(+)
            and so.item_cd = li.item_cd(+)
            and (
                ls.cmmn_nm_knj is not null
                or ls.adrs_knj1 is not null
                or ls.tel_no is not null
                )
        group by li.mjr_prod_cd,
            li.mjr_prod_nm,
            li.item_cd,
            li.item_nm_kn,
            so.str_cd,
            ls.cmmn_nm_knj,
            ls.adrs_knj1,
            ls.tel_no
        ),
        
final as (
select 
    mjr_prod_cd::varchar(256) as mjr_pdt,
	mjr_prod_nm::varchar(256) as ph_nm,
	item_cd::varchar(256) as item_cd,
	item_nm_kn::varchar(256) as item_nm_kn,
	str_cd::varchar(256) as str_cd,
	cmmn_nm_knj::varchar(256) as lgl_nm_kn,
	adrs_knj1::varchar(256) as adrs_knj1,
	tel_no::varchar(256) as tel_no,
	pc::number(6,0) as pc,
from result)


select * from final

