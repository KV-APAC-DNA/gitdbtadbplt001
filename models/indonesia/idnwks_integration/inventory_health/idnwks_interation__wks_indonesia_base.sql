with 
edw_indonesia_lppb_analysis as 
(   
    select * from {{ ref('idnedw_integration__edw_indonesia_lppb_analysis') }}
),
edw_vw_os_time_dim as 
(
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),

trans as
(
    select tmd."year" as year,
    cast(tmd.mnth_id as varchar) as month,
    nvl(nullif(a.jj_sap_dstrbtr_nm, ''), 'NA') as jj_sap_dstrbtr_nm,
    a.dstrbtr_grp_cd,
    nvl(nullif(a.jj_sap_cd_mp_prod_id, ''), 'NA') AS SKU_CD,
    a.dstrbtr_grp_nm as sap_prnt_cust_desc,
    sum(sellin_qty) si_sls_qty,
    sum(gross_sellin_val) si_gts_val,
    case
        when tmd.mnth_id <= '202012' then sum(end_inv_qty)
        when tmd.mnth_id >= '202101' then sum(nvl(stock_on_hand_qty, 0) + nvl(intransit_qty, 0))
    end as inventory_quantity,
    case
        when tmd.mnth_id <= '202012' then sum(gross_end_inv_val)
        when tmd.mnth_id >= '202101' then sum(nvl(stock_on_hand_val, 0) + nvl(intransit_hna, 0))
    end as inventory_val,
    sum(sellout_qty) as so_sls_qty,
    sum(gross_sellout_val) as so_trd_sls
from (
        select *
        from (
                select *
                from edw_indonesia_lppb_analysis
                where (
                        dstrbtr_grp_cd is not null
                        and dstrbtr_grp_cd <> ''
                    )
                    and jj_mnth_id <= 202012
                    AND UPPER(NVL(FRANCHISE, 'NA')) NOT IN ('OTX')
                union all
                select *
                from edw_indonesia_lppb_analysis
                where (
                        dstrbtr_grp_cd is not null
                        and dstrbtr_grp_cd <> ''
                    )
                    and jj_mnth_id >= 202101
                    AND UPPER(NVL(FRANCHISE, 'NA')) NOT IN ('OTX')
                    and nvl(jj_mnth_id, 999912) in (
                        select nvl(jj_mnth_id, 999912)
                        from edw_indonesia_lppb_analysis
                        where jj_mnth_id >= 202101
                        group by jj_mnth_id
                        having (
                                sum(nvl (stock_on_hand_val, 0)) > 0
                                and sum(nvl (intransit_hna, 0)) > 0
                            )
                    )
            ) as a
        where jj_year >= (date_part(year, current_timestamp) -6)
    ) as a
    left join (
        select distinct "year",
            qrtr_no,
            mnth_id,
            mnth_no
        from edw_vw_os_time_dim
    ) as tmd on a.jj_mnth_id = cast(tmd.mnth_id as int)
group by tmd."year",
    tmd.mnth_id,
    a.jj_sap_dstrbtr_nm,
    a.dstrbtr_grp_cd,
    a.jj_sap_cd_mp_prod_id,
    a.dstrbtr_grp_nm
),
final as
(
    select 
    year::number(18,0) as year,
	month::varchar(23) as month,
	jj_sap_dstrbtr_nm::varchar(75) as jj_sap_dstrbtr_nm,
	dstrbtr_grp_cd::varchar(25) as dstrbtr_grp_cd,
	sku_cd::varchar(50) as sku_cd,
	sap_prnt_cust_desc::varchar(155) as sap_prnt_cust_desc,
	si_sls_qty::number(38,4) as si_sls_qty,
	si_gts_val::number(38,4) as si_gts_val,
	inventory_quantity::number(38,4) as inventory_quantity,
	inventory_val::number(38,4) as inventory_val,
	so_sls_qty::number(38,4) as so_sls_qty,
	so_trd_sls::number(38,4) as so_trd_sls
    from trans
)
select * from final