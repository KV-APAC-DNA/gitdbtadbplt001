
with vw_customer_dim as
(
    select * from {{ ref('pcfedw_integration__vw_customer_dim') }}
),
vw_material_dim as
(
    select * from {{ ref('pcfedw_integration__vw_material_dim') }}
),
vw_apo_parent_child_dim as
(
    select * from {{ ref('pcfedw_integration__vw_apo_parent_child_dim') }}
),
edw_promax_ciw_snapshot as
(
    select * from {{ ref('pcfedw_integration__edw_promax_ciw_snapshot_temp') }}
),
final as
(
    select ------- dna data
        eps.snapshot_date::timestamp_ntz(9) as snapshot_date,
        eps.snapshot_month::varchar(10) as snapshot_month,
        eps.snapshot_year::number(18,0) as snapshot_year,
        eps.jj_period::number(18,0) as jj_period,
        eps.jj_wk::varchar(1) as jj_wk,
        eps.jj_mnth::number(18,0) as jj_mnth,
        eps.jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
        eps.jj_mnth_long::varchar(10) as jj_mnth_long,
        eps.jj_qrtr::number(18,0) as jj_qrtr,
        eps.jj_year::number(18,0) as jj_year,
        ltrim(vcd.cust_no, 0::varchar(10)) as cust_no,
        vcd.country::varchar(20) as cmp_desc,
        vcd.channel_desc::varchar(20) as channel_desc,
        vcd.cust_nm::varchar(100) as cust_nm,
        vcd.sales_office_desc::varchar(30) as sales_office_desc,
        vcd.sales_grp_desc::varchar(30) as sales_grp_desc,
        ltrim(vmd.matl_id, 0::varchar(40)) as matl_id,
        vmd.matl_desc::varchar(100) as matl_desc,
        ltrim(vapcd.parent_id, 0::varchar(18)) as parent_matl_id,
        mstrcd.parent_matl_desc::varchar(100) as parent_matl_desc,
        vmd.fran_desc::varchar(100) as fran_desc,
        vmd.grp_fran_desc::varchar(100) as grp_fran_desc,
        vmd.matl_type_desc::varchar(40) as matl_type_desc,
        vmd.prod_fran_desc::varchar(100) as prod_fran_desc,
        vmd.prod_mjr_desc::varchar(100) as prod_mjr_desc,
        vmd.prod_mnr_desc::varchar(100) as prod_mnr_desc,
        eps.base_curr_cd::varchar(10) as base_curr_cd,
        eps.to_ccy::varchar(5) as to_ccy,
        eps.px_qty::number(38,0) as px_qty,
        eps.px_gts::number(38,7) as px_gts,
        eps.px_eff_val::float as px_eff_val,
        eps.px_jgf_si_val::float as px_jgf_si_val,
        eps.px_pmt_terms_val::float as px_pmt_terms_val,
        eps.px_datains_val::float as px_datains_val,
        eps.px_exp_adj_val::float as px_exp_adj_val,
        eps.px_jgf_sd_val::float as px_jgf_sd_val,
        eps.px_ciw_tot::float as px_ciw_tot,
        eps.px_nts::number(38,7) as px_nts,
    from edw_promax_ciw_snapshot eps,
        vw_customer_dim vcd,
        vw_material_dim vmd,
        vw_apo_parent_child_dim vapcd,
        (
            select distinct master_code,
                parent_matl_desc
            from vw_apo_parent_child_dim
            where cmp_id = '7470'
            union all
            select distinct master_code,
                parent_matl_desc
            from vw_apo_parent_child_dim
            where master_code not in (
                    select distinct master_code
                    from vw_apo_parent_child_dim
                    where cmp_id = '7470'
                )
        ) mstrcd
    where to_date(eps.snapshot_date) > '2017-05-21'
        and trim(eps.cust_no) = (ltrim(vcd.cust_no(+), '0'))
        and trim(eps.matl_id) = (ltrim(vmd.matl_id(+), '0'))
        and (
            decode(
                trim(eps.cmp_desc),
                'Australia',
                '7470',
                'New Zealand',
                '8361'
            ) = trim(vapcd.cmp_id(+))
            and trim(eps.matl_id) = (ltrim(vapcd.matl_id(+), '0'))
        )
        and trim(vapcd.master_code) = trim(mstrcd.master_code(+))
)
select * from final