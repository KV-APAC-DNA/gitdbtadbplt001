with edw_time_dim as
(
  select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
vw_sales_reporting as
(
  select * from {{ ref('pcfedw_integration__vw_sales_reporting') }}
),
projprd as
(
  select 
  left(replace(add_months(to_date(t1.jj_mnth_id||'01','yyyymmdd'),-1),'-',''),6) as prev_jj_period
  from edw_time_dim t1
  where to_date(cal_date) = current_date+1
),
transformed as
(
  select 
    substring(current_timestamp(), 1, 19) as snapshot_date,
    upper(monthname(current_timestamp)) as snapshot_month,
    year(current_timestamp) as snapshot_year,
    jj_period,
    jj_wk,
    jj_mnth,
    jj_mnth_shrt,
    jj_mnth_long,
    jj_qrtr,
    jj_year,
    cust_no,
    country as cmp_desc,
    channel_desc,
    cust_nm,
    sales_office_desc,
    sales_grp_desc,
    matl_id,
    matl_desc,
    parent_matl_id,
    parent_matl_desc,
    fran_desc,
    grp_fran_desc,
    matl_type_desc,
    prod_fran_desc,
    prod_mjr_desc,
    prod_mnr_desc,
    local_curr_cd as base_curr_cd,
    to_ccy,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(sales_qty)
            else sum(px_qty)
        end
    ) as px_qty,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(gts)
            else sum(px_gts)
        end
    ) as px_gts,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(eff_val)
            else sum(px_eff_val)
        end
    ) as px_eff_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(jgf_si_val)
            else sum(px_jgf_si_val)
        end
    ) as px_jgf_si_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(pmt_terms_val)
            else sum(px_pmt_terms_val)
        end
    ) as px_pmt_terms_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(datains_val)
            else sum(px_datains_val)
        end
    ) as px_datains_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(exp_adj_val)
            else sum(px_exp_adj_val)
        end
    ) as px_exp_adj_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(jgf_sd_val)
            else sum(px_jgf_sd_val)
        end
    ) as px_jgf_sd_val,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(tot_ciw_val)
            else sum(px_ciw_tot)
        end
    ) as px_ciw_tot,
    (
        case
            when jj_period <= projprd.prev_jj_period then sum(nts_val)
            else sum(px_nts)
        end
    ) as px_nts
  from vw_sales_reporting,
  projprd
  where jj_year >=
    (
      select jj_year
      from edw_time_dim
      where to_date(cal_date) = to_date(current_date)
    )
    and trim(pac_subsource_type) in 
    (
        'PX_FORECAST',
        'PX_MASTER',
        'PX_TERMS',
        'SAPBW_ACTUAL'
    )
    and cust_no is not null
    group by 
      snapshot_date,
      snapshot_month,
      snapshot_year,
      jj_period,
      jj_wk,
      jj_mnth,
      jj_mnth_shrt,
      jj_mnth_long,
      jj_qrtr,
      jj_year,
      cust_no,
      cmp_desc,
      channel_desc,
      cust_nm,
      sales_office_desc,
      sales_grp_desc,
      matl_id,
      matl_desc,
      parent_matl_id,
      parent_matl_desc,
      fran_desc,
      grp_fran_desc,
      matl_type_desc,
      prod_fran_desc,
      prod_mjr_desc,
      prod_mnr_desc,
      base_curr_cd,
      to_ccy,
      prev_jj_period
),
final as
(
    select 
        snapshot_date::timestamp_ntz(9) as snapshot_date,
        snapshot_month::varchar(11) as snapshot_month,
        snapshot_year::varchar(12) as snapshot_year,
        jj_period::number(18,0) as jj_period,
        jj_wk::number(38,0) as jj_wk,
        jj_mnth::number(18,0) as jj_mnth,
        jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
        jj_mnth_long::varchar(10) as jj_mnth_long,
        jj_qrtr::number(18,0) as jj_qrtr,
        jj_year::number(18,0) as jj_year,
        cust_no::varchar(10) as cust_no,
        cmp_desc::varchar(20) as cmp_desc,
        channel_desc::varchar(20) as channel_desc,
        cust_nm::varchar(100) as cust_nm,
        sales_office_desc::varchar(30) as sales_office_desc,
        sales_grp_desc::varchar(30) as sales_grp_desc,
        matl_id::varchar(40) as matl_id,
        matl_desc::varchar(100) as matl_desc,
        parent_matl_id::varchar(18) as parent_matl_id,
        parent_matl_desc::varchar(100) as parent_matl_desc,
        fran_desc::varchar(100) as fran_desc,
        grp_fran_desc::varchar(100) as grp_fran_desc,
        matl_type_desc::varchar(40) as matl_type_desc,
        prod_fran_desc::varchar(100) as prod_fran_desc,
        prod_mjr_desc::varchar(100) as prod_mjr_desc,
        prod_mnr_desc::varchar(100) as prod_mnr_desc,
        base_curr_cd::varchar(10) as base_curr_cd,
        to_ccy::varchar(5) as to_ccy,
        px_qty::number(38,5) as px_qty,
        px_gts::number(38,7) as px_gts,
        px_eff_val::number(38,7) as px_eff_val,
        px_jgf_si_val::number(38,7) as px_jgf_si_val,
        px_pmt_terms_val::number(38,7) as px_pmt_terms_val,
        px_datains_val::number(38,7) as px_datains_val,
        px_exp_adj_val::number(38,7) as px_exp_adj_val,
        px_jgf_sd_val::number(38,7) as px_jgf_sd_val,
        px_ciw_tot::number(38,7) as px_ciw_tot,
        px_nts::number(38,7) as px_nts
    from transformed
)
select * from final