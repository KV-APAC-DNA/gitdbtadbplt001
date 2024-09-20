with source as 
(
    select * from {{ source('indedw_integration', 'edw_rpt_sss_zonal') }}
)
select
    zone_name as "zone_name",
    mth_mm as "mth_mm",
    cur_yr as "cur_yr",
    prev_yr as "prev_yr",
    cur_mnth as "cur_mnth",
    prev_mnth as "prev_mnth",
    cur_nocb as "cur_nocb",
    prev_nocb as "prev_nocb",
    nocb_growth_percent as "nocb_growth_percent",
    cur_achvmnt_nr as "cur_achvmnt_nr",
    prev_achvmnt_nr as "prev_achvmnt_nr",
    achievement_nr_growth_percent as "achievement_nr_growth_percent",
    cur_achvmnt_nr_tot as "cur_achvmnt_nr_tot",
    prev_achvmnt_nr_tot as "prev_achvmnt_nr_tot",
    tot_growth_percent as "tot_growth_percent",
    cur_achvmnt_nr_signature as "cur_achvmnt_nr_signature",
    prev_achvmnt_nr_signature as "prev_achvmnt_nr_signature",
    signature_growth_percent as "signature_growth_percent",
    cur_achvmnt_nr_premium as "cur_achvmnt_nr_premium",
    prev_achvmnt_nr_premium as "prev_achvmnt_nr_premium",
    premium_growth_percent as "premium_growth_percent",
    cur_achvmnt_nr_nonprog as "cur_achvmnt_nr_nonprog",
    prev_achvmnt_nr_nonprog as "prev_achvmnt_nr_nonprog",
    nonprog_growth_percent as "nonprog_growth_percent",
    load_datetime as "load_datetime"
from source
