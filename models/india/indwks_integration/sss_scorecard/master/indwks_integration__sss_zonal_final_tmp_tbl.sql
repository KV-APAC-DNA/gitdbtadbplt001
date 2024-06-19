with 
sss_zonal_nocb_achnr_growth_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_nocb_achnr_growth_tbl') }}
),
sss_zonal_totgrowth_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_totgrowth_tbl') }}
),
sss_zonal_sign_growth_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_sign_growth_tbl') }}
),
sss_zonal_prem_growth_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_prem_growth_tbl') }}
),
sss_zonal_nonprog_growth_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_nonprog_growth_tbl') }}
),
trans as
(
    SELECT nocb.zone_name,
       nocb.mth_mm,
       nocb.cur_yr,
       nocb.prev_yr,
       nocb.cur_mnth,
       nocb.prev_mnth,
       nocb.cur_nocb,
       nocb.prev_nocb,
       nocb.nocb_growth_percent,
       nocb.cur_achvmnt_nr,
       nocb.prev_achvmnt_nr,
       nocb.achievement_nr_growth_percent,
       tot.cur_achvmnt_nr_tot,
       tot.prev_achvmnt_nr_tot,
       tot.tot_growth_percent,
       sig.cur_achvmnt_nr_signature,
       sig.prev_achvmnt_nr_signature,
       sig.signature_growth_percent,
       prem.cur_achvmnt_nr_premium,
       prem.prev_achvmnt_nr_premium,
       prem.premium_growth_percent,
       nonprog.cur_achvmnt_nr_nonprog,
       nonprog.prev_achvmnt_nr_nonprog,
       nonprog.nonprog_growth_percent
FROM   sss_zonal_nocb_achnr_growth_tbl nocb
INNER JOIN sss_zonal_totgrowth_tbl  tot
ON  nocb.zone_name = tot.zone_name
AND nocb.mth_mm = tot.mth_mm
INNER JOIN sss_zonal_sign_growth_tbl sig
ON  nocb.zone_name = sig.zone_name
AND nocb.mth_mm = sig.mth_mm
INNER JOIN sss_zonal_prem_growth_tbl prem
ON  nocb.zone_name = prem.zone_name
AND nocb.mth_mm = prem.mth_mm
INNER JOIN sss_zonal_nonprog_growth_tbl nonprog
ON  nocb.zone_name = nonprog.zone_name
AND nocb.mth_mm = nonprog.mth_mm
),
final as 
(
    select 
    zone_name::varchar(50) as zone_name,
	mth_mm::number(18,0) as mth_mm,
	cur_yr::number(18,0) as cur_yr,
	prev_yr::number(18,0) as prev_yr,
	cur_mnth::varchar(3) as cur_mnth,
	prev_mnth::varchar(3) as prev_mnth,
	cur_nocb::number(38,0) as cur_nocb,
	prev_nocb::number(38,0) as prev_nocb,
	nocb_growth_percent::number(34,22) as nocb_growth_percent,
	cur_achvmnt_nr::number(38,6) as cur_achvmnt_nr,
	prev_achvmnt_nr::number(38,6) as prev_achvmnt_nr,
	achievement_nr_growth_percent::number(38,4) as achievement_nr_growth_percent,
	cur_achvmnt_nr_tot::number(38,6) as cur_achvmnt_nr_tot,
	prev_achvmnt_nr_tot::number(38,6) as prev_achvmnt_nr_tot,
	tot_growth_percent::number(38,4) as tot_growth_percent,
	cur_achvmnt_nr_signature::number(38,6) as cur_achvmnt_nr_signature,
	prev_achvmnt_nr_signature::number(38,6) as prev_achvmnt_nr_signature,
	signature_growth_percent::number(38,4) as signature_growth_percent,
	cur_achvmnt_nr_premium::number(38,6) as cur_achvmnt_nr_premium,
	prev_achvmnt_nr_premium::number(38,6) as prev_achvmnt_nr_premium,
	premium_growth_percent::number(38,4) as premium_growth_percent,
	cur_achvmnt_nr_nonprog::number(38,6) as cur_achvmnt_nr_nonprog,
	prev_achvmnt_nr_nonprog::number(38,6) as prev_achvmnt_nr_nonprog,
	nonprog_growth_percent::number(38,4) as nonprog_growth_percent
    from trans
)
select * from final