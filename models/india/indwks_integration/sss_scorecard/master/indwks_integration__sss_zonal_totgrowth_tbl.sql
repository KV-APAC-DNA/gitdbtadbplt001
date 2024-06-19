with 
sss_zonal_tot_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_tot_tbl') }}
),
trans as 
(
    SELECT tot_cur_yr.zone_name,
       tot_cur_yr.mth_mm,
       tot_cur_yr.fisc_yr as cur_yr,
       tot_prev_yr.fisc_yr as prev_yr,
       tot_cur_yr.month as cur_mnth ,
       tot_prev_yr.month as prev_mnth,
       tot_cur_yr.achievement_nr as cur_achvmnt_nr_tot,
       tot_prev_yr.achievement_nr as prev_achvmnt_nr_tot,
       ((tot_cur_yr.achievement_nr - tot_prev_yr.achievement_nr)/tot_prev_yr.achievement_nr)*100 AS tot_growth_percent 
FROM (SELECT tot_cur.zone_name,
             tot_cur.mth_mm,
             tot_cur.fisc_yr,
             tot_cur.month,
             tot_cur.achievement_nr
      FROM sss_zonal_tot_tbl tot_cur,
           (SELECT EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '12 months')) AS strt_year,
                   EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '12 months')) AS start_mnth) AS inn
      WHERE tot_cur.mth_mm >= CASE WHEN start_mnth < 10 THEN strt_year||0||start_mnth ELSE strt_year||start_mnth END
      ) tot_cur_yr,
      (SELECT tot_prev.zone_name,
              tot_prev.mth_mm,
              tot_prev.fisc_yr,
              tot_prev.month,
              tot_prev.achievement_nr
      FROM sss_zonal_tot_tbl tot_prev,
           (SELECT EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '12 months')) AS end_year,
                   EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '12 months')) AS end_mnth,
                   EXTRACT(YEAR FROM ((CURRENT_DATE-INTERVAL '12 months')-INTERVAL '12 months')) AS strt_year,
                   EXTRACT(MONTH FROM ((CURRENT_DATE-INTERVAL '12 months')-INTERVAL '12 months')) AS start_mnth) AS inn
      WHERE tot_prev.mth_mm >= CASE WHEN start_mnth < 10 THEN strt_year||0||start_mnth ELSE strt_year||start_mnth END
      AND   tot_prev.mth_mm < CASE WHEN start_mnth < 10 THEN end_year||0||end_mnth ELSE end_year||end_mnth END
      ) tot_prev_yr
WHERE tot_cur_yr.zone_name = tot_prev_yr.zone_name
AND   tot_cur_yr.month = tot_prev_yr.month
AND   tot_cur_yr.fisc_yr <> tot_prev_yr.fisc_yr
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
	cur_achvmnt_nr_tot::number(38,6) as cur_achvmnt_nr_tot,
	prev_achvmnt_nr_tot::number(38,6) as prev_achvmnt_nr_tot,
	tot_growth_percent::number(38,4) as tot_growth_percent
    from trans
)
select * from final