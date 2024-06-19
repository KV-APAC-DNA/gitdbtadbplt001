with 
sss_zonal_nocb_achnr_tbl as 
(
    select * from {{ ref('indwks_integration__sss_zonal_nocb_achnr_tbl') }}
),
trans as 
(
    SELECT nocb_cur_yr.zone_name,
       nocb_cur_yr.mth_mm,
       nocb_cur_yr.fisc_yr as cur_yr,
       nocb_prev_yr.fisc_yr as prev_yr,
       nocb_cur_yr.month as cur_mnth,
       nocb_prev_yr.month as prev_mnth,
       nocb_cur_yr.nocb as cur_nocb,
       nocb_prev_yr.nocb as prev_nocb,
       (CAST((nocb_cur_yr.nocb - nocb_prev_yr.nocb) AS DECIMAL(10,2))/nocb_prev_yr.nocb)*100 AS nocb_growth_percent,
       nocb_cur_yr.achievement_nr as cur_achvmnt_nr,
       nocb_prev_yr.achievement_nr as prev_achvmnt_nr,
       ((nocb_cur_yr.achievement_nr - nocb_prev_yr.achievement_nr)/nocb_prev_yr.achievement_nr)*100 AS achievement_nr_growth_percent 
FROM (SELECT nocb_cur.zone_name,
             nocb_cur.mth_mm,
             nocb_cur.fisc_yr,
             nocb_cur.month,
             nocb_cur.nocb,
             nocb_cur.achievement_nr
      FROM sss_zonal_nocb_achnr_tbl nocb_cur,
           (SELECT EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '12 months')) AS strt_year,
                   EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '12 months')) AS start_mnth) AS inn
      WHERE nocb_cur.mth_mm >= CASE WHEN start_mnth < 10 THEN strt_year||0||start_mnth ELSE strt_year||start_mnth END
      ) nocb_cur_yr,
      (SELECT nocb_prev.zone_name,
              nocb_prev.mth_mm,
              nocb_prev.fisc_yr,
              nocb_prev.month,
              nocb_prev.nocb,
              nocb_prev.achievement_nr
      FROM sss_zonal_nocb_achnr_tbl nocb_prev,
           (SELECT EXTRACT(YEAR FROM (CURRENT_DATE-INTERVAL '12 months')) AS end_year,
                   EXTRACT(MONTH FROM (CURRENT_DATE-INTERVAL '12 months')) AS end_mnth,
                   EXTRACT(YEAR FROM ((CURRENT_DATE-INTERVAL '12 months')-INTERVAL '12 months')) AS strt_year,
                   EXTRACT(MONTH FROM ((CURRENT_DATE-INTERVAL '12 months')-INTERVAL '12 months')) AS start_mnth) AS inn
      WHERE nocb_prev.mth_mm >= CASE WHEN start_mnth < 10 THEN strt_year||0||start_mnth ELSE strt_year||start_mnth END
      AND   nocb_prev.mth_mm < CASE WHEN start_mnth < 10 THEN end_year||0||end_mnth ELSE end_year||end_mnth END
      ) nocb_prev_yr
WHERE nocb_cur_yr.zone_name = nocb_prev_yr.zone_name
AND   nocb_cur_yr.month = nocb_prev_yr.month
AND   nocb_cur_yr.fisc_yr <> nocb_prev_yr.fisc_yr
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
	achievement_nr_growth_percent::number(38,4) as achievement_nr_growth_percent
    from trans
)
select * from final