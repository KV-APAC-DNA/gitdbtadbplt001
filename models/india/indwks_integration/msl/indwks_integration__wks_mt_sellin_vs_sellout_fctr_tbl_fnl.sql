with wks_mt_sellin_vs_sellout_fctr_tbl as(
    select * from {{ ref('indwks_integration__wks_mt_sellin_vs_sellout_fctr_tbl') }}
),
union1 as(
    SELECT *
    FROM wks_mt_sellin_vs_sellout_fctr_tbl a
    WHERE mth_mm_cal >= mth_mm_fi
	AND (
		mth_mm_cal,
		account_name,
		rnk
		) IN (
		SELECT mth_mm_cal,
			account_name,
			MIN(rnk)
		FROM wks_mt_sellin_vs_sellout_fctr_tbl
		WHERE mth_mm_cal >= mth_mm_fi
		GROUP BY 1,
			2
        )
),
union2 as(
    SELECT *
FROM wks_mt_sellin_vs_sellout_fctr_tbl a
WHERE mth_mm_cal < (
		SELECT MIN(mth_mm_fi)
		FROM wks_mt_sellin_vs_sellout_fctr_tbl b
		WHERE a.account_name = b.account_name
		)
	AND (
		account_name,
		rnk
		) IN (
		SELECT account_name,
			MAX(rnk)
		FROM wks_mt_sellin_vs_sellout_fctr_tbl
		GROUP BY 1
		)
),
transformed as(
    select * from union1
    union all
    select * from union2
)
select * from transformed