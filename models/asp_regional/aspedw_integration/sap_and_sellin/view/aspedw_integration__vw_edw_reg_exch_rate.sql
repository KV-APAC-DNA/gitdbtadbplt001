with edw_vw_os_time_dim as (
    select * from {{ ref('sgpedw_integration__edw_vw_os_time_dim') }}
),
edw_crncy_exch as (
    select * from {{ ref('aspedw_integration__edw_crncy_exch') }}
),
final as(
SELECT currency.cntry_key
	,currency.cntry_nm
	,currency.rate_type
	,currency.from_ccy
	,currency.to_ccy
	,currency.valid_date
	,currency.jj_year
	,currency.jj_mnth_id
	,(currency.exch_rate)::NUMERIC(15, 5) AS exch_rate
	,currency.to_ratio
	,currency.from_ratio
FROM (
	SELECT DISTINCT t1.cntry_key
		,t1.cntry_nm
		,t1.rate_type
		,t1.from_ccy
		,t1.to_ccy
		,((t2.cal_date_id)::NUMERIC(18, 0))::NUMERIC(9, 0) AS valid_date
		,t1."year" AS jj_year
		,(t1.mnth_id)::CHARACTER VARYING AS jj_mnth_id
		,t1.exch_rate
		,t1.to_ratio
		,t1.from_ratio
	FROM (
		SELECT CASE 
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('JPY' IS NULL)
							)
						)
					THEN 'JP'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('THB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('THB' IS NULL)
							)
						)
					THEN 'TH'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('IDR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('IDR' IS NULL)
							)
						)
					THEN 'ID'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('PHP'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('PHP' IS NULL)
							)
						)
					THEN 'PH'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('MYR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('MYR' IS NULL)
							)
						)
					THEN 'MY'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('INR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('INR' IS NULL)
							)
						)
					THEN 'IN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('KRW' IS NULL)
							)
						)
					THEN 'KR'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('HKD' IS NULL)
							)
						)
					THEN 'HK'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('TWD' IS NULL)
							)
						)
					THEN 'TW'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('AUD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('AUD' IS NULL)
							)
						)
					THEN 'AU'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('RMB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('RMB' IS NULL)
							)
						)
					THEN 'CN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('VND'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('VND' IS NULL)
							)
						)
					THEN 'VN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('SGD' IS NULL)
							)
						)
					THEN 'SG'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('NZD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('NZD' IS NULL)
							)
						)
					THEN 'NZ'::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END AS cntry_key
			,CASE 
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('JPY' IS NULL)
							)
						)
					THEN 'JAPAN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('THB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('THB' IS NULL)
							)
						)
					THEN 'THAILAND'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('IDR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('IDR' IS NULL)
							)
						)
					THEN 'INDONESIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('PHP'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('PHP' IS NULL)
							)
						)
					THEN 'PHILIPPINES'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('MYR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('MYR' IS NULL)
							)
						)
					THEN 'MALAYSIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('INR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('INR' IS NULL)
							)
						)
					THEN 'INDIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('KRW' IS NULL)
							)
						)
					THEN 'Korea'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('HKD' IS NULL)
							)
						)
					THEN 'Hong Kong'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('TWD' IS NULL)
							)
						)
					THEN 'TAIWAN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('AUD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('AUD' IS NULL)
							)
						)
					THEN 'Australia'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('RMB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('RMB' IS NULL)
							)
						)
					THEN 'CHINA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('VND'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('VND' IS NULL)
							)
						)
					THEN 'VIETNAM'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('SGD' IS NULL)
							)
						)
					THEN 'SINGAPORE'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('NZD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('NZD' IS NULL)
							)
						)
					THEN 'NEW ZEALAND'::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END AS cntry_nm
			,COALESCE(ce.rate_type, eotd.rate_type) AS rate_type
			,COALESCE(ce.from_ccy, eotd.from_ccy) AS from_ccy
			,COALESCE(ce.to_ccy, eotd.to_ccy) AS to_ccy
			,COALESCE(ce."year", eotd."year") AS "year"
			,COALESCE(ce.mnth_id, eotd.mnth_id) AS mnth_id
			,COALESCE(ce.exch_rate, eotd.max_mnth_exch_rate) AS exch_rate
			,COALESCE(ce.to_ratio, eotd.to_ratio) AS to_ratio
			,COALESCE(ce.from_ratio, eotd.from_ratio) AS from_ratio
		FROM (
			(
				SELECT t1."year"
					,t1.mnth_id
					,t2.rate_type
					,t2.from_ccy
					,t2.to_ccy
					,t2.mnth_id AS max_mnth_id
					,t2.exch_rate AS max_mnth_exch_rate
					,t2.to_ratio
					,t2.from_ratio
				FROM (
					SELECT DISTINCT edw_vw_os_time_dim."year"
						,edw_vw_os_time_dim.mnth_id
					FROM edw_vw_os_time_dim
					) t1
					,(
						SELECT max_currency_exchng.rate_type
							,max_currency_exchng.from_ccy
							,max_currency_exchng.to_ccy
							,max_currency_exchng.mnth_id
							,max_currency_exchng.exch_rate
							,max_currency_exchng.to_ratio
							,max_currency_exchng.from_ratio
						FROM (
							SELECT curr_exch.rate_type
								,curr_exch.from_ccy
								,curr_exch.to_ccy
								,curr_exch.mnth_id
								,curr_exch.exch_rate
								,curr_exch.to_ratio
								,curr_exch.from_ratio
							FROM (
								SELECT curr_exch.ex_rt_typ AS rate_type
									,curr_exch.from_crncy AS from_ccy
									,curr_exch.to_crncy AS to_ccy
									,eotd2.mnth_id
									,eotd."year"
									,curr_exch.valid_date
									,curr_exch.ex_rt AS exch_rate
									,curr_exch.to_ratio
									,curr_exch.from_ratio
								FROM (
									SELECT edw_crncy_exch.ex_rt_typ
										,edw_crncy_exch.from_crncy
										,edw_crncy_exch.to_crncy
										,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
										,edw_crncy_exch.ex_rt
										,edw_crncy_exch.to_ratio
										,edw_crncy_exch.from_ratio
									FROM edw_crncy_exch
									WHERE ((
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
									,(
										SELECT DISTINCT edw_vw_os_time_dim."year"
											,edw_vw_os_time_dim.mnth_id
											,edw_vw_os_time_dim.cal_date_id
										FROM edw_vw_os_time_dim
										) eotd
									,(
										SELECT DISTINCT edw_vw_os_time_dim."year"
											,edw_vw_os_time_dim.mnth_id
										FROM edw_vw_os_time_dim
										) eotd2
								WHERE (
										(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
										AND (eotd."year" = eotd2."year")
										)
								) curr_exch
								,(
									SELECT currency_exchng.rate_type
										,currency_exchng.from_ccy
										,currency_exchng.to_ccy
										,currency_exchng.to_ratio
										,currency_exchng.from_ratio
										,"max" (currency_exchng.mnth_id) AS mnth_id
									FROM (
										SELECT curr_exch.ex_rt_typ AS rate_type
											,curr_exch.from_crncy AS from_ccy
											,curr_exch.to_crncy AS to_ccy
											,eotd2.mnth_id
											,eotd."year"
											,curr_exch.valid_date
											,curr_exch.ex_rt AS exch_rate
											,curr_exch.to_ratio
											,curr_exch.from_ratio
										FROM (
											SELECT edw_crncy_exch.ex_rt_typ
												,edw_crncy_exch.from_crncy
												,edw_crncy_exch.to_crncy
												,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
												,edw_crncy_exch.ex_rt
												,edw_crncy_exch.to_ratio
												,edw_crncy_exch.from_ratio
											FROM edw_crncy_exch
											WHERE (
													(
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
											,(
												SELECT DISTINCT edw_vw_os_time_dim."year"
													,edw_vw_os_time_dim.mnth_id
													,edw_vw_os_time_dim.cal_date_id
												FROM edw_vw_os_time_dim
												) eotd
											,(
												SELECT DISTINCT edw_vw_os_time_dim."year"
													,edw_vw_os_time_dim.mnth_id
												FROM edw_vw_os_time_dim
												) eotd2
										WHERE (
												(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
												AND (eotd."year" = eotd2."year")
												)
										) currency_exchng
									GROUP BY currency_exchng.rate_type
										,currency_exchng.from_ccy
										,currency_exchng.to_ccy
										,currency_exchng.to_ratio
										,currency_exchng.from_ratio
									) max_mnth
							WHERE (
									(
										(
											((curr_exch.rate_type)::TEXT = (max_mnth.rate_type)::TEXT)
											AND ((curr_exch.from_ccy)::TEXT = (max_mnth.from_ccy)::TEXT)
											)
										AND ((curr_exch.to_ccy)::TEXT = (max_mnth.to_ccy)::TEXT)
										)
									AND (curr_exch.mnth_id = max_mnth.mnth_id)
									)
							) max_currency_exchng
						) t2
				) eotd LEFT JOIN (
				SELECT currency_exchng.rate_type
					,currency_exchng.from_ccy
					,currency_exchng.to_ccy
					,currency_exchng."year"
					,currency_exchng.mnth_id
					,currency_exchng.valid_date
					,currency_exchng.exch_rate
					,currency_exchng.to_ratio
					,currency_exchng.from_ratio
				FROM (
					SELECT curr_exch.ex_rt_typ AS rate_type
						,curr_exch.from_crncy AS from_ccy
						,curr_exch.to_crncy AS to_ccy
						,eotd2.mnth_id
						,eotd."year"
						,curr_exch.valid_date
						,curr_exch.ex_rt AS exch_rate
						,curr_exch.to_ratio
						,curr_exch.from_ratio
					FROM (
						SELECT edw_crncy_exch.ex_rt_typ
							,edw_crncy_exch.from_crncy
							,edw_crncy_exch.to_crncy
							,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
							,edw_crncy_exch.ex_rt
							,edw_crncy_exch.to_ratio
							,edw_crncy_exch.from_ratio
						FROM edw_crncy_exch
						WHERE (
								(
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
						,(
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
								,edw_vw_os_time_dim.cal_date_id
							FROM edw_vw_os_time_dim
							) eotd
						,(
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
							FROM edw_vw_os_time_dim
							) eotd2
					WHERE (
							(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
							AND (eotd."year" = eotd2."year")
							)
					) currency_exchng
				) ce ON ((ce.mnth_id = eotd.mnth_id))
			)
		) t1
		,(
			SELECT DISTINCT edw_vw_os_time_dim.mnth_id
				,min(edw_vw_os_time_dim.cal_date_id) OVER (
					PARTITION BY edw_vw_os_time_dim."year" order by null ROWS BETWEEN UNBOUNDED PRECEDING
						AND UNBOUNDED FOLLOWING
					) AS cal_date_id
			FROM edw_vw_os_time_dim
			) t2
	WHERE (t1.mnth_id = t2.mnth_id)
	
	UNION ALL
	
	SELECT DISTINCT t1.cntry_key
		,t1.cntry_nm
		,t1.rate_type
		,t1.from_ccy
		,t1.from_ccy AS to_ccy
		,((t2.cal_date_id)::NUMERIC(18, 0))::NUMERIC(9, 0) AS valid_date
		,t1."year" AS jj_year
		,(t1.mnth_id)::CHARACTER VARYING AS jj_mnth_id
		,1 AS exch_rate
		,t1.to_ratio
		,t1.from_ratio
	FROM (
		SELECT CASE 
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('JPY' IS NULL)
							)
						)
					THEN 'JP'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('THB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('THB' IS NULL)
							)
						)
					THEN 'TH'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('IDR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('IDR' IS NULL)
							)
						)
					THEN 'ID'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('PHP'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('PHP' IS NULL)
							)
						)
					THEN 'PH'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('MYR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('MYR' IS NULL)
							)
						)
					THEN 'MY'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('INR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('INR' IS NULL)
							)
						)
					THEN 'IN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('KRW' IS NULL)
							)
						)
					THEN 'KR'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('HKD' IS NULL)
							)
						)
					THEN 'HK'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('TWD' IS NULL)
							)
						)
					THEN 'TW'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('AUD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('AUD' IS NULL)
							)
						)
					THEN 'AU'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('RMB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('RMB' IS NULL)
							)
						)
					THEN 'CN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('VND'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('VND' IS NULL)
							)
						)
					THEN 'VN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('SGD' IS NULL)
							)
						)
					THEN 'SG'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('NZD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('NZD' IS NULL)
							)
						)
					THEN 'NZ'::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END AS cntry_key
			,CASE 
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('JPY'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('JPY' IS NULL)
							)
						)
					THEN 'JAPAN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('THB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('THB' IS NULL)
							)
						)
					THEN 'THAILAND'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('IDR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('IDR' IS NULL)
							)
						)
					THEN 'INDONESIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('PHP'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('PHP' IS NULL)
							)
						)
					THEN 'PHILIPPINES'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('MYR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('MYR' IS NULL)
							)
						)
					THEN 'MALAYSIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('INR'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('INR' IS NULL)
							)
						)
					THEN 'INDIA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('KRW'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('KRW' IS NULL)
							)
						)
					THEN 'Korea'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('HKD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('HKD' IS NULL)
							)
						)
					THEN 'Hong Kong'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('TWD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('TWD' IS NULL)
							)
						)
					THEN 'TAIWAN'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('AUD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('AUD' IS NULL)
							)
						)
					THEN 'Australia'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('RMB'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('RMB' IS NULL)
							)
						)
					THEN 'CHINA'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('VND'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('VND' IS NULL)
							)
						)
					THEN 'VIETNAM'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('SGD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('SGD' IS NULL)
							)
						)
					THEN 'SINGAPORE'::CHARACTER VARYING
				WHEN (
						((COALESCE(ce.from_ccy, eotd.from_ccy))::TEXT = ('NZD'::CHARACTER VARYING)::TEXT)
						OR (
							(COALESCE(ce.from_ccy, eotd.from_ccy) IS NULL)
							AND ('NZD' IS NULL)
							)
						)
					THEN 'NEW ZEALAND'::CHARACTER VARYING
				ELSE NULL::CHARACTER VARYING
				END AS cntry_nm
			,COALESCE(ce.rate_type, eotd.rate_type) AS rate_type
			,COALESCE(ce.from_ccy, eotd.from_ccy) AS from_ccy
			,COALESCE(ce.to_ccy, eotd.to_ccy) AS to_ccy
			,COALESCE(ce."year", eotd."year") AS "year"
			,COALESCE(ce.mnth_id, eotd.mnth_id) AS mnth_id
			,COALESCE(ce.exch_rate, eotd.max_mnth_exch_rate) AS exch_rate
			,COALESCE(ce.to_ratio, eotd.to_ratio) AS to_ratio
			,COALESCE(ce.from_ratio, eotd.from_ratio) AS from_ratio
		FROM (
			(
				SELECT t1."year"
					,t1.mnth_id
					,t2.rate_type
					,t2.from_ccy
					,t2.to_ccy
					,t2.mnth_id AS max_mnth_id
					,t2.exch_rate AS max_mnth_exch_rate
					,t2.to_ratio
					,t2.from_ratio
				FROM (
					SELECT DISTINCT edw_vw_os_time_dim."year"
						,edw_vw_os_time_dim.mnth_id
					FROM edw_vw_os_time_dim
					) t1
					,(
						SELECT max_currency_exchng.rate_type
							,max_currency_exchng.from_ccy
							,max_currency_exchng.to_ccy
							,max_currency_exchng.mnth_id
							,max_currency_exchng.exch_rate
							,max_currency_exchng.to_ratio
							,max_currency_exchng.from_ratio
						FROM (
							SELECT curr_exch.rate_type
								,curr_exch.from_ccy
								,curr_exch.to_ccy
								,curr_exch.mnth_id
								,curr_exch.exch_rate
								,curr_exch.to_ratio
								,curr_exch.from_ratio
							FROM (
								SELECT curr_exch.ex_rt_typ AS rate_type
									,curr_exch.from_crncy AS from_ccy
									,curr_exch.to_crncy AS to_ccy
									,eotd2.mnth_id
									,eotd."year"
									,curr_exch.valid_date
									,curr_exch.ex_rt AS exch_rate
									,curr_exch.to_ratio
									,curr_exch.from_ratio
								FROM (
									SELECT edw_crncy_exch.ex_rt_typ
										,edw_crncy_exch.from_crncy
										,edw_crncy_exch.to_crncy
										,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
										,edw_crncy_exch.ex_rt
										,edw_crncy_exch.to_ratio
										,edw_crncy_exch.from_ratio
									FROM edw_crncy_exch
									WHERE (
											(
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
									,(
										SELECT DISTINCT edw_vw_os_time_dim."year"
											,edw_vw_os_time_dim.mnth_id
											,edw_vw_os_time_dim.cal_date_id
										FROM edw_vw_os_time_dim
										) eotd
									,(
										SELECT DISTINCT edw_vw_os_time_dim."year"
											,edw_vw_os_time_dim.mnth_id
										FROM edw_vw_os_time_dim
										) eotd2
								WHERE (
										(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
										AND (eotd."year" = eotd2."year")
										)
								) curr_exch
								,(
									SELECT currency_exchng.rate_type
										,currency_exchng.from_ccy
										,currency_exchng.to_ccy
										,currency_exchng.to_ratio
										,currency_exchng.from_ratio
										,"max" (currency_exchng.mnth_id) AS mnth_id
									FROM (
										SELECT curr_exch.ex_rt_typ AS rate_type
											,curr_exch.from_crncy AS from_ccy
											,curr_exch.to_crncy AS to_ccy
											,eotd2.mnth_id
											,eotd."year"
											,curr_exch.valid_date
											,curr_exch.ex_rt AS exch_rate
											,curr_exch.to_ratio
											,curr_exch.from_ratio
										FROM (
											SELECT edw_crncy_exch.ex_rt_typ
												,edw_crncy_exch.from_crncy
												,edw_crncy_exch.to_crncy
												,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
												,edw_crncy_exch.ex_rt
												,edw_crncy_exch.to_ratio
												,edw_crncy_exch.from_ratio
											FROM edw_crncy_exch
											WHERE (
													(
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
											,(
												SELECT DISTINCT edw_vw_os_time_dim."year"
													,edw_vw_os_time_dim.mnth_id
													,edw_vw_os_time_dim.cal_date_id
												FROM edw_vw_os_time_dim
												) eotd
											,(
												SELECT DISTINCT edw_vw_os_time_dim."year"
													,edw_vw_os_time_dim.mnth_id
												FROM edw_vw_os_time_dim
												) eotd2
										WHERE (
												(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
												AND (eotd."year" = eotd2."year")
												)
										) currency_exchng
									GROUP BY currency_exchng.rate_type
										,currency_exchng.from_ccy
										,currency_exchng.to_ccy
										,currency_exchng.to_ratio
										,currency_exchng.from_ratio
									) max_mnth
							WHERE (
									(
										(
											((curr_exch.rate_type)::TEXT = (max_mnth.rate_type)::TEXT)
											AND ((curr_exch.from_ccy)::TEXT = (max_mnth.from_ccy)::TEXT)
											)
										AND ((curr_exch.to_ccy)::TEXT = (max_mnth.to_ccy)::TEXT)
										)
									AND (curr_exch.mnth_id = max_mnth.mnth_id)
									)
							) max_currency_exchng
						) t2
				) eotd LEFT JOIN (
				SELECT currency_exchng.rate_type
					,currency_exchng.from_ccy
					,currency_exchng.to_ccy
					,currency_exchng."year"
					,currency_exchng.mnth_id
					,currency_exchng.valid_date
					,currency_exchng.exch_rate
					,currency_exchng.to_ratio
					,currency_exchng.from_ratio
				FROM (
					SELECT curr_exch.ex_rt_typ AS rate_type
						,curr_exch.from_crncy AS from_ccy
						,curr_exch.to_crncy AS to_ccy
						,eotd2.mnth_id
						,eotd."year"
						,curr_exch.valid_date
						,curr_exch.ex_rt AS exch_rate
						,curr_exch.to_ratio
						,curr_exch.from_ratio
					FROM (
						SELECT edw_crncy_exch.ex_rt_typ
							,edw_crncy_exch.from_crncy
							,edw_crncy_exch.to_crncy
							,(((99999999)::NUMERIC)::NUMERIC(18, 0) - (edw_crncy_exch.vld_from)::NUMERIC(18, 0)) AS valid_date
							,edw_crncy_exch.ex_rt
							,edw_crncy_exch.to_ratio
							,edw_crncy_exch.from_ratio
						FROM edw_crncy_exch
						WHERE (
								(
        (edw_crncy_exch.from_crncy)::TEXT IN ('JPY', 'THB', 'IDR', 'PHP', 'MYR', 'INR', 'KRW', 'HKD', 'TWD', 'AUD', 'RMB', 'VND', 'SGD', 'NZD')
        AND (edw_crncy_exch.to_crncy)::TEXT = 'USD'
    )
    AND (edw_crncy_exch.ex_rt_typ)::TEXT = 'BWAR'
								)
						) curr_exch
						,(
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
								,edw_vw_os_time_dim.cal_date_id
							FROM edw_vw_os_time_dim
							) eotd
						,(
							SELECT DISTINCT edw_vw_os_time_dim."year"
								,edw_vw_os_time_dim.mnth_id
							FROM edw_vw_os_time_dim
							) eotd2
					WHERE (
							(((curr_exch.valid_date)::CHARACTER VARYING)::TEXT = eotd.cal_date_id)
							AND (eotd."year" = eotd2."year")
							)
					) currency_exchng
				) ce ON ((ce.mnth_id = eotd.mnth_id))
			)
		) t1
		,(
			SELECT DISTINCT edw_vw_os_time_dim.mnth_id
				,min(edw_vw_os_time_dim.cal_date_id) OVER (
					PARTITION BY edw_vw_os_time_dim."year" order by null ROWS BETWEEN UNBOUNDED PRECEDING
						AND UNBOUNDED FOLLOWING
					) AS cal_date_id
			FROM edw_vw_os_time_dim
			) t2
	WHERE (t1.mnth_id = t2.mnth_id)
	) currency
WHERE (
		(currency.jj_year)::DOUBLE PRECISION >= (DATE_PART('year', CURRENT_TIMESTAMP) - 3)
    AND (currency.to_ccy)::TEXT = 'USD'
		)
)

select * from final

