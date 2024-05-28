with itg_all_distributor_sellin_sales_fact as(
select * from DEV_DNA_CORE.IDNITG_INTEGRATION.ITG_ALL_DISTRIBUTOR_SELLIN_SALES_FACT
),
edw_time_dim as(
select * from DEV_DNA_CORE.IDNEDW_INTEGRATION.EDW_TIME_DIM
),
edw_billing_fact as(
select * from DEV_DNA_CORE.ASPEDW_Integration.EDW_BILLING_FACT
),
union1 as(
      SELECT to_date(t1.bill_dt) AS bill_dt
		,(upper(trim((t1.bill_doc)::TEXT)))::CHARACTER VARYING AS bill_doc
		,t2.jj_mnth_id
		,t1.jj_sap_dstrbtr_id
		,t1.jj_sap_prod_id
		,sum(t1.qty) AS sellin_qty
		,sum(t1.net_val) AS sellin_val
		,sum(t1.net_val) AS gross_sellin_val
	FROM itg_all_distributor_sellin_sales_fact t1
		,edw_time_dim t2
	WHERE (
			(to_date(t1.bill_dt) = to_date(t2.cal_date))
			AND (
				(upper((t1.bill_type_id)::TEXT) = ('F2'::CHARACTER VARYING)::TEXT)
				OR (upper((t1.bill_type_id)::TEXT) = ('RE'::CHARACTER VARYING)::TEXT)
				)
			)
	GROUP BY to_date(t1.bill_dt)
		,upper(trim((t1.bill_doc)::TEXT))
		,t2.jj_mnth_id
		,t1.jj_sap_dstrbtr_id
		,t1.jj_sap_prod_id
),
derived_table1 as(
	select * from union1 
    union all
	SELECT to_date((t1.
BILL_DT)::TIMESTAMP without TIME zone) 
		,(upper(trim(ltrim((t1.bill_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT))))::CHARACTER VARYING AS bill_doc
		,t2.jj_mnth_id
		,(
			CASE 
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120166'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120166' IS NULL)
							)
						)
					THEN ('115330'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120167'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120167' IS NULL)
							)
						)
					THEN ('115327'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120168'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120168' IS NULL)
							)
						)
					THEN ('115329'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120169'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120169' IS NULL)
							)
						)
					THEN ('115328'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120170'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120170' IS NULL)
							)
						)
					THEN ('116193'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120165'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120165' IS NULL)
							)
						)
					THEN ('116193'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120171'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120171' IS NULL)
							)
						)
					THEN ('116194'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120173'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120173' IS NULL)
							)
						)
					THEN ('116206'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120174'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120174' IS NULL)
							)
						)
					THEN ('116207'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('120172'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('120172' IS NULL)
							)
						)
					THEN ('116195'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('123537'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('123537' IS NULL)
							)
						)
					THEN ('119756'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('123685'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('123685' IS NULL)
							)
						)
					THEN ('123877'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('123686'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('123686' IS NULL)
							)
						)
					THEN ('123878'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('123687'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('123687' IS NULL)
							)
						)
					THEN ('123879'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('123688'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('123688' IS NULL)
							)
						)
					THEN ('123880'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('126000'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('126000' IS NULL)
							)
						)
					THEN ('125881'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('126001'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('126001' IS NULL)
							)
						)
					THEN ('125821'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('126003'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('126003' IS NULL)
							)
						)
					THEN ('125822'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('126002'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('126002' IS NULL)
							)
						)
					THEN ('125823'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('131389'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('131389' IS NULL)
							)
						)
					THEN ('119756'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('121413'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('121413' IS NULL)
							)
						)
					THEN ('122155'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130200'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130200' IS NULL)
							)
						)
					THEN ('130122'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130196'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130196' IS NULL)
							)
						)
					THEN ('130118'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130197'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130197' IS NULL)
							)
						)
					THEN ('125687'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130199'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130199' IS NULL)
							)
						)
					THEN ('129735'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130198'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130198' IS NULL)
							)
						)
					THEN ('125686'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130961'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130961' IS NULL)
							)
						)
					THEN ('130962'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('130774'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('130774' IS NULL)
							)
						)
					THEN ('123877'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('131653'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('131653' IS NULL)
							)
						)
					THEN ('109676'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('133685'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('133685' IS NULL)
							)
						)
					THEN ('133699'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('131587'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('131587' IS NULL)
							)
						)
					THEN ('131588'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('134432'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('134432' IS NULL)
							)
						)
					THEN ('134433'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('134679'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('134679' IS NULL)
							)
						)
					THEN ('134433'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('131595'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('131595' IS NULL)
							)
						)
					THEN ('131594'::CHARACTER VARYING)::TEXT
				WHEN (
						(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) = ('135355'::CHARACTER VARYING)::TEXT)
						OR (
							(ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) IS NULL)
							AND ('135355' IS NULL)
							)
						)
					THEN ('135362'::CHARACTER VARYING)::TEXT
				ELSE ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT)
				END
			)::CHARACTER VARYING AS jj_sap_dstrbtr_id
		,(ltrim((t1.material)::TEXT, ((0)::CHARACTER VARYING)::TEXT))::CHARACTER VARYING AS jj_sap_prod_id
		,sum(t1.bill_qty) AS sellin_qty
		,sum(t1.rebate_bas) AS sellin_val
		,sum(t1.subtotal_1) AS gross_sellin_val
	FROM EDW_BILLING_FACT t1, EDW_TIME_DIM t2
	WHERE (
			(
				(
					(
						(to_date((t1.bill_dt)::TIMESTAMP without TIME zone) = to_date(t2.cal_date))
						AND (upper((t1.loc_currcy)::TEXT) = ('IDR'::CHARACTER VARYING)::TEXT)
						)
					AND (
						(
							(
								(
									(
										(upper((t1.bill_type)::TEXT) = ('ZF2I'::CHARACTER VARYING)::TEXT)
										OR (upper((t1.bill_type)::TEXT) = ('ZL2I'::CHARACTER VARYING)::TEXT)
										)
									OR (upper((t1.bill_type)::TEXT) = ('ZC2I'::CHARACTER VARYING)::TEXT)
									)
								OR (upper((t1.bill_type)::TEXT) = ('ZG2I'::CHARACTER VARYING)::TEXT)
								)
							OR (upper((t1.bill_type)::TEXT) = ('S1'::CHARACTER VARYING)::TEXT)
							)
						OR (upper((t1.bill_type)::TEXT) = ('S2'::CHARACTER VARYING)::TEXT)
						)
					)
				AND (upper((t1.material)::TEXT) <> ('REBATE'::CHARACTER VARYING)::TEXT)
				)
			AND (ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT) <> ('590092'::CHARACTER VARYING)::TEXT)
			)
	GROUP BY to_date((t1.bill_dt)::TIMESTAMP without TIME zone)
		,upper(trim(ltrim((t1.bill_num)::TEXT, ((0)::CHARACTER VARYING)::TEXT)))
		,t2.jj_mnth_id
		,ltrim((t1.sold_to)::TEXT, ((0)::CHARACTER VARYING)::TEXT)
		,ltrim((t1.material)::TEXT, ((0)::CHARACTER VARYING)::TEXT)
),
transformed as(
SELECT ((((derived_table1.jj_mnth_id)::CHARACTER VARYING)::TEXT || (derived_table1.jj_sap_dstrbtr_id)::TEXT) || (derived_table1.jj_sap_prod_id)::TEXT) AS rec_key
	,((derived_table1.jj_sap_dstrbtr_id)::TEXT || (derived_table1.jj_sap_prod_id)::TEXT) AS cust_prod_cd
	,derived_table1.bill_dt
	,derived_table1.bill_doc
	,derived_table1.jj_mnth_id
	,derived_table1.jj_sap_dstrbtr_id
	,derived_table1.jj_sap_prod_id
	,sum(derived_table1.sellin_qty) AS sellin_qty
	,sum(derived_table1.sellin_val) AS sellin_val
	,sum(derived_table1.gross_sellin_val) AS gross_sellin_val
FROM derived_table1
GROUP BY ((((derived_table1.jj_mnth_id)::CHARACTER VARYING)::TEXT || (derived_table1.jj_sap_dstrbtr_id)::TEXT) || (derived_table1.jj_sap_prod_id)::TEXT)
	,((derived_table1.jj_sap_dstrbtr_id)::TEXT || (derived_table1.jj_sap_prod_id)::TEXT)
	,derived_table1.bill_dt
	,derived_table1.bill_doc
	,derived_table1.jj_mnth_id
	,derived_table1.jj_sap_dstrbtr_id
	,derived_table1.jj_sap_prod_id
)
select * from transformed