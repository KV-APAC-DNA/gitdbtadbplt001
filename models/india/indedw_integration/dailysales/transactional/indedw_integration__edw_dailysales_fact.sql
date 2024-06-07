with itg_dailysales as(
	select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_DAILYSALES
),
itg_salesmanmaster as(
	select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_SALESMANMASTER
),
itg_dailysales_undelivered as(
	select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_DAILYSALES_UNDELIVERED
),
itg_salesreturn as(
	select * from DEV_DNA_CORE.SNAPINDITG_INTEGRATION.ITG_SALESRETURN
),
union1 as (
	SELECT SRC.DistCode,
	SRC.SalInvNo,
	SRC.SalInvDate,
	SRC.RtrCode,
	SRC.RtrName,
	SRC.PrdCode,
	SRC.SalesmanCode,
	SRC.SalesmanName,
	SRC.SalesRouteCode,
	SRC.SalesRouteName,
	SRC.PrdQty,
	SRC.PrdGrossAmt,
	SRC.PrdTaxAmt,
	SRC.PrdNetAmt,
	SRC.NRValue,
	SRC.Achievement_NR_Val,
	SRC.SRNRefType,
	SRC.SalInvMode,
	SRC.SalInvType,
	NULL AS SKU_Rec_Qty,
	NULL AS SKU_Rec_Amt,
	NULL AS SUM_QPS_QTY,
	NULL AS QPS_Amt,
	SRC.Achievement_Amt,
	SRC.prdschdiscamt,
	SRC.No_of_Lines,
	NULL AS No_of_Reco_SKU_Lines,
	SRC.createddate,
	cast('D' AS VARCHAR(1)) AS SALEFLAG,
	SRC.CRT_DTTM,
	SRC.UPDT_DTTM,
	CASE 
		WHEN SRC.STATUS::TEXT = 'Inactive'::CHARACTER VARYING::TEXT
			THEN 'InActive'::CHARACTER VARYING
		ELSE SRC.STATUS
		END AS STATUS,
	SRC.PrdFreeQty FROM (
	SELECT DS.DistCode,
		DS.SalInvNo,
		CAST(SUBSTRING(DS.SalInvDate, 1, 4) || SUBSTRING(DS.SalInvDate, 6, 2) || SUBSTRING(DS.SalInvDate, 9, 2) AS INTEGER) AS SalInvDate,
		DS.RtrCode,
		DS.RtrName,
		DS.PrdCode,
		DS.SalesmanCode,
		DS.SalesmanName,
		DS.SalesRouteCode,
		DS.SalesRouteName,
		Sum(DS.PrdQty) AS PrdQty,
		Sum(DS.PrdFreeQty) AS PrdFreeQty,
		Sum(DS.PrdGrossAmt) AS PrdGrossAmt,
		Sum(DS.PrdTaxAmt) AS PrdTaxAmt,
		Sum(DS.PrdNetAmt) AS PrdNetAmt,
		Sum(DS.NRValue) AS NRValue,
		Sum(DS.NRValue * DS.PrdQty) AS Achievement_NR_Val,
		'NA' AS SRNRefType,
		DS.SalInvMode,
		DS.SalInvType,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN SUM(DS.prdqty) ELSE NULL END SKU_Rec_Qty,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN Sum(DS.prdgrossamt + DS.prdtaxamt) ELSE NULL END SKU_Rec_Amt,
		--Sum(ordb.schincrqty/Dup.dup_count) as SUM_QPS_QTY,
		Sum(DS.prdgrossamt + DS.prdtaxamt) AS Achievement_Amt,
		Sum(DS.prdschdiscamt) AS prdschdiscamt,
		Count(1) AS No_of_Lines,
		--SUM(CASE WHEN ordb.RecommendedSKU='Yes' then 1 ELSE 0 END) No_of_Reco_SKU_Lines, 
		max(DS.createddate) AS createddate,
		max(DS.crt_dttm) AS crt_dttm,
		max(DS.updt_dttm) AS updt_dttm,
		salesmanmstr.STATUS
	FROM itg_dailysales DS
	LEFT OUTER JOIN (
		SELECT *
		FROM (
			SELECT distcode,
				smcode,
				rmcode,
				createddate,
				initcap(STATUS) AS STATUS,
				row_number() OVER (
					PARTITION BY distcode,
					smcode,
					rmcode ORDER BY createddate DESC
					) AS rnk
			FROM itg_salesmanmaster
			)
		WHERE rnk = 1
		) salesmanmstr ON DS.distcode = salesmanmstr.distcode
		AND DS.salesmancode = salesmanmstr.smcode
		AND DS.salesroutecode = salesmanmstr.rmcode
	GROUP BY DS.DistCode,
		DS.SalInvNo,
		DS.SalInvDate,
		DS.RtrCode,
		DS.RtrName,
		DS.PrdCode,
		DS.SalesmanCode,
		DS.SalesmanName,
		DS.SalesRouteCode,
		DS.SalesRouteName,
		DS.SalInvMode,
		DS.SalInvType,
		salesmanmstr.STATUS
	) SRC
),
union2 as(
	SELECT A.DistCode,
	A.SalInvNo,
	A.SalInvDate,
	A.RtrCode,
	A.RtrName,
	A.PrdCode,
	A.SalesmanCode,
	A.SalesmanName,
	A.SalesRouteCode,
	A.SalesRouteName,
	A.PrdQty,
	A.PrdGrossAmt,
	A.PrdTaxAmt,
	A.PrdNetAmt,
	A.NRValue,
	A.Achievement_NR_Val,
	A.SRNRefType,
	A.SalInvMode,
	A.SalInvType,
	NULL AS SKU_Rec_Qty,
	NULL AS SKU_Rec_Amt,
	NULL AS SUM_QPS_QTY,
	NULL AS QPS_Amt,
	A.Achievement_Amt,
	A.prdschdiscamt,
	A.No_of_Lines,
	NULL AS No_of_Reco_SKU_Lines,
	A.createddate,
	A.SALEFLAG,
	A.CRT_DTTM,
	A.UPDT_DTTM,
	CASE 
		WHEN A.STATUS::TEXT = 'Inactive'::CHARACTER VARYING::TEXT
			THEN 'InActive'::CHARACTER VARYING
		ELSE A.STATUS
		END AS STATUS,
	A.PrdFreeQty FROM (
	SELECT DS_U.DistCode,
		DS_U.SalInvNo,
		CAST(SUBSTRING(DS_U.SalInvDate, 1, 4) || SUBSTRING(DS_U.SalInvDate, 6, 2) || SUBSTRING(DS_U.SalInvDate, 9, 2) AS INTEGER) AS SalInvDate,
		DS_U.RtrCode,
		DS_U.RtrName,
		DS_U.PrdCode,
		DS_U.SalesmanCode,
		DS_U.SalesmanName,
		DS_U.SalesRouteCode,
		DS_U.SalesRouteName,
		Sum(DS_U.PrdQty) AS PrdQty,
		Sum(DS_U.PrdFreeQty) AS PrdFreeQty,
		Sum(DS_U.PrdGrossAmt) AS PrdGrossAmt,
		Sum(DS_U.PrdTaxAmt) AS PrdTaxAmt,
		Sum(DS_U.PrdNetAmt) AS PrdNetAmt,
		Sum(DS_U.NRValue) AS NRValue,
		Sum(DS_U.NRValue * DS_U.PrdQty) AS Achievement_NR_Val,
		'NA' AS SRNRefType,
		DS_U.SalInvMode,
		DS_U.SalInvType,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN SUM(DS_U.prdqty) ELSE NULL END SKU_Rec_Qty,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN Sum(DS_U.prdgrossamt + DS_U.prdtaxamt) ELSE NULL END SKU_Rec_Amt,
		--Sum(ordb.schincrqty/Dup.dup_count) as SUM_QPS_QTY,
		Sum(DS_U.prdgrossamt + DS_U.prdtaxamt) AS Achievement_Amt,
		Sum(DS_U.prdschdiscamt) AS prdschdiscamt,
		Count(1) AS No_of_Lines,
		--SUM(CASE WHEN ordb.RecommendedSKU='Yes' then 1 ELSE 0 END) No_of_Reco_SKU_Lines, 
		max(DS_U.createddate) AS createddate,
		cast('U' AS VARCHAR(1)) AS SALEFLAG,
		max(DS_U.CRT_DTTM) AS CRT_DTTM,
		getdate() AS UPDT_DTTM,
		salesmanmstr.STATUS
	FROM itg_dailysales_undelivered DS_U
	LEFT OUTER JOIN (
		SELECT *
		FROM (
			SELECT distcode,
				smcode,
				rmcode,
				createddate,
				initcap(STATUS) AS STATUS,
				row_number() OVER (
					PARTITION BY distcode,
					smcode,
					rmcode ORDER BY createddate DESC
					) AS rnk
			FROM itg_salesmanmaster
			)
		WHERE rnk = 1
		) salesmanmstr ON DS_U.distcode = salesmanmstr.distcode
		AND DS_U.salesmancode = salesmanmstr.smcode
		AND DS_U.salesroutecode = salesmanmstr.rmcode
	WHERE DS_U.del_ind = 'N'
	GROUP BY DS_U.DistCode,
		DS_U.SalInvNo,
		DS_U.SalInvDate,
		DS_U.RtrCode,
		DS_U.RtrName,
		DS_U.PrdCode,
		DS_U.SalesmanCode,
		DS_U.SalesmanName,
		DS_U.SalesRouteCode,
		DS_U.SalesRouteName,
		DS_U.SalInvMode,
		DS_U.SalInvType,
		salesmanmstr.STATUS
	) A
),
union3 as (
	SELECT SRC.DistCode,
	SRC.PrdSalInvNo,
	SRC.SRNDate,
	SRC.RtrCode,
	SRC.RtrName,
	SRC.PrdCode,
	SRC.SalesmanCode,
	SRC.SalesmanName,
	SRC.SalesRouteCode,
	SRC.SalesRouteName,
	SRC.PrdSalQty,
	SRC.PrdGrossAmt,
	SRC.PrdTaxAmt,
	SRC.PrdNetAmt,
	SRC.NRValue,
	SRC.Achievement_NR_Val,
	SRC.SRNRefType,
	SRC.SRNMode,
	SRC.srntype,
	NULL AS SKU_Rec_Qty,
	NULL AS SKU_Rec_Amt,
	NULL AS SUM_QPS_QTY,
	NULL AS QPS_Amt,
	SRC.Achievement_Amt,
	SRC.prdschdiscamt,
	SRC.No_of_Lines,
	NULL AS No_of_Reco_SKU_Lines,
	SRC.createddate,
	cast('R' AS VARCHAR(1)) AS SALEFLAG,
	SRC.CRT_DTTM,
	SRC.UPDT_DTTM,
	CASE 
		WHEN SRC.STATUS::TEXT = 'Inactive'::CHARACTER VARYING::TEXT
			THEN 'InActive'::CHARACTER VARYING
		ELSE SRC.STATUS
		END AS STATUS,
	NULL AS PrdFreeQty FROM (
	SELECT SR.DistCode,
		SR.PrdSalInvNo,
		CAST(SUBSTRING(SR.SRNDate, 1, 4) || SUBSTRING(SR.SRNDate, 6, 2) || SUBSTRING(SR.SRNDate, 9, 2) AS INTEGER) AS SRNDate,
		SR.RtrCode,
		SR.RtrName,
		SR.PrdCode,
		SR.SalesmanCode,
		SR.SalesmanName,
		SR.SalesRouteCode,
		SR.SalesRouteName,
		SUM((SR.PrdSalQty) * (- 1)) AS PrdSalQty,
		SUM((SR.PrdGrossAmt) * (- 1)) AS PrdGrossAmt,
		SUM((SR.PrdTaxAmt) * (- 1)) AS PrdTaxAmt,
		SUM((SR.PrdNetAmt) * (- 1)) AS PrdNetAmt,
		SUM(SR.NRValue) AS NRValue,
		Sum(((SR.NRValue * (SR.PrdSalQty + SR.prdunsalqty))) * (- 1)) AS Achievement_NR_Val,
		SR.SRNRefType,
		SR.SRNMode,
		SR.srntype,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN SUM(SR.PrdSalQty) ELSE NULL END SKU_Rec_Qty,
		--CASE WHEN ordb.RecommendedSKU='Yes' THEN SUM(SR.prdgrossamt + SR.prdtaxamt) ELSE NULL END SKU_Rec_Amt,
		--Sum(ordb.schincrqty/Dup.dup_count) as SUM_QPS_QTY,
		Sum((SR.prdgrossamt + SR.prdtaxamt) * (- 1)) AS Achievement_Amt,
		Sum(SR.prdschdiscamt) AS prdschdiscamt,
		Count(1) AS No_of_Lines,
		--SUM(CASE WHEN ordb.RecommendedSKU='Yes' then 1 ELSE 0 END) No_of_Reco_SKU_Lines, 
		max(SR.createddate) AS createddate,
		max(CRT_DTTM) AS CRT_DTTM,
		max(updt_dttm) AS updt_dttm,
		salesmanmstr.STATUS
	FROM itg_salesreturn SR
	LEFT OUTER JOIN (
		SELECT *
		FROM (
			SELECT distcode,
				smcode,
				rmcode,
				createddate,
				initcap(STATUS) AS STATUS,
				row_number() OVER (
					PARTITION BY distcode,
					smcode,
					rmcode ORDER BY createddate DESC
					) AS rnk
			FROM itg_salesmanmaster
			)
		WHERE rnk = 1
		) salesmanmstr ON SR.distcode = salesmanmstr.distcode
		AND SR.salesmancode = salesmanmstr.smcode
		AND SR.salesroutecode = salesmanmstr.rmcode
	GROUP BY SR.DistCode,
		SR.PrdSalInvNo,
		SR.SRNDate,
		SR.RtrCode,
		SR.RtrName,
		SR.PrdCode,
		SR.SalesmanCode,
		SR.SalesmanName,
		SR.SalesRouteCode,
		SR.SalesRouteName,
		SR.SRNRefType,
		SR.SRNMode,
		SR.srntype,
		salesmanmstr.STATUS
	) SRC
),
transformed as(
	select * from union1
	union all
	select * from union2
	union all
	select * from union3
)
select * from transformed