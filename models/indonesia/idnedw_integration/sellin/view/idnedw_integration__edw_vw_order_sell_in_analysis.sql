with edw_invoice_fact as (
    select * from  {{ ref('aspedw_integration__edw_invoice_fact') }}
),
edw_time_dim as (
    select * from {{ source('idnedw_integration', 'edw_time_dim') }}
),
edw_distributor_dim as (
    select * from {{ ref('idnedw_integration__edw_distributor_dim') }}
),
edw_product_dim as (
    select * from {{ ref('idnedw_integration__edw_product_dim') }}
),
itg_mds_id_lav_sellin_target as (
    select * from {{ ref('idnitg_integration__itg_mds_id_lav_sellin_target') }}
),
abc as (
    select 
        date((sof.bill_dt)) as invoice_dt
        ,(upper(trim(ltrim((sof.bill_doc)::text, ((0)::character varying)::text))))::character varying as bill_doc
        ,t2.jj_mnth_id
        ,(ltrim((sof.cust_num)::text, ('0'::character varying)::text))::character varying as jj_sap_dstrbtr_id1
        ,(ltrim((sof.matl_num)::text, ('0'::character varying)::text))::character varying as jj_sap_prod_id
        ,(null::numeric)::numeric(18, 0) as sellin_qty
        ,(null::numeric)::numeric(18, 0) as sellin_val
        ,(null::numeric)::numeric(18, 0) as gross_sellin_val
        ,sum(sof.net_ord_val) as order_net
        ,sum(sof.ord_qty_pc) as order_qty
        ,sum(sof.net_ord_val) as order_gross
        ,sum(sof.est_nts) as nts_order_val
        ,date((sof.doc_dt)) as order_dt
        ,sof.sls_doc
        ,t2.jj_year
        ,t2.jj_qrtr
        ,t2.jj_mnth
        ,t2.jj_mnth_desc
        ,t2.jj_mnth_no
        ,t2.jj_mnth_long
    from (
        edw_invoice_fact sof 
        join edw_time_dim t2 on ((date((sof.doc_dt)) = date(t2.cal_date)))
        )
    where (
            (sof.sls_org)::text = ('2000'::character varying)::text
            and ltrim((sof.sold_to_prty)::text, ((0)::character varying)::text) <> ('590092'::character varying)::text
            and upper((sof.doc_curr)::text) = ('IDR'::character varying)::text
            )
    group by 
        date((sof.bill_dt))
        ,upper(trim(ltrim((sof.bill_doc)::text, ((0)::character varying)::text)))
        ,t2.jj_mnth_id
        ,ltrim((sof.cust_num)::text, ('0'::character varying)::text)
        ,ltrim((sof.matl_num)::text, ('0'::character varying)::text)
        ,date((sof.doc_dt))
        ,sof.sls_doc
        ,t2.jj_year
        ,t2.jj_qrtr
        ,t2.jj_mnth
        ,t2.jj_mnth_desc
        ,t2.jj_mnth_no
        ,t2.jj_mnth_long

    union

    select 
        date(t1.bill_dt) as invoice_dt
        ,(upper(trim(ltrim((t1.bill_num)::text, ((0)::character varying)::text))))::character varying as bill_doc
        ,t2.jj_mnth_id
        ,(ltrim((t1.sold_to)::text, ('0'::character varying)::text))::character varying as jj_sap_dstrbtr_id1
        ,(ltrim((t1.material)::text, ('0'::character varying)::text))::character varying as jj_sap_prod_id
        ,sum(t1.bill_qty) as sellin_qty
        ,sum(t1.rebate_bas) as sellin_val
        ,sum(t1.subtotal_1) as gross_sellin_val
        ,(null::numeric)::numeric(18, 0) as order_net
        ,(null::numeric)::numeric(18, 0) as order_qty
        ,(null::numeric)::numeric(18, 0) as order_gross
        ,(null::numeric)::numeric(18, 0) as nts_order_val
        ,null::date as order_dt
        ,null::character varying as sls_doc
        ,t2.jj_year
        ,t2.jj_qrtr
        ,t2.jj_mnth
        ,t2.jj_mnth_desc
        ,t2.jj_mnth_no
        ,t2.jj_mnth_long
    from (
        edw_billing_fact t1 join edw_time_dim t2 on date(t1.bill_dt) = date(t2.cal_date)
        )
    where (
            upper((t1.loc_currcy)::text) = ('IDR'::character varying)::text
            and (
                upper((t1.bill_type)::text) = ('ZF2I'::character varying)::text
                or upper((t1.bill_type)::text) = ('ZL2I'::character varying)::text
                or upper((t1.bill_type)::text) = ('ZC2I'::character varying)::text
                or upper((t1.bill_type)::text) = ('ZG2I'::character varying)::text
                or upper((t1.bill_type)::text) = ('S1'::character varying)::text
                or upper((t1.bill_type)::text) = ('S2'::character varying)::text
                )
            and upper((t1.material)::text) <> ('REBATE'::character varying)::text
            and ltrim((t1.sold_to)::text, ((0)::character varying)::text) <> ('590092'::character varying)::text
            )
    group by 
        date(t1.bill_dt)
        ,upper(trim(ltrim((t1.bill_num)::text, ((0)::character varying)::text)))
        ,t2.jj_mnth_id
        ,ltrim((t1.sold_to)::text, ('0'::character varying)::text)
        ,ltrim((t1.material)::text, ('0'::character varying)::text)
        ,t2.jj_year
        ,t2.jj_qrtr
        ,t2.jj_mnth
        ,t2.jj_mnth_desc
        ,t2.jj_mnth_no
        ,t2.jj_mnth_long
),
transformed_1  as (
    SELECT 
            'ORDER_SELLIN' AS datasrc,
            abc.invoice_dt
			,abc.bill_doc
            ,abc.jj_year
			,abc.jj_qrtr
			,abc.jj_mnth_id
            ,abc.jj_mnth
			,abc.jj_mnth_desc
            ,abc.jj_mnth_no
			,abc.jj_mnth_long
            ,edd.dstrbtr_grp_cd
			,edd.dstrbtr_grp_nm
			,abc.jj_sap_dstrbtr_id1 as jj_sap_dstrbtr_id
            ,edd.jj_sap_dstrbtr_nm
            ,((((edd.jj_sap_dstrbtr_nm)::TEXT || (' ^'::CHARACTER VARYING)::TEXT) || (edd.jj_sap_dstrbtr_id)::TEXT))::CHARACTER VARYING AS dstrbtr_cd_nm
			,edd.area
			,edd.region as region
			,edd.bdm_nm
			,edd.rbm_nm
            ,edd.STATUS AS dstrbtr_status
            ,epd.jj_sap_prod_id
            ,epd.jj_sap_prod_desc
            ,epd.jj_sap_upgrd_prod_id
			,epd.jj_sap_upgrd_prod_desc
			,epd.jj_sap_cd_mp_prod_id
			,epd.jj_sap_cd_mp_prod_desc
            ,((((epd.jj_sap_upgrd_prod_desc)::TEXT || (' ^'::CHARACTER VARYING)::TEXT) || (epd.jj_sap_upgrd_prod_id)::TEXT))::CHARACTER VARYING AS sap_prod_code_name
            ,epd.franchise
			,epd.brand
			,epd.variant1 AS sku_grp_or_variant
			,epd.variant2 AS sku_grp1_or_variant1
			,epd.variant3 AS sku_grp2_or_variant2
            ,((((epd.variant3)::TEXT || (' '::CHARACTER VARYING)::TEXT) || (COALESCE((epd.put_up)::CHARACTER VARYING, ''::CHARACTER VARYING))::TEXT))::CHARACTER VARYING AS sku_grp3_or_variant3
            ,epd.STATUS AS prod_status
            ,abc.sellin_qty
			,abc.sellin_val
			,abc.gross_sellin_val
			,abc.order_net
			,abc.order_qty
			,abc.order_gross
			,abc.nts_order_val
			,abc.order_dt
			,abc.sls_doc as order_doc
			,NULL AS target_niv
	        ,NULL AS target_hna

		FROM (
			(
				abc LEFT JOIN edw_distributor_dim edd ON (
						(
							(
								(trim((edd.jj_sap_dstrbtr_id)::TEXT) = ltrim((abc.jj_sap_dstrbtr_id1)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
								AND (abc.jj_mnth_id >= (edd.effective_from)::NUMERIC(18, 0))
								)
							AND (abc.jj_mnth_id <= (edd.effective_to)::NUMERIC(18, 0))
							)
						)
				) LEFT JOIN edw_product_dim epd ON (
					(
						(
							(trim((epd.jj_sap_prod_id)::TEXT) = trim((abc.jj_sap_prod_id)::TEXT))
							AND (abc.jj_mnth_id >= (epd.effective_from)::NUMERIC(18, 0))
							)
						AND (abc.jj_mnth_id <= (epd.effective_to)::NUMERIC(18, 0))
						)
					)
			)
),
transformed_2 as (
    SELECT 
    'TARGET' AS datasrc
	,NULL AS invoice_dt
	,NULL AS bill_doc
	,t2.jj_year
	,t2.jj_qrtr
	,(
		substring (
			replace (
				(mds.jj_year_month)::TEXT
				,('.'::CHARACTER VARYING)::TEXT
				,(''::CHARACTER VARYING)::TEXT
				)
			,1
			,6
			)
		)::CHARACTER VARYING AS jj_mnth_id
	,t2.jj_mnth
	,t2.jj_mnth_desc
	,t2.jj_mnth_no
	,t2.jj_mnth_long
	,CASE 
		WHEN ((mds.region_code)::TEXT = ('NATIONAL ACCOUNT'::CHARACTER VARYING)::TEXT)
			THEN mds.distrbutor_code_code
		ELSE CASE 
				WHEN ((mds.region_code)::TEXT = ('MTI'::CHARACTER VARYING)::TEXT)
					THEN NULL::CHARACTER VARYING
				ELSE edd.dstrbtr_grp_cd
				END
		END AS dstrbtr_grp_cd
	,CASE 
		WHEN ((mds.region_code)::TEXT = ('NATIONAL ACCOUNT'::CHARACTER VARYING)::TEXT)
			THEN mds.distrbutor_code_name
		ELSE CASE 
				WHEN ((mds.region_code)::TEXT = ('MTI'::CHARACTER VARYING)::TEXT)
					THEN NULL::CHARACTER VARYING
				ELSE edd.dstrbtr_grp_nm
				END
		END AS dstrbtr_grp_nm
	,edd.jj_sap_dstrbtr_id
	,edd.jj_sap_dstrbtr_nm
	,edd.jj_sap_dstrbtr_id AS dstrbtr_cd_nm
	,edd.area
	,mds.region_code AS region
	,edd.bdm_nm
	,edd.rbm_nm
	,edd.STATUS AS dstrbtr_status
	,NULL AS jj_sap_prod_id
	,NULL AS jj_sap_prod_desc
	,NULL AS jj_sap_upgrd_prod_id
	,NULL AS jj_sap_upgrd_prod_desc
	,NULL AS jj_sap_cd_mp_prod_id
	,NULL AS jj_sap_cd_mp_prod_desc
	,NULL AS sap_prod_code_name
	,(upper((mds.franchise_code)::TEXT))::CHARACTER VARYING AS franchise
	,(upper((mds.brand_code)::TEXT))::CHARACTER VARYING AS brand
	,NULL AS sku_grp_or_variant
	,NULL AS sku_grp1_or_variant1
	,NULL AS sku_grp2_or_variant2
	,NULL AS sku_grp3_or_variant3
	,NULL AS prod_status
	,NULL AS sellin_qty
	,NULL AS sellin_val
	,NULL AS gross_sellin_val
	,NULL AS order_net
	,NULL AS order_qty
	,NULL AS order_gross
	,NULL AS nts_order_val
	,NULL AS order_dt
	,NULL AS order_doc
	,mds.niv AS target_niv
	,mds.hna AS target_hna
FROM (
	(
		itg_mds_id_lav_sellin_target mds JOIN (
			SELECT DISTINCT edw_time_dim.jj_year
				,edw_time_dim.jj_qrtr
				,edw_time_dim.jj_mnth
				,edw_time_dim.jj_mnth_desc
				,edw_time_dim.jj_mnth_no
				,edw_time_dim.jj_mnth_long
			FROM edw_time_dim
			) t2 ON ((trim((mds.jj_year_month)::TEXT) = trim((t2.jj_mnth)::TEXT)))
		) LEFT JOIN edw_distributor_dim edd ON (
			(
				(
					(trim((edd.jj_sap_dstrbtr_id)::TEXT) = ltrim((mds.distrbutor_code_code)::TEXT, ('0'::CHARACTER VARYING)::TEXT))
					AND (
						substring (
							replace (
								(mds.jj_year_month)::TEXT
								,('.'::CHARACTER VARYING)::TEXT
								,(''::CHARACTER VARYING)::TEXT
								)
							,1
							,6
							) >= (edd.effective_from)::TEXT
						)
					)
				AND (
					substring (
						replace (
							(mds.jj_year_month)::TEXT
							,('.'::CHARACTER VARYING)::TEXT
							,(''::CHARACTER VARYING)::TEXT
							)
						,1
						,6
						) <= (edd.effective_to)::TEXT
					)
				)
			)
	)
),
transformed as (
    select * from transformed_1
    union 
    select * from transformed_1
)

select * from transformed