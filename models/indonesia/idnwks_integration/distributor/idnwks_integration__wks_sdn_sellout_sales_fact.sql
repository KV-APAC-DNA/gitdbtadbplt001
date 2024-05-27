with sdl_SDN_raw_sellout_sales_fact as(
    select * from DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_SDN_RAW_SELLOUT_SALES_FACT
),
EDW_DISTRIBUTOR_CHANNEL_DIM as(
    select * from DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_CHANNEL_DIM
),
EDW_DISTRIBUTOR_METADATA_LKP as(
    select * from DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_METADATA_LKP
),
EDW_TIME_DIM as(
    select * from DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_TIME_DIM
),
EDW_DISTRIBUTOR_DIM as(
    select * from DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_DIM
),
EDW_DISTRIBUTOR_PRODUCT_DIM as(
    select * from DEV_DNA_CORE.SNAPIDNEDW_INTEGRATION.EDW_DISTRIBUTOR_PRODUCT_DIM
),

wks_sdn_sellout_sales_fact AS (
		SELECT
			--<transkey> as TRANS_KEY,
			TRIM(SSRSSF.FAKTUR) AS BILL_DOC,
			to_date(SSRSSF.TGL_FAKTUR) AS BILL_DT,
			TRIM(ETD.JJ_MNTH_ID) AS JJ_MNTH_ID,
			TRIM(ETD.JJ_WK) AS JJ_WK,
			--'SDN' AS DSTRBTR_grp_CD,
            UPPER(
            CASE TRIM(SSRSSF.KD_CABANG)
                WHEN '23' THEN '01'
                WHEN '24' THEN '02'
                WHEN '25' THEN '03'
                WHEN '26' THEN '04'
                WHEN '27' THEN '14'
                WHEN '28' THEN '21'
                WHEN '29' THEN '39'
                WHEN '30' THEN '49'
                WHEN '31' THEN '51'
                WHEN '32' THEN '50'
                ELSE TRIM(SSRSSF.KD_CABANG)
            END
            ) AS DSTRBTR_ID,
			TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)) AS JJ_SAP_DSTRBTR_ID,
			TRIM(UPPER(SSRSSF.CUST_ID)) AS DSTRBTR_CUST_ID,
			TRIM(UPPER(SSRSSF.PRODUK_ID)) AS DSTRBTR_PROD_ID,
			TRIM(UPPER(EDPD.JJ_SAP_PROD_ID)) AS JJ_SAP_PROD_ID,
			TRIM(UPPER(EDCD.JNJ_CHNL_TYPE_ID)) AS DSTRBTN_CHNL,
			--'18' AS GRP_OUTLET,
			TRIM(UPPER(SSRSSF.Salesman_ID)) AS DSTRBTR_SLSMN_ID,
			(SSRSSF.QTY / EDPD.denominator) AS SLS_QTY, --------/* New logic */ -------------
			--TRIM(SSRSSF.QTY/decode(EDPD.denominator,0,1,EDPD.denominator)) AS SLS_QTY,  --------/* New logic */ -------------
			CASE 
				WHEN franchise = 'OTX'
					THEN SSRSSF.BRUTO * EDML.NET_VAL_CAL_FACTOR
				ELSE SSRSSF.BRUTO * EDML.GROSS_VAL_CAL_FACTOR
				END AS GRS_VAL,
			--SSRSSF.BRUTO*EDML.GROSS_VAL_CAL_FACTOR AS GRS_VAL,
			SSRSSF.BRUTO * EDML.NET_VAL_CAL_FACTOR AS JJ_NET_VAL,
			(SSRSSF.DISC_RUTIN + SSRSSF.DISC_PRINSIPAL + SSRSSF.DISC_EXTRA + SSRSSF.DISC_COD) * EDML.TRADE_DISC_CAL_FACTOR AS TRD_DSCNT,
			SSRSSF.NETTO * EDML.DIST_NET_VAL_CAL_FACTOR AS DSTRBTR_NET_VAL,
			DECODE((EDPD.JJ_SAP_PROD_ID), '43120650', SSRSSF.QTY / 25, '43120651', SSRSSF.QTY / 25, '43138651', SSRSSF.QTY / 12, '43138650', SSRSSF.QTY / 12, '43138654', SSRSSF.QTY / 10, '43138652', SSRSSF.QTY / 10, '47214650', SSRSSF.QTY / 12, '46701654', SSRSSF.QTY / 12, /*Included New codes */
				'46701655', SSRSSF.QTY / 12, SSRSSF.QTY) AS SLS_QTY_RAW --------/* Existing logic and new column added */ -------------
		FROM sdl_SDN_RAW_SELLOUT_SALES_FACT AS SSRSSF,
			EDW_DISTRIBUTOR_DIM AS EDD,
			(
				SELECT *
				FROM EDW_DISTRIBUTOR_PRODUCT_DIM
				WHERE trim(upper(DSTRBTR_grp_CD)) = 'SDN_SPR'
				) AS EDPD,
			(
				SELECT *
				FROM EDW_DISTRIBUTOR_CHANNEL_DIM
				WHERE trim(upper(DSTRBTR_grp_CD)) = 'SDN'
				) AS EDCD,
			EDW_TIME_DIM AS ETD,
			(
				SELECT NET_VAL_CAL_FACTOR,
					GROSS_VAL_CAL_FACTOR,
					TRADE_DISC_CAL_FACTOR,
					DIST_NET_VAL_CAL_FACTOR
				FROM EDW_DISTRIBUTOR_METADATA_LKP
				WHERE TRIM(UPPER(DSTRBTR_CD)) = 'SDN'
				) AS EDML
		WHERE TRIM(UPPER(EDD.DSTRBTR_ID(+))) = UPPER(DECODE(TRIM(SSRSSF.KD_CABANG), '23', '01', '24', '02', '25', '03', '26', '04', '27', '14', '28', '21', '29', '39', '30', '49', '31', '51', '32', '50', TRIM(SSRSSF.KD_CABANG)))
			AND TRIM(UPPER(EDPD.DSTRBTR_PROD_ID(+))) = TRIM(UPPER(SSRSSF.PRODUK_ID))
			AND TRIM(UPPER(EDCD.DSTRBTR_CHNL_TYPE_ID(+))) = TRIM(UPPER(SSRSSF.SEGMENT_ID))
			-- AND   EDCD.DSTRBTR_GRP_CD = 'SDN'
			AND to_date(ETD.CAL_DATE) = to_date(SSRSSF.TGL_FAKTUR)
		),
-- AND   EDPD.DSTRBTR_GRP_CD = 'SDN_SPR' 
transformed as(
    SELECT 
        (w.DSTRBTR_ID || w.DSTRBTR_CUST_ID || w.DSTRBTR_PROD_ID || w.BILL_DOC || TO_CHAR(w.BILL_DT, 'YYYY-MM-DD')) AS TRANS_KEY,
        w.BILL_DOC as BILL_DOC,
        w.BILL_DT as  BILL_DT,
        w.JJ_MNTH_ID as  JJ_MNTH_ID,
        w.JJ_WK as  JJ_WK,
        'SDN' as  DSTRBTR_grp_CD,
        w.DSTRBTR_ID as  DSTRBTR_ID,
        w.JJ_SAP_DSTRBTR_ID as  JJ_SAP_DSTRBTR_ID,
        w.DSTRBTR_CUST_ID as  DSTRBTR_CUST_ID,
        w.DSTRBTR_PROD_ID as  DSTRBTR_PROD_ID,
        w.JJ_SAP_PROD_ID as  JJ_SAP_PROD_ID,
        w.DSTRBTN_CHNL as  DSTRBTN_CHNL,
        '18' as  GRP_OUTLET,
        w.DSTRBTR_SLSMN_ID as  DSTRBTR_SLSMN_ID,
        w.SLS_QTY as  SLS_QTY,
        w.GRS_VAL as  GRS_VAL,
        w.JJ_NET_VAL as  JJ_NET_VAL,
        w.TRD_DSCNT as  TRD_DSCNT,
        w.DSTRBTR_NET_VAL as  DSTRBTR_NET_VAL,
        (
            CASE 
                WHEN w.SLS_QTY < 0
                    THEN w.SLS_QTY * - 1
                ELSE 0
                END
            ) AS RTRN_QTY,
        (
            CASE 
                WHEN w.SLS_QTY < 0
                    THEN w.JJ_NET_VAL * - 1
                ELSE 0
                END
            ) AS RTRN_VAL,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9),
        NULL::timestamp_ntz(9) as UPDT_DTTM,
        w.SLS_QTY_RAW as SLS_QTY_RAW --------/* Existing logic and new column added */ -------------
    FROM WKS_SDN_SELLOUT_SALES_FACT w
)
select * from transformed