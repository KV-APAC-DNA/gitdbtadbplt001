with 
sdl_mds_id_ref_pp_customer as 
(
    select * from dev_dna_load.idnsdl_raw.sdl_mds_id_ref_pp_customer
),
sdl_sdn_raw_sellout_sales_fact as 
(
    select * from dev_dna_load.idnsdl_raw.sdl_sdn_raw_sellout_sales_fact
),
edw_distributor_channel_dim as 
(
    select * from idnedw_integration.edw_distributor_channel_dim
),
edw_distributor_dim as 
(
    select * from idnedw_integration.edw_distributor_dim
),
wks_sdn_customer as 
(
    select * from idnwks_integration.wks_sdn_customer
),
itg_distributor_customer_dim as 
(
    select * from idnitg_integration.itg_distributor_customer_dim
),
edw_outlet_type_dim as 
(
    select * from idnedw_integration.edw_outlet_type_dim
),
edw_tiering_metadata as 
(
    select * from idnedw_integration.edw_tiering_metadata
),
trans as 
(
WITH WKS_PP_CUSTOMER
AS
(
SELECT
DISTINCT
TRIM(UPPER(DECODE(PPSSF.KODE_CABANG,'BANJARMASIN','PBJM1','YOGYAKARTA','PYGY1','SOLO','PSOL1',PPSSF.KODE_CABANG))) AS DSTRBTR_ID,
TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)) as JJ_SAP_DSTRBTR_ID,
TRIM(UPPER(PPSSF.KODE_OUTLET)) as DSTRBTR_CUST_ID,
TRIM(SPCM.NAMA_OUTLET) as DSTRBTR_CUST_NM,
TRIM(SPCM.ALAMAT) as DSTRBTR_CUST_ADDR,
TRIM(SPCM.AREA) as DTRBTR_CUST_DISTRICT,
TRIM(UPPER(SPCM.MARKET_CODE)) as DSTRBTR_CHNL,
TRIM(SPCM.KODE_POS) as KODEPOS,
TRIM(UPPER(EDCD.JNJ_CHNL_TYPE_ID)) as JNJ_CHNL_TYPE_ID
FROM
(SELECT DECODE(KODE_CABANG,'1820','PBJM1','1822','PYGY1','1828','PSOL1',KODE_CABANG) AS KODE_CABANG,
  KODE_GRP_CHAINSTORE AS KODE_GRP_CHAINSTORE,
  NAMA_CHAINSTORE AS NAMA_CHAINSTORE,
  MARKET_CODE AS MARKET_CODE,
  MARKET_DESC AS MARKET_DESC,
  KODE_OUTLET AS KODE_OUTLET,
  NAMA_OUTLET AS NAMA_OUTLET,
  ALAMAT AS ALAMAT,
  AREA AS AREA,
  KODE_POS AS KODE_POS
  FROM SDL_MDS_ID_REF_PP_CUSTOMER) AS SPCM,
SDL_PP_RAW_SELLOUT_SALES_FACT AS PPSSF,
(SELECT *
       FROM EDW_DISTRIBUTOR_CHANNEL_DIM
       WHERE DSTRBTR_GRP_CD = 'PP') AS EDCD,
EDW_DISTRIBUTOR_DIM AS EDD
WHERE
TRIM(UPPER(EDD.DSTRBTR_ID(+)))=TRIM(UPPER(DECODE(PPSSF.KODE_CABANG,'BANJARMASIN','PBJM1','YOGYAKARTA','PYGY1','SOLO','PSOL1',PPSSF.KODE_CABANG))) AND
TRIM(UPPER(SPCM.KODE_OUTLET(+)))=TRIM(UPPER(PPSSF.KODE_OUTLET)) AND
TRIM(UPPER(SPCM.KODE_CABANG(+))) = TRIM(UPPER(DECODE(PPSSF.KODE_CABANG,'BANJARMASIN','PBJM1','YOGYAKARTA','PYGY1','SOLO','PSOL1',PPSSF.KODE_CABANG))) AND
TRIM(UPPER(EDCD.DSTRBTR_CHNL_TYPE_ID(+)))=TRIM(UPPER(SPCM.MARKET_CODE))
)
SELECT
DISTINCT
WSPSSF.JJ_SAP_DSTRBTR_ID||WSPSSF.DSTRBTR_CUST_ID AS KEY_OUTLET,
TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID)),
EDD.JJ_SAP_DSTRBTR_NM,
WSPSSF.DSTRBTR_CUST_ID,
MAX(WSPSSF.DSTRBTR_CUST_NM) OVER (PARTITION BY WSPSSF.DSTRBTR_CUST_ID) AS DSTRBTR_CUST_NM,
MAX(WSPSSF.DSTRBTR_CUST_ADDR)OVER (PARTITION BY WSPSSF.DSTRBTR_CUST_ID) AS DSTRBTR_CUST_ADDR,
WSPSSF.DTRBTR_CUST_DISTRICT AS AREA,
'Others' AS CUST_GRP,
EOTD.CHNL,
EOTD.OUTLET_TYPE,
'Others' AS CHNL_GRP,
NULL AS JJID,
WSPSSF.KODEPOS AS PST_CD,
WSPSSF.DSTRBTR_CUST_ID AS DSTRBTR_CUST_ID_MAP,
MAX(WSPSSF.DSTRBTR_CUST_NM) OVER (PARTITION BY WSPSSF.DSTRBTR_CUST_ID) AS DSTRBTR_CUST_NM_MAP,
'Non Reg.Discount' AS CHNL_GRP2,
(convert_timezone('UTC', current_timestamp()) -6) AS CUST_CRTD_DT,
'Others' AS CUST_GRP2,
convert_timezone('UTC', current_timestamp()),
CAST(NULL AS DATE)
FROM
WKS_PP_CUSTOMER WSPSSF,
ITG_DISTRIBUTOR_CUSTOMER_DIM EDCD,
EDW_DISTRIBUTOR_DIM EDD,
EDW_OUTLET_TYPE_DIM EOTD
WHERE
UPPER(TRIM(EDCD.KEY_OUTLET(+)))=UPPER(WSPSSF.JJ_SAP_DSTRBTR_ID||WSPSSF.DSTRBTR_CUST_ID) AND
TRIM(UPPER(EDD.JJ_SAP_DSTRBTR_ID(+)))=WSPSSF.JJ_SAP_DSTRBTR_ID AND
EOTD.CHNL_ID(+)=DECODE(WSPSSF.JNJ_CHNL_TYPE_ID,NULL,'0','','0',WSPSSF.JNJ_CHNL_TYPE_ID)
AND
EDCD.KEY_OUTLET IS NULL 
),
trans_a as
(
    select 
      KEY_OUTLET,
  JJ_SAP_DSTRBTR_ID,
  JJ_SAP_DSTRBTR_NM,
  CUST_ID,
  CUST_NM,
  ADDRESS,
  CITY,
  CUST_GRP,
  CHNL,
  OUTLET_TYPE,
  CHNL_GRP,
  JJID,
  PST_CD,
  CUST_ID_MAP,
  CUST_NM_MAP,
  CHNL_GRP2,
  CUST_CRTD_DT,
  CUST_GRP2,
  CRTD_DTTM,
  UPDT_DTTM
  from trans
),
final as 
(
    select 
        a.KEY_OUTLET,
        a.JJ_SAP_DSTRBTR_ID,
        a.JJ_SAP_DSTRBTR_NM,
        a.CUST_ID,
        a.CUST_NM,
        a.ADDRESS,
        a.CITY,
        a.CUST_GRP,
        a.CHNL,
        a.OUTLET_TYPE,
        a.CHNL_GRP,
        a.JJID,
        a.PST_CD,
        a.CUST_ID_MAP,
        a.CUST_NM_MAP,
        a.CHNL_GRP2,
        a.CUST_CRTD_DT,
        tiering.customer_group2 as CUST_GRP2,
        a.CRTD_DTTM,
        a.UPDT_DTTM
  from trans_a as a
  left join 
  edw_tiering_metadata tiering
where WKS.chnl=tiering.channel
and WKS.outlet_type=tiering.sub_channel
)
select * from final