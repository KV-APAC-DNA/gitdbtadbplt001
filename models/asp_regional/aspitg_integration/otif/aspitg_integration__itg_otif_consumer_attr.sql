--Import CTE
{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['RGN_MKT_CD','FISC_YR_MO_NUM'],
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where (trim(RGN_MKT_CD) || FISC_YR_MO_NUM) in (
        select distinct trim(RGN_MKT_CD) || FISC_YR_MO_NUM from {{ source('aspsdl_raw', 'sdl_otif_consumer_attr') }});
        {% endif %}"
    )
}}
with source as (
    select *
    from {{ source('aspsdl_raw', 'sdl_otif_consumer_attr') }}
),


--Logical CTE

--Final CTE
final as (
    select
	SLS_ORDR_NUM::VARCHAR(255) AS SLS_ORDR_NUM,
SLS_ORDR_LINE_NBR::NUMBER(38) AS SLS_ORDR_LINE_NBR,
AFFL_IND::VARCHAR(255) AS AFFL_IND,
NO_CHRG_IND::VARCHAR(255) AS NO_CHRG_IND,
SLS_ORDR_TYPE_CD::VARCHAR(255) AS SLS_ORDR_TYPE_CD,
SLS_ORDR_RSN_CD::VARCHAR(255) AS SLS_ORDR_RSN_CD,
LINE_ITEM_CAT_CD::VARCHAR(255) AS LINE_ITEM_CAT_CD,
CUST_PO_NUM::VARCHAR(255) AS CUST_PO_NUM,
DELV_CMPLT_CD::VARCHAR(255) AS DELV_CMPLT_CD,
SLORG_NUM::VARCHAR(255) AS SLORG_NUM,
SLS_TERR_NUM::VARCHAR(255) AS SLS_TERR_NUM,
SLS_GRP_CD::VARCHAR(255) AS SLS_GRP_CD,
RESP_ENT_CUST_NM::VARCHAR(255) AS RESP_ENT_CUST_NM,
KEY_CUST_NM::VARCHAR(255) AS KEY_CUST_NM,
RESP_ENT_CUST_NUM::VARCHAR(255) AS RESP_ENT_CUST_NUM,
KEY_CUST_NUM::VARCHAR(255) AS KEY_CUST_NUM,
SOLD_TO_CUST_NM::VARCHAR(255) AS SOLD_TO_CUST_NM,
SOLD_TO_CUST_NUM::VARCHAR(255) AS SOLD_TO_CUST_NUM,
SHIP_TO_CUST_NM::VARCHAR(255) AS SHIP_TO_CUST_NM,
SHIP_TO_CUST_CITY_NM::VARCHAR(255) AS SHIP_TO_CUST_CITY_NM,
SHIP_TO_CUST_ST_PRVNC_CD::VARCHAR(255) AS SHIP_TO_CUST_ST_PRVNC_CD,
SOLD_TO_CUST_CLUS_CD::VARCHAR(255) AS SOLD_TO_CUST_CLUS_CD,
SOLD_TO_CUST_SUB_CLUS_CD::VARCHAR(255) AS SOLD_TO_CUST_SUB_CLUS_CD,
SOLD_TO_CUST_REGN_MKT_CD::VARCHAR(255) AS SOLD_TO_CUST_REGN_MKT_CD,
SLORG_RGN_CD::VARCHAR(255) AS SLORG_RGN_CD,
SLORG_CLUS_CD::VARCHAR(255) AS SLORG_CLUS_CD,
SLORG_SUB_CLUS_CD::VARCHAR(255) AS SLORG_SUB_CLUS_CD,
SLORG_MKT_NM::VARCHAR(255) AS SLORG_MKT_NM,
GEO_CLUS_NM::VARCHAR(255) AS GEO_CLUS_NM,
SUB_CLUS_CD::VARCHAR(255) AS SUB_CLUS_CD,
RGN_MKT_CD::VARCHAR(255) AS RGN_MKT_CD,
GCCH_CUST_PARNT_NM::VARCHAR(255) AS GCCH_CUST_PARNT_NM,
GCCH_RTL_BNR_CD::VARCHAR(255) AS GCCH_RTL_BNR_CD,
GCCH_CUST_CHNL_CD::VARCHAR(255) AS GCCH_CUST_CHNL_CD,
LCL_MATL_PARNT_CD::VARCHAR(255) AS LCL_MATL_PARNT_CD,
MATL_SHRT_DESC::VARCHAR(255) AS MATL_SHRT_DESC,
MATL_CAT_GRP_CD::VARCHAR(255) AS MATL_CAT_GRP_CD,
MATL_GRP_CD::VARCHAR(255) AS MATL_GRP_CD,
NPI_FLAG::VARCHAR(255) AS NPI_FLAG,
DSTN_CHN_STS_CD::VARCHAR(255) AS DSTN_CHN_STS_CD,
TRD_CSTM_CD::VARCHAR(255) AS TRD_CSTM_CD,
MATL_NUM::VARCHAR(255) AS MATL_NUM,
MFG_SITE_NM::VARCHAR(255) AS MFG_SITE_NM,
MRP_CNTL_NM::VARCHAR(255) AS MRP_CNTL_NM,
DMD_PLNR_NM::VARCHAR(255) AS DMD_PLNR_NM,
MFGR_NUM::VARCHAR(255) AS MFGR_NUM,
DP_CLS_NM::VARCHAR(255) AS DP_CLS_NM,
GCPH_GFO_DESC::VARCHAR(255) AS GCPH_GFO_DESC,
GCPH_BRND_DESC::VARCHAR(255) AS GCPH_BRND_DESC,
GCPH_SUB_BRND_DESC::VARCHAR(255) AS GCPH_SUB_BRND_DESC,
GCPH_VARIENT_DESC::VARCHAR(255) AS GCPH_VARIENT_DESC,
GCPH_NEED_ST_DESC::VARCHAR(255) AS GCPH_NEED_ST_DESC,
GCPH_CAT_DESC::VARCHAR(255) AS GCPH_CAT_DESC,
GCPH_SUB_CAT_DESC::VARCHAR(255) AS GCPH_SUB_CAT_DESC,
GCPH_SGMNT_DESC::VARCHAR(255) AS GCPH_SGMNT_DESC,
GCPH_SUB_SGMNT_DESC::VARCHAR(255) AS GCPH_SUB_SGMNT_DESC,
LCL_PROD_HIER_LVL_1_DESC::VARCHAR(255) AS LCL_PROD_HIER_LVL_1_DESC,
LCL_PROD_HIER_LVL_2_DESC::VARCHAR(255) AS LCL_PROD_HIER_LVL_2_DESC,
LCL_PROD_HIER_LVL_3_DESC::VARCHAR(255) AS LCL_PROD_HIER_LVL_3_DESC,
LCL_PROD_HIER_LVL_4_DESC::VARCHAR(255) AS LCL_PROD_HIER_LVL_4_DESC,
LCL_PROD_HIER_LVL_5_DESC::VARCHAR(255) AS LCL_PROD_HIER_LVL_5_DESC,
RGN_GLOBL_BU_DESC::VARCHAR(255) AS RGN_GLOBL_BU_DESC,
RGN_SUB_BU_DESC::VARCHAR(255) AS RGN_SUB_BU_DESC,
RGN_BRND_DESC::VARCHAR(255) AS RGN_BRND_DESC,
RGN_SUB_BRND_DESC::VARCHAR(255) AS RGN_SUB_BRND_DESC,
RGN_FRAN_DESC::VARCHAR(255) AS RGN_FRAN_DESC,
RGN_OPER_CO_CD::VARCHAR(255) AS RGN_OPER_CO_CD,
RGN_CAT_DESC::VARCHAR(255) AS RGN_CAT_DESC,
PLNT_CD::VARCHAR(255) AS PLNT_CD,
PLNT_RGN_CD::VARCHAR(255) AS PLNT_RGN_CD,
PLNT_CITY_NM::VARCHAR(255) AS PLNT_CITY_NM,
PLNT_CTRY_CD::VARCHAR(255) AS PLNT_CTRY_CD,
PLNT_NM::VARCHAR(255) AS PLNT_NM,
TOP_CUST_NM::VARCHAR(255) AS TOP_CUST_NM,
PLNT_NM_CITY_CONCAT::VARCHAR(255) AS PLNT_NM_CITY_CONCAT,
SLS_UOM_CD::VARCHAR(255) AS SLS_UOM_CD,
UOM_CONV_FCTR::NUMBER AS UOM_CONV_FCTR,
BASE_UOM_CD::VARCHAR(255) AS BASE_UOM_CD,
CUM_SCHED_LINE_CNFRM_QTY::NUMBER AS CUM_SCHED_LINE_CNFRM_QTY,
CR_CHK_HLD_STS_CD::VARCHAR(255) AS CR_CHK_HLD_STS_CD,
INC_BLOCK_HDR_CODE::VARCHAR(255) AS INC_BLOCK_HDR_CODE,
INC_BLOCK_LN_CODE::VARCHAR(255) AS INC_BLOCK_LN_CODE,
REJ_RSN_FM_DESC::VARCHAR(255) AS REJ_RSN_FM_DESC,
REJ_RSN_CD::VARCHAR(255) AS REJ_RSN_CD,
REJ_RSN_DESC::VARCHAR(255) AS REJ_RSN_DESC,
DELV_HDR_BLK_CD::VARCHAR(255) AS DELV_HDR_BLK_CD,
DELV_HDR_BLK_DESC::VARCHAR(255) AS DELV_HDR_BLK_DESC,
DELV_LINE_BLK_CD::VARCHAR(255) AS DELV_LINE_BLK_CD,
DELV_LINE_BLK_DESC::VARCHAR(255) AS DELV_LINE_BLK_DESC,
FISC_YR_NBR::NUMBER(38) AS FISC_YR_NBR,
FISC_YR_MO_NUM::VARCHAR(255) AS FISC_YR_MO_NUM,
FISC_YR_WK_NUM::VARCHAR(255) AS FISC_YR_WK_NUM,
FRD_FISC_YR_NBR::NUMBER(38) AS FRD_FISC_YR_NBR,
FRD_FISC_YR_MO_NUM::VARCHAR(255) AS FRD_FISC_YR_MO_NUM,
FRD_FISC_YR_WK_NUM::VARCHAR(255) AS FRD_FISC_YR_WK_NUM,
SHIP_PT_CAL_KEY_NUM::VARCHAR(255) AS SHIP_PT_CAL_KEY_NUM,
RTE_NUM::VARCHAR(255) AS RTE_NUM,
FST_ACT_SHIP_COMP_DTTM::TIMESTAMP_NTZ(9) AS FST_ACT_SHIP_COMP_DTTM,
FST_PLAN_SHIP_COMP_DTTM::TIMESTAMP_NTZ(9) AS FST_PLAN_SHIP_COMP_DTTM,
VICS_BILL_OF_LAD_NUM::VARCHAR(255) AS VICS_BILL_OF_LAD_NUM,
TMS_LD_ID::VARCHAR(255) AS TMS_LD_ID,
CARR_NUM::VARCHAR(255) AS CARR_NUM,
WHSE_CUT_RSN_CD::VARCHAR(255) AS WHSE_CUT_RSN_CD,
LAST_UPDT_DTTM::TIMESTAMP_NTZ(9) AS LAST_UPDT_DTTM,
SRC_SYS_NM::VARCHAR(255) AS SRC_SYS_NM,
SLS_ORDR_CRT_YR_NUM::NUMBER(38) AS SLS_ORDR_CRT_YR_NUM,
SLS_ORDR_CRT_MO_NUM::VARCHAR(255) AS SLS_ORDR_CRT_MO_NUM,
ALT_SRC_SYS_CD::VARCHAR(255) AS ALT_SRC_SYS_CD,
GCGH_RGN_NM::VARCHAR(255) AS GCGH_RGN_NM,
SLS_ORDR_QTY::NUMBER AS SLS_ORDR_QTY,
SLS_ORDR_LINE_CRT_DT::DATE AS SLS_ORDR_LINE_CRT_DT,
SLS_ORDR_LINE_CRT_TM::VARCHAR(255) AS SLS_ORDR_LINE_CRT_TM,
RQST_DELV_DTTM::TIMESTAMP_NTZ(9) AS RQST_DELV_DTTM,
SHIP_TO_CUST_CNTRY_NM::VARCHAR(255) AS SHIP_TO_CUST_CNTRY_NM,
GEO_CTRY_NM::VARCHAR(255) AS GEO_CTRY_NM,
SHIP_PT_NUM::VARCHAR(255) AS SHIP_PT_NUM,
SOLD_TO_CUST_ST_PRVNC_CD::VARCHAR(255) AS SOLD_TO_CUST_ST_PRVNC_CD,
RTE_CAL_KEY::VARCHAR(255) AS RTE_CAL_KEY,
EXPTD_FRD_DTTM::TIMESTAMP_NTZ(9) AS EXPTD_FRD_DTTM,
ALLC_TIME::VARCHAR(255) AS ALLC_TIME,
PICK_PACK_TIME::VARCHAR(255) AS PICK_PACK_TIME,
SHIP_LD_TIME::VARCHAR(255) AS SHIP_LD_TIME,
SHIP_ORIG_RTE_DAYS::NUMBER AS SHIP_ORIG_RTE_DAYS,
DELV_DOC_NUM::VARCHAR(255) AS DELV_DOC_NUM,
FST_PLAN_GI_DTTM::TIMESTAMP_NTZ(9) AS FST_PLAN_GI_DTTM,
FNL_CNFRM_QTY::NUMBER AS FNL_CNFRM_QTY,
FNL_ACTL_SKU_DELV_QTY::NUMBER AS FNL_ACTL_SKU_DELV_QTY,
EXPTD_RDD_ADJ_IND_CD::VARCHAR(255) AS EXPTD_RDD_ADJ_IND_CD,
BASE_UOM_ORDR_QTY::NUMBER(38) AS BASE_UOM_ORDR_QTY,
CALC_SLS_ORDR_ADJ_CRT_DTTM::TIMESTAMP_NTZ(9) AS CALC_SLS_ORDR_ADJ_CRT_DTTM,
FST_PLAN_DELV_DTTM::TIMESTAMP_NTZ(9) AS FST_PLAN_DELV_DTTM,
FST_MATL_AVLBLTY_DTTM::TIMESTAMP_NTZ(9) AS FST_MATL_AVLBLTY_DTTM,
LAST_MATL_AVLBLTY_DTTM::TIMESTAMP_NTZ(9) AS LAST_MATL_AVLBLTY_DTTM,
FST_ACTL_SHIP_DTTM::TIMESTAMP_NTZ(9) AS FST_ACTL_SHIP_DTTM,
FST_ACTL_DELV_DTTM::TIMESTAMP_NTZ(9) AS FST_ACTL_DELV_DTTM,
LAST_ACTL_DELV_DTTM::TIMESTAMP_NTZ(9) AS LAST_ACTL_DELV_DTTM,
CNT_OF_DELV_DOC_NUM::NUMBER(38) AS CNT_OF_DELV_DOC_NUM,
LINE_DENOM_MEAS_AT_ALLC::NUMBER AS LINE_DENOM_MEAS_AT_ALLC,
LINE_DENOM_MEAS_AT_SC::NUMBER AS LINE_DENOM_MEAS_AT_SC,
LINE_DENOM_MEAS_AT_DELV::NUMBER AS LINE_DENOM_MEAS_AT_DELV,
CARR_APPT_IND_CD::VARCHAR(255) AS CARR_APPT_IND_CD,
TRNST_TIME_IND_CD::VARCHAR(255) AS TRNST_TIME_IND_CD,
SHIP_TYPE_CD::VARCHAR(255) AS SHIP_TYPE_CD,
CARR_APPT_DTTM::TIMESTAMP_NTZ(9) AS CARR_APPT_DTTM,
MIN_PLAN_GI_DTTM::TIMESTAMP_NTZ(9) AS MIN_PLAN_GI_DTTM,
FST_DELV_DOC_NUM::VARCHAR(255) AS FST_DELV_DOC_NUM,
BH_IND_CD::VARCHAR(255) AS BH_IND_CD,
MSNG_POD_IND_CD::VARCHAR(255) AS MSNG_POD_IND_CD,
CALC_RQST_DELV_DTTM::TIMESTAMP_NTZ(9) AS CALC_RQST_DELV_DTTM,
EXPTD_RQST_DELV_DTTM::TIMESTAMP_NTZ(9) AS EXPTD_RQST_DELV_DTTM,
DENOM_UNIT_QTY_ALLC::NUMBER AS DENOM_UNIT_QTY_ALLC,
DENOM_UNIT_QTY_SC::NUMBER AS DENOM_UNIT_QTY_SC,
DENOM_UNIT_QTY_DELV::NUMBER AS DENOM_UNIT_QTY_DELV,
EXPTD_RDD_BASE_UOM_CUM_DELV_QTY::NUMBER AS EXPTD_RDD_BASE_UOM_CUM_DELV_QTY,
LATE_DELV_IND_CD::VARCHAR(255) AS LATE_DELV_IND_CD,
EXPTD_MATL_ALLC_DT::TIMESTAMP_NTZ(9) AS EXPTD_MATL_ALLC_DT,
EXPTD_SHIP_DATE::TIMESTAMP_NTZ(9) AS EXPTD_SHIP_DATE,
POM_SEG_CD::VARCHAR(255) AS POM_SEG_CD,
GREENLIGHT_SEG_CD::VARCHAR(255) AS GREENLIGHT_SEG_CD,
EXPTD_SDD_BASE_UOM_CUM_DELV_QTY::NUMBER AS EXPTD_SDD_BASE_UOM_CUM_DELV_QTY,
OTIF_EXCL_IND::VARCHAR(255) AS OTIF_EXCL_IND,
NUMRTR_UNIT_QTY_SC::NUMBER AS NUMRTR_UNIT_QTY_SC,
NUMRTR_UNIT_QTY_DELV::NUMBER AS NUMRTR_UNIT_QTY_DELV,
NUMRTR_UNIT_QTY_ALLC::NUMBER AS NUMRTR_UNIT_QTY_ALLC,
LINE_NUMRTR_MEAS_AT_SC::NUMBER AS LINE_NUMRTR_MEAS_AT_SC,
LINE_NUMRTR_MEAS_AT_ALLC::NUMBER AS LINE_NUMRTR_MEAS_AT_ALLC,
LINE_NUMRTR_MEAS_AT_DELV::NUMBER AS LINE_NUMRTR_MEAS_AT_DELV,
GI_LATE_IND::VARCHAR(255) AS GI_LATE_IND,
EXP_MAD_ICMPT_LINE_BLK_IND::VARCHAR(255) AS EXP_MAD_ICMPT_LINE_BLK_IND,
EXP_MAD_ICMPT_HDR_BLK_IND::VARCHAR(255) AS EXP_MAD_ICMPT_HDR_BLK_IND,
CRIT_FM_DESC_OTIF_SC::VARCHAR(255) AS CRIT_FM_DESC_OTIF_SC,
CRIT_FM_DESC_OTIF_DELV::VARCHAR(255) AS CRIT_FM_DESC_OTIF_DELV,
CRIT_FM_DESC_OTIF_ALLC::VARCHAR(255) AS CRIT_FM_DESC_OTIF_ALLC,
TRANS_MISS_FM_QTY::NUMBER(38) AS TRANS_MISS_FM_QTY,
PA_MISS_FM_QTY::NUMBER(38) AS PA_MISS_FM_QTY,
OM_MISS_FM_QTY::NUMBER(38) AS OM_MISS_FM_QTY,
DC_MISS_FM_QTY::NUMBER(38) AS DC_MISS_FM_QTY,
CUST_MISS_FM_QTY::NUMBER(38) AS CUST_MISS_FM_QTY,
TRANS_MISS_FM_IND::VARCHAR(255) AS TRANS_MISS_FM_IND,
PA_MISS_FM_IND::VARCHAR(255) AS PA_MISS_FM_IND,
OM_MISS_FM_IND::VARCHAR(255) AS OM_MISS_FM_IND,
DC_MISS_FM_IND::VARCHAR(255) AS DC_MISS_FM_IND,
CUST_MISS_FM_IND::VARCHAR(255) AS CUST_MISS_FM_IND,
TRANS_MISS_FM_DESC::VARCHAR(255) AS TRANS_MISS_FM_DESC,
PA_MISS_FM_DESC::VARCHAR(255) AS PA_MISS_FM_DESC,
OM_MISS_FM_DESC::VARCHAR(255) AS OM_MISS_FM_DESC,
DC_MISS_FM_DESC::VARCHAR(255) AS DC_MISS_FM_DESC,
CUST_MISS_FM_DESC::VARCHAR(255) AS CUST_MISS_FM_DESC,
TRANS_SUB_FAIL_MODE_DESC::VARCHAR(255) AS TRANS_SUB_FAIL_MODE_DESC,
PA_SUB_FAIL_MODE_DESC::VARCHAR(255) AS PA_SUB_FAIL_MODE_DESC,
OM_SUB_FAIL_MODE_DESC::VARCHAR(255) AS OM_SUB_FAIL_MODE_DESC,
DC_SUB_FAIL_MODE_DESC::VARCHAR(255) AS DC_SUB_FAIL_MODE_DESC,
CUST_SUB_FAIL_MODE_DESC::VARCHAR(255) AS CUST_SUB_FAIL_MODE_DESC,
CRIT_FM_QTY_TRANS_DELV::NUMBER AS CRIT_FM_QTY_TRANS_DELV,
CRIT_PA_FM_QTY_SC::NUMBER AS CRIT_PA_FM_QTY_SC,
CRIT_PA_FM_QTY_DELV::NUMBER AS CRIT_PA_FM_QTY_DELV,
CRIT_PA_FM_QTY_ALLC::NUMBER AS CRIT_PA_FM_QTY_ALLC,
CRIT_OM_FM_QTY_SC::NUMBER AS CRIT_OM_FM_QTY_SC,
CRIT_OM_FM_QTY_DELV::NUMBER AS CRIT_OM_FM_QTY_DELV,
CRIT_OM_FM_QTY_ALLC::NUMBER AS CRIT_OM_FM_QTY_ALLC,
CRIT_DC_FM_QTY_SC::NUMBER AS CRIT_DC_FM_QTY_SC,
CRIT_DC_FM_QTY_DELV::NUMBER AS CRIT_DC_FM_QTY_DELV,
CRIT_CUST_FM_QTY_SC::NUMBER AS CRIT_CUST_FM_QTY_SC,
CRIT_CUST_FM_QTY_DELV::NUMBER AS CRIT_CUST_FM_QTY_DELV,
CRIT_CUST_FM_QTY_ALLC::NUMBER AS CRIT_CUST_FM_QTY_ALLC,
CRIT_OTIF_FM_IND_SC::VARCHAR(255) AS CRIT_OTIF_FM_IND_SC,
CRIT_OTIF_FM_IND_DELV::VARCHAR(255) AS CRIT_OTIF_FM_IND_DELV,
CRIT_OTIF_FM_IND_ALLC::VARCHAR(255) AS CRIT_OTIF_FM_IND_ALLC,
CRIT_FM_DESC_DELV::VARCHAR(255) AS CRIT_FM_DESC_DELV,
CRIT_FM_DESC_ALLC::VARCHAR(255) AS CRIT_FM_DESC_ALLC,
FFL_PRCS_FST_PT_FM_DESC::VARCHAR(255) AS FFL_PRCS_FST_PT_FM_DESC,
DRVR_DETL::VARCHAR(255) AS DRVR_DETL,
EXP_MAD_DELV_BLK_LINE_IND::VARCHAR(255) AS EXP_MAD_DELV_BLK_LINE_IND,
EXP_MAD_DELV_BLK_HDR_IND::VARCHAR(255) AS EXP_MAD_DELV_BLK_HDR_IND,
EXP_MAD_BASE_UOM_CUM_ALLC_QTY::NUMBER AS EXP_MAD_BASE_UOM_CUM_ALLC_QTY,
EXP_FRD_BASE_UOM_CUM_ALLC_QTY::NUMBER AS EXP_FRD_BASE_UOM_CUM_ALLC_QTY,
EXP_MAD_CR_HLD_IND::VARCHAR(255) AS EXP_MAD_CR_HLD_IND,
EXP_MAD_COMP_DELV_HDR_BLK_IND::VARCHAR(255) AS EXP_MAD_COMP_DELV_HDR_BLK_IND,
OPEN_BLK_IND::VARCHAR(255) AS OPEN_BLK_IND,
LATE_ALLC_BLK_IND::VARCHAR(255) AS LATE_ALLC_BLK_IND,
FCTR_DNMNTR_MEAS::NUMBER AS FCTR_DNMNTR_MEAS,
FCTR_NUMRTR_MEAS::NUMBER AS FCTR_NUMRTR_MEAS,
SLS_ORDR_CRT_DTTM::TIMESTAMP_NTZ(9) AS SLS_ORDR_CRT_DTTM,
SHIP_TO_CUST_NUM::VARCHAR(255) AS SHIP_TO_CUST_NUM,
PRIR_BRND_NM::VARCHAR(255) AS PRIR_BRND_NM,
LOGL_KEY_CMBN_COL_NM::VARCHAR(255) AS LOGL_KEY_CMBN_COL_NM,
LOGL_KEY_CMBN_COL_VAL::VARCHAR(255) AS LOGL_KEY_CMBN_COL_VAL,
CLUS_CD::VARCHAR(255) AS CLUS_CD,
RGN_CD::VARCHAR(255) AS RGN_CD,
FST_BILL_NUM::VARCHAR(255) AS FST_BILL_NUM,
CUST_SEG::VARCHAR(255) AS CUST_SEG,
BRAND_SEG::VARCHAR(255) AS BRAND_SEG,
DMD_PLN_BO::VARCHAR(255) AS DMD_PLN_BO,
TRADE_CUST_MG::VARCHAR(255) AS TRADE_CUST_MG,
BRND_MKT_COM::VARCHAR(255) AS BRND_MKT_COM,
MFG_SITE_TYPE_CD::VARCHAR(255) AS MFG_SITE_TYPE_CD,
CUST_SLS_GRP1_CD::VARCHAR(255) AS CUST_SLS_GRP1_CD,
CUST_SLS_GRP2_CD::VARCHAR(255) AS CUST_SLS_GRP2_CD,
CUST_SLS_GRP3_CD::VARCHAR(255) AS CUST_SLS_GRP3_CD,
SUP_GRP_NM::VARCHAR(255) AS SUP_GRP_NM,
SUP_NM::VARCHAR(255) AS SUP_NM,
SUP_NUM::VARCHAR(255) AS SUP_NUM,
DAI_BTCH_ID::NUMBER AS DAI_BTCH_ID,
DAI_UPDT_DTTM::TIMESTAMP_NTZ(9) AS DAI_UPDT_DTTM,
DAI_CRT_DTTM::TIMESTAMP_NTZ(9) AS DAI_CRT_DTTM,
SRC_SYS_CD::VARCHAR(255) AS SRC_SYS_CD,
current_timestamp()::timestamp_ntz(9) as CRTD_DTTM
from source
)

--Final select
select * from final
