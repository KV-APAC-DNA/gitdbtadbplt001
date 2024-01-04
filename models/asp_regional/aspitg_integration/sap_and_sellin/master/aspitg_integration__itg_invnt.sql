{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['request_number','data_packet','data_record'],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspwks_integration__wks_itg_invnt')}}
),

--Logical CTE
final as(
     SELECT
        request_number as REQUEST_NUMBER,
        data_packet as DATA_PACKET,
        data_record as DATA_RECORD,
        loc_currcy as LCL_CRNCY,
        base_uom as BASE_UNIT,
        material as MATL_NO,
        stor_loc as STRG_LOC,
        stocktype as STK_TYPE,
        stockcat as STK_CAT,
        comp_code as CO_CD,
        mat_plant as MATL_PLNT_VIEW,
        bic_zbatch as BTCH_NUM,
        plant as PLNT,
        whse_num as WHSE_NUM,
        strge_bin as STRG_BIN,
        strge_type as STRG_TYPE,
        indspecstk as SPL_STCK_VAL,
        bic_zmdsobkz as SPL_STCK_INDICA,
        doc_date as DOC_DT,
        val_class as VALUT_CLS,
        id_valarea as VALUT_AREA,
        val_type as VALUT_TYPE,
        pstng_date as PSTNG_DT,
        calday as CAL_DAY,
        bic_zmat_wh as WH_MSTR,
        version as VERS,
        vtype as VAL_TYPE,
        vendor as VEND,
        sold_to as SOLD_TO_PRTY,
        bic_zmoveinds as MVMT_IND_SEC,
        currency as CRNCY,
        fiscvarnt as FISC_YR_VRNT,
        fiscyear as FISC_YR,
        calyear as CAL_YR,
        calmonth2 as CAL_MO,
        calquart1 as QTR,
        calquarter as CAL_YR_QTR,
        calweek as CAL_YR_WK,
        fiscper3 as PSTNG_PER,
        halfyear1 as HALF_YR,
        weekday1 as WKDAY,
        fiscper as FISC_YR_PER,
        calmonth as CAL_YR_MON,
        recvs_val as STCK_REC_VAL,
        issvs_val as ISS_STCK_VAL,
        issblostck as ISS_BLOK_QTY,
        isscnsstck as CNG_STK_QTY_ISS,
        issqmstck as ISS_QTY_QUAL,
        isstransst as ISS_QTY_TRST,
        recblostck as RCPT_QTY_BLOK,
        reccnsstck as CNS_STCK_RCPT,
        recqmstck as RCPT_QTY_QUAL,
        rectransst as RCPT_QTY_TRST,
        issscrp as ISS_QTY_SCRAP,
        issvalscrp as ISS_VAL_SCRAP,
        rectotstck as RCPT_TOT_STCK,
        isstotstck as ISS_TOT_STCK,
        issvalstck as ISS_QTY_STCK_VAL,
        recvalstck as REC_QTY_VAL_STCK,
        venconcon as VDR_CNSGNMNT_STCK_CNVAL,
        recvalqm as REC_VAL_Q_STCK,
        recvalblo as REC_VAL_BLOK,
        issvalbloc as ISS_VAL_BLOK,
        issvalqm as ISS_VAL_Q_STCK,
        rectrfstva as REC_VAL_TRST,
        isstrfstva as ISS_VAL_TRST,
        issccnsval as ISS_VAL_CONS_STCK,
        recccnsval as RCPT_VAL_CONS_STK,
        cppvlc as BW_PRCH_VAL,
        cpquabu as BW_AMT_BUNITM,
        bic_zmddelct as DELV_COST,
        g_avv040 as STD_MATL_COST,
        price_unit as PRC_UNIT,
        bic_zlincount as LINE_CNT,
    current_timestamp()::timestamp_ntz(9)as crt_dttm, 
    current_timestamp()::timestamp_ntz(9)as updt_dttm
    from source
)

select * from final