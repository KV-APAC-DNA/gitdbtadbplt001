{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy= "merge",
        unique_key=  ['fisc_yr_per','vers'],
        merge_exclude_columns=["crt_dttm"]
    )
}}

--import CTE

with source as (
    select * from {{ ref('aspwks_integration__wks_itg_copa17_trans')}}
),

--Logical CTE

final as(
 SELECT
    request_number as REQUEST_NUMBER,
    data_packet as DATA_PACKET,
    data_record as DATA_RECORD,
    fiscper as FISC_YR_PER,
    fiscvarnt as FISC_YR_VRNT,
    fiscyear as FISC_YR,
    calday as CAL_DAY,
    fiscper3 as PSTNG_PER,
    calmonth as CAL_YR_MO,
    calyear as CAL_YR,
    version as VERS,
    vtype as VAL_TYPE,
    comp_code as CO_CD,
    co_area as CNTL_AREA,
    profit_ctr as PRFT_CTR,
    salesemply as SLS_EMP_HIST,
    salesorg as SLS_ORG,
    sales_grp as SLS_GRP,
    sales_off as SLS_OFF,
    cust_group as CUST_GRP,
    distr_chan as DSTN_CHNL,
    sales_dist as SLS_DSTRC,
    customer as CUST,
    material as MATL,
    cust_sales as CUST_SLS_VIEW,
    division as DIV,
    plant as PLNT,
    zmercref as MERCIA_REF,
    zz_mvgr1 as B3_BASE_PROD,
    zz_mvgr2 as B4_VRNT,
    zz_mvgr3 as B5_PUT_UP,
    zz_mvgr4 as B1_MEGA_BRND,
    zz_mvgr5 as B2_BRND,
    region as RGN,
    country as CTRY,
    prodh6 as PROD_MINOR,
    prodh5 as PROD_MAJ,
    prodh4 as PROD_FRAN,
    prodh3 as FRAN,
    prodh2 as FRAN_GRP,
    prodh1 as OPER_GRP,
    zfis_quar as FISC_QTR,
    mat_sales as MATL2,
    bill_type as BILL_TYPE,
    jjfiscwe as FISC_WK,
    amocac as AMT_GRP_CRCY,
    amoccc as AMT_OBJ_CRCY,
    currency as CRNCY,
    obj_curr as OBJ_CRNCY,
    account as ACCT_NUM,
    chrt_accts as CHRT_OF_ACCT,
    zz_wwme as MGMT_ENTITY,
    zsalesper as SLS_PRSN_RESPONS,
    bus_area as BUSN_AREA,
    grossamttc as GA,
    curkey_tc as TC,
    mat_plant as MATL_PLNT_VIEW,
    quantity as QTY,
    unit as UOM,
    zqtyieu as SLS_VOL_IEU,
    zunitieu as UN_SLS_VOL__IEU,
    zbpt_dc as BPT_DSTN_CHNL,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
--final select
select * from final