{{
    config(
        sql_header= "ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized= "incremental",
        incremental_strategy= "merge",
        unique_key= ["request_number","data_packet","data_record"],
        merge_exclude_columns= ["crt_dttm"]
    )
}}

--Import CTE
with source as (
    select * from {{ ref('aspwks_integration__wks_itg_copa_trans') }}
),

--Logical CTE

--Final CTE
final as (
    select
        request_number as REQUEST_NUMBER,
        data_packet as DATA_PACKET,
        data_record as DATA_RECORD,
        comp_code as CO_CD ,
        co_area as CNTL_AREA ,
        profit_ctr as PRFT_CTR ,
        salesorg as SLS_ORG ,
        material as MATL ,
        customer as CUST_NUM ,
        division as DIV ,
        plant as PLNT ,
        chrt_accts as CHRT_ACCT ,
        account as ACCT_NUM ,
        distr_chan as DSTR_CHNL ,
        fiscvarnt as FISC_YR_VAR ,
        version as VERS ,
        recordmode as BW_DELTA_UPD_MODE ,
        bill_type as BILL_TYP ,
        sales_off as SLS_OFF ,
        country as CNTRY_KEY ,
        salesdeal as SLS_DEAL ,
        sales_grp as SLS_GRP ,
        salesemply as SLS_EMP_HIST ,
        sales_dist as SLS_DIST ,
        cust_group as CUST_GRP ,
        cust_sales as CUST_SLS ,
        bus_area as BUSS_AREA ,
        vtype as VAL_TYPE_RPT ,
        zmercref as MERCIA_REF ,
        calday as CALN_DAY ,
        calmonth as CALN_YR_MO ,
        fiscyear as FISC_YR ,
        fiscper3 as PSTNG_PER ,
        fiscper as FISC_YR_PER ,
        zz_mvgr1 as B_BASE_PROD ,
        zz_mvgr2 as B_VAR ,
        zz_mvgr3 as B_PUT_UP ,
        zz_mvgr4 as B_MEGA_BRND ,
        zz_mvgr5 as B_BRND ,
        region as REG ,
        prodh6 as PROD_MINOR ,
        prodh5 as PROD_MAJ ,
        prodh4 as PROD_FRAN ,
        prodh3 as FRAN ,
        prodh2 as GRAN_GRP ,
        prodh1 as OPER_GRP ,
        zsalesper as SLS_PRSN_RESP ,
        mat_sales as MATL_SLS ,
        prod_hier as PROD_HIER ,
        zz_wwme as MGMT_ENTITY ,
        zfamocac as FX_AMT_CNTL_AREA_CRNCY ,
        amocac as AMT_CNTL_AREA_CRNCY ,
        currency as CRNCY_KEY ,
        amoccc as AMT_OBJ_CRNCY ,
        obj_curr as OBJ_CRNCY_CO_OBJ ,
        grossamttc as GRS_AMT_TRANS_CRNCY ,
        curkey_tc as CRNCY_KEY_TRANS_CRNCY ,
        quantity as QTY ,
        unit as UOM ,
        zqtyieu as SLS_VOL ,
        zunitieu as UN_SLS_VOL ,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

--Final select
select * from final 