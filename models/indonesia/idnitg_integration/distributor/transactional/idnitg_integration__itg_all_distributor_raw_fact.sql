{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook=["delete from {{this}} itg USING DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_CSA_RAW_SELLOUT_SALES_FACT sdl where TRIM(UPPER(nvl(sdl.CABANG, 'NA'))) = TRIM(UPPER(nvl(itg.Col1, 'NA'))) and TRIM(UPPER(nvl(sdl.KODECUST, 'NA'))) = TRIM(UPPER(nvl(itg.Col7, 'NA'))) and TRIM(UPPER(nvl(sdl.SHIPCODE, 'NA'))) = TRIM(UPPER(nvl(itg.Col18, 'NA'))) and TRIM(UPPER(nvl(sdl.KODEBARANG, 'NA'))) = TRIM(UPPER(nvl(itg.Col2, 'NA'))) and TRIM(UPPER(nvl(sdl.NO_INV, 'NA'))) = TRIM(UPPER(nvl(itg.Col5, 'NA'))) and trim(to_date(sdl.TGL_INV)) = trim(itg.Col4) and TRIM(UPPER(itg.dstrbtr_grp_nm)) = TRIM(UPPER('CSA')) and TRIM(UPPER(itg.file_type)) = TRIM(UPPER('Sales'));",
                    "delete from {{this}} itg USING DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_DNR_RAW_SELLOUT_SALES_FACT sdl where TRIM(upper(nvl(sdl.SalesOffice_ID, 'NA'))) = TRIM(upper(nvl(itg.Col1, 'NA'))) and TRIM(upper(nvl(sdl.Cust_ID, 'NA'))) = TRIM(upper(nvl(itg.Col5, 'NA'))) and TRIM(upper(nvl(sdl.Prd_ID, 'NA'))) = TRIM(upper(nvl(itg.Col8, 'NA'))) and TRIM(upper(nvl(sdl.Bill_Doc, 'NA'))) = TRIM(upper(nvl(itg.Col3, 'NA'))) and TRIM(upper(nvl(sdl.Bill_Date, 'NA'))) = TRIM(upper(nvl(itg.Col4, 'NA'))) and trim(upper(itg.dstrbtr_grp_nm)) = 'DNR' and trim(upper(itg.file_type)) = upper('Sales');",
                    "delete from {{this}} itg USING DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_PP_RAW_SELLOUT_SALES_FACT sdl where TRIM(upper(nvl(sdl.KODE_CABANG, 'NA'))) = TRIM(upper(nvl(itg.Col10, 'NA'))) and TRIM(upper(nvl(sdl.KODE_OUTLET, 'NA'))) = TRIM(upper(nvl(itg.Col3, 'NA'))) and TRIM(upper(nvl(sdl.KODE_PRODUK, 'NA'))) = TRIM(upper(nvl(itg.Col4, 'NA'))) and TRIM(upper(nvl(sdl.NO_FAKTUR, 'NA'))) = TRIM(upper(nvl(itg.Col1, 'NA'))) and TRIM(upper(nvl(sdl.TGL_FAKTUR, 'NA'))) = TRIM(upper(nvl(itg.Col2, 'NA'))) and trim(upper(itg.dstrbtr_grp_nm)) = 'PP' and trim(upper(itg.file_type)) = trim(upper('Sales'));",
                    "delete from {{this}} itg USING DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_SDN_RAW_SELLOUT_SALES_FACT sdl where trim(upper(nvl(sdl.KD_CABANG, 'NA'))) = trim(upper(nvl(itg.Col20, 'NA'))) and trim(upper(nvl(sdl.CUST_ID, 'NA'))) = trim(upper(nvl(itg.Col3, 'NA'))) and trim(upper(nvl(sdl.PRODUK_ID, 'NA'))) = trim(upper(nvl(itg.Col10, 'NA'))) and trim(upper(nvl(sdl.FAKTUR, 'NA'))) = trim(upper(nvl(itg.Col2, 'NA')))  and trim(upper(itg.dstrbtr_grp_nm)) = 'SDN' and trim(upper(itg.file_type)) = upper('Sales');",
                    "delete from {{this}} itg USING DEV_DNA_LOAD.SNAPIDNSDL_RAW.SDL_SPR_RAW_SELLOUT_SALES_FACT sdl where trim(upper(nvl(sdl.KD_CABANG, 'NA'))) = trim(upper(nvl(itg.Col20, 'NA'))) and trim(upper(nvl(sdl.CUST_ID, 'NA'))) = trim(upper(nvl(itg.Col3, 'NA'))) and trim(upper(nvl(sdl.PRODUK_ID, 'NA'))) = trim(upper(nvl(itg.Col10, 'NA'))) and trim(upper(nvl(sdl.FAKTUR, 'NA'))) = trim(upper(nvl(itg.Col2, 'NA')))  and trim(upper(itg.dstrbtr_grp_nm)) = upper('SPR') and trim(upper(itg.file_type)) = upper('Sales');"]
    )
}}

with union1 as(
    select * from {{ ref('idnwks_integration__wks_csa_raw_sellout_sales_fact') }}
),
union2 as(
    select * from {{ ref('idnwks_integration__wks_dnr_raw_sellout_sales_fact') }}
),
union3 as(
    select * from {{ ref('idnwks_integration__wks_pp_raw_sellout_sales_fact') }}
),
union4 as(
    select * from {{ ref('idnwks_integration__wks_sdn_raw_sellout_sales_fact') }}
),
union5 as(
    select * from {{ ref('idnwks_integration__wks_spr_raw_sellout_sales_fact') }}
),
transformed as(
    select * from union1 
    union all
    select * from union2 
    union all
    select * from union3 
    union all
    select * from union4 
    union all
    select * from union5 
)
select * from transformed