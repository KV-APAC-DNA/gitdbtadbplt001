{{
    config
    (
        materialized="incremental",
        incremental_strategy='append',
        pre_hook= ["delete from {{this}} where (invnt_dt,vend_prod_cd,src_sys_cd,ctry_cd) in
                    (
                        select distinct invnt_dt,vend_prod_cd,src_sys_cd,ctry_cd from
                        (
                            select * from {{ ref('ntawks_integration__wks_itg_pos_poya_invnt') }}
                        )
                        where chng_flg = 'U'
                    );",
                    "delete from {{this}} where (invnt_dt,vend_prod_cd,src_sys_cd,ctry_cd,str_cd) in
                    (
                        select distinct invnt_dt,vend_prod_cd,src_sys_cd,ctry_cd,str_cd from
                        (
                            select * from {{ ref('ntawks_integration__wks_itg_pos_pxcivilia_invnt') }}
                            union all
                            select * from {{ ref('ntawks_integration__wks_itg_pos_rtmart_invnt') }}
                        )
                        where chng_flg = 'U'
                    );"
                ]
    )
}}

with wks_itg_pos_poya_invnt as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_poya_invnt') }}
),
wks_itg_pos_pxcivilia_invnt as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_pxcivilia_invnt') }}
),
wks_itg_pos_rtmart_invnt as (
    select * from {{ ref('ntawks_integration__wks_itg_pos_rtmart_invnt') }}
),
final as 
(    
    SELECT 
        invnt_dt::date as invnt_dt,
        vend_cd::varchar(40) as vend_cd,
        vend_nm::varchar(100) as vend_nm,
        vend_prod_cd::varchar(40) as vend_prod_cd,
        vend_prod_nm::varchar(100) as vend_prod_nm,
        ean_num::varchar(40) as ean_num,
        str_cd::varchar(40) as str_cd,
        str_nm::varchar(100) as str_nm,
        invnt_qty::number(18,0) as invnt_qty,
        invnt_amt::number(16,5) as invnt_amt,
        unit_prc_amt::number(16,5) as unit_prc_amt,
        per_box_qty::number(16,5) as per_box_qty,
        cust_invnt_qty::number(16,5) as cust_invnt_qty,
        box_invnt_qty::number(16,5) as box_invnt_qty,
        wk_hold_sls::number(16,5) as wk_hold_sls,
        wk_hold::number(16,5) as wk_hold,
        fst_recv_dt::varchar(10) as fst_recv_dt,
        dsct_dt::varchar(10) as dsct_dt,
        dc::varchar(40) as dc,
        stk_cls::varchar(40) as stk_cls,
        crncy_cd::varchar(10) as crncy_cd,
        src_sys_cd::varchar(30) as src_sys_cd,
        ctry_cd::varchar(10) as ctry_cd,
        CASE 
            WHEN CHNG_FLG = 'I'
                THEN current_timestamp
            ELSE TGT_CRT_DTTM
            END AS CRT_DTTM,
        current_timestamp AS UPD_DTTM
    FROM 
    (
        select * from wks_itg_pos_poya_invnt
        union all
        select * from wks_itg_pos_pxcivilia_invnt
        union all
        select * from wks_itg_pos_rtmart_invnt
    )a
)
select * from final