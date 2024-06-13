with sdl_hk_ims_wingkeung_inv as (
    select * from {{ source('ntasdl_raw', 'sdl_hk_ims_wingkeung_inv') }}
),
itg_ims_invnt as (
    -- select * from {{ source('ntaitg_integration', 'itg_ims_invnt_temp') }}
    select * from ntaitg_integration__itg_ims_invnt_temp
),
final as (
    select src.invnt_dt,
        src.dstr_cd,
        dstr_nm,
        src.prod_cd,
        prod_nm,
        src.ean_num,
        cust_nm,
        invnt_qty,
        invnt_amt,
        avg_prc_amt,
        safety_stock,
        bad_invnt_qty,
        book_invnt_qty,
        convs_amt,
        prch_disc_amt,
        end_invnt_qty,
        batch_no,
        uom,
        sls_rep_cd,
        sls_rep_nm,
        src.ctry_cd,
        crncy_cd,
        src.chn_uom,
        tgt.crt_dttm as tgt_crt_dttm,
        convert_timezone('UTC', current_timestamp()) as updt_dttm,
        case
            when tgt.crt_dttm is null then 'I'
            else 'U'
        end as chng_flg
    from (
            select date as invnt_dt,
                '110256' as dstr_cd,
                'WING KEUNG MEDICINE COMPANY LTD.' as dstr_nm,
                prod_code as prod_cd,
                max(chn_desp) as prod_nm,
                null as ean_num,
                --max(customer) as cust_nm,
                'HKW2' as cust_nm,
                sum(closing) as invnt_qty,
                sum(amount) as invnt_amt,
                null as avg_prc_amt,
                null as safety_stock,
                null as bad_invnt_qty,
                null as book_invnt_qty,
                null as convs_amt,
                null as prch_disc_amt,
                null as end_invnt_qty,
                null as batch_no,
                null as uom,
                null as sls_rep_cd,
                null as sls_rep_nm,
                'HK' as ctry_cd,
                'HKD' as crncy_cd,
                max(chn_uom) as chn_uom
            from sdl_hk_ims_wingkeung_inv
            group by date,
                prod_code
        ) src
        left join (
            select invnt_dt,
                prod_cd,
                ean_num,
                ctry_cd,
                crt_dttm
            from itg_ims_invnt
            where dstr_cd = '110256'
        ) tgt on coalesce (src.invnt_dt, '1900-01-01') = coalesce (tgt.invnt_dt, '1900-01-01')
        and coalesce (src.prod_cd, '#') = coalesce (tgt.prod_cd, '#')
        and coalesce (src.ean_num, '#') = coalesce (tgt.ean_num, '#')
        and coalesce (src.ctry_cd, '#') = coalesce (tgt.ctry_cd, '#')
)
select * from final