with source as(
    select * from {{ ref('ntawks_integration__wks_edw_ims_invnt_std') }}
),
final as(
    select 
        to_date(invnt_dt) as invnt_dt,
        dstr_cd::varchar(30) as dstr_cd,
        dstr_nm::varchar(100) as dstr_nm,
        prod_cd::varchar(20) as prod_cd,
        prod_nm::varchar(200) as prod_nm,
        ean_num::varchar(20) as ean_num,
        cust_nm::varchar(100) as cust_nm,
        invnt_qty::number(21,5) as invnt_qty,
        invnt_amt::number(21,5) as invnt_amt,
        avg_prc_amt::number(21,5) as avg_prc_amt,
        safety_stock::number(21,5) as safety_stock,
        bad_invnt_qty::number(21,5) as bad_invnt_qty,
        book_invnt_qty::number(21,5) as book_invnt_qty,
        convs_amt::number(21,5) as convs_amt,
        prch_disc_amt::number(21,5) as prch_disc_amt,
        end_invnt_qty::number(21,5) as end_invnt_qty,
        batch_no::varchar(20) as batch_no,
        uom::varchar(20) as uom,
        sls_rep_cd::varchar(20) as sls_rep_cd,
        sls_rep_nm::varchar(50) as sls_rep_nm,
        ctry_cd::varchar(2) as ctry_cd,
        crncy_cd::varchar(3) as crncy_cd,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        chn_uom::varchar(100) as chn_uom,
        storage_name::varchar(50) as storage_name,
        area::varchar(50) as area
    from source
)
select * from final
