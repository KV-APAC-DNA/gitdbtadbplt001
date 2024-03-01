with source as(
    select * from {{ ref('aspedw_integration__edw_account_ciw_dim') }}
),
transformed as(
    select
        YEAR(CONVERT_TIMEZONE('Asia/Bangkok', CURRENT_TIMESTAMP())) AS year,
        case
            when (
            substring(bravo_acct_l4, 7, 3) = '513' or substring(bravo_acct_l4, 7, 3) = '514'
            )
            THEN 'TP'
            when (
            substring(bravo_acct_l4, 7, 3) = '512' and substring(bravo_acct_l4, 7, 9) <> '512200'
            )
            THEN 'TT'
            when substring(bravo_acct_l4, 7, 9) = '512200'
            then 'Return'
            else null
            end as area,
            ciw_acct_l3_txt as category,
            ciw_acct_l4_txt as account_name,
            ltrim(acct_num, 0) as account_num,
            to_char(current_timestamp()::timestampntz(9), 'YYYYMMDDHH24MISSFF3')::varchar(255) as cdl_dttm,
            current_timestamp()::timestampntz(9) as crtd_dttm,
            current_timestamp()::timestampntz(9) as updt_dttm
    from source
    where upper(chrt_acct) = 'CCOA'
    and (
        substring(bravo_acct_l4, 7, 3) = '512'
        or substring(bravo_acct_l4, 7, 3) = '513'
        or substring(bravo_acct_l4, 7, 3) = '514'
    )
),
final as(
    select
        year::number(18,0) as year,
        area::varchar(50) as area,
        category::varchar(256) as category,
        account_name::varchar(256) as account_name,
        account_num::varchar(50) as account_num,
        cdl_dttm::varchar(255) as cdl_dttm,
        crtd_dttm as crtd_dttm,
        updt_dttm as updt_dttm
    from transformed
)
select * from final