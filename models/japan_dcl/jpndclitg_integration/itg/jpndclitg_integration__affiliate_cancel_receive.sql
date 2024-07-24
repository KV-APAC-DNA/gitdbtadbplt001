with source 
as
(
select * from DEV_DNA_LOAD.SNAPJPDCLSDL_RAW.AFFILIATE_CANCEL_RECEIVE
)

,

transformed 
as
(
    select 
    achievement,
    click_dt,
    accrual_dt,
    asp,
    unique_id,
    media_name,
    asp_control_no,
    sale_num,
    amount_including_tax,
    amount_excluded_tax,
    orderdate,
    webid,
    NULL as status,
    NULL as SOURCE_FILE_DATE,
    from source
)

select * from transformed