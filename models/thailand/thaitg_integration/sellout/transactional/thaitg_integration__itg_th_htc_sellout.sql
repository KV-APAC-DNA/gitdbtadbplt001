{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','orderno','orderdate','arcode','linenumber']
    )
}}


with sdl_mds_th_htc_sellout as (
  select * from {{ source('thasdl_raw', 'sdl_mds_th_htc_sellout') }}
  ),
  final as (
  select
    trim(distributorid)::varchar(10) as distributorid,
    trim(invoice)::varchar(255) as orderno,
    trim(date_invoice)::timestamp_ntz(9) as orderdate,
    trim(customer_code)::varchar(20) as arcode,
    trim(customer_name)::varchar(500) as arname,
    trim(city)::varchar(255) as city,
    trim(region)::varchar(20) as region,
    trim(saledistrict)::varchar(200) as saledistrict,
    trim(saleoffice)::varchar(255) as saleoffice,
    trim(saleareacode)::varchar(255) as salegroup,
    trim(artypecode)::varchar(20) as artypecode,
    trim(saleemployee)::varchar(255) as saleemployee,
    trim(salename)::varchar(350) as salename,
    trim(product_code)::varchar(25) as productcode,
    trim(product_name)::varchar(300) as productdesc,
    trim(megabrand)::varchar(10) as megabrand,
    trim(brand)::varchar(10) as brand,
    trim(baseproduct)::varchar(20) as baseproduct,
    trim(variant)::varchar(10) as variant,
    trim(putup)::varchar(10) as putup,
    trim(grossprice)::number(19,6) as grossprice,
    trim(qty_pcs)::number(19,6) as qty,
    trim("sum of subamt")::number(19,6) as subamt1,
    trim(discount_amount)::number(19,6) as discount,
    trim(sub_amt_2)::number(19,6) as subamt2,
    trim(discount_btline)::number(19,6) as discountbtline,
    trim(totalbeforevat)::number(19,6) as totalbeforevat,
    trim(total)::number(19,6) as total,
    trim(linenumber)::number(18,0) as linenumber,
    trim(iscancel)::number(18,0) as iscancel,
    trim(cndocno)::varchar(255) as cndocno,
    trim(cnreasoncode)::varchar(255) as cnreasoncode,
    trim(promotioncode3)::varchar(255) as promotionheader1,
    trim(promotioncode4)::varchar(255) as promotionheader2,
    trim(promotioncode5)::varchar(255) as promotionheader3,
    trim(promotion_code)::varchar(255) as promodesc1,
    trim(promotioncode1)::varchar(255) as promodesc2,
    trim(promotioncode2)::varchar(255) as promodesc3,
    trim(promo_id)::varchar(255) as promocode1,
    trim(promotion_code2)::varchar(255) as promocode2,
    trim(promotion_code3)::varchar(255) as promocode3,
    trim(0)::number(18,4) as avgdiscount,
    ''::varchar(50) as run_id,
    current_timestamp()::timestamp_ntz(9) as crt_dttm
  from sdl_mds_th_htc_sellout
  )
select * from final
  