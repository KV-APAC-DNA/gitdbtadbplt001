{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid', 'orderno', 'orderdate', 'arcode', 'linenumber']
    )
}}

with source as(
    select * from {{ source('thasdl_raw','sdl_th_dms_sellout_fact') }}
),
final as 
(
select
  distributorid::varchar(10) as distributorid,
  orderno::varchar(255) as orderno,
  cast(orderdate as timestamp_ntz(9)) as orderdate,
  arcode::varchar(20) as arcode,
  arname::varchar(500) as arname,
  city::varchar(255) as city,
  region::varchar(20) as region,
  saledistrict::varchar(200) as saledistrict,
  saleoffice::varchar(255) as saleoffice,
  salegroup::varchar(255) as salegroup,
  artypecode::varchar(20) as artypecode,
  saleemployee::varchar(255) as saleemployee,
  salename::varchar(350) as salename,
  productcode::varchar(25) as productcode,
  productdesc::varchar(300) as productdesc,
  megabrand::varchar(10) as megabrand,
  brand::varchar(10) as brand,
  baseproduct::varchar(20) as baseproduct,
  variant::varchar(10) as variant,
  putup::varchar(10) as putup,
  cast(grossprice as decimal(19, 6)) as grossprice,
  cast(qty as decimal(19, 6)) as qty,
  cast(subamt1 as decimal(19, 6)) as subamt1,
  cast(discount as decimal(19, 6)) as discount,
  cast(subamt2 as decimal(19, 6)) as subamt2,
  cast(discountbtline as decimal(19, 6)) as discountbtline,
  cast(totalbeforevat as decimal(19, 6)) as totalbeforevat,
  cast(total as decimal(19, 6)) as total,
  cast(linenumber as int) as linenumber,
  cast(iscancel as int) as iscancel,
  cndocno::varchar(255) as cndocno,
  cnreasoncode::varchar(255) as cnreasoncode,
  promotionheader1::varchar(255) as promotionheader1,
  promotionheader2::varchar(255) as promotionheader2,
  promotionheader3::varchar(255) as promotionheader3,
  promodesc1::varchar(255) as promodesc1,
  promodesc2::varchar(255) as promodesc2,
  promodesc3::varchar(255) as promodesc3,
  promocode1::varchar(255) as promocode1,
  promocode2::varchar(255) as promocode2,
  promocode3::varchar(255) as promocode3,
  cast(avgdiscount as decimal(18, 6)) as avgdiscount,
  current_timestamp()::timestamp_ntz(9) as curr_date ,
  null as run_id
  from source
)

select * from final