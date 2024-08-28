with source as
(
    select * from {{ ref('indwks_integration__wks_lks_pricelist') }}
),
final as 
(
    SELECT
    productcode::varchar(50) as productcode,
    productname::varchar(100) as productname,
    pack::varchar(100) as pack,
    mrpperpack::float as mrpperpack,
    billingunit::float as billingunit,
    mrpperunit::float as mrpperunit,
    zdst::float as zdst,
    retailermarginpercent::float as retailermarginpercent,
    retailermarginamt::float as retailermarginamt,
    retailerprice::float as retailerprice,
    td::float as td,
    excisedutyper::float as excisedutyper,
    vatrds2ret::float as vatrds2ret,
    vatrds2retamt::float as vatrds2retamt,
    baseprice::float as baseprice,
    tdamt::float as tdamt,
    vatjnj2rds::float as vatjnj2rds,
    vatjnj2rdsamt::float as vatjnj2rdsamt,
    listprice::float as listprice,
    cd::float as cd,
    excisedutyamt::float as excisedutyamt,
    excisecessamt::float as excisecessamt,
    excisedutytot::float as excisedutytot,
    nr::float as nr,
    state::varchar(100) as state,
    startdate::timestamp_ntz(9) as startdate,
    enddate::varchar(100) as enddate,
    insertedon::timestamp_ntz(9) as insertedon,
    statecode::number(18,0) as statecode,
    userid::varchar(200) as userid,
    cststate::varchar(10) as cststate,
    calculateflag::varchar(10) as calculateflag,
    filecode::number(18,0) as filecode,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
    file_name::varchar as file_name
    FROM source
)
select * from final