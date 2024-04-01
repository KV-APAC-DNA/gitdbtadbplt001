{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} WHERE (UPPER(TRIM(distributorid)),UPPER(TRIM(arcode))) IN (SELECT DISTINCT UPPER(TRIM(distributorid)),UPPER(TRIM(arcode)) FROM {{ source('thasdl_raw', 'sdl_th_dms_chana_customer_dim') }});"
    )
}}



with source as(
    select *,
    dense_rank() over(partition by UPPER(TRIM(distributorid)),UPPER(TRIM(arcode)) order by filename desc) as rnk
    from {{ source('thasdl_raw', 'sdl_th_dms_chana_customer_dim') }}
),
final as(
    select
        distributorid::varchar(10) as distributorid,
        arcode::varchar(20) as arcode,
        arname::varchar(500) as arname,
        araddress::varchar(500) as araddress,
        telephone::varchar(150) as telephone,
        fax::varchar(150) as fax,
        city::varchar(500) as city,
        region::varchar(20) as region,
        saledistrict::varchar(200) as saledistrict,
        saleoffice::varchar(10) as saleoffice,
        salegroup::varchar(10) as salegroup,
        artypecode::varchar(10) as artypecode,
        saleemployee::varchar(150) as saleemployee,
        salename::varchar(250) as salename,
        billno::varchar(500) as billno,
        billmoo::varchar(250) as billmoo,
        billsoi::varchar(255) as billsoi,
        billroad::varchar(255) as billroad,
        billsubdist::varchar(30) as billsubdist,
        billdistrict::varchar(30) as billdistrict,
        billprovince::varchar(30) as billprovince,
        billzipcode::varchar(50) as billzipcode,
        activestatus::number(18,0) as activestatus,
        left(routestep1,10)::varchar(10) as routestep1,
        left(routestep2,10)::varchar(10) as routestep2,
        left(routestep3,10)::varchar(10) as routestep3,
        left(routestep4,10)::varchar(10) as routestep4,
        left(routestep5,10)::varchar(10) as routestep5,
        left(routestep6,10)::varchar(10) as routestep6,
        left(routestep7,10)::varchar(10) as routestep7,
        left(routestep8,10)::varchar(10) as routestep8,
        left(routestep9,10)::varchar(10) as routestep9,
        left(routestep10,10)::varchar(10) as routestep10,
        store::varchar(200) as store,
        pricelevel::varchar(50) as pricelevel,
        salesareaname::varchar(150) as salesareaname,
        branchcode::varchar(50) as branchcode,
        branchname::varchar(150) as branchname,
        frequencyofvisit::varchar(50) as frequencyofvisit,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        current_timestamp()::timestamp_ntz(9) as crt_dttm
    from source
    where rnk=1
)
select * from final