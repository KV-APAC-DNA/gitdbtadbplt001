{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook = "{% if is_incremental() %}
                DELETE FROM {{this}}
                  WHERE modifieddate >=(
                  SELECT min(modifieddate)
                  FROM {{ source('indsdl_raw', 'sdl_csl_schemeutilization') }}
                  where DATEDIFF(day, CreatedDate, ModifiedDate) <= 7
              );
               DELETE FROM {{this}}
                  WHERE distcode || invoiceno IN (
                  select distcode || invoiceno
                  FROM {{ source('indsdl_raw', 'sdl_csl_schemeutilization') }}
                  where DATEDIFF(day, CreatedDate, ModifiedDate) > 7
              );
                {% endif %}"
    )
}}

with source as 
(
    select * from {{ source('indsdl_raw', 'sdl_csl_schemeutilization') }}
),
final as
(
    select
        distcode::varchar(50) as distcode,
        schemecode::varchar(50) as schemecode,
        schemedescription::varchar(200) as schemedescription,
        invoiceno::varchar(50) as invoiceno,
        rtrcode::varchar(50) as rtrcode,
        company::varchar(100) as company,
        schdate::timestamp_ntz(9) as schdate,
        schemetype::varchar(50) as schemetype,
        schemeutilizedamt::number(18,6) as schemeutilizedamt,
        schemefreeproduct::varchar(50) as schemefreeproduct,
        schemeutilizedqty::number(18,0) as schemeutilizedqty,
        companyschemecode::varchar(50) as companyschemecode,
        createddate::timestamp_ntz(9) as createddate,
        migrationflag::varchar(1) as migrationflag,
        schememode::varchar(50) as schememode,
        syncid::number(38,0) as syncid,
        schlinecount::number(18,0) as schlinecount,
        schvaluetype::varchar(100) as schvaluetype,
        slabid::number(18,0) as slabid,
        billedprdccode::varchar(100) as billedprdccode,
        billedprdbatcode::varchar(100) as billedprdbatcode,
        billedqty::number(18,0) as billedqty,
        schdiscperc::number(38,6) as schdiscperc,
        freeprdbatcode::varchar(100) as freeprdbatcode,
        billedrate::number(38,6) as billedrate,
        modifieddate::timestamp_ntz(9) as modifieddate,
        servicecrnrefno::varchar(100) as servicecrnrefno,
        rtrurccode::varchar(100) as rtrurccode,
        current_timestamp()::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)
select * from final
