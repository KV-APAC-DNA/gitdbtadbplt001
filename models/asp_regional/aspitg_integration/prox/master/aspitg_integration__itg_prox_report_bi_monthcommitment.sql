with source as
(
    select * from {{ source('aspsdl_raw', 'sdl_prox_report_bi_monthcommitment') }}
),
final as
(
    select rowid::number(38,0) as rowid,
    contractno::varchar(50) as contractno,
    expenseid::varchar(50) as expenseid,
    year::number(38,0) as year,
    expensemonth::number(38,0) as expensemonth,
    financemonth::number(38,0) as financemonth,
    commitment::number(18,2) as commitment,
    flag::varchar(50) as flag,
    applicationid::varchar(50) as applicationid,
    filename::varchar(100) as filename,
    run_id::varchar(50) as run_id,
    crt_dttm::timestamp_ntz(9) as crt_dttm
    from source
)
select * from final