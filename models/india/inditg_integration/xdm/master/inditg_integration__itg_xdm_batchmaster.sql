with source as
(
    select * from {{ source('indsdl_raw', 'sdl_xdm_batchmaster') }}
),
final as
(
    SELECT statecode::varchar(10) as statecode,
        prodcode::varchar(50) as prodcode,
        mrp::number(18,6) as mrp,
        lsp::number(18,6) as lsp,
        sellrate::number(18,6) as sellrate,
        purchrate::number(18,6) as purchrate,
        claimrate::number(18,6) as claimrate,
        netrate::number(18,6) as netrate,
        createddt::timestamp_ntz(9) as createddt,
        filename::varchar(100) as filename,
        run_id::varchar(50) as run_id,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        current_timestamp()::timestamp_ntz AS updt_dttm
       from source
)
select * from final