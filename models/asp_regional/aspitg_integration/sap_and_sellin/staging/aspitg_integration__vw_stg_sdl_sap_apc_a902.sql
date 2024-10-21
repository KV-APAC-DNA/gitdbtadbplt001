
with source as(
    select * from {{ source('apc_access', 'apc_a902') }}
),
final as(
    select
        mandt as mandt,
        kappl as kappl,
        kschl as kschl,
        vkorg as vkorg,
        matnr as matnr,
        datbi as datbi,
        datab as datab,
        knumh as knumh,
        -- _deleted_ as _deleted_,
        -- _pk_ as _pk_,
        _createtime_::timestamp_ntz(9) as crt_dttm,
        -- _kafkaoffset_ as _kafkaoffset_,
        -- _upt_ as _upt_,
        -- _kafkatimestamp_ as _kafkatimestamp_,
        -- _infa_bigint_sequence_ as _infa_bigint_sequence_,
        pk_md5 as pk_md5,
        null as file_name
    from source
    where _deleted_='F'

)
select * from final