with source as(
    select * from {{ source('BWA_ACCESS', 'BWA_LIST_PRICE') }}
),
final as(
    select 
    SALESORG as sls_org,
    MATERIAL as material,
    CONDRECNO as cond_rec_no,
    MATL_GROUP as matl_grp,
    VALIDTO as valid_to,
    KNART as knart,
    DATEFROM as dt_from,
    AMOUNT as amount,
    CURRENCY as currency,
    UNIT as unit,
    RECORDMODE as record_mode,
    COMP_CODE as comp_cd,
    PRICE_UNIT as price_unit,
    BIC_ZCURRFPA as zcurrfpa,
    _INGESTIONTIMESTAMP_ as CDL_DTTM,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as crt_dttm,
    NULL as File_Name
    from source
)
select * from final