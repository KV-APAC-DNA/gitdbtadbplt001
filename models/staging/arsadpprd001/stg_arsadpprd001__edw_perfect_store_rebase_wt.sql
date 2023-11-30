with 

source as (

    select * from {{ source('arsadpprd001', 'edw_perfect_store_rebase_wt') }}

),

renamed as (

    select

    from source

)

select * from renamed
