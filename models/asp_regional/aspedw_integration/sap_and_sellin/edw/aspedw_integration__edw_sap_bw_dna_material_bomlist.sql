{{
    config(
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="table",
        transient=false
    )
}}

with 

source as (
    select * from {{ ref('aspitg_integration__itg_sap_bw_dna_material_bomlist') }}
),

final as (
    select
        *
    from source
)

select * from final