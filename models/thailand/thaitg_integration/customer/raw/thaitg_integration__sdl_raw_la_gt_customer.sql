{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as(
    select * from {{ source('thasdl_raw', 'sdl_la_gt_customer') }}
),
final as(
    select 
           distributorid as distributorid,
       arcode as arcode,
       arname as arname,
       araddress as araddress,
       telephone as telephone,
       fax as fax,
       city as city,
       region as region,
       saledistrict as saledistrict,
       saleoffice as saleoffice,
       salegroup as salegroup,
       artypecode as artypecode,
       saleemployee as saleemployee,
       salename as salename,
       billno as billno,
       billmoo as billmoo,
       billsoi as billsoi,
       billroad as billroad,
       billsubdist as billsubdist,
       billdistrict as billdistrict,
       billprovince as billprovince,
       billzipcode as billzipcode,
       activestatus as activestatus,
       routestep1 as routestep1,
       routestep2 as routestep2,
       routestep3 as routestep3,
       routestep4 as routestep4,
       routestep5 as routestep5,
       routestep6 as routestep6,
       routestep7 as routestep7,
       latitude as latitude,
       longitude as longitude,
       routestep10 as routestep10,
       store as store,
       pricelevel as pricelevel,
       salesareaname as salesareaname,
       branchcode as branchcode,
       branchname as branchname,
       frequencyofvisit as frequencyofvisit,
       filename as filename,
       run_id as run_id,
       crt_dttm as crt_dttm
    from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)
select * from final