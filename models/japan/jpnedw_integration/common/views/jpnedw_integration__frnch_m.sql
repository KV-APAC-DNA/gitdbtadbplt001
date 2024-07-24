with edi_frnch_m as(
    select * from {{ ref('jpnedw_integration__edi_frnch_m') }}
),
transformed as(
    SELECT edi_frnch_m.create_dt
        ,edi_frnch_m.create_user
        ,edi_frnch_m.update_dt
        ,edi_frnch_m.update_user
        ,edi_frnch_m.ph_cd
        ,edi_frnch_m.ph_lvl
        ,edi_frnch_m.ph_nm
    FROM edi_frnch_m
)
select * from transformed