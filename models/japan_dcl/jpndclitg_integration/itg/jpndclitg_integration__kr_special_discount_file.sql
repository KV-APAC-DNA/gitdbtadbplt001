 

with
    source as (select * from dev_dna_load.snapjpdclsdl_raw.kr_special_discount_file),

    transformed as (
        select
            patternid,
            ctl_flg,
            c_dichanelid,
            c_dsdmnumber,
            c_dsdmsendkubun,
            c_dsdmsenddate,
            c_dsdmname,
            c_dsextension1,
            c_dsextension2,
            c_dsextension3,
            c_dsextension4,
            c_dsextension5,
            dsitemid,
            dsitemname,
            extraction_date,
            source_file_date,
            inserted_date,
            inserted_by,
            updated_date,
            updated_by
        from source
    )

select *
from transformed
