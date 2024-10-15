{{
    config(
        post_hook = "UPDATE {{this}} 
                    SET C_DSDMNUMBER = 'C99' || TO_CHAR(current_timestamp(), 'YYMMDD') || LPAD(C_DSDMNUMBER, 2, '0');"
        ) 
}} 

with
    source as (select * from {{source('jpdclsdl_raw','kr_special_discount_file')}}),

    transformed as (
        select
            *
        from source
    )

select *
from transformed
