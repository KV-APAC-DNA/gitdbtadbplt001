with edw_distributor_ivy_user_master as 
(
    select * from idnedw_integration.edw_distributor_ivy_user_master
),
itg_distributor_salesman_dim as 
(
    select * from idnitg_integration.itg_distributor_salesman_dim
),
final3 as
(
    select 
    distinct 
       dis_code::varchar(20) as jj_sap_dstrbtr_id,
       dis_name::varchar(200) as jj_sap_dstrbtr_nm,
       sr_code::varchar(100) as slsmn_id,
       sr_name::varchar(100) as slsmn_nm,
       (dis_code||sr_code)::varchar(70) as rec_key,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as crtd_dttm,
       convert_timezone('UTC',current_timestamp())::timestamp_ntz(9) as updt_dttm,
       sr_code::varchar(255) as sfa_id,
	   '200001'::varchar(10) as effective_from,
	   '999912'::varchar(10) as effective_to 
    from edw_distributor_ivy_user_master um
    WHERE NOT EXISTS (SELECT 1
                  FROM {{this}} sd
                  WHERE (UPPER(TRIM(um.dis_code)) ||UPPER(TRIM(um.sr_code))) = UPPER(TRIM(rec_key)))
AND   (UPPER(TRIM(um.dis_code)) <> '' AND UPPER(TRIM(um.sr_code)) <> '')
AND   (UPPER(TRIM(um.dis_code)) IS NOT NULL AND UPPER(TRIM(um.sr_code)) IS NOT NULL)
),


final as 
(
    select 
        jj_sap_dstrbtr_id::varchar(20) as jj_sap_dstrbtr_id,
        jj_sap_dstrbtr_nm::varchar(200) as jj_sap_dstrbtr_nm,
        slsmn_id::varchar(100) as slsmn_id,
        slsmn_nm::varchar(100) as slsmn_nm,
        rec_key::varchar(70) as rec_key,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm,
        sfa_id::varchar(255) as sfa_id,
        effective_from::varchar(10) as effective_from,
        effective_to::varchar(10) as effective_to
    from itg_distributor_salesman_dim
)

,final2 as 
(
    select * from final 
    union all
    select * from final3
)
select * from final2
