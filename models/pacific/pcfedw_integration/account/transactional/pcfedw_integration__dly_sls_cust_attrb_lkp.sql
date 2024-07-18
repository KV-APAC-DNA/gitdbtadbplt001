with source as
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_cust_attrb_temp') }}
),
final as
(
select 
sls_org_code::varchar(500) as sls_org_code,
sls_org_name::varchar(500) as sls_org_name,
sls_org_id::number(20,4) as sls_org_id,
cmp_id_code::varchar(500) as cmp_id_code,
cmp_id_name::varchar(500) as cmp_id_name,
cmp_id_id::number(20,4) as cmp_id_id,
dstr_chnl_code::varchar(500) as dstr_chnl_code,
dstr_chnl_name::varchar(500) as dstr_chnl_name,
dstr_chnl_id::number(20,4) as dstr_chnl_id,
sls_ofc_code::varchar(500) as sls_ofc_code,
sls_ofc_name::varchar(500) as sls_ofc_name,
sls_ofc_id::number(20,4) as sls_ofc_id,
sls_grp_code::varchar(500) as sls_grp_code,
sls_grp_name::varchar(500) as sls_grp_name,
sls_grp_id::number(20,4) as sls_grp_id,
fcst_chnl_code::varchar(500) as fcst_chnl_code,
fcst_chnl_name::varchar(500) as fcst_chnl_name,
fcst_chnl_id::number(20,4) as fcst_chnl_id,
customer_segmentation_level_1_code::varchar(500) as customer_segmentation_level_1_code,
customer_segmentation_level_1_name::varchar(500) as customer_segmentation_level_1_name,
customer_segmentation_level_1_id::number(20,4) as customer_segmentation_level_1_id,
customer_segmentation_level_2_code::varchar(500) as customer_segmentation_level_2_code,
customer_segmentation_level_2_name::varchar(500) as customer_segmentation_level_2_name,
customer_segmentation_level_2_id::number(20,4) as customer_segmentation_level_2_id,
active_status_code::varchar(500) as active_status_code,
active_status_name::varchar(500) as active_status_name,
active_status_id::number(20,4) as active_status_id,
current_timestamp()::timestamp_ntz(9) as crt_dttm
from source
)
select * from final