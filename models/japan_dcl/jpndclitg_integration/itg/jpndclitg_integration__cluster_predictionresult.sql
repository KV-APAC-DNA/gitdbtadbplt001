{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append"       
    )
}}

with cluster_predictionresult as (
    select * from {{source('jpdclsdl_raw','cluster_predictionresult')}}
),
transformed as (
select NVL(LPAD(customer_id,10,'0'),'0000000000') as customer_id,
Cluster5_cd,
'Src_File_Dt' as source_file_date 
from cluster_predictionresult    
),
final as (
select
customer_id::varchar(60) as customer_id,
cluster5_cd::number(18,0) as cluster5_cd,
source_file_date::varchar(30) as source_file_date,
sysdate()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
null::timestamp_ntz(9) as updated_date,
null::varchar(9) as updated_by
from transformed
)
select * from final
