{% macro sap_transaction_processed_files(source_table_name,source_view_name,target_table_name,file_name_column=None) %}
    
{% set create_table %}
    create  table if not exists aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES (
    source_table_name varchar(200),
    source_view_name varchar(200),
    target_table_name varchar(200),
    act_file_name varchar(500),
    inserted_on timestamp_ntz(9),
    is_deleted boolean default 'False'
);
{% endset %}

{% do run_query(create_table) %}

{% set insert_processed_files_records %}

insert into aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES 
with table_ as (
select 
    '{{source_table_name}}' as source_table_name,
    '{{source_view_name}}' as source_view_name,
    '{{target_table_name}}' as target_table_name,
    {% if file_name_column==None %}
        file_name 
      {%else%}
        {{file_name_column}}
    {% endif %}
        as act_file_name
from {{this}}
group by act_file_name
),
processed_files as (
select source_table_name,source_view_name,target_table_name,act_file_name, 'F' as is_deleted
from aspwks_integration.SAP_TRANSACTIONAL_PROCESSED_FILES
where target_table_name='{{target_table_name}}'
)
select t.*,current_timestamp()::timestamp_ntz(9) as inserted_on,'F' as is_deleted from table_ t
where not exists (select act_file_name from processed_files pf where pf.act_file_name=t.act_file_name)

{% endset %}

{% do run_query(insert_processed_files_records) %}
{% endmacro %}