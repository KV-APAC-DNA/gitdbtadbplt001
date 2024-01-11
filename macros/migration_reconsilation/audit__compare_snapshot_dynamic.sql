{% macro audit__compare_snapshot_dynamic() %}
{% set input_list=[
['DEV_DNA_CORE','snapaspedw_integration','edw_list_price',ref('aspedw_integration__edw_list_price'),"md5(concat(sls_org,'_',material,'_',cond_rec_no,'_',cdl_dttm))"]    
]
%}
--drop table if exists {{target.schema}}.model_validations;
create table if not exists {{target.schema}}.model_validations(
    model_name  text,
    column_name  text,
    perfect_match  number,
    null_in_a  number,
    null_in_b  number,
    missing_from_a  number,
    missing_from_b  number,
    conflicting_values  number
);
{% for item in input_list %}
insert into {{target.schema}}.model_validations
with {{item[2]}} as (
    {% set c_pk= item[4]%}
    {{
        audit_helper.compare_all_columns(
            a_relation=api.Relation.create(
                database=item[0],
                schema=item[1],
                identifier=item[2]
            ),
            b_relation=item[3],
            primary_key=c_pk
        )
    }}
)
select '{{item[2]}}' as model_name,* from {{item[2]}};
{% endfor %}
{% endmacro %}