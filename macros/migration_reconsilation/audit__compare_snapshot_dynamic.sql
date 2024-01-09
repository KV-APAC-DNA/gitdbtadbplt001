{% macro audit__compare_snapshot_dynamic() %}
{% set input_list=[
    ['DEV_DNA_CORE','snapaspitg_integration','itg_copa17_trans',ref('aspitg_integration__itg_copa17_trans'),"md5(concat(fisc_yr_per,'_',vers))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_invnt',ref('aspitg_integration__itg_invnt'),"md5(concat(request_number,'_',data_packet,'_',data_record))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_invc_sls',ref('aspitg_integration__itg_invc_sls'),"md5(concat(request_number,'_',data_packet,'_',data_record))"]
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
select "{{item[2]}}" as model_name,* from {{item[2]}};
{% endfor %}
{% endmacro %}