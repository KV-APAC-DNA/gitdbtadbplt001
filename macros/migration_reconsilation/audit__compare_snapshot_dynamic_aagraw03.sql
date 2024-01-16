{% macro audit__compare_snapshot_dynamic_aagraw03() %}
{% set input_list=[
    ['DEV_DNA_CORE','snapaspedw_integration','edw_account_dim',ref('aspedw_integration__edw_account_dim'),"md5(concat(chrt_acct,'_',acct_num,'_',obj_ver))"],
    ['DEV_DNA_CORE','snapaspedw_integration','edw_material_plant_dim',ref('aspedw_integration__edw_material_plant_dim'),"md5(concat(plnt,'_',matl_plnt_view))"],
    ['DEV_DNA_CORE','snapaspedw_integration','edw_material_sales_dim',ref('aspedw_integration__edw_material_sales_dim'),"md5(concat(sls_org,'_',dstr_chnl,'_',matl_num))"],
    ['DEV_DNA_CORE','snapaspedw_integration','edw_plant_dim',ref('aspedw_integration__edw_plant_dim'),"md5(concat(plnt))"],
    ['DEV_DNA_CORE','snapaspedw_integration','edw_sap_bw_dna_material_bomlist',ref('aspedw_integration__edw_sap_bw_dna_material_bomlist'),"md5(concat(bom,'_',plant,'_',matl_num,'_',component,'_',createdon,'_',validfrom,'_',validto,'_',validfrom_zvlfromc,'_',validto_zvltoi))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_acct',ref('aspitg_integration__itg_acct'),"md5(concat(chrt_acct,'_',acct_num,'_',obj_ver))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_acct_text',ref('aspitg_integration__itg_acct_text'),"md5(concat(chrt_acct,'_',acct_num,'_',lang_key))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_matl_plnt',ref('aspitg_integration__itg_matl_plnt'),"md5(concat(plnt,'_',matl_plnt_view))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_matl_plnt_text',ref('aspitg_integration__itg_matl_plnt_text'),"md5(concat(plnt,'_',plnt_mat,'_',lang_key))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_matl_sls',ref('aspitg_integration__itg_matl_sls'),"md5(concat(sls_org,'_',dstr_chnl,'_',matl))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_matl_sls_text',ref('aspitg_integration__itg_matl_sls_text'),"md5(concat(sls_org,'_',dstn_chnl,'_',mat_sls,'_',lang_key))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_plnt',ref('aspitg_integration__itg_plnt'),"md5(concat(plnt))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_plnt_text',ref('aspitg_integration__itg_plnt_text'),"md5(concat(plnt))"],
    ['DEV_DNA_CORE','snapaspitg_integration','itg_sap_bw_dna_material_bomlist',ref('aspitg_integration__itg_sap_bw_dna_material_bomlist'),"md5(concat(zbomno,'_',plant,'_',material,'_',component,'_',createdon,'_',validfrom,'_',validto,'_',zvlfromc,'_',zvltoi))"]
]
%}
--drop table if exists {{target.schema}}.model_validations;
{% set db=env_var('DBT_ENV_CORE_DB') %}
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
                database=db,
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
