
{#
Note to use this macro audit_helper needs to be installed first
#}
{% macro compare_snapshot_static(primary_key=None,src_database=None,src_schema=None,src_table=None,tgt_model=None) %}
    



{% if primary_key==None %}
    {{ log("""
    ========================================================
    Primary Key is not passed correctly.
        Primary Key should be passed a a list of columns. 
        Ex. ['col1','col2'], in case of 1 column is pk ['col1']
    ========================================================
    """, info=True) }}
    {{ exceptions.raise_compiler_error("""Primary Key is not passed correctly.
        Primary Key should be passed a a list of columns. 
        Ex. ['col1','col2'], in case of 1 column is pk ['col1']
        """) }}
{% elif src_database==None or src_schema==None or src_table==None or tgt_model==None%}
    {{ log("""
    ========================================================
    Either of the below required fields are not specified:
    1.src_database -> Source database in which your table to compare with target model resides in.
    2.src_schema -> Source schema in which your table to compare with target model resides in.
    3. src_table -> Source table which is your table to compare with target model.
    4. tgt_model -> Your target model which needs to be compared.
    ========================================================
    """, info=True) }}
    {{ exceptions.raise_compiler_error("""Either of the below required fields are not specified:
    1.src_database -> Source database in which your table to compare with target model resides in.
    2.src_schema -> Source schema in which your table to compare with target model resides in.
    3. src_table -> Source table which is your table to compare with target model.
    4. tgt_model -> Your target model which needs to be compared.
        """) }}
{%else%}

    {% set concatenated_primary_key = primary_key|join(",'_',") %}
    {% set c_pk= "md5(concat(" + concatenated_primary_key + "))" %}
    {{
    audit_helper.compare_all_columns(
        a_relation=api.Relation.create(
            database=src_database,
            schema=src_schema,
            identifier=src_table
        ),
        b_relation=tgt_model,
        primary_key=c_pk
    )
    }}
{% endif %}



{% endmacro %}
