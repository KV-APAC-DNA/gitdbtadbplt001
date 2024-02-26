{% macro test_lookup__ff(model,failure_reason=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filters=None,additional_filter=None)%}


{% set column_names = dbt_utils.star(
    from=model)
%}

    select 
        '{{failure_reason}}' AS failure_reason,
        {{column_names}}

    from (
        select 
        {{column_names}}
        from {{model}}
        where 
            not ({{column}}) in (
                select distinct
                    ({{lookup_column}})
                from  {{lookup_table}}
                {%- if lookup_filter !=None -%}
                where {{lookup_filter}}
                
    {% endif %}
    )
    {%- if filters !=None -%}
                and    {{filter}}
    {% endif %}
    ) 
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}           
{% endmacro %}