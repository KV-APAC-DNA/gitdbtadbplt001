{% test test_lookup__ff(model,failure_reason="Missing records from master table",column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filters=None,additional_filter=None,extra_col=None)%}


{% set column_names = dbt_utils.star(from=model, quote_identifiers=False)
%}

    select 
        '{{failure_reason}}' AS failure_reason,
        {{column_names}}

    from (
        select 
        {{column_names}}
        {% if extra_col!=None %}
        ,    {{extra_col}}
        {% endif %}
        from {{model}}
        where 
            not {{column}} in (
                select distinct
                    {{lookup_column}}
                from  {{lookup_table}}
                {% if lookup_filter !=None %}
                where {{lookup_filter}}
                
    {% endif %}
    )
    {%- if filters !=None -%}
                and    {{filters}}
    {% endif %}
    ) 
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}           
{% endtest %}