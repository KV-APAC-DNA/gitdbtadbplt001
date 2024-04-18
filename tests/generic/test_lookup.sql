{% test test_lookup(model,select_columns=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filter=None,additional_filter=None,failure_reason="'KEY COLUMN IS NOT PRESENT IN LOOKUP TABLE'")%}

{% if select_columns!=None %}

    select 
        {{failure_reason}} as failure_reason,
        {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}

    from (
        select distinct 
            {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
        from {{model}}
        where 
            ({{column}}) not in (
                select distinct
                    {{lookup_column}}
                from  {{lookup_table}}

                {%- if lookup_filter !=None %}
                where {{lookup_filter}}
                
    {% endif %}
    )
    {%- if filter !=None -%}
                and    {{filter}}
    {% endif %}
    ) 
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}          
{% endif %}
    
{% endtest %}