{% test test_lookup_year(model,select_columns=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filter=None,additional_filter=None)%}

{% if select_columns!=None %}

    select 
        'KEY COLUMN IS NOT PRESENT IN LOOKUP TABLE' AS failure_reason,
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
        {%- for item in select_columns %}

             trim({{item}}) in (
                select distinct
                    cast(A.{{item}} as decimal(31)) 
                from  {{lookup_table}} as A,(SELECT
        CAST(TRIM({{item}}) AS DECIMAL(31)) AS "YEAR",
        REGEXP_LIKE(CAST(TRIM{{item}} AS DECIMAL(31)), '(20)[0-9]{2}') AS RESULT
      FROM {{lookup_table}}
    ) AS B 
    {% endfor %}
                {%- if lookup_filter !=None -%}
                 {% set space_before_where = ' ' %}
                 {{ space_before_where }}where {{lookup_filter}}
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