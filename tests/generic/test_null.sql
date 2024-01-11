{% test test_null(model,not_null_columns=None)%}

{% if not_null_columns!=None %}

    select 

        'KEY COLUMN IS NULL/BLANK' AS failure_reason,
        {%- for item in not_null_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    from {{model}}
    where 
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} OR
            {%- endif -%}
        {% endfor %}
{% endif %}
    
{% endtest %}