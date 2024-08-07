{% test test_null(model,not_null_columns=None,select_columns=None,filter=none,condition="OR")%}
{% if not_null_columns!=None %}
    select
        'KEY COLUMN IS NULL/BLANK' AS failure_reason,
    {% if select_columns!=None %}
        {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    {% else %}
        {%- for item in not_null_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    {% endif %}
    from {{model}}
    where
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} {{condition}}
            {%- endif -%}
        {% endfor %}
{%- endif -%}
{%- if filter !=None %}
                and {{filter}} 
            {% endif %}
{% endtest %}