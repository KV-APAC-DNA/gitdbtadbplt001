{% test test_null__ff(model,failure_reason="Null records present",not_null_columns=None,condition="OR")%}
{% if not_null_columns!=None %}
    select
        '{{failure_reason}}' AS failure_reason,
    *
    from {{model}}
    where
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} {{condition}}
            {%- endif -%}
        {% endfor %}
{%- endif -%}
{% endtest %}