{% macro test_null__ff(model,failure_reason=None,not_null_columns=None)%}
{% if not_null_columns!=None %}
    select
        '{{failure_reason}}' AS failure_reason,
    *
    from {{model}}
    where
        {%- for item in not_null_columns %}
        (trim({{item}}) is null or trim({{item}}) = '')
            {%- if not loop.last %} OR
            {%- endif -%}
        {% endfor %}
{%- endif -%}
{% endmacro %}