{% test test_column(model,compare_columns=None,select_columns=None)%}
{% if compare_columns!=None %}
    select distinct
       UPPER('{{compare_columns[1]}}' || ' IS NOT MATCHING WITH ' || '{{compare_columns[0]}}') AS failure_reason,
    {% if select_columns!=None %}
        {%- for item in select_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    {% else %}
        {%- for item in compare_columns %}
        trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    {% endif %}
    from {{model}}
    where
        SUBSTRING(UPPER(TRIM({{compare_columns[1]}})), 1, 3) <> UPPER(TRIM({{compare_columns[0]}}))
{%- endif -%}
{% endtest %}