{% test test_format(model,select_columns=None,failure_reason="'Invalid Records'",where_clause=None)%}
select distinct
    {{failure_reason}} AS failure_reason,
    {% if select_columns!=None %}
        {%- for item in select_columns %}
            trim({{item}}) as {{item}}
        {%- if not loop.last -%},
        {%- endif -%}
        {% endfor %}
    {% else %}
            *
    {% endif %}
    from {{model}}
    {% if where_clause!=None %}
    where
        {{where_clause}}
    {% endif %}
{% endtest %}