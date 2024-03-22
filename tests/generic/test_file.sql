{% test test_file(model,compare_columns=None,select_columns=None)%}
{% if compare_columns!=None %}
    select distinct
        'DISTRIBUTORID IS NOT MATCHING WITH DISTRIBUTOR AVAILABLE IN FILE NAME' AS failure_reason,
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
        SUBSTRING(SPLIT_PART(UPPER(TRIM({{compare_columns[0]}})),'_',2),1,3) <> UPPER(TRIM({{compare_columns[1]}}))
        {%- endif -%}
{% endtest %}