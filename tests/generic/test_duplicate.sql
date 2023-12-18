{% test duplicate_records(model,group_by_columns=None)%}
{% if group_by_columns!=None %}
    select
        'Duplicate records present' AS failure_reason,
        {% for item in group_by_columns %}
            {{item}},
        {% endfor %}
        count(*) as counts
    from {{model}}
    group by
    {% for item in group_by_columns %}
    {{item}}
    {%- if not loop.last -%},
    {% endif %}
    {% endfor %}
    having count(*) >1
{% endif %}
{% endtest %}