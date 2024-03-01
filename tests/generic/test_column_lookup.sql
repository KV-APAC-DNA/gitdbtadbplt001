{% test test_column_lookup(model,select_columns=None,lpad_column=None,column=none,lookup_table=None,lookup_column=None,lookup_filter=None,filter=None,additional_filter=None)%}
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
        {%- for item2 in lpad_column %}
             trim({{lookup_column}})||LPAD(TRIM({{item2}}), 2, 0) in (
                select distinct
                    (A.{{lookup_column}}||LPAD(TRIM(A.{{item2}}), 2, 0)) as year_month
                from  {{lookup_table}} as A,(SELECT
        (trim({{lookup_column}})||LPAD(TRIM({{item2}}), 2, 0)) as year_month,
        (REGEXP_LIKE((trim({{lookup_column}})||LPAD(TRIM({{item2}}), 2, 0)) , '(20)[0-9]{2}(01|02|03|04|05|06|07|08|09|10|11|12)')) AS RESULT
      FROM {{lookup_table}}
    ) AS B
        {% endfor %}
                {%- if lookup_filter !=None -%}
                 {% set space_before_where = ' ' %}
                 {{ space_before_where }}where {{lookup_filter}}
                {% endif %}
    {%- if filter !=None -%}
                and    {{filter}}
    {% endif %}
    )
    {%- if additional_filter !=None -%}
    where    {{additional_filter}}
    {% endif %}
{% endif %}
{% endtest %}