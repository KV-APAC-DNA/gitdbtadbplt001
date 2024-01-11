{% macro test_duplicate_v2(model,group_by_columns=None,select_columns=None,need_counts='yes')%}

{% if group_by_columns!=None %}
    {% set c_pk = "md5(concat(" + group_by_columns|join(",'_',") + "))" %} 
        with grouped_by as(
            select 
                'Duplicate records present' AS failure_reason,
                {%- for item in group_by_columns %}
                    {{item}}
                    {%- if not loop.last -%},
                    {%- endif -%}
                {%- endfor -%}
                {%- if need_counts=='yes' -%},
                count(*) as counts
                {% endif %}
            from {{model}}
            group by 
            {% for item in group_by_columns -%}
                {{item}}
            {%- if not loop.last -%},
            {% endif %}
            {%- endfor %}
            having count(*) >1
        )
        {% if select_columns!=None %}
        select 
                
            'Duplicate records present' AS failure_reason,
            {%- for item in select_columns %}
                trim({{item}}) as {{item}}
                {%- if not loop.last -%},
                {%- endif -%}
            {%- endfor %}
        from {{model}}
        where {{c_pk}} in (select {{c_pk}} from grouped_by)
        {% else %}
        select * from grouped_by
        {% endif %}
    
{% endif %}
    
{% endmacro %}