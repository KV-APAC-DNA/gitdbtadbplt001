{% macro run_sql(script=None) %}
    {% do run_query(script) %}
    
{% endmacro %}