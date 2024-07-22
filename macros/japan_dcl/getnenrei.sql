{% macro getnenrei(in_yyyymmdd_f, in_yyyymmdd_t) %}
    {% set in_yyyymmdd_f_cast = in_yyyymmdd_f|as_number %}
    {% set in_yyyymmdd_t_cast = in_yyyymmdd_t|as_number %}
    {% set NENREI = floor((in_yyyymmdd_f_cast - in_yyyymmdd_t_cast) / 10000) %}
    {% if in_yyyymmdd_f_cast == 99999999 or in_yyyymmdd_f_cast is none or in_yyyymmdd_t_cast == 99999999 or in_yyyymmdd_t_cast is none or in_yyyymmdd_f_cast < in_yyyymmdd_t_cast %}
        999
    {% elif NENREI > 100 %}
        999
    {% else %}
        {{ NENREI }}
    {% endif %}
{% endmacro %}