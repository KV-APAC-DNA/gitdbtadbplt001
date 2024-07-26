{% macro macrotest() %}
    {% set month = 1 %}
    {% set end_loop =run_query("select count(*) from jp_dcl_edw.kr_comm_point_para").columns[0].values[i] %}
    {% for i in range(1, end_loop) %}
    {% set target_year = run_query("select target_year from jp_dcl_edw.kr_comm_point_para").columns[0].values[i] %}
    {% set term_end_month = run_query("select substring(term_end,5,2) from jp_dcl_edw.kr_comm_point_para").columns[0].values[i] %}
    
    {% set current_year = run_query("select extract(year from CONVERT_TIMEZONE('UTC','Asia/Tokyo',SYSDATE)) as year").columns[0].values[0] %}

    {% if current_year > target_year %}
        {% set end_month = 12 %}
    {% else %}
        {% set end_month = term_end_month %}
    {% endif %}
    {% set query %}
    insert into jp_dcl_edw.wk_rankdt_tmp
    select cast(target_year as varchar) || lpad(month,2,'00'), 0 from jp_dcl_edw.kr_comm_point_para;
    {% endset %}

    {% do run_query(query) %}
    {% set month = month + 1 %}
    {% if month < end_month %}{% break %}{% endif %} 

    {% endfor %}

{% endmacro %}
