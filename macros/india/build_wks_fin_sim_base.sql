{% macro build_wks_fin_sim_base() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                indwks_integration.wks_fin_sim_base
            {% else %}
                {{schema}}.indwks_integration__wks_fin_sim_base
    {% endif %}
    {% endset %}
    {% set query %}
        create or replace table
        {% if target.name=='prod' %}
                    indwks_integration.wks_fin_sim_base
                {% else %}
                    {{schema}}.indwks_integration__wks_fin_sim_base
                {% endif %}
    (
        matl_num varchar(100),
        chrt_acct varchar(4),
        acct_num varchar(10),
        dstr_chnl varchar(2),
        ctry_key varchar(3),
        caln_yr_mo number(18,0),
        fisc_yr number(18,0),
        fisc_yr_per number(18,0),
        amt_obj_crncy number(38,5),
        qty number(38,5),
        acct_hier_desc varchar(100),
        acct_hier_shrt_desc varchar(100),
        chnl_desc1 varchar(500),
        chnl_desc2 varchar(500),
        bw_gl varchar(200),
        nature varchar(500),
        sap_gl varchar(500),
        descp varchar(500),
        bravo_mapping varchar(500),
        sku_desc varchar(500),
        brand_combi varchar(500),
        franchise varchar(500),
        "group" varchar(500),
        mrp number(38,2),
        cogs_per_unit number(38,2),
        plan varchar(10),
        brand_group_1 varchar(500),
        brand_group_2 varchar(500),
        co_cd varchar(16777216),
        brand_combi_var varchar(200)
    );
    {% endset %}

    {% do run_query(query) %}
    
    {% for i in range(1, 14) %}
        {% set table_name = 'wks_fin_sim_base_temp' ~ i %}
        {% set sql %}
            DELETE FROM
            {% if target.name=='prod' %}
                    indwks_integration.{{ table_name }}
                {% else %}
                    {{schema}}.indwks_integration__{{ table_name }}
                {% endif %}
        {% endset %}
        {% do run_query(sql) %}
    {% endfor %}
{% endmacro %}
