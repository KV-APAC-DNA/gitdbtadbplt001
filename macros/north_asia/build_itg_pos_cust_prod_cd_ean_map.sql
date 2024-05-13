{% macro build_itg_pos_cust_prod_cd_ean_map() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    ntaitg_integration.itg_pos_cust_prod_cd_ean_map
                {% else %}
                    {{schema}}.itg_pos_cust_prod_cd_ean_map
                {% endif %}
         (
            cust_nm varchar(100),
            cust_hier_cd varchar(100),
            cust_prod_cd varchar(100),
            barcd varchar(100),
            sap_prod_cd varchar(100),
            crt_dttm timestamp_ntz(9),
            upd_dttm timestamp_ntz(9)
        );

        
                                
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



