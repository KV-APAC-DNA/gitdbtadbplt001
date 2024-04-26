{% macro build_itg_mds_ph_pos_pricelist() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_pos_pricelist
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_pos_pricelist
                {% endif %}
         (
                        code varchar(100),
                        item_cd varchar(50),
                        item_desc varchar(255),
                        jj_mnth_id varchar(50),
                        consumer_bar_cd varchar(50),
                        shippers_bar_cd varchar(50),
                        dz_per_case number(15,4),
                        lst_price_case number(15,4),
                        lst_price_dz number(15,4),
                        lst_price_unit number(15,4),
                        srp number(15,4),
                        status varchar(50),
                        status_nm varchar(255),
                        last_chg_datetime timestamp_ntz(9),
                        effective_from timestamp_ntz(9),
                        effective_to timestamp_ntz(9),
                        active varchar(10),
                        crtd_dttm timestamp_ntz(9),
                        updt_dttm timestamp_ntz(9)

        );

    {% endset %}

    {% do run_query(query) %}
{% endmacro %}



