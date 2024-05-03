{% macro build_itg_mds_ph_distributor_product() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.build_itg_mds_ph_distributor_product
                {% else %}
                    {{schema}}.phlitg_integration__build_itg_mds_ph_distributor_product
                {% endif %}
         (
                	dstrbtr_item_cd varchar(50),
                    dstrbtr_item_nm varchar(255),
                    sap_item_cd varchar(50),
                    sap_item_nm varchar(255),
                    dstrbtr_grp_cd varchar(50),
                    dstrbtr_grp_nm varchar(255),
                    promo_strt_period varchar(50),
                    promo_end_period varchar(50),
                    promo_reg_ind varchar(50),
                    promo_reg_nm varchar(50),
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



