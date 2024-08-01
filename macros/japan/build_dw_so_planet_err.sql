{% macro build_dw_so_planet_err_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                jpnitg_integration.dw_so_planet_err_temp
            {% else %}
                {{schema}}.jpnitg_integration__dw_so_planet_err_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    jpnitg_integration.dw_so_planet_err
                {% else %}
                    {{schema}}.jpnitg_integration__dw_so_planet_err
                {% endif %}
    (       jcp_rec_seq number(10,0),
            id number(10,0),
            rcv_dt varchar(256),
            test_flag varchar(256),
            bgn_sndr_cd varchar(256),
            ws_cd varchar(256),
            rtl_type varchar(256),
            rtl_cd varchar(256),
            trade_type varchar(256),
            shp_date varchar(256),
            shp_num varchar(256),
            trade_cd varchar(256),
            dep_cd varchar(256),
            chg_cd varchar(256),
            person_in_charge varchar(256),
            person_name varchar(256),
            rtl_name varchar(256),
            rtl_ho_cd varchar(256),
            rtl_address_cd varchar(256),
            data_type varchar(256),
            opt_fld varchar(256),
            item_nm varchar(256),
            item_cd_typ varchar(256),
            item_cd varchar(256),
            qty varchar(256),
            qty_type varchar(256),
            price varchar(256),
            price_type varchar(256),
            bgn_sndr_cd_gln varchar(256),
            rcv_cd_gln varchar(256),
            ws_cd_gln varchar(256),
            shp_ws_cd varchar(256),
            shp_ws_cd_gln varchar(256),
            rep_name_kanji varchar(256),
            rep_info varchar(256),
            trade_cd_gln varchar(256),
            rtl_cd_gln varchar(256),
            rtl_name_kanji varchar(256),
            rtl_ho_cd_gln varchar(256),
            item_cd_gtin varchar(256),
            item_nm_kanji varchar(256),
            unt_prc varchar(256),
            net_prc varchar(256),
            sales_chan_type varchar(256),
            jcp_create_date timestamp_ntz(9),
            jcp_rcv_dt_dupli varchar(256),
            jcp_test_flag_dupli varchar(256),
            jcp_qty_dupli varchar(256),
            jcp_price_dupli varchar(256),
            jcp_unt_prc_dupli varchar(256),
            jcp_net_prc_dupli varchar(256),
            export_flag varchar(256)
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            jpnitg_integration.dw_so_planet_err
        {% else %}
            {{schema}}.jpnitg_integration__dw_so_planet_err
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}