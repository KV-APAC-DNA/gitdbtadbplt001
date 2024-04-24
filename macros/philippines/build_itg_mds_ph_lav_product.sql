{% macro build_itg_mds_ph_lav_product() %}
    
    {% set query %}
        CREATE TABLE if not exists 
        {% if target.name=='prod' %}
                    phlitg_integration.itg_mds_ph_lav_product
                {% else %}
                    {{schema}}.phlitg_integration__itg_mds_ph_lav_product
                {% endif %}
        (
                		item_cd varchar(30),
                        item_nm varchar(255),
                        ims_otc_tag varchar(50),
                        ims_otc_tag_nm varchar(50),
                        npi_strt_period varchar(50),
                        price_lst_period varchar(50),
                        promo_strt_period varchar(50),
                        promo_end_period varchar(255),
                        promo_reg_ind varchar(50),
                        promo_reg_nm varchar(50),
                        hero_sku_ind varchar(50),
                        hero_sku_nm varchar(50),
                        rpt_grp_1 varchar(50),
                        rpt_grp_1_desc varchar(255),
                        rpt_grp_2 varchar(50),
                        rpt_grp_2_desc varchar(255),
                        rpt_grp_3 varchar(50),
                        rpt_grp_3_desc varchar(255),
                        rpt_grp_4 varchar(50),
                        rpt_grp_4_desc varchar(255),
                        rpt_grp_5 varchar(50),
                        rpt_grp_5_desc varchar(255),
                        scard_brand_cd varchar(50),
                        scard_brand_desc varchar(255),
                        scard_franchise_cd varchar(50),
                        scard_franchise_desc varchar(255),
                        scard_put_up_cd varchar(50),
                        scard_put_up_desc varchar(255),
                        scard_varient_cd varchar(50),
                        scard_varient_desc varchar(255),
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



