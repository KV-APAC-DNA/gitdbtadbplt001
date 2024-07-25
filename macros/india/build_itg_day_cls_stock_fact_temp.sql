{% macro build_itg_day_cls_stock_fact_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                inditg_integration.itg_day_cls_stock_fact_temp
            {% else %}
                {{schema}}.inditg_integration__itg_day_cls_stock_fact_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    inditg_integration.itg_day_cls_stock_fact
                {% else %}
                    {{schema}}.inditg_integration__itg_day_cls_stock_fact
                {% endif %}
    (
        distcode varchar(50),
        transdate timestamp_ntz(9) ,
        lcnid number(18,0),
        lcncode varchar(100) ,
        prdid number(18,0) ,
        prdcode varchar(100) ,
        salopenstock number(18,0) ,
        unsalopenstock number(18,0) ,
        offeropenstock number(18,0) ,
        salpurchase number(18,0) ,
        unsalpurchase number(18,0) ,
        offerpurchase number(18,0) ,
        salpurreturn number(18,0) ,
        unsalpurreturn number(18,0) ,
        offerpurreturn number(18,0),
        salsales number(18,0) ,
        unsalsales number(18,0) ,
        offersales number(18,0),
        salstockin number(18,0),
        unsalstockin number(18,0),
        offerstockin number(18,0),
        salstockout number(18,0) ,
        unsalstockout number(18,0) ,
        offerstockout number(18,0) ,
        damagein number(18,0) ,
        damageout number(18,0) ,
        salsalesreturn number(18,0) ,
        unsalsalesreturn number(18,0) ,
        offersalesreturn number(18,0) ,
        salstkjurin number(18,0)  ,
        unsalstkjurin number(18,0) ,
        offerstkjurin number(18,0) ,
        salstkjurout number(18,0) ,
        unsalstkjurout number(18,0) ,
        offerstkjurout number(18,0),
        salbattfrin number(18,0) ,
        unsalbattfrin number(18,0),
        offerbattfrin number(18,0) ,
        salbattfrout number(18,0) ,
        unsalbattfrout number(18,0) ,
        offerbattfrout number(18,0),
        sallcntfrin number(18,0) ,
        unsallcntfrin number(18,0),
        offerlcntfrin number(18,0),
        sallcntfrout number(18,0) ,
        unsallcntfrout number(18,0) ,
        offerlcntfrout number(18,0),
        salreplacement number(18,0),
        offerreplacement number(18,0) ,
        salclsstock number(18,0),
        unsalclsstock number(18,0),
        offerclsstock number(18,0) ,
        uploaddate timestamp_ntz(9) ,
        uploadflag varchar(10) ,
        createddate timestamp_ntz(9),
        syncid number(38,0) ,
        crt_dttm timestamp_ntz(9),
        nr float
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            inditg_integration.itg_day_cls_stock_fact
        {% else %}
            {{schema}}.inditg_integration__itg_day_cls_stock_fact
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
