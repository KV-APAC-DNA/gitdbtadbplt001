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
        distcode::varchar(50) as distcode,
        transdate::timestamp_ntz(9) as transdate,
        lcnid::number(18,0) as lcnid,
        lcncode::varchar(100) as lcncode,
        prdid::number(18,0) as prdid,
        prdcode::varchar(100) as prdcode,
        salopenstock::number(18,0) as salopenstock,
        unsalopenstock::number(18,0) as unsalopenstock,
        offeropenstock::number(18,0) as offeropenstock,
        salpurchase::number(18,0) as salpurchase,
        unsalpurchase::number(18,0) as unsalpurchase,
        offerpurchase::number(18,0) as offerpurchase,
        salpurreturn::number(18,0) as salpurreturn,
        unsalpurreturn::number(18,0) as unsalpurreturn,
        offerpurreturn::number(18,0) as offerpurreturn,
        salsales::number(18,0) as salsales,
        unsalsales::number(18,0) as unsalsales,
        offersales::number(18,0) as offersales,
        salstockin::number(18,0) as salstockin,
        unsalstockin::number(18,0) as unsalstockin,
        offerstockin::number(18,0) as offerstockin,
        salstockout::number(18,0) as salstockout,
        unsalstockout::number(18,0) as unsalstockout,
        offerstockout::number(18,0) as offerstockout,
        damagein::number(18,0) as damagein,
        damageout::number(18,0) as damageout,
        salsalesreturn::number(18,0) as salsalesreturn,
        unsalsalesreturn::number(18,0) as unsalsalesreturn,
        offersalesreturn::number(18,0) as offersalesreturn,
        salstkjurin::number(18,0) as salstkjurin,
        unsalstkjurin::number(18,0) as unsalstkjurin,
        offerstkjurin::number(18,0) as offerstkjurin,
        salstkjurout::number(18,0) as salstkjurout,
        unsalstkjurout::number(18,0) as unsalstkjurout,
        offerstkjurout::number(18,0) as offerstkjurout,
        salbattfrin::number(18,0) as salbattfrin,
        unsalbattfrin::number(18,0) as unsalbattfrin,
        offerbattfrin::number(18,0) as offerbattfrin,
        salbattfrout::number(18,0) as salbattfrout,
        unsalbattfrout::number(18,0) as unsalbattfrout,
        offerbattfrout::number(18,0) as offerbattfrout,
        sallcntfrin::number(18,0) as sallcntfrin,
        unsallcntfrin::number(18,0) as unsallcntfrin,
        offerlcntfrin::number(18,0) as offerlcntfrin,
        sallcntfrout::number(18,0) as sallcntfrout,
        unsallcntfrout::number(18,0) as unsallcntfrout,
        offerlcntfrout::number(18,0) as offerlcntfrout,
        salreplacement::number(18,0) as salreplacement,
        offerreplacement::number(18,0) as offerreplacement,
        salclsstock::number(18,0) as salclsstock,
        unsalclsstock::number(18,0) as unsalclsstock,
        offerclsstock::number(18,0) as offerclsstock,
        uploaddate::timestamp_ntz(9) as uploaddate,
        uploadflag::varchar(10) as uploadflag,
        createddate::timestamp_ntz(9) as createddate,
        syncid::number(38,0) as syncid,
        crt_dttm::timestamp_ntz(9) as crt_dttm,
        nr::float as nr
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
