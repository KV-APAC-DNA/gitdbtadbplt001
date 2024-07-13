{% macro build_itg_retailermaster_temp() %}
    {% set tablename %}
    {% if target.name=='prod' %}
                inditg_integration.itg_retailermaster_temp
            {% else %}
                {{schema}}.inditg_integration__itg_retailermaster_temp
    {% endif %}
    {% endset %}
    {% set query %}
        CREATE TABLE if not exists
        {% if target.name=='prod' %}
                    inditg_integration.itg_retailermaster
                {% else %}
                    {{schema}}.inditg_integration__itg_retailermaster
                {% endif %}
    (   
        distcode varchar(50),
        rtrid number(18,0),
        rtrcode varchar(50),
        rtrname varchar(100),
        csrtrcode varchar(50),
        rtrcatlevelid varchar(30),
        rtrcategorycode varchar(50),
        classcode varchar(50),
        keyaccount varchar(50),
        regdate timestamp_ntz(9),
        relationstatus varchar(50),
        parentcode varchar(50),
        geolevel varchar(50),
        geolevelvalue varchar(100),
        status number(18,0),
        createdid number(18,0),
        createddate timestamp_ntz(9),
        rtraddress1 varchar(100),
        rtraddress2 varchar(100),
        rtraddress3 varchar(100),
        rtrpincode varchar(20),
        villageid number(18,0),
        villagecode varchar(100),
        villagename varchar(100),
        mode varchar(100),
        uploadflag varchar(10),
        approvalremarks varchar(400),
        syncid number(38,0),
        druglno varchar(100),
        rtrcrbills number(18,0),
        rtrcrlimit number(38,6),
        rtrcrdays number(18,0),
        rtrdayoff number(18,0),
        rtrtinno varchar(100),
        rtrcstno varchar(100),
        rtrlicno varchar(100),
        rtrlicexpirydate varchar(100),
        rtrdrugexpirydate varchar(100),
        rtrpestlicno varchar(100),
        rtrpestexpirydate varchar(100),
        approved number(18,0),
        rtrphoneno varchar(100),
        rtrcontactperson varchar(100),
        rtrtaxgroup varchar(100),
        rtrtype varchar(50),
        rtrtaxable varchar(1),
        rtrshippadd1 varchar(200),
        rtrshippadd2 varchar(200),
        rtrshippadd3 varchar(200),
        rtrfoodlicno varchar(200),
        rtrfoodexpirydate timestamp_ntz(9),
        rtrfoodgracedays number(18,0),
        rtrdruggracedays number(18,0),
        rtrcosmeticlicno varchar(200),
        rtrcosmeticexpirydate timestamp_ntz(9),
        rtrcosmeticgracedays number(18,0),
        crt_dttm timestamp_ntz(9),
        updt_dttm timestamp_ntz(9),
        actv_flg varchar(1),
        rtrlatitude varchar(40),
        rtrlongitude varchar(40),
        rtruniquecode varchar(100),
        file_rec_dt date
    );
        create or replace table {{tablename}} clone
        {% if target.name=='prod' %}
            inditg_integration.itg_retailermaster
        {% else %}
            {{schema}}.inditg_integration__itg_retailermaster
        {% endif %};
    {% endset %}

    {% do run_query(query) %}
{% endmacro %}
