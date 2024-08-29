{% macro update_kr_comm_point_para() %}
    {% if execute %}
    {% if target.name=='prod' %}
        {% set source_table %}
                        JPDCLEDW_INTEGRATION.cld_m
                    {% endset %}
       

    {% else %}
        {% set source_table %}
                        jpndcledw_integration__cld_m
                    {% endset %}
       
    {% endif %}


    {% set query %}
    
    select case when cnt = 0 then 'FALSE' else 'TRUE' end from (
        select count(*) as cnt 
        from {{source_table}}
        where to_date(ymd_dt) = to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',current_timestamp())) 
        and day_of_week = 'Mon' 
        and month_445 <> (
            select month_445 
            from {{source_table}}
            where to_date(ymd_dt) = to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',dateadd(day,-1,current_timestamp() )))
        )
    );
    {% endset %}
    {% set result = run_query(query).columns[0][0] %}

    {% if result == 'TRUE' %}
        TRUNCATE TABLE 
        {% if target.name=='prod' %}
                    jpdcledw_integration.kr_comm_point_para
                {% else %}
                    {{schema}}.jpndcledw_integration__kr_comm_point_para
                {% endif %};
        INSERT INTO  {% if target.name=='prod' %}
                    jpdcledw_integration.kr_comm_point_para
                {% else %}
                    {{schema}}.jpndcledw_integration__kr_comm_point_para
                {% endif %} 
                (
            term_start, term_end, target_year, last_year, this_year, next_year, 
            last_yearym, this_yearym, keikatuki, fromdate, todate, 
            bonus_fromdate, bonus_todate, updatedt, memo, source_file_date, bd_target_year
        ) VALUES (
            REPLACE(cast(to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',dateadd(day,-365,current_timestamp() ))) as VARCHAR),'-',''),
            REPLACE(cast(to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',dateadd(day,-1,current_timestamp() ))) as VARCHAR),'-',''),
            (select year_445 from {{source_table}} where to_date(ymd_dt) = to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',dateadd(day,-1,current_timestamp() )))),
            '2020%', '2021%', '2022%', '202001', '202112', 1, '20220101', '20220130', 
            '20220102', '20220130', '20211227 16:44:37', 
            '2022年1月2日分までの抽出の為(単票No.D28315〜28318)※2021/12/27平井作成',
            REPLACE(cast(to_date(CONVERT_TIMEZONE('UTC','Asia/Tokyo',current_timestamp())) as VARCHAR),'-',''),
            '2021'
        );
        {%endif%}
    {% endif %}
{% endmacro %}
