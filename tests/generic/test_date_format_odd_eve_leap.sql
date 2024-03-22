{% test test_date_format_odd_eve_leap(model,model_nm,filter=None,select_columns=None,date_column=None,failure_reason=None) %}
{% if select_columns!=None %}
    SELECT
    {%- for item in select_columns %}
    Trim({{item}}) AS {{item}},
    {% endfor %}
{% endif %}
{% if failure_reason!=None %}
      {{failure_reason}} AS Reason
{% endif %}
{% if date_column!=None %}
FROM {{model_nm}} AS A
WHERE
  {{date_column}} IN (
    SELECT DISTINCT
      ODD_MON.{{date_column}}
    FROM (
      SELECT DISTINCT
        {{date_column}} AS {{date_column}},
       regexp_like ({{date_column}}
        ,  '(20)[0-9]{2}-(01|03|05|07|08|10|12)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31)') AS RESULT
      FROM {{model_nm}}
    ) AS ODD_MON, (
      SELECT DISTINCT
        
          {{date_column}}
         AS {{date_column}},
        regexp_like((
          {{date_column}}
        ),  '(20)[0-9]{2}-(04|06|09|11)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30)') AS RESULT
      FROM {{model_nm}}
    ) AS EVEN_MON, (
      SELECT DISTINCT
        (
          {{date_column}}
        ) AS {{date_column}},
        CASE
          WHEN MOD(CAST(DATE_PART(YEAR, (
            CAST({{date_column}} AS DATE)
          )) AS INT), 4) = 0
          THEN regexp_like( (
            {{date_column}}
          ), '(20)[0-9]{2}-(02)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29)')
          ELSE 
            regexp_like({{date_column}}
          , '(20)[0-9]{2}-(02)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28)')
        END AS RESULT
      FROM {{model_nm}}
    ) AS FEB
    {% if filter!=None %}
            WHERE {{filter}}
    {% endif %}
    
  )
{% endif %}
{% endtest %}