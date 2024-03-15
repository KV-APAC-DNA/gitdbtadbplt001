{% test test_date_format_1(model,select_columns=None,date_column=None,failure_reason=None) %}
{% if select_columns!=None %}
    SELECT
    {%- for item in select_columns %}
    Trim({{item}}) AS {{item}},
    {% endfor %}
    {% if failure_reason!=None %}
      {{failure_reason}} AS Reason
    {% endif %}
{%- for item in date_column %}   
FROM {{model}} AS A
WHERE
  {{date_column}} IN (
    SELECT DISTINCT
      (
        ODD_MON.{{date_column}}
      )
    FROM (
      SELECT DISTINCT
        (
          {{date_column}}
        ) AS {{date_column}},
        (
          {{date_column}}
        ) SIMILAR TO '(20)[0-9]{2}-(01|03|05|07|08|10|12)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31)' AS RESULT
      FROM {{model}}
    ) AS ODD_MON, (
      SELECT DISTINCT
        (
          {{date_column}}
        ) AS {{date_column}},
        (
          {{date_column}}
        ) SIMILAR TO '(20)[0-9]{2}-(04|06|09|11)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30)' AS RESULT
      FROM {{model}}
    ) AS EVEN_MON, (
      SELECT DISTINCT
        (
          {{date_column}}
        ) AS {{date_column}},
        CASE
          WHEN MOD(CAST(DATE_PART(YEAR, (
            TRUNC(CAST({{date_column}} AS DATE))
          )) AS INT), 4) = 0
          THEN (
            {{date_column}}
          ) SIMILAR TO '(20)[0-9]{2}-(02)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29)'
          ELSE (
            {{date_column}}
          ) SIMILAR TO '(20)[0-9]{2}-(02)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28)'
        END AS RESULT
      FROM {{model}}
    ) AS FEB
    {% if filter!=None %}
                        WHERE {{filter}}
                    {% endif %}
  )
{% endfor %}
{% endif %}
{% endtest %}