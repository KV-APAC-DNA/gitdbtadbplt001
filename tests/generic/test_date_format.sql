{% test test_date_format(model,select_columns=None,date_column=None) %}
{% if select_columns!=None %}
    SELECT
    {%- for item in select_columns %}
    {{item}} AS "error value",
    '{{item}}' AS "Validation Column",
    {% endfor %}
  'Y' AS REJECT
    {%- for item in date_column %}
    
FROM (
  SELECT
    *
  FROM {{model}}
  WHERE
    (
      NOT SUBSTRING(CAST({{item}} AS INT), 5, 2) BETWEEN '01' AND '12'
      OR LENGTH({{item}}) <> 8
      OR CASE
        WHEN SUBSTRING(CAST({{item}} AS INT), 5, 2) IN ('02')
        AND (
          (
            CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 4 = 0
            AND CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 100 <> 0
          )
          OR CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 400 = 0
        )
        THEN NOT SUBSTRING(CAST({{item}} AS INT), 7, 2) BETWEEN '01' AND '29'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{item}} AS INT), 5, 2) IN ('01', '03', '05', '07', '08', '10', '12')
        THEN NOT SUBSTRING(CAST({{item}} AS INT), 7, 2) BETWEEN '01' AND '31'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{item}} AS INT), 5, 2) IN ('04', '06', '09', '11')
        THEN NOT SUBSTRING(CAST({{item}} AS INT), 7, 2) BETWEEN '01' AND '30'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{item}} AS INT), 5, 2) IN ('02')
        AND (
          CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 4 <> 0
          OR (
            CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 4 = 0
            AND CAST(SUBSTRING(CAST({{item}} AS INT), 1, 4) AS INT) % 100 = 0
          )
        )
        THEN NOT SUBSTRING(CAST({{item}} AS INT), 7, 2) BETWEEN '01' AND '28'
      END
    )
)
{% endfor %}

{% endif %}

{% endtest %}