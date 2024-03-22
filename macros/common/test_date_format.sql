{% macro test_date_format(model,select_columns=[],date_column=None) %}
    SELECT
  {{date_column}} AS "error value",
  '{{date_column}}' AS "Validation Column",
  {% for col in select_columns
      %}
      {{col}} ,
  {% endfor %} 
  'Y' AS REJECT
FROM (
  SELECT
    *
  FROM {{model}}
  WHERE
    (
      NOT SUBSTRING(CAST({{date_column}} AS INT), 5, 2) BETWEEN '01' AND '12'
      OR LENGTH({{date_column}}) <> 8
      OR CASE
        WHEN SUBSTRING(CAST({{date_column}} AS INT), 5, 2) IN ('02')
        AND (
          (
            CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 4 = 0
            AND CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 100 <> 0
          )
          OR CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 400 = 0
        )
        THEN NOT SUBSTRING(CAST({{date_column}} AS INT), 7, 2) BETWEEN '01' AND '29'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{date_column}} AS INT), 5, 2) IN ('01', '03', '05', '07', '08', '10', '12')
        THEN NOT SUBSTRING(CAST({{date_column}} AS INT), 7, 2) BETWEEN '01' AND '31'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{date_column}} AS INT), 5, 2) IN ('04', '06', '09', '11')
        THEN NOT SUBSTRING(CAST({{date_column}} AS INT), 7, 2) BETWEEN '01' AND '30'
      END
      OR CASE
        WHEN SUBSTRING(CAST({{date_column}} AS INT), 5, 2) IN ('02')
        AND (
          CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 4 <> 0
          OR (
            CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 4 = 0
            AND CAST(SUBSTRING(CAST({{date_column}} AS INT), 1, 4) AS INT) % 100 = 0
          )
        )
        THEN NOT SUBSTRING(CAST({{date_column}} AS INT), 7, 2) BETWEEN '01' AND '28'
      END
    )
)
{% endmacro %}