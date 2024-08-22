{% test test_date_format_filename(model,select_columns=None,date_column=None) %}
{% if select_columns!=None %}
    SELECT
    {% set file_name_columns = [
        'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME',
        'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
    ] | map('lower') | list %}
    -- Get the actual columns in the model and convert to lowercase
    {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
    {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
    {%- for col in reversed_columns %}
        {% if col in file_name_columns%}
            {{ col }} as file_name
        {% if select_columns or not_null_columns %},{% endif %}
            {% break %}
        {% endif %}
    {%- endfor %}
    {%- for item in select_columns %}
        {% if item | lower not in  file_name_columns %}
            {{item}} AS "error value",
            '{{item}}' AS "Validation Column",
            {%- if not loop.last -%},{%- endif -%}
        {%- endif -%}
    {% endfor %}
  'Y' AS REJECT
    -- Define the possible file name columns and convert to lowercase
    
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