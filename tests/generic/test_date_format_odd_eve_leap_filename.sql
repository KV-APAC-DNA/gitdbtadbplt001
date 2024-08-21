{% test test_date_format_odd_eve_leap_filename(model,model_nm=model,filter=None,select_columns=None,date_column=None,failure_reason=None) %}
{% if select_columns!=None %}
    SELECT {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}


            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
                        -- Log the actual columns and file name columns to debug
            {{ log('Actual Columns: ' ~ actual_columns, info=True) }}
            {{ log('File Name Columns: ' ~ file_name_columns, info=True) }}
            {{ log('File Name Columns_reversed: ' ~ reversed_columns, info=True) }}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
    {%- for item in select_columns %}
    Trim({{item}}) AS {{item}},
    {% endfor %}
{% endif %}
{% if failure_reason!=None %}
      {{failure_reason}} AS Reason
{% endif %}
{% if date_column!=None %}
FROM {{model}} AS A
WHERE
  (file_name,{{date_column}}) IN (
    SELECT DISTINCT ODD_MON.file_name,
      ODD_MON.{{date_column}}
    FROM (
      SELECT DISTINCT 
      {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}


            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
            -- Log the actual columns and file name columns to debug
            {{ log('Actual Columns: ' ~ actual_columns, info=True) }}
            {{ log('File Name Columns: ' ~ file_name_columns, info=True) }}
            {{ log('File Name Columns_reversed: ' ~ reversed_columns, info=True) }}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
        {{date_column}} AS {{date_column}},
       regexp_like ({{date_column}}
        ,  '(20)[0-9]{2}-(01|03|05|07|08|10|12)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31)') AS RESULT
      FROM {{model_nm}}
    ) AS ODD_MON, (
      SELECT DISTINCT
        {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}


            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
          {{date_column}}
         AS {{date_column}},
        regexp_like((
          {{date_column}}
        ),  '(20)[0-9]{2}-(04|06|09|11)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30)') AS RESULT
      FROM {{model_nm}}
    ) AS EVEN_MON, (
      SELECT DISTINCT
      {% set file_name_columns = [
                'CDL_SOURCE_FILE', 'FILE_NM', 'SOURCE_FILE_NAME', 'FILENAME', 
                'file_name', 'filename', 'SRC_FILE', 'LOAD_FILE_NM', 'FILE_NAME', 'src_file'
            ] | map('lower') | list %}


            -- Get the actual columns in the model and convert to lowercase
            {% set actual_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|list  %}
            {% set reversed_columns = adapter.get_columns_in_relation(model) | map(attribute='name') | map('lower')|reverse  %}
            -- Loop through file_name_columns to find the first matching column in actual_columns
            {%- for col in reversed_columns %}
                {% if col in file_name_columns%}
                    {{ col }} as file_name
                {% if select_columns or not_null_columns %},{% endif %}
                   {% break %}
                {% endif %}   
            {%- endfor %}
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
    where ODD_MON.file_name = EVEN_MON.file_name
    and EVEN_MON.file_name = FEB.file_name
    {% if filter!=None %}
            and {{filter}}
    {% endif %}
    
  )
{% endif %}
{% endtest %}