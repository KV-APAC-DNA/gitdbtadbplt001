{% macro encryption_1( col) %}
    {% if col!=None %}
         CASE WHEN LENGTH({{col}})::string = 10 THEN
                    SUBSTR({{col}}, 7, 1) || SUBSTR({{col}}, 5, 1) || SUBSTR({{col}}, 8, 1) || SUBSTR({{col}}, 6, 1) || SUBSTR({{col}}, 2, 1) || SUBSTR({{col}}, 4, 1) || SUBSTR({{col}}, 1, 1) || SUBSTR({{col}}, 3, 1) || SUBSTR({{col}}, 9, 1) || SUBSTR({{col}}, 10, 1)
                WHEN LENGTH({{col}}) = 9 THEN
                SUBSTR({{col}}, 6, 1) || SUBSTR({{col}}, 4, 1) || SUBSTR({{col}}, 7, 1) || SUBSTR({{col}}, 5, 1) || SUBSTR({{col}}, 1, 1) || SUBSTR({{col}}, 3, 1) || '0' || SUBSTR({{col}}, 2, 1) || SUBSTR({{col}}, 8, 1) || SUBSTR({{col}}, 9, 1)
                WHEN LENGTH({{col}}) = 8 THEN
                    SUBSTR({{col}}, 5, 1) || SUBSTR({{col}}, 3, 1) || SUBSTR({{col}}, 6, 1) || SUBSTR({{col}}, 4, 1) || '0' || SUBSTR({{col}}, 2, 1) || '0' || SUBSTR({{col}}, 1, 1) || SUBSTR({{col}}, 7, 1) || SUBSTR({{col}}, 8, 1)
                WHEN LENGTH({{col}}) = 7 THEN
                    SUBSTR({{col}}, 4, 1) || SUBSTR({{col}}, 2, 1) || SUBSTR({{col}}, 5, 1) || SUBSTR({{col}}, 3, 1) || '0' || SUBSTR({{col}}, 1, 1) || '0' || '0' || SUBSTR({{col}}, 6, 1) || SUBSTR({{col}}, 7, 1)
                WHEN LENGTH({{col}}) = 6 THEN
                    SUBSTR({{col}}, 3, 1) || SUBSTR({{col}}, 1, 1) || SUBSTR({{col}}, 4, 1) || SUBSTR({{col}}, 2, 1) || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 5, 1) || SUBSTR({{col}}, 6, 1)
                WHEN LENGTH({{col}}) = 5 THEN
                    SUBSTR({{col}}, 2, 1) || '0' || SUBSTR({{col}}, 3, 1) || SUBSTR({{col}}, 1, 1) || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 4, 1) || SUBSTR({{col}}, 5, 1)
                WHEN LENGTH({{col}}) = 4 THEN
                    SUBSTR({{col}}, 1, 1) || '0' || SUBSTR({{col}}, 2, 1) || '0' || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 3, 1) || SUBSTR({{col}}, 4, 1)
                WHEN LENGTH({{col}}) = 3 THEN
                '0' || '0' || SUBSTR({{col}}, 1, 1) || '0' || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 2, 1) || SUBSTR({{col}}, 3, 1)
                WHEN LENGTH({{col}}) = 2 THEN
                    {{col}}='0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 1, 1) || SUBSTR({{col}}, 2, 1)
                WHEN LENGTH({{col}}) = 1 THEN
                    {{col}}='0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || '0' || SUBSTR({{col}}, 1, 1)
                ELSE
                    ''
            END
    {% endif %}
{% endmacro %}