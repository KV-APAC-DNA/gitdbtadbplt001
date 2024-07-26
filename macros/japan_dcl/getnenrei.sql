{% macro getnenrei(in_yyyymmdd_f, in_yyyymmdd_t) %}
    (
    CASE
        WHEN {{ in_yyyymmdd_f }} = 99999999 THEN 999
        WHEN {{ in_yyyymmdd_f }} IS NULL THEN 999
        WHEN {{ in_yyyymmdd_t }} = 99999999 THEN 999
        WHEN {{ in_yyyymmdd_t }} IS NULL THEN 999
        WHEN {{ in_yyyymmdd_f }} < {{ in_yyyymmdd_t }} THEN 999
        WHEN (({{ in_yyyymmdd_f }} - {{ in_yyyymmdd_t }}) / 10000) > 100 THEN 999
        ELSE floor(({{ in_yyyymmdd_f }} - {{ in_yyyymmdd_t }}) / 10000)
    END
    )
{% endmacro %}