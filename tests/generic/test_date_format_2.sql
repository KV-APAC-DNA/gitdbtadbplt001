{% test test_date_format_2(model,select_columns=None,date_column=None,failure_reason=None,filter=None) %}
{% if select_columns!=None %}
    SELECT
    {%- for item in select_columns %}
    convert(VARCHAR,{{item}}),
    {% endfor %}
    {% if failure_reason!=None %}
      {{failure_reason}} AS Reason
    {% endif %}
{%- for item in date_column %}   
FROM os_sdl.sdl_JNJ_Mer_COP A
WHERE COP_DATE IN (SELECT DISTINCT (ODD_MON.COP_DATE)

                        FROM (SELECT DISTINCT (COP_DATE) COP_DATE,

                                     (COP_DATE) SIMILAR TO '(20)[0-9]{2}-(01|03|05|07|08|10|12)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31)' RESULT

                              FROM os_sdl.sdl_JNJ_Mer_COP) ODD_MON,

                             (SELECT DISTINCT (COP_DATE) AS COP_DATE,

                                     (COP_DATE) SIMILAR TO '(20)[0-9]{2}-(02|04|06|09|11)-(01|02|03|04|05|06|07|08|09|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30)' RESULT

                              FROM os_sdl.sdl_JNJ_Mer_COP) EVEN_MON


                        WHERE (ODD_MON.COP_DATE) = (EVEN_MON.COP_DATE)
                        AND   (ODD_MON.RESULT) = (EVEN_MON.RESULT)
)
{% endfor %}
{% endif %}
{% endtest %}