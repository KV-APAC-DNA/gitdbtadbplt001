{% macro get_birthday_edit(in_yyyymmdd) %}    
    {% set C_ERR_Y = '9999'%}
    {% set C_ERR_M = '99'%}
    {% set C_ERR_D = '99'%}
    {% set combine = C_ERR_Y + C_ERR_M + C_ERR_D %}

    case when {{in_yyyymmdd}} is null then {{combine}}
         when len({{in_yyyymmdd}}) = 1 then {{combine}}
         when len({{in_yyyymmdd}}) = 2 then {{combine}}
         when len({{in_yyyymmdd}}) = 3 then {{C_ERR_Y}}||'0'||{{in_yyyymmdd}}
         when len({{in_yyyymmdd}}) = 4 and left({{in_yyyymmdd}},2) >= 10 and left({{in_yyyymmdd}},3) <=  12 then {{C_ERR_Y}}||{{in_yyyymmdd}}
         when len({{in_yyyymmdd}}) = 5 and substring({{in_yyyymmdd}},2,2) >= 1 and substring({{in_yyyymmdd}},2,2) <=  12 then {{C_ERR_Y}}||substring({{in_yyyymmdd}},2,4)
         when len({{in_yyyymmdd}}) = 6 and substring({{in_yyyymmdd}},3,2) >= 1 and substring({{in_yyyymmdd}},3,2) <=  12 then {{C_ERR_Y}}||substring({{in_yyyymmdd}},3,4)
         when len({{in_yyyymmdd}}) = 7 and substring({{in_yyyymmdd}},4,2) >= 1 and substring({{in_yyyymmdd}},4,2) <=  12 then {{C_ERR_Y}}||substring({{in_yyyymmdd}},4,4)
         when len({{in_yyyymmdd}}) = 8 and substring({{in_yyyymmdd}},5,2) >= 1 and substring({{in_yyyymmdd}},5,2) <=  12 then {{in_yyyymmdd}}
         when len({{in_yyyymmdd}}) = 8 and substring({{in_yyyymmdd}},5,2) > 12 then {{C_ERR_Y}}||substring({{in_yyyymmdd}},5,4)
         end    
{% endmacro %}
