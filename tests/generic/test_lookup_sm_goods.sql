{% test test_lookup_sm_goods(model,model_nm,model_nm_2,select_columns=None,filter=None)%}

SELECT 'Product mapping CHECK' AS "error value",

       'Validation Column' as "validation column",

       'Column not present in MDS Product table ' AS Validation,

       'Y' AS REJECT

FROM

(
    {% if select_columns!=None %}
      select distinct
        {%- for item in select_columns %}
           {{item}}
        {%- if not loop.last -%},
    {%- endif -%}
        {% endfor %}
FROM (SELECT *
      FROM ( SELECT DISTINCT MNTH_ID,
                   ITEM_CD AS CUST_ITEM_CD,
                   CUST_CD,
                   SAP_ITEM_CD,
                   CUST_CONV_FACTOR,
                   JNJ_PC_PER_CUST_UNIT,
                   LST_PERIOD,
                   EARLY_BK_PERIOD
            FROM {{model_nm}}
            WHERE ACTIVE = 'Y'
            AND   UPPER(CUST_CD) = 'SM') IPPPD,
           (SELECT SPLIT_PART(SPLIT_PART (FILE_NAME,'.',1),'_',3) AS JJ_MNTH_ID,
                   ARTICLE_NUMBER AS POS_PROD_CD,
                   SITE_CODE AS STORE_CD,
                   RECEIVED_QTY AS QTY,
                   FILE_NAME AS FILE_NM
                  FROM {{model}}
                  ) SSM
      WHERE UPPER(TRIM(IPPPD.CUST_ITEM_CD (+))) = UPPER(TRIM(SSM.POS_PROD_CD))

      AND   IPPPD.MNTH_ID (+) = SSM.JJ_MNTH_ID) AS SALES,

     (SELECT *

      FROM {{model_nm_2}}

      WHERE ACTIVE = 'Y') AS IPP

WHERE IPP.JJ_MNTH_ID (+) = SALES.JJ_MNTH_ID

AND   IPP.ITEM_CD (+) = SALES.SAP_ITEM_CD

) 
    {%- if filter !=None -%}
        where    {{filter}}
    {% endif %} 
{% endif %}
  
{% endtest %}