{% macro build_update_no_dup() %}

    {% set query %}
        UPDATE {{ ref('jpnwks_integration__wk_so_planet_no_dup') }} as target
                    SET jcp_rec_seq = seq_value.max_seq_value + subquery.row_num
                    FROM (
                        SELECT id, 
                            ROW_NUMBER() OVER (ORDER BY id) as row_num
                        FROM {{ ref('jpnwks_integration__wk_so_planet_no_dup') }}
                        WHERE jcp_rec_seq IS NULL
                    ) as subquery
                    CROSS JOIN (
                        SELECT MAX_VALUE::number as max_seq_value
                        FROM {{ ref('jpnedw_integration__mt_constant_seq') }} 
                        WHERE IDENTIFY_CD = 'SEQUENCE_NO'
                    ) as seq_value
                    WHERE target.id = subquery.id and target.jcp_rec_seq is null;

                    UPDATE {{ ref('jpnedw_integration__mt_constant_seq') }}
                    SET MAX_VALUE=(select max(JCP_REC_SEQ) from {{ ref('jpnwks_integration__wk_so_planet_no_dup') }});
      {% endset %}
    {% do run_query(query) %}
{% endmacro %}