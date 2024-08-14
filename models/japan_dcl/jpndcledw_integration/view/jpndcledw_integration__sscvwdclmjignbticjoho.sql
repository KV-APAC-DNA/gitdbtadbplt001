with zcmmnkbn as (
select * from {{ source('jpdclitg_integration', 'zcmmnkbn') }}
),
sscmnhin as (
select * from {{ source('jpdclitg_integration', 'sscmnhin') }}
),
sscmhsoko as (
select * from {{ source('jpdclitg_integration', 'sscmhsoko') }}
),
sscmnichinshitukbn as (
select * from {{ source('jpdclitg_integration', 'sscmnichinshitukbn') }}
),
zcmmnsoshitaik as (
select * from {{ source('jpdclitg_integration', 'zcmmnsoshitaik') }}
),
ssctnmjignbtic as (
select * from {{ source('jpdclitg_integration', 'ssctnmjignbtic') }}
),
final as (
SELECT 
  al001.kaisha_cd, 
  al001.ymd, 
  al001.hin_cd, 
  al004.hin_nmr, 
  COALESCE(
    sum(al001.mji_ic_zandk_qut), 
    (
      (0):: numeric
    ):: numeric(18, 0)
  ) AS zaikocnt 
FROM 
  (
    (
      (
        (
          (
            ssctnmjignbtic al001 
            LEFT JOIN sscmhsoko al002 ON (
              (
                (
                  (al001.kaisha_cd):: text = (al002.kaisha_cd):: text
                ) 
                AND (
                  (al001.soko_naibu_no):: text = (al002.soko_naibu_no):: text
                )
              )
            )
          ) 
          LEFT JOIN sscmnhin al004 ON (
            (
              (
                (al001.kaisha_cd):: text = (al004.kaisha_cd):: text
              ) 
              AND (
                (al001.hin_cd):: text = (al004.hin_cd):: text
              )
            )
          )
        ) 
        LEFT JOIN sscmnichinshitukbn al005 ON (
          (
            (
              (al001.kaisha_cd):: text = (al005.kaisha_cd):: text
            ) 
            AND (
              (al001.ic_hnst_kbn_cd):: text = (al005.ic_hnst_kbn_cd):: text
            )
          )
        )
      ) 
      LEFT JOIN zcmmnsoshitaik al006 ON (
        (
          (
            (al001.kaisha_cd):: text = (al006.kaisha_cd):: text
          ) 
          AND (
            (al001.ic_bmn_naibu_no):: text = (al006.bmn_naibukanri_no):: text
          )
        )
      )
    ) 
    LEFT JOIN zcmmnkbn al007 ON (
      (
        (
          (al007.kbn_id):: text = (
            'ic_jokyo_kbn' :: character varying
          ):: text
        ) 
        AND (
          (al001.ic_jokyo_kbn):: text = (al007.kbn_chi):: text
        )
      )
    )
  ) 
GROUP BY 
  al001.kaisha_cd, 
  al001.ymd, 
  al001.hin_cd, 
  al004.hin_nmr
)
select * from final