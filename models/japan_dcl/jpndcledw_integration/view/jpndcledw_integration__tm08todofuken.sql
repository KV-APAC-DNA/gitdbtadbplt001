with hanyo_attr as (
select * from {{ ref('jpndcledw_integration__hanyo_attr') }}
),
final as (
SELECT 
  hanyo_attr.attr1 AS todofukenname, 
  hanyo_attr.attr2 AS chihoname1, 
  "substring"(
    (hanyo_attr.attr3):: text, 
    1, 
    (
      regexp_instr(
        (hanyo_attr.attr3):: text, 
        (',' :: character varying):: text, 
        1, 
        1
      ) -1
    )
  ) AS todofuken_ido, 
  "substring"(
    (hanyo_attr.attr3):: text, 
    (
      regexp_instr(
        (hanyo_attr.attr3):: text, 
        (',' :: character varying):: text, 
        1, 
        1
      ) + 1
    )
  ) AS todofuken_keido, 
  "substring"(
    (hanyo_attr.attr4):: text, 
    1, 
    (
      regexp_instr(
        (hanyo_attr.attr4):: text, 
        (',' :: character varying):: text, 
        1, 
        1
      ) -1
    )
  ) AS chiho_ido, 
  "substring"(
    (hanyo_attr.attr4):: text, 
    (
      regexp_instr(
        (hanyo_attr.attr4):: text, 
        (',' :: character varying):: text, 
        1, 
        1
      ) + 1
    )
  ) AS chiho_keido, 
  hanyo_attr.attr5 AS todofukencode 
FROM 
  hanyo_attr 
WHERE 
  (
    (hanyo_attr.kbnmei):: text = ('TODOFUKEN' :: character varying):: text
  )
)
select * from final