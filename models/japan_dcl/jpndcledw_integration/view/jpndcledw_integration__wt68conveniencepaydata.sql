with tm64kessai_nm as (
select * from DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.TM64KESSAI_NM
),
tbecorder as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.TBECORDER
),
c_tbeckesai as (
select * from DEV_DNA_CORE.SNAPJPDCLITG_INTEGRATION.C_TBECKESAI
),
final as (
SELECT 
  DISTINCT o.diordercode AS orderid, 
  to_char(
    o.dsorderdt, 
    (
      'YYYY/MM/DD HH24:MI:SS' :: character varying
    ):: text
  ) AS juchdate, 
  to_char(
    "k".dsuriagedt, 
    (
      'YYYY/MM/DD HH24:MI:SS' :: character varying
    ):: text
  ) AS shukadate, 
  "k".c_dikesaiid AS kesaiid, 
  o.diecusrid AS kokyaid, 
  to_char(
    "k".diseikyuprc, 
    (
      '999G999G999' :: character varying
    ):: text
  ) AS seikyuprc, 
  COALESCE(
    nm.name, '※要確認※' :: character varying
  ) AS kesaihoho, 
  CASE WHEN (
    ("k".dishukkasts):: text = ('1010' :: character varying):: text
  ) THEN '未出荷' :: character varying WHEN (
    ("k".dishukkasts):: text = ('1020' :: character varying):: text
  ) THEN '受注確定済' :: character varying WHEN (
    ("k".dishukkasts):: text = ('1030' :: character varying):: text
  ) THEN '一部出荷指示済' :: character varying WHEN (
    ("k".dishukkasts):: text = ('1040' :: character varying):: text
  ) THEN '出荷指示済' :: character varying WHEN (
    ("k".dishukkasts):: text = ('1050' :: character varying):: text
  ) THEN '一部出荷済' :: character varying WHEN (
    ("k".dishukkasts):: text = ('1060' :: character varying):: text
  ) THEN '出荷済' :: character varying WHEN (
    ("k".dishukkasts):: text = ('5010' :: character varying):: text
  ) THEN '与信待未出荷' :: character varying WHEN (
    ("k".dishukkasts):: text = ('5020' :: character varying):: text
  ) THEN '与信待出荷保留' :: character varying WHEN (
    ("k".dishukkasts):: text = ('9010' :: character varying):: text
  ) THEN '出荷保留' :: character varying WHEN (
    ("k".dishukkasts):: text = ('9910' :: character varying):: text
  ) THEN '出荷対象外' :: character varying ELSE '※要確認※' :: character varying END AS shukasts, 
  CASE WHEN (
    ("k".dicardnyukinsts):: text = ('0' :: character varying):: text
  ) THEN '入金なし' :: character varying WHEN (
    ("k".dicardnyukinsts):: text = ('1' :: character varying):: text
  ) THEN '合致' :: character varying WHEN (
    ("k".dicardnyukinsts):: text = ('2' :: character varying):: text
  ) THEN '不足' :: character varying WHEN (
    ("k".dicardnyukinsts):: text = ('3' :: character varying):: text
  ) THEN '過入金' :: character varying ELSE '※要確認※' :: character varying END AS nyukinshogosts, 
  CASE WHEN (
    ("k".dinyukinsts):: text = ('0' :: character varying):: text
  ) THEN '未入金' :: character varying WHEN (
    ("k".dinyukinsts):: text = ('1' :: character varying):: text
  ) THEN '入金済' :: character varying WHEN (
    ("k".dinyukinsts):: text = ('2' :: character varying):: text
  ) THEN '回収不能' :: character varying WHEN (
    ("k".dinyukinsts):: text = ('5' :: character varying):: text
  ) THEN '督促対象' :: character varying WHEN (
    ("k".dinyukinsts):: text = ('7' :: character varying):: text
  ) THEN '自己破産' :: character varying WHEN (
    ("k".dinyukinsts):: text = ('9' :: character varying):: text
  ) THEN '回収なし' :: character varying ELSE '※要確認※' :: character varying END AS nyukinsts, 
  CASE WHEN (
    ("k".dihoryu):: text = ('0' :: character varying):: text
  ) THEN NULL :: character varying WHEN (
    ("k".dihoryu):: text = ('1' :: character varying):: text
  ) THEN '保留' :: character varying ELSE '※要確認※' :: character varying END AS horyu, 
  CASE WHEN (
    ("k".dicancel):: text = ('0' :: character varying):: text
  ) THEN NULL :: character varying WHEN (
    ("k".dicancel):: text = ('1' :: character varying):: text
  ) THEN 'キャンセル' :: character varying ELSE '※要確認※' :: character varying END AS "cancel", 
  to_char(
    "k".dsnyukindt, 
    (
      'YYYY/MM/DD HH24:MI:SS' :: character varying
    ):: text
  ) AS nyukindate, 
  to_char(
    o.dsorderdt, 
    ('YYYYMMDD' :: character varying):: text
  ) AS juchdateym, 
  to_char(
    "k".dsnyukindt, 
    ('YYYYMMDD' :: character varying):: text
  ) AS nyukindateym 
FROM 
  (
    (
      tbecorder o 
      JOIN c_tbeckesai "k" ON (
        (
          (o.diorderid = "k".diorderid) 
          AND (
            ("k".dielimflg):: text = ('0' :: character varying):: text
          )
        )
      )
    ) 
    LEFT JOIN tm64kessai_nm nm ON (
      (
        ("k".dskessaihoho):: text = (nm.code):: text
      )
    )
  ) 
WHERE 
  (
    (
      (
        (o.dielimflg):: text = ('0' :: character varying):: text
      ) 
      AND (
        o.dsorderdt >= to_date(
          ('20170201' :: character varying):: text, 
          ('YYYYMMDD' :: character varying):: text
        )
      )
    ) 
    AND (
      ("k".dskessaihoho):: text = ('1' :: character varying):: text
    )
  ) 
ORDER BY 
  o.diordercode DESC, 
  "k".c_dikesaiid DESC
)
select * from final