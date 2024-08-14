with 
teikikeiyaku_data_mart_uni as (
select * from {{ ref('jpndcledw_integration__teikikeiyaku_data_mart_uni') }}
),
cim05opera as (
select * from {{ ref('jpndcledw_integration__cim05opera') }}
),
c_tbecregularmeisai as (
select * from {{ ref('jpndclitg_integration__c_tbecregularmeisai') }}
),
tbecitem as (
select * from {{ ref('jpndclitg_integration__tbecitem') }}
),
final as (
SELECT 
  tr."契約日付", 
  tr.nextid, 
  op.opecode, 
  (
    (
      (op.bumoncode):: text || (' ' :: character varying):: text
    ) || (op.opename):: text
  ) AS opename, 
  tr."定期契約顧客no.", 
  tr.item_id, 
  tr.item_name, 
  tr.cnt_item, 
  tr.total_revenue, 
  tr.diitemsalescost, 
  tr.contract_kubun, 
  tr.order_id, 
  tr.kessai_id, 
  op.opecode AS userid, 
  op.logincode 
FROM 
  (
    (
      SELECT 
        t.keiyakubi AS "契約日付", 
        ltrim(
          (
            {{encryption_1('t.c_diusrid')}}
          ):: text, 
          ('0' :: character varying):: text
        ) AS nextid, 
        t.c_diregularcontractid AS "定期契約顧客no.", 
        tm.diprepusr AS userid, 
        t.dsitemid AS item_id, 
        i.dsitemname AS item_name, 
        count(t.dsitemid) AS cnt_item, 
        sum(t.diitemsalesprc) AS total_revenue, 
        t.contract_kbn AS contract_kubun, 
        t.diordercode AS order_id, 
        t.c_dikesaiid AS kessai_id, 
        t.dimeisaiid, 
        i.diitemsalescost, 
        t.c_dsregularmeisaiid 
      FROM 
        teikikeiyaku_data_mart_uni t, 
        tbecitem i, 
        (
          SELECT 
            c_tbecregularmeisai.c_diregularcontractid, 
            c_tbecregularmeisai.c_dsregularmeisaiid, 
            c_tbecregularmeisai.c_dstodokedate, 
            c_tbecregularmeisai.diprepusr, 
            c_tbecregularmeisai.c_dsteikifirstflg 
          FROM 
            c_tbecregularmeisai 
          WHERE 
            (
              (
                c_tbecregularmeisai.c_dsteikifirstflg
              ):: text = (
                (1):: character varying
              ):: text
            )
        ) tm 
      WHERE 
        (
          (
            (
              (
                (t.dsitemid):: text = (i.dsitemid):: text
              ) 
              AND (
                t.c_dsregularmeisaiid = tm.c_dsregularmeisaiid
              )
            ) 
            AND (
              (t.keiyakubi):: text >= (
                (20190101):: character varying
              ):: text
            )
          ) 
          AND (
            (t.contract_kbn):: text = ('新規' :: character varying):: text
          )
        ) 
      GROUP BY 
        t.keiyakubi, 
        ltrim(
          (
            {{encryption_1('t.c_diusrid')}}
          ):: text, 
          ('0' :: character varying):: text
        ), 
        t.c_diregularcontractid, 
        tm.diprepusr, 
        t.dsitemid, 
        i.dsitemname, 
        t.contract_kbn, 
        t.diordercode, 
        t.c_dikesaiid, 
        t.dimeisaiid, 
        i.diitemsalescost, 
        t.c_dsregularmeisaiid
    ) tr 
    LEFT JOIN (
      SELECT 
        c.logincode, 
        c.opename, 
        c.opecode, 
        c.bumoncode 
      FROM 
        cim05opera c 
      WHERE 
        (
          (c.ciflg):: text = ('NEXT' :: character varying):: text
        )
    ) op ON (
      (
        (
          (tr.userid):: character varying
        ):: text = (op.opecode):: text
      )
    )
  )
  )
select * from final