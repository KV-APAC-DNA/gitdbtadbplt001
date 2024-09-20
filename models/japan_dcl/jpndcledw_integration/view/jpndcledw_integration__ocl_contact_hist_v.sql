with tboutcallcontactdata as(
    select * from {{ ref('jpndcledw_integration__tboutcallcontactdata') }}
),

final as (
SELECT '0' AS sort_key,
    'Ctrlフラグ' AS ctrl_flg,
    '顧客ID' AS customer_id,
    'チャネル' AS channel,
    'コンタクト履歴番号' AS contact_no,
    '送付区分' AS send_kbn,
    '送付日' AS send_date,
    '件名' AS title,
    '拡張項目1' AS ext_data_1,
    '拡張項目2' AS ext_data_2,
    '拡張項目3' AS ext_data_3,
    '拡張項目4' AS ext_data_4,
    '拡張項目5' AS ext_data_5

UNION ALL

SELECT '1' AS sort_key,
    tboutcallcontactdata.dsctrlflg AS ctrl_flg,
    (lpad((tboutcallcontactdata.diusrid)::TEXT, 10, (0)::TEXT))::CHARACTER VARYING AS customer_id,
    tboutcallcontactdata.dschannel AS channel,
    tboutcallcontactdata.dscontactno AS contact_no,
    tboutcallcontactdata.dssendkbn AS send_kbn,
    tboutcallcontactdata.dssenddate AS send_date,
    tboutcallcontactdata.dstitle AS title,
    tboutcallcontactdata.dsextdat1 AS ext_data_1,
    tboutcallcontactdata.dsextdat2 AS ext_data_2,
    tboutcallcontactdata.dsextdat3 AS ext_data_3,
    tboutcallcontactdata.dsextdat4 AS ext_data_4,
    tboutcallcontactdata.dsextdat5 AS ext_data_5
FROM tboutcallcontactdata
ORDER BY 1)

select * from final