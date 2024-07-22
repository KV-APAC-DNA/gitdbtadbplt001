WITH kesai_h_data_mart_mv_kizuna
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.kesai_h_data_mart_mv_kizuna
  ),
kesai_m_data_mart_mv_kizuna
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.kesai_m_data_mart_mv_kizuna
  ),
item_z_h_hen_v
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.item_z_h_hen_v
  ),
item_zaiko_v
AS (
  SELECT *
  FROM dev_dna_core.snapjpdcledw_integration.item_zaiko_v
  ),
transformed
AS (
  SELECT ORD.KOKYANO
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｵｰﾙｲﾝﾜﾝｹﾞﾙ' THEN MEI.BUN_SURYO ---CHANGE FOR KIZUNA
                ELSE 0
           END) CNT_ATTR001
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｵｰﾙｲﾝﾜﾝｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR001
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｵｰﾙｲﾝﾜﾝｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR001
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾘｰﾑ' THEN MEI.BUN_SURYO  --CHANGE FOR KIZUNA
                ELSE 0
           END) CNT_ATTR002
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾘｰﾑ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR002
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾘｰﾑ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR002
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_美白' THEN MEI.BUN_SURYO --CHANGE FOR KIZUNA
                ELSE 0
           END) CNT_ATTR003
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_美白' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR003
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_美白' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR003
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_美白' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR004
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_美白' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR004
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_美白' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR004
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_保湿' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR005
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_保湿' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR005
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_保湿' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR005
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR006
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR006
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR006
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '食品' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR007
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '食品' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR007
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '食品' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR007
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR008
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR008
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR008
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ_ｹﾞﾙ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR009
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ_ｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR009
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｸﾚﾝｼﾞﾝｸﾞ_ｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR009
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ(ｿｰﾌﾟ)' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR010
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ(ｿｰﾌﾟ)' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR010
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｳｫｯｼﾝｸﾞ(ｿｰﾌﾟ)' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR010
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾎﾞﾃﾞｨｹｱ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR011
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾎﾞﾃﾞｨｹｱ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR011
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾎﾞﾃﾞｨｹｱ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR011
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾊﾟｯｸ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR012
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾊﾟｯｸ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR012
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾊﾟｯｸ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR012
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_保湿' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR013
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_保湿' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR013
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_保湿' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR013
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾄﾗｲｱﾙ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR014
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾄﾗｲｱﾙ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR014
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾄﾗｲｱﾙ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR014
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾋﾟｰﾘﾝｸﾞ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR015
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾋﾟｰﾘﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR015
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾋﾟｰﾘﾝｸﾞ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR015
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '医薬品' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR016
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '医薬品' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR016
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '医薬品' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR016
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾌﾞｰｽﾀｰ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR017
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾌﾞｰｽﾀｰ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR017
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾌﾞｰｽﾀｰ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR017
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_小物' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR018
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_小物' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR018
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_小物' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR018
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'BB' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR019
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'BB' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR019
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'BB' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR019
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_下地' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR020
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_下地' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR020
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_下地' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR020
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾌｧﾝﾃﾞ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR021
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾌｧﾝﾃﾞ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR021
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾌｧﾝﾃﾞ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR021
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_シワ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR022
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_シワ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR022
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_シワ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR022
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV(ﾐﾙｸ)' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR023
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV(ﾐﾙｸ)' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR023
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV(ﾐﾙｸ)' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR023
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_カラー' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR024
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_カラー' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR024
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_カラー' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR024
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_育毛・保湿' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR025
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_育毛・保湿' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR025
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_育毛・保湿' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR025
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_ｼｬﾝﾄﾘ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR026
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_ｼｬﾝﾄﾘ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR026
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ヘアケア_ｼｬﾝﾄﾘ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR026
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ふきとり' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR027
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ふきとり' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR027
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ふきとり' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR027
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '無料ｻﾝﾌﾟﾙｾｯﾄ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR028
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '無料ｻﾝﾌﾟﾙｾｯﾄ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR028
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '無料ｻﾝﾌﾟﾙｾｯﾄ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR028
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞBB' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR029
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞBB' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR029
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞBB' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR029
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞｹﾞﾙ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR030
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR030
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾗﾎﾞﾗﾎﾞｹﾞﾙ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR030
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR031
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR031
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR031
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR032
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR032
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'UV' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR032
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾎﾟｲﾝﾄﾒｲｸ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR033
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾎﾟｲﾝﾄﾒｲｸ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR033
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾎﾟｲﾝﾄﾒｲｸ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR033
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾘｯﾌﾟ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR034
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾘｯﾌﾟ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR034
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾘｯﾌﾟ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR034
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ｱｸﾈ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR035
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ｱｸﾈ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR035
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ｱｸﾈ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR035
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾘｯﾌﾟ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR036
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾘｯﾌﾟ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR036
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容液_ﾘｯﾌﾟ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR036
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ニキビ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR037
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ニキビ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR037
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '化粧水_ニキビ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR037
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾊﾟｳﾀﾞｰ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR038
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾊﾟｳﾀﾞｰ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR038
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾒｲｸ_ﾊﾟｳﾀﾞｰ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR038
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｮｯﾊﾟｰ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR039
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｮｯﾊﾟｰ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR039
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｮｯﾊﾟｰ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR039
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'メンズ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR040
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'メンズ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR040
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'メンズ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR040
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容機器' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR041
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容機器' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR041
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = '美容機器' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR041
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾍﾞﾋﾞｰ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR042
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾍﾞﾋﾞｰ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR042
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾍﾞﾋﾞｰ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR042
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｪｲｶｰ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR043
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｪｲｶｰ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR043
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｪｲｶｰ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR043
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾌﾞﾗﾝﾄ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR044
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾌﾞﾗﾝﾄ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR044
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ﾌﾞﾗﾝﾄ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR044
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｰｽﾞﾗﾎﾞ専売' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR045
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｰｽﾞﾗﾎﾞ専売' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR045
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｼｰｽﾞﾗﾎﾞ専売' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR045
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｾﾄﾞﾅ専売' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR046
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｾﾄﾞﾅ専売' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR046
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｾﾄﾞﾅ専売' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR046
      ,SUM(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｽﾎﾟｲﾄ' THEN MEI.BUN_SURYO
                ELSE 0
           END) CNT_ATTR047
      ,MIN(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｽﾎﾟｲﾄ' THEN ORD.SHUKADATE
                ELSE NULL
           END) FST_ATTR047
      ,MAX(CASE WHEN ITM.Z_B7_ADD_ATTR8 = 'ｽﾎﾟｲﾄ' THEN ORD.SHUKADATE
                ELSE 0
           END) LST_ATTR047
      ,NULL INSERTED_BY
      ,NULL UPDATED_BY
/*FROM   jp_dcl_edw.KESAI_H_DATA_MART_MV_kizuna ORD
       INNER JOIN jp_dcl_edw.KESAI_M_DATA_MART_MV_kizuna MEI
               ON MEI.SALENO_KEY = ORD.SALENO_KEY
       LEFT  JOIN (SELECT DISTINCT
                          Z_ITEM_CD
                         ,Z_ITEM_NM
                         ,Z_B7_ADD_ATTR1
                         ,Z_B7_ADD_ATTR2
                         ,Z_B7_ADD_ATTR3
                         ,Z_B7_ADD_ATTR4
                         ,Z_B7_ADD_ATTR5
                         ,Z_B7_ADD_ATTR6
                         ,Z_B7_ADD_ATTR7
                         ,Z_B7_ADD_ATTR8
                         ,Z_B7_ADD_ATTR9
                   FROM jp_dcl_edw.ITEM_DATA_MART_MV) ITM
               ON ITM.Z_ITEM_CD = MEI.BUN_ITEMCODE_P
WHERE MEI.BUN_ITEMCODE_P IS NOT NULL
AND   MEI.ANBUNMEISAINUKIKINGAKU_P > 0
GROUP BY ORD.KOKYANO)
;*/
FROM   kesai_h_data_mart_mv_kizuna ORD
       INNER JOIN kesai_m_data_mart_mv_kizuna MEI
               ON MEI.SALENO_KEY = ORD.SALENO_KEY
       left join item_z_h_hen_v HENKAN 
               on  mei.bun_itemcode = henkan.h_itemcode
       LEFT JOIN (SELECT DISTINCT 
                          z_itemcode as Z_ITEM_CD ,
                          z_itemname as Z_ITEM_NM ,
                          bumon7_add_attr1 as Z_B7_ADD_ATTR1 ,
                          bumon7_add_attr2 as Z_B7_ADD_ATTR2 ,
                          bumon7_add_attr3 as Z_B7_ADD_ATTR3 ,
                          bumon7_add_attr4 as Z_B7_ADD_ATTR4 ,
                          bumon7_add_attr5 as Z_B7_ADD_ATTR5 ,
                          bumon7_add_attr6 as Z_B7_ADD_ATTR6 ,
                          bumon7_add_attr7 as Z_B7_ADD_ATTR7 ,
                          bumon7_add_attr8 as Z_B7_ADD_ATTR8 ,
                          bumon7_add_attr9 as Z_B7_ADD_ATTR9
                   FROM	item_zaiko_v) ITM
               ON  ITM.Z_ITEM_CD = henkan.z_itemcode
WHERE 	mei.bun_meisainukikingaku >0 and ITM.Z_ITEM_CD is not null
GROUP BY ORD.KOKYANO
),
final
AS (
  SELECT 
        kokyano::varchar(30) as kokyano,
        cnt_attr001::number(38,18) as cnt_attr001,
        fst_attr001::number(38,18) as fst_attr001,
        lst_attr001::number(38,18) as lst_attr001,
        cnt_attr002::number(38,18) as cnt_attr002,
        fst_attr002::number(38,18) as fst_attr002,
        lst_attr002::number(38,18) as lst_attr002,
        cnt_attr003::number(38,18) as cnt_attr003,
        fst_attr003::number(38,18) as fst_attr003,
        lst_attr003::number(38,18) as lst_attr003,
        cnt_attr004::number(38,18) as cnt_attr004,
        fst_attr004::number(38,18) as fst_attr004,
        lst_attr004::number(38,18) as lst_attr004,
        cnt_attr005::number(38,18) as cnt_attr005,
        fst_attr005::number(38,18) as fst_attr005,
        lst_attr005::number(38,18) as lst_attr005,
        cnt_attr006::number(38,18) as cnt_attr006,
        fst_attr006::number(38,18) as fst_attr006,
        lst_attr006::number(38,18) as lst_attr006,
        cnt_attr007::number(38,18) as cnt_attr007,
        fst_attr007::number(38,18) as fst_attr007,
        lst_attr007::number(38,18) as lst_attr007,
        cnt_attr008::number(38,18) as cnt_attr008,
        fst_attr008::number(38,18) as fst_attr008,
        lst_attr008::number(38,18) as lst_attr008,
        cnt_attr009::number(38,18) as cnt_attr009,
        fst_attr009::number(38,18) as fst_attr009,
        lst_attr009::number(38,18) as lst_attr009,
        cnt_attr010::number(38,18) as cnt_attr010,
        fst_attr010::number(38,18) as fst_attr010,
        lst_attr010::number(38,18) as lst_attr010,
        cnt_attr011::number(38,18) as cnt_attr011,
        fst_attr011::number(38,18) as fst_attr011,
        lst_attr011::number(38,18) as lst_attr011,
        cnt_attr012::number(38,18) as cnt_attr012,
        fst_attr012::number(38,18) as fst_attr012,
        lst_attr012::number(38,18) as lst_attr012,
        cnt_attr013::number(38,18) as cnt_attr013,
        fst_attr013::number(38,18) as fst_attr013,
        lst_attr013::number(38,18) as lst_attr013,
        cnt_attr014::number(38,18) as cnt_attr014,
        fst_attr014::number(38,18) as fst_attr014,
        lst_attr014::number(38,18) as lst_attr014,
        cnt_attr015::number(38,18) as cnt_attr015,
        fst_attr015::number(38,18) as fst_attr015,
        lst_attr015::number(38,18) as lst_attr015,
        cnt_attr016::number(38,18) as cnt_attr016,
        fst_attr016::number(38,18) as fst_attr016,
        lst_attr016::number(38,18) as lst_attr016,
        cnt_attr017::number(38,18) as cnt_attr017,
        fst_attr017::number(38,18) as fst_attr017,
        lst_attr017::number(38,18) as lst_attr017,
        cnt_attr018::number(38,18) as cnt_attr018,
        fst_attr018::number(38,18) as fst_attr018,
        lst_attr018::number(38,18) as lst_attr018,
        cnt_attr019::number(38,18) as cnt_attr019,
        fst_attr019::number(38,18) as fst_attr019,
        lst_attr019::number(38,18) as lst_attr019,
        cnt_attr020::number(38,18) as cnt_attr020,
        fst_attr020::number(38,18) as fst_attr020,
        lst_attr020::number(38,18) as lst_attr020,
        cnt_attr021::number(38,18) as cnt_attr021,
        fst_attr021::number(38,18) as fst_attr021,
        lst_attr021::number(38,18) as lst_attr021,
        cnt_attr022::number(38,18) as cnt_attr022,
        fst_attr022::number(38,18) as fst_attr022,
        lst_attr022::number(38,18) as lst_attr022,
        cnt_attr023::number(38,18) as cnt_attr023,
        fst_attr023::number(38,18) as fst_attr023,
        lst_attr023::number(38,18) as lst_attr023,
        cnt_attr024::number(38,18) as cnt_attr024,
        fst_attr024::number(38,18) as fst_attr024,
        lst_attr024::number(38,18) as lst_attr024,
        cnt_attr025::number(38,18) as cnt_attr025,
        fst_attr025::number(38,18) as fst_attr025,
        lst_attr025::number(38,18) as lst_attr025,
        cnt_attr026::number(38,18) as cnt_attr026,
        fst_attr026::number(38,18) as fst_attr026,
        lst_attr026::number(38,18) as lst_attr026,
        cnt_attr027::number(38,18) as cnt_attr027,
        fst_attr027::number(38,18) as fst_attr027,
        lst_attr027::number(38,18) as lst_attr027,
        cnt_attr028::number(38,18) as cnt_attr028,
        fst_attr028::number(38,18) as fst_attr028,
        lst_attr028::number(38,18) as lst_attr028,
        cnt_attr029::number(38,18) as cnt_attr029,
        fst_attr029::number(38,18) as fst_attr029,
        lst_attr029::number(38,18) as lst_attr029,
        cnt_attr030::number(38,18) as cnt_attr030,
        fst_attr030::number(38,18) as fst_attr030,
        lst_attr030::number(38,18) as lst_attr030,
        cnt_attr031::number(38,18) as cnt_attr031,
        fst_attr031::number(38,18) as fst_attr031,
        lst_attr031::number(38,18) as lst_attr031,
        cnt_attr032::number(38,18) as cnt_attr032,
        fst_attr032::number(38,18) as fst_attr032,
        lst_attr032::number(38,18) as lst_attr032,
        cnt_attr033::number(38,18) as cnt_attr033,
        fst_attr033::number(38,18) as fst_attr033,
        lst_attr033::number(38,18) as lst_attr033,
        cnt_attr034::number(38,18) as cnt_attr034,
        fst_attr034::number(38,18) as fst_attr034,
        lst_attr034::number(38,18) as lst_attr034,
        cnt_attr035::number(38,18) as cnt_attr035,
        fst_attr035::number(38,18) as fst_attr035,
        lst_attr035::number(38,18) as lst_attr035,
        cnt_attr036::number(38,18) as cnt_attr036,
        fst_attr036::number(38,18) as fst_attr036,
        lst_attr036::number(38,18) as lst_attr036,
        cnt_attr037::number(38,18) as cnt_attr037,
        fst_attr037::number(38,18) as fst_attr037,
        lst_attr037::number(38,18) as lst_attr037,
        cnt_attr038::number(38,18) as cnt_attr038,
        fst_attr038::number(38,18) as fst_attr038,
        lst_attr038::number(38,18) as lst_attr038,
        cnt_attr039::number(38,18) as cnt_attr039,
        fst_attr039::number(38,18) as fst_attr039,
        lst_attr039::number(38,18) as lst_attr039,
        cnt_attr040::number(38,18) as cnt_attr040,
        fst_attr040::number(38,18) as fst_attr040,
        lst_attr040::number(38,18) as lst_attr040,
        cnt_attr041::number(38,18) as cnt_attr041,
        fst_attr041::number(38,18) as fst_attr041,
        lst_attr041::number(38,18) as lst_attr041,
        cnt_attr042::number(38,18) as cnt_attr042,
        fst_attr042::number(38,18) as fst_attr042,
        lst_attr042::number(38,18) as lst_attr042,
        cnt_attr043::number(38,18) as cnt_attr043,
        fst_attr043::number(38,18) as fst_attr043,
        lst_attr043::number(38,18) as lst_attr043,
        cnt_attr044::number(38,18) as cnt_attr044,
        fst_attr044::number(38,18) as fst_attr044,
        lst_attr044::number(38,18) as lst_attr044,
        cnt_attr045::number(38,18) as cnt_attr045,
        fst_attr045::number(38,18) as fst_attr045,
        lst_attr045::number(38,18) as lst_attr045,
        cnt_attr046::number(38,18) as cnt_attr046,
        fst_attr046::number(38,18) as fst_attr046,
        lst_attr046::number(38,18) as lst_attr046,
        cnt_attr047::number(38,18) as cnt_attr047,
        fst_attr047::number(38,18) as fst_attr047,
        lst_attr047::number(38,18) as lst_attr047,
        current_timestamp()::timestamp_ntz(9) AS inserted_date,
        inserted_by::varchar(100) as inserted_by,
        current_timestamp()::timestamp_ntz(9) AS updated_date,
        updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final