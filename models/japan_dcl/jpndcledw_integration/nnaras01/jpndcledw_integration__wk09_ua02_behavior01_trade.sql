with dm_kesai_mart_dly_general as (
  select 
    * 
  from 
    DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.DM_KESAI_MART_DLY_GENERAL
), 
edw_mds_jp_dcl_product_master as (
  select 
    * 
  from 
    DEV_DNA_CORE.SNAPJPDCLEDW_INTEGRATION.EDW_MDS_JP_DCL_PRODUCT_MASTER
), 
AID_MAIN AS (
    WITH AID AS (
      select 
        kokyano, 
        K.h_o_item_code as h_o_item_code_vc, 
        lag (order_dt, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_order_dt, 
        K.order_dt as order_dt, 
        lag (mds.attr10, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_attr10, 
        mds.attr10 
      from 
        (
          select 
            kokyano, 
            h_o_item_code, 
            order_dt 
          from 
            dm_kesai_mart_dly_general k 
          where 
            K.channel in (
              '通販', 'Web', '直営・百貨店'
            ) 
            and (
              K.juchkbn :: text = 0 :: character varying :: text 
              OR K.juchkbn :: text = 1 :: character varying :: text 
              OR K.juchkbn :: text = 2 :: character varying :: text
            ) 
            and K.meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
            and K.order_dt >= '2019-01-01' 
          group by 
            kokyano, 
            h_o_item_code, 
            order_dt
        ) K 
        left join edw_mds_jp_dcl_product_master mds on K.h_o_item_code = mds.itemcode 
      where 
        mds.attr10 is not null 
        and mds.attr10 in ('AID', 'AIDSP') 
      order by 
        kokyano asc, 
        order_dt asc
    ), 
    latest_order_dt AS (
      select 
        kokyano, 
        max(order_dt) order_dt_max 
      from 
        AID 
      group by 
        kokyano
    ), 
    AID_MULTI AS (
      select 
        * 
      from 
        (
          select 
            AID.kokyano Customer_No, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'AID'
              ) then AID.order_dt END
            ) AS Standard_New_Date_AID, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'AIDSP'
              ) then AID.order_dt END
            ) AS Premium_New_Date_AID, 
            (
              CASE WHEN (
                lag_attr10 = 'AID' 
                and attr10 = 'AIDSP'
              ) then AID.order_dt END
            ) AS Tradeup_Date_AID, 
            (
              CASE WHEN (
                lag_attr10 = 'AIDSP' 
                and attr10 = 'AID'
              ) then AID.order_dt END
            ) AS Tradedown_Date_AID, 
            NULL AS Trade_flag_AID 
          from 
            AID 
          UNION 
          SELECT 
            AID.kokyano Customer_No, 
            NULL :: date AS Standard_New_Date_AID, 
            NULL :: date AS Premium_New_Date_AID, 
            NULL :: date AS Tradeup_Date_AID, 
            NULL :: date AS Tradedown_Date_AID, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'AID' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_New_AID' WHEN (
                lag_attr10 = 'AID' 
                and attr10 = 'AID' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_Repeat_AID' WHEN (
                lag_attr10 is null 
                and attr10 = 'AIDSP' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_New_AID' WHEN (
                lag_attr10 = 'AIDSP' 
                and attr10 = 'AIDSP' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_Repeat_AID' WHEN (
                lag_attr10 = 'AID' 
                and attr10 = 'AIDSP' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradeup_AID' WHEN (
                lag_attr10 = 'AIDSP' 
                and attr10 = 'AID' 
                and AID.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradedown_AID' END
            ) AS Trade_flag_AID 
          from 
            AID 
            LEFT join latest_order_dt on AID.kokyano = latest_order_dt.kokyano
        ) 
      where 
        Standard_New_Date_AID is not null 
        or Premium_New_Date_AID is not null 
        or Tradeup_Date_AID is not null 
        or Tradedown_Date_AID is not null 
        or Trade_flag_AID is not null 
      order by 
        Customer_No
    ), 
    A AS (
      select 
        Customer_No, 
        Trade_flag_AID 
      from 
        AID_MULTI 
      WHERE 
        Trade_flag_AID IS NOT NULL
    ), 
    B AS (
      select 
        Customer_No, 
        max(Standard_New_Date_AID) Standard_New_Date_AID, 
        max(Premium_New_Date_AID) Premium_New_Date_AID, 
        max(Tradeup_Date_AID) Tradeup_Date_AID, 
        max(Tradedown_Date_AID) Tradedown_Date_AID 
      from 
        AID_MULTI 
      group by 
        Customer_No
    ) 
    select 
      A.Customer_No, 
      B.Standard_New_Date_AID, 
      B.Premium_New_Date_AID, 
      B.Tradeup_Date_AID, 
      B.Tradedown_Date_AID, 
      A.Trade_flag_AID 
    from 
      A 
      LEFT JOIN B ON A.Customer_No = B.Customer_No
  ), 
  VC100L_MAIN AS (
    WITH VC100L AS (
      select 
        kokyano, 
        K.h_o_item_code as h_o_item_code_vc, 
        lag (order_dt, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_order_dt, 
        K.order_dt as order_dt, 
        lag (mds.attr10, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_attr10, 
        mds.attr10 
      from 
        (
          select 
            kokyano, 
            h_o_item_code, 
            order_dt 
          from 
            dm_kesai_mart_dly_general k 
          where 
            K.channel in (
              '通販', 'Web', '直営・百貨店'
            ) 
            and (
              K.juchkbn :: text = 0 :: character varying :: text 
              OR K.juchkbn :: text = 1 :: character varying :: text 
              OR K.juchkbn :: text = 2 :: character varying :: text
            ) 
            and K.meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
            and K.order_dt >= '2019-01-01' 
          group by 
            kokyano, 
            h_o_item_code, 
            order_dt
        ) K 
        left join edw_mds_jp_dcl_product_master mds on K.h_o_item_code = mds.itemcode 
      where 
        mds.attr10 is not null 
        and mds.attr10 in ('VC100L', 'VC100LSP')
    ), 
    latest_order_dt AS (
      select 
        kokyano, 
        max(order_dt) order_dt_max 
      from 
        VC100L 
      group by 
        kokyano
    ), 
    VC100L_MULTI AS (
      select 
        * 
      from 
        (
          select 
            VC100L.kokyano Customer_No, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'VC100L'
              ) then VC100L.order_dt END
            ) AS Standard_New_Date_VC100L, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'VC100LSP'
              ) then VC100L.order_dt END
            ) AS Premium_New_Date_VC100L, 
            (
              CASE WHEN (
                lag_attr10 = 'VC100L' 
                and attr10 = 'VC100LSP'
              ) then VC100L.order_dt END
            ) AS Tradeup_Date_VC100L, 
            (
              CASE WHEN (
                lag_attr10 = 'VC100LSP' 
                and attr10 = 'VC100L'
              ) then VC100L.order_dt END
            ) AS Tradedown_Date_VC100L, 
            NULL AS Trade_flag_VC100L 
          from 
            VC100L 
          UNION 
          SELECT 
            VC100L.kokyano Customer_No, 
            NULL :: date AS Standard_New_Date_VC100L, 
            NULL :: date AS Premium_New_Date_VC100L, 
            NULL :: date AS Tradeup_Date_VC100L, 
            NULL :: date AS Tradedown_Date_VC100L, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'VC100L' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_New_VC100L' WHEN (
                lag_attr10 = 'VC100L' 
                and attr10 = 'VC100L' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_Repeat_VC100L' WHEN (
                lag_attr10 is null 
                and attr10 = 'VC100LSP' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_New_VC100L' WHEN (
                lag_attr10 = 'VC100LSP' 
                and attr10 = 'VC100LSP' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_Repeat_VC100L' WHEN (
                lag_attr10 = 'VC100L' 
                and attr10 = 'VC100LSP' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradeup_VC100L' WHEN (
                lag_attr10 = 'VC100LSP' 
                and attr10 = 'VC100L' 
                and VC100L.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradedown_VC100L' END
            ) AS Trade_flag_VC100L 
          from 
            VC100L 
            LEFT join latest_order_dt on VC100L.kokyano = latest_order_dt.kokyano
        ) 
      where 
        Standard_New_Date_VC100L is not null 
        or Premium_New_Date_VC100L is not null 
        or Tradeup_Date_VC100L is not null 
        or Tradedown_Date_VC100L is not null 
        or Trade_flag_VC100L is not null 
      order by 
        Customer_No
    ), 
    A AS (
      select 
        Customer_No, 
        Trade_flag_VC100L 
      from 
        VC100L_MULTI 
      WHERE 
        Trade_flag_VC100L IS NOT NULL
    ), 
    B AS (
      select 
        Customer_No, 
        max(Standard_New_Date_VC100L) Standard_New_Date_VC100L, 
        max(Premium_New_Date_VC100L) Premium_New_Date_VC100L, 
        max(Tradeup_Date_VC100L) Tradeup_Date_VC100L, 
        max(Tradedown_Date_VC100L) Tradedown_Date_VC100L 
      from 
        VC100L_MULTI 
      group by 
        Customer_No
    ) 
    select 
      A.Customer_No, 
      B.Standard_New_Date_VC100L, 
      B.Premium_New_Date_VC100L, 
      B.Tradeup_Date_VC100L, 
      B.Tradedown_Date_VC100L, 
      A.Trade_flag_VC100L 
    from 
      A 
      LEFT JOIN B ON A.Customer_No = B.Customer_No
  ), 
  ACGEL_MAIN AS (
    WITH ACGEL AS (
      select 
        kokyano, 
        K.h_o_item_code as h_o_item_code_vc, 
        lag (order_dt, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_order_dt, 
        K.order_dt as order_dt, 
        lag (mds.attr10, 1) over (
          partition by kokyano 
          order by 
            order_dt asc
        ) lag_attr10, 
        mds.attr10 
      from 
        (
          select 
            kokyano, 
            h_o_item_code, 
            order_dt 
          from 
            dm_kesai_mart_dly_general k 
          where 
            K.channel in (
              '通販', 'Web', '直営・百貨店'
            ) 
            and (
              K.juchkbn :: text = 0 :: character varying :: text 
              OR K.juchkbn :: text = 1 :: character varying :: text 
              OR K.juchkbn :: text = 2 :: character varying :: text
            ) 
            and K.meisainukikingaku <> 0 :: numeric :: numeric(18, 0) 
            and K.order_dt >= '2019-01-01' 
          group by 
            kokyano, 
            h_o_item_code, 
            order_dt
        ) K 
        left join edw_mds_jp_dcl_product_master mds on K.h_o_item_code = mds.itemcode 
      where 
        mds.attr10 is not null 
        and mds.attr10 in (
          'ACGEL', 'ACGELﾌﾟﾗｾﾝﾀ'
        )
    ), 
    latest_order_dt AS (
      select 
        kokyano, 
        max(order_dt) order_dt_max 
      from 
        ACGEL 
      group by 
        kokyano
    ), 
    ACGEL_MULTI AS (
      select 
        * 
      from 
        (
          select 
            ACGEL.kokyano Customer_No, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'ACGEL'
              ) then ACGEL.order_dt END
            ) AS Standard_New_Date_ACGEL, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'ACGELﾌﾟﾗｾﾝﾀ'
              ) then ACGEL.order_dt END
            ) AS Premium_New_Date_ACGEL, 
            (
              CASE WHEN (
                lag_attr10 = 'ACGEL' 
                and attr10 = 'ACGELﾌﾟﾗｾﾝﾀ'
              ) then ACGEL.order_dt END
            ) AS Tradeup_Date_ACGEL, 
            (
              CASE WHEN (
                lag_attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and attr10 = 'ACGEL'
              ) then ACGEL.order_dt END
            ) AS Tradedown_Date_ACGEL, 
            NULL AS Trade_flag_ACGEL 
          from 
            ACGEL 
          UNION 
          SELECT 
            ACGEL.kokyano Customer_No, 
            NULL :: date AS Standard_New_Date_ACGEL, 
            NULL :: date AS Premium_New_Date_ACGEL, 
            NULL :: date AS Tradeup_Date_ACGEL, 
            NULL :: date AS Tradedown_Date_ACGEL, 
            (
              CASE WHEN (
                lag_attr10 is null 
                and attr10 = 'ACGEL' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_New_ACGEL' WHEN (
                lag_attr10 = 'ACGEL' 
                and attr10 = 'ACGEL' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Standard_Repeat_ACGEL' WHEN (
                lag_attr10 is null 
                and attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_New_ACGEL' WHEN (
                lag_attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Premium_Repeat_ACGEL' WHEN (
                lag_attr10 = 'ACGEL' 
                and attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradeup_ACGEL' WHEN (
                lag_attr10 = 'ACGELﾌﾟﾗｾﾝﾀ' 
                and attr10 = 'ACGEL' 
                and ACGEL.order_dt = latest_order_dt.order_dt_max
              ) then 'Tradedown_ACGEL' END
            ) AS Trade_flag_ACGEL 
          from 
            ACGEL 
            LEFT join latest_order_dt on ACGEL.kokyano = latest_order_dt.kokyano
        ) 
      where 
        Standard_New_Date_ACGEL is not null 
        or Premium_New_Date_ACGEL is not null 
        or Tradeup_Date_ACGEL is not null 
        or Tradedown_Date_ACGEL is not null 
        or Trade_flag_ACGEL is not null 
      order by 
        Customer_No
    ), 
    A AS (
      select 
        Customer_No, 
        Trade_flag_ACGEL 
      from 
        ACGEL_MULTI 
      WHERE 
        Trade_flag_ACGEL IS NOT NULL
    ), 
    B AS (
      select 
        Customer_No, 
        max(Standard_New_Date_ACGEL) Standard_New_Date_ACGEL, 
        max(Premium_New_Date_ACGEL) Premium_New_Date_ACGEL, 
        max(Tradeup_Date_ACGEL) Tradeup_Date_ACGEL, 
        max(Tradedown_Date_ACGEL) Tradedown_Date_ACGEL 
      from 
        ACGEL_MULTI 
      group by 
        Customer_No
    ) 
    select 
      A.Customer_No, 
      B.Standard_New_Date_ACGEL, 
      B.Premium_New_Date_ACGEL, 
      B.Tradeup_Date_ACGEL, 
      B.Tradedown_Date_ACGEL, 
      A.Trade_flag_ACGEL 
    from 
      A 
      LEFT JOIN B ON A.Customer_No = B.Customer_No
  ),
 transformed as (select 
    distinct * 
  from 
    (
      select 
        AI.Customer_No, 
        AI.Standard_New_Date_AID, 
        AI.Premium_New_Date_AID, 
        AI.Tradeup_Date_AID, 
        AI.Tradedown_Date_AID, 
        AI.Trade_flag_AID, 
        VC.Standard_New_Date_VC100L, 
        VC.Premium_New_Date_VC100L, 
        VC.Tradeup_Date_VC100L, 
        VC.Tradedown_Date_VC100L, 
        VC.Trade_flag_VC100L, 
        AC.Standard_New_Date_ACGEL, 
        AC.Premium_New_Date_ACGEL, 
        AC.Tradeup_Date_ACGEL, 
        AC.Tradedown_Date_ACGEL, 
        AC.Trade_flag_ACGEL 
      from 
        AID_MAIN AI 
        LEFT JOIN VC100L_MAIN VC ON AI.Customer_No = VC.Customer_No 
        LEFT JOIN ACGEL_MAIN AC ON AI.Customer_No = AC.Customer_No 
      UNION ALL 
      select 
        VC.Customer_No, 
        AI.Standard_New_Date_AID, 
        AI.Premium_New_Date_AID, 
        AI.Tradeup_Date_AID, 
        AI.Tradedown_Date_AID, 
        AI.Trade_flag_AID, 
        VC.Standard_New_Date_VC100L, 
        VC.Premium_New_Date_VC100L, 
        VC.Tradeup_Date_VC100L, 
        VC.Tradedown_Date_VC100L, 
        VC.Trade_flag_VC100L, 
        AC.Standard_New_Date_ACGEL, 
        AC.Premium_New_Date_ACGEL, 
        AC.Tradeup_Date_ACGEL, 
        AC.Tradedown_Date_ACGEL, 
        AC.Trade_flag_ACGEL 
      from 
        VC100L_MAIN VC 
        LEFT JOIN AID_MAIN AI ON VC.Customer_No = AI.Customer_No 
        LEFT JOIN ACGEL_MAIN AC ON VC.Customer_No = AC.Customer_No 
      union ALL 
      select 
        AC.Customer_No, 
        AI.Standard_New_Date_AID, 
        AI.Premium_New_Date_AID, 
        AI.Tradeup_Date_AID, 
        AI.Tradedown_Date_AID, 
        AI.Trade_flag_AID, 
        VC.Standard_New_Date_VC100L, 
        VC.Premium_New_Date_VC100L, 
        VC.Tradeup_Date_VC100L, 
        VC.Tradedown_Date_VC100L, 
        VC.Trade_flag_VC100L, 
        AC.Standard_New_Date_ACGEL, 
        AC.Premium_New_Date_ACGEL, 
        AC.Tradeup_Date_ACGEL, 
        AC.Tradedown_Date_ACGEL, 
        AC.Trade_flag_ACGEL 
      from 
        ACGEL_MAIN AC 
        LEFT JOIN AID_MAIN AI ON AC.Customer_No = AI.Customer_No 
        LEFT JOIN VC100L_MAIN VC ON AC.Customer_No = VC.Customer_No
    )
),
final as (
select
customer_no::varchar(60) as customer_no,
standard_new_date_aid::date as standard_new_date_aid,
premium_new_date_aid::date as premium_new_date_aid,
tradeup_date_aid::date as tradeup_date_aid,
tradedown_date_aid::date as tradedown_date_aid,
trade_flag_aid::varchar(60) as trade_flag_aid,
standard_new_date_vc100L::date as standard_new_date_vc100,
premium_new_date_vc100L::date as premium_new_date_vc100,
tradeup_date_vc100L::date as tradeup_date_vc100,
tradedown_date_vc100L::date as tradedown_date_vc100,
trade_flag_vc100L::varchar(60) as trade_flag_vc100,
standard_new_date_acgel::date as standard_new_date_acgel,
premium_new_date_acgel::date as premium_new_date_acgel,
tradeup_date_acgel::date as tradeup_date_acgel,
tradedown_date_acgel::date as tradedown_date_acgel,
trade_flag_acgel::varchar(60) astrade_flag_acgel
from transformed
)
select * from final
