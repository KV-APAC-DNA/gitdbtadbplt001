{{
    config
    (
        materialized = 'incremental',
        incremental_strategy = 'append',
        pre_hook = "
                    {% if is_incremental() %}
                    DELETE
                    FROM {{this}}
                    WHERE order_no IN (
                            SELECT distinct order_no
                            FROM {{source('jpdclsdl_raw','rakutenorderdetail')}}
                            );
                    {% endif %}
                    "
    )
}}

with sources
as
(
    select * from {{source('jpdclsdl_raw','rakutenorderdetail')}}
), 
final as
(
    SELECT DISTINCT 
        ORDER_NO::VARCHAR(1000) as ORDER_NO,
        STATUS::VARCHAR(1000) as STATUS,
        SUB_STATUS_ID::VARCHAR(1000) as SUB_STATUS_ID,
        SUB_STATUS::VARCHAR(1000) as SUB_STATUS,
        ORDER_DATETIME::VARCHAR(1000) as ORDER_DATETIME,
        ORDER_DATE::VARCHAR(1000) as ORDER_DATE,
        ORDER_TIME::VARCHAR(1000) as ORDER_TIME,
        CANCEL_DUE_DATE::VARCHAR(1000) as CANCEL_DUE_DATE,
        ORDER_CHECK_DATETIME::VARCHAR(1000) as ORDER_CHECK_DATETIME,
        ORDER_CONFIRM_DATETIME::VARCHAR(1000) as ORDER_CONFIRM_DATETIME,
        SHIP_CHECK_DATETIME::VARCHAR(1000) as SHIP_CHECK_DATETIME,
        SHIP_COMPLETE_DATETIME::VARCHAR(1000) as SHIP_COMPLETE_DATETIME,
        PAYMENT_TYPE::VARCHAR(1000) as PAYMENT_TYPE,
        CCARD_TYPE::VARCHAR(1000) as CCARD_TYPE,
        CCARD_TIMES::VARCHAR(1000) as CCARD_TIMES,
        SHIP_TYPE::VARCHAR(1000) as SHIP_TYPE,
        SHIP_CATEGORY::VARCHAR(1000) as SHIP_CATEGORY,
        ORDER_TYPE::VARCHAR(1000) as ORDER_TYPE,
        MLTPL_SHIP_FLG::VARCHAR(1000) as MLTPL_SHIP_FLG,
        SHIP_ADDR_MATCH_FLG::VARCHAR(1000) as SHIP_ADDR_MATCH_FLG,
        REMOTE_ISLAND_FLG::VARCHAR(1000) as REMOTE_ISLAND_FLG,
        RAKUTEN_CONFIRM_FLG::VARCHAR(1000) as RAKUTEN_CONFIRM_FLG,
        WARNING_DISPLAY_FLG::VARCHAR(1000) as WARNING_DISPLAY_FLG,
        RAKUTEN_CUSTOMER_FLG::VARCHAR(1000) as RAKUTEN_CUSTOMER_FLG,
        PURCHASE_HISTORY_MODIFICATION_FLG::VARCHAR(1000) as PURCHASE_HISTORY_MODIFICATION_FLG,
        PRODUCT_TOTAL_PRICE::VARCHAR(1000) as PRODUCT_TOTAL_PRICE,
        TAX::VARCHAR(1000) as TAX,
        SHIP_FEE::VARCHAR(1000) as SHIP_FEE,
        SHIP_FEE_INPERSON::VARCHAR(1000) as SHIP_FEE_INPERSON,
        BILLING_AMT::VARCHAR(1000) as BILLING_AMT,
        TOTAL_PRICE::VARCHAR(1000) as TOTAL_PRICE,
        RAKUTEN_POINT::VARCHAR(1000) as RAKUTEN_POINT,
        RAKUTEN_COUPON::VARCHAR(1000) as RAKUTEN_COUPON,
        CIW_COUPON_STORE::VARCHAR(1000) as CIW_COUPON_STORE,
        CIW_COUPON_RAKUTEN::VARCHAR(1000) as CIW_COUPON_RAKUTEN,
        POST_CODE1::VARCHAR(1000) as POST_CODE1,
        POST_CODE2::VARCHAR(1000) as POST_CODE2,
        ADDR1::VARCHAR(1000) as ADDR1,
        ADDR2::VARCHAR(1000) as ADDR2,
        ADDR3::VARCHAR(1000) as ADDR3,
        LASTNAME::VARCHAR(1000) as LASTNAME,
        FIRSTNAME::VARCHAR(1000) as FIRSTNAME,
        LASTNAME_KANA::VARCHAR(1000) as LASTNAME_KANA,
        FIRSTNAME_KANA::VARCHAR(1000) as FIRSTNAME_KANA,
        MOBILE1::VARCHAR(1000) as MOBILE1,
        MOBILE2::VARCHAR(1000) as MOBILE2,
        MOBILE3::VARCHAR(1000) as MOBILE3,
        EMAIL_ADDR::VARCHAR(1000) as EMAIL_ADDR,
        GENDER::VARCHAR(1000) as GENDER,
        APPLICATION_NO::VARCHAR(1000) as APPLICATION_NO,
        DELIVERY_CNT::VARCHAR(1000) as DELIVERY_CNT,
        RECIPIENT_ID::VARCHAR(1000) as RECIPIENT_ID,
        RECIPIENT_SORYO::VARCHAR(1000) as RECIPIENT_SORYO,
        RECIPIENT_INPERSON_FEE::VARCHAR(1000) as RECIPIENT_INPERSON_FEE,
        RECIPIENT_TOTAL_TAX::VARCHAR(1000) as RECIPIENT_TOTAL_TAX,
        RECIPIENT_PRODUCT_TOTAL_AMT::VARCHAR(1000) as RECIPIENT_PRODUCT_TOTAL_AMT,
        RECIPIENT_TOAL_PRICE::VARCHAR(1000) as RECIPIENT_TOAL_PRICE,
        NOSHI::VARCHAR(1000) as NOSHI,
        RECIPIENT_POST_CODE1::VARCHAR(1000) as RECIPIENT_POST_CODE1,
        RECIPIENT_POST_CODE2::VARCHAR(1000) as RECIPIENT_POST_CODE2,
        RECIPIENT_ADDR1::VARCHAR(1000) as RECIPIENT_ADDR1,
        RECIPIENT_ADDR2::VARCHAR(1000) as RECIPIENT_ADDR2,
        RECIPIENT_ADDR3::VARCHAR(1000) as RECIPIENT_ADDR3,
        RECIPIENT_LASTNAME::VARCHAR(1000) as RECIPIENT_LASTNAME,
        RECIPIENT_FIRSTNAME::VARCHAR(1000) as RECIPIENT_FIRSTNAME,
        RECIPIENT_LASTNAME_KANA::VARCHAR(1000) as RECIPIENT_LASTNAME_KANA,
        RECIPIENT_FIRSTNAME_KANA::VARCHAR(1000) as RECIPIENT_FIRSTNAME_KANA,
        RECIPIENT_MOBILE1::VARCHAR(1000) as RECIPIENT_MOBILE1,
        RECIPIENT_MOBILE2::VARCHAR(1000) as RECIPIENT_MOBILE2,
        RECIPIENT_MOBILE3::VARCHAR(1000) as RECIPIENT_MOBILE3,
        ITEM_MEISAI_ID::VARCHAR(1000) as ITEM_MEISAI_ID,
        ITEM_ID::VARCHAR(1000) as ITEM_ID,
        ITEM_NM::VARCHAR(1000) as ITEM_NM,
        ITEM_NO::VARCHAR(1000) as ITEM_NO,
        ITEM_CONTROL_NO::VARCHAR(1000) as ITEM_CONTROL_NO,
        UNIT_PRICE::VARCHAR(1000) as UNIT_PRICE,
        QTY::VARCHAR(1000) as QTY,
        SHIPMENT_FEE_FLG::VARCHAR(1000) as SHIPMENT_FEE_FLG,
        TAX_FLG::VARCHAR(1000) as TAX_FLG,
        INPERSON_FEE_FLG::VARCHAR(1000) as INPERSON_FEE_FLG,
        CONVERSATION_W_USER::VARCHAR(1000) as CONVERSATION_W_USER,
        POINT_MULTIPLIER::VARCHAR(1000) as POINT_MULTIPLIER,
        DELIVERY_INFO::VARCHAR(1000) as DELIVERY_INFO,
        INVENTORY_TYPE::VARCHAR(1000) as INVENTORY_TYPE,
        WRAP_TITLE1::VARCHAR(1000) as WRAP_TITLE1,
        WRAP_NAME1::VARCHAR(1000) as WRAP_NAME1,
        WRAP_PRICE1::VARCHAR(1000) as WRAP_PRICE1,
        WRAP_TAX_FLG1::VARCHAR(1000) as WRAP_TAX_FLG1,
        WRAP_TYPE1::VARCHAR(1000) as WRAP_TYPE1,
        WRAP_TITLE2::VARCHAR(1000) as WRAP_TITLE2,
        WRAP_NAME2::VARCHAR(1000) as WRAP_NAME2,
        WRAP_PRICE2::VARCHAR(1000) as WRAP_PRICE2,
        WRAP_TAX_FLG2::VARCHAR(1000) as WRAP_TAX_FLG2,
        WRAP_TYPE2::VARCHAR(1000) as WRAP_TYPE2,
        DELIVERY_TIME::VARCHAR(1000) as DELIVERY_TIME,
        DELIVERY_DT_INFO::VARCHAR(1000) as DELIVERY_DT_INFO,
        PERSON_INCHARGE::VARCHAR(1000) as PERSON_INCHARGE,
        NOTE::VARCHAR(1000) as NOTE,
        MSG_TO_CUSTOMER::VARCHAR(1000) as MSG_TO_CUSTOMER,
        GIFT_REQUEST::VARCHAR(1000) as GIFT_REQUEST,
        COMMENT::VARCHAR(1000) as COMMENT,
        DEVICE::VARCHAR(1000) as DEVICE,
        MAIL_CARRIER_CD::VARCHAR(1000) as MAIL_CARRIER_CD,
        ASURAKU_FLG::VARCHAR(1000) as ASURAKU_FLG,
        PHARMA_ITEM_FLG::VARCHAR(1000) as PHARMA_ITEM_FLG,
        RAKUTEN_SUPER_DEAL_FLG::VARCHAR(1000) as RAKUTEN_SUPER_DEAL_FLG,
        MEMBERSHIP_PRGM_TYPE::VARCHAR(1000) as MEMBERSHIP_PRGM_TYPE,
        SETTLEMENT_FEE::VARCHAR(1000) as SETTLEMENT_FEE,
        CONTRIB_FEE::VARCHAR(1000) as CONTRIB_FEE,
        STORE_FEE::VARCHAR(1000) as STORE_FEE,
        EXCLUDED_TAX::VARCHAR(1000) as EXCLUDED_TAX,
        SETTLEMENT_TAX::VARCHAR(1000) as SETTLEMENT_TAX,
        WRAP_TAX_RATE1::VARCHAR(1000) as WRAP_TAX_RATE1,
        WRAP_TAX1::VARCHAR(1000) as WRAP_TAX1,
        WRAP_TAX_RATE2::VARCHAR(1000) as WRAP_TAX_RATE2,
        WRAP_TAX2::VARCHAR(1000) as WRAP_TAX2,
        DELIVERY_EXCLUDED_TAX::VARCHAR(1000) as DELIVERY_EXCLUDED_TAX,
        DELIVERY_SHIP_FEE_TAX::VARCHAR(1000) as DELIVERY_SHIP_FEE_TAX,
        DELIVERY_INPERSON_FEE_TAX::VARCHAR(1000) as DELIVERY_INPERSON_FEE_TAX,
        PRODUCT_TAX_RATE::VARCHAR(1000) as PRODUCT_TAX_RATE,
        PRODUCT_PRICE_TAXIN::VARCHAR(1000) as PRODUCT_PRICE_TAXIN,
        TAX_RATE_TAX10::VARCHAR(1000) as TAX_RATE_TAX10,
        BILLING_AMT_TAX10::VARCHAR(1000) as BILLING_AMT_TAX10,
        TAX10::VARCHAR(1000) as TAX10,
        TOTAL_PRICE_TAX10::VARCHAR(1000) as TOTAL_PRICE_TAX10,
        PAYMENT_FEE_TAX10::VARCHAR(1000) as PAYMENT_FEE_TAX10,
        COUPON_TAX10::VARCHAR(1000) as COUPON_TAX10,
        POINT_TAX10::VARCHAR(1000) as POINT_TAX10,
        TAX_RATE_TAX8::VARCHAR(1000) as TAX_RATE_TAX8,
        BILLING_AMT_TAX8::VARCHAR(1000) as BILLING_AMT_TAX8,
        TAX8::VARCHAR(1000) as TAX8,
        TOTAL_PRICE_TAX8::VARCHAR(1000) as TOTAL_PRICE_TAX8,
        PAYMENT_FEE_TAX8::VARCHAR(1000) as PAYMENT_FEE_TAX8,
        COUPON_TAX8::VARCHAR(1000) as COUPON_TAX8,
        POINT_TAX8::VARCHAR(1000) as POINT_TAX8,
        TAX_RATE_NO_TAX::VARCHAR(1000) as TAX_RATE_NO_TAX,
        BILLING_AMT_NO_TAX::VARCHAR(1000) as BILLING_AMT_NO_TAX,
        NO_TAX::VARCHAR(1000) as NO_TAX,
        TOTAL_PRICE_NO_TAX::VARCHAR(1000) as TOTAL_PRICE_NO_TAX,
        PAYMENT_FEE_NO_TAX::VARCHAR(1000) as PAYMENT_FEE_NO_TAX,
        COUPON_TAX_NO_TAX::VARCHAR(1000) as COUPON_TAX_NO_TAX,
        POINT_TAX_NO_TAX::VARCHAR(1000) as POINT_TAX_NO_TAX,
        SINGLE_ITEM_DEL_FLG::VARCHAR(1000) as SINGLE_ITEM_DEL_FLG,
        DELIVERY_COMPANY::VARCHAR(1000) as DELIVERY_COMPANY,
        RECEIPT_CNT::VARCHAR(1000) as RECEIPT_CNT,
        RECEIPT_FIRST_SSUE_DT::VARCHAR(1000) as RECEIPT_FIRST_SSUE_DT,
        RECEIPT_LST_ISSUE_DT::VARCHAR(1000) as RECEIPT_LST_ISSUE_DT,
        PAYMENT_DUE_DT::VARCHAR(1000) as PAYMENT_DUE_DT,
        PAYMENT_CHANGE_DUE_DT::VARCHAR(1000) as PAYMENT_CHANGE_DUE_DT,
        REFUND_DUE_DT::VARCHAR(1000) as REFUND_DUE_DT,
        STORE_COUPON_CD::VARCHAR(1000) as STORE_COUPON_CD,
        STORE_COUPON_NM::VARCHAR(1000) as STORE_COUPON_NM,
        RAKUTEN_COUPON_CD::VARCHAR(1000) as RAKUTEN_COUPON_CD,
        RAKUTEN_COUPON_NM::VARCHAR(1000) as RAKUTEN_COUPON_NM,
        WARNING_INFO::VARCHAR(1000) as WARNING_INFO,
        SKU_NO::VARCHAR(1000) as SKU_NO,
        SKU_NO_INTEGRATION::VARCHAR(1000) as SKU_NO_INTEGRATION,
        SKU_INFO::VARCHAR(1000) as SKU_INFO,
        SOURCE_FILE_DATE::VARCHAR(30) as SOURCE_FILE_DATE,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as INSERTED_DATE,
        null::VARCHAR(100) as INSERTED_BY,
        CURRENT_TIMESTAMP()::TIMESTAMP_NTZ(9) as UPDATED_DATE,
        null::VARCHAR(9) as UPDATED_BY
    FROM sources --tmp_unique_rakutenorderdetail
)
select * from final