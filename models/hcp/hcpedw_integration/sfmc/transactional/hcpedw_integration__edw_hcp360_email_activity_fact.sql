with itg_hcp360_sfmc_open_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_open_data') }}
),
itg_hcp360_sfmc_bounce_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_bounce_data') }}
),
itg_hcp360_sfmc_click_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_click_data') }}
),
itg_hcp360_sfmc_sent_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_sent_data') }}
),
itg_hcp360_sfmc_forward_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_forward_data') }}
),
itg_hcp360_sfmc_unsubscribe_data as(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_unsubscribe_data') }}
),
final as
(   
select 
ACTIVITY_TYPE::VARCHAR(20) as ACTIVITY_TYPE,
COUNTRY::VARCHAR(10) as CNTRY_CD,
SUBSCRIBER_KEY::VARCHAR(100) as SUBSCRIBER_KEY,
JOB_ID::VARCHAR(20) as JOB_ID,
ACTIVITY_DATE::TIMESTAMP_NTZ(9) as ACTIVITY_DATE,
EMAIL_ID::VARCHAR(20) as EMAIL_ID,
EMAIL_NAME::VARCHAR(100) as EMAIL_NAME,
EMAIL_SUBJECT::VARCHAR(200) as EMAIL_SUBJECT,
EMAIL_DELIVERED_FLAG::VARCHAR(1) as EMAIL_DELIVERED_FLAG,
TRIGGERER_SEND_DEFINITION_OBJECT_ID::VARCHAR(50) as TRIGGERER_SEND_DEFINITION_OBJECT_ID,
IS_UNIQUE::VARCHAR(10) as IS_UNIQUE,
URL::VARCHAR(1000) as URL,
LINK_NAME::VARCHAR(200) as LINK_NAME,
LINK_CONTENT::VARCHAR(1000) as LINK_CONTENT,
CRT_DTTM::TIMESTAMP_NTZ(9) as CRT_DTTM,
UPDT_DTTM::TIMESTAMP_NTZ(9) as UPDT_DTTM,
SENT_ACTIVITY_DATE::TIMESTAMP_NTZ(9) as SENT_ACTIVITY_DATE
from
(
    SELECT
        'SENT' AS ACTIVITY_TYPE,
        'INDIA' AS COUNTRY,
        SENT.SUBSCRIBER_KEY,
        SENT.JOB_ID,
        SENT.EVENT_DATE AS ACTIVITY_DATE,
        SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
        SENT.EMAIL_ID,
        SENT.EMAIL_NAME,
        SENT.EMAIL_SUBJECT,
        CASE
                WHEN BOUNCE.SUBSCRIBER_KEY IS NULL THEN 'Y'
                ELSE 'N'
        END AS EMAIL_DELIVERED_FLAG,
        SENT.TRIGGERER_SEND_DEFINITION_OBJECT_ID,
        NULL AS IS_UNIQUE,
        NULL AS URL,
        NULL AS LINK_NAME,
        NULL AS LINK_CONTENT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        ITG_HCP360_SFMC_SENT_DATA SENT
        LEFT JOIN ITG_HCP360_SFMC_BOUNCE_DATA BOUNCE ON rtrim(SENT.SUBSCRIBER_KEY) = rtrim(BOUNCE.SUBSCRIBER_KEY)
        AND rtrim(SENT.EMAIL_NAME) = rtrim(BOUNCE.EMAIL_NAME)
        AND rtrim(SENT.JOB_ID) = rtrim(BOUNCE.JOB_ID)
UNION ALL
SELECT
        'OPEN' AS ACTIVITY_TYPE,
        'INDIA' AS COUNTRY,
        O.SUBSCRIBER_KEY,
        O.JOB_ID,
        O.EVENT_DATE AS ACTIVITY_DATE,
        SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
        O.EMAIL_ID,
        O.EMAIL_NAME,
        O.EMAIL_SUBJECT,
        NULL AS EMAIL_DELIVERED_FLAG,
        NULL AS TRIGGERER_SEND_DEFINITION_OBJECT_ID,
        O.IS_UNIQUE,
        NULL AS URL,
        NULL AS LINK_NAME,
        NULL AS LINK_CONTENT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        ITG_HCP360_SFMC_OPEN_DATA O
        LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = O.SUBSCRIBER_KEY
        AND SENT.EMAIL_NAME = O.EMAIL_NAME
        AND SENT.JOB_ID = O.JOB_ID
UNION ALL
SELECT
        'CLICK' AS ACTIVITY_TYPE,
        'INDIA' AS COUNTRY,
        CLICK.SUBSCRIBER_KEY,
        CLICK.JOB_ID,
        CLICK.EVENT_DATE AS ACTIVITY_DATE,
        SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
        NULL AS EMAIL_ID,
        CLICK.EMAIL_NAME,
        CLICK.EMAIL_SUBJECT,
        NULL AS EMAIL_DELIVERED_FLAG,
        NULL AS TRIGGERER_SEND_DEFINITION_OBJECT_ID,
        CLICK.IS_UNIQUE,
        CLICK.URL,
        CLICK.LINK_NAME,
        CLICK.LINK_CONTENT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        ITG_HCP360_SFMC_CLICK_DATA CLICK
        LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = CLICK.SUBSCRIBER_KEY
        AND SENT.EMAIL_NAME = CLICK.EMAIL_NAME
        AND SENT.JOB_ID = CLICK.JOB_ID
UNION ALL
SELECT
        'UNSUBSCRIBE' AS ACTIVITY_TYPE,
        'INDIA' AS COUNTRY,
        unsub.SUBSCRIBER_KEY,
        unsub.JOB_ID,
        unsub.EVENT_DATE AS ACTIVITY_DATE,
        SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
        unsub.EMAIL_ID,
        unsub.EMAIL_NAME,
        unsub.EMAIL_SUBJECT,
        NULL AS EMAIL_DELIVERED_FLAG,
        NULL AS TRIGGERER_SEND_DEFINITION_OBJECT_ID,
        NULL AS IS_UNIQUE,
        NULL AS URL,
        NULL AS LINK_NAME,
        NULL AS LINK_CONTENT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        itg_hcp360_sfmc_unsubscribe_data unsub
        LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = unsub.SUBSCRIBER_KEY
        AND SENT.EMAIL_NAME = unsub.EMAIL_NAME
        AND SENT.JOB_ID = unsub.JOB_ID
UNION ALL
SELECT
        'FORWARD' AS ACTIVITY_TYPE,
        'INDIA' AS COUNTRY,
        fwd.SUBSCRIBER_KEY,
        fwd.JOB_ID,
        fwd.EVENT_DATE AS ACTIVITY_DATE,
        SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
        fwd.EMAIL_ID,
        fwd.EMAIL_NAME,
        fwd.EMAIL_SUBJECT,
        NULL AS EMAIL_DELIVERED_FLAG,
        fwd.TRIGGERER_SEND_DEFINITION_OBJECT_ID AS TRIGGERER_SEND_DEFINITION_OBJECT_ID,
        NULL AS IS_UNIQUE,
        NULL AS URL,
        NULL AS LINK_NAME,
        NULL AS LINK_CONTENT,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as crt_dttm,
        convert_timezone('UTC', current_timestamp())::timestamp_ntz(9) as updt_dttm
FROM
        ITG_HCP360_SFMC_FORWARD_DATA fwd
        LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = fwd.SUBSCRIBER_KEY
        AND SENT.EMAIL_NAME = fwd.EMAIL_NAME
        AND SENT.JOB_ID = fwd.JOB_ID
)
)
select * from final