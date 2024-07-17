with itg_hcp360_sfmc_sent_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_sent_data') }}
),
itg_hcp360_sfmc_bounce_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_bounce_data') }}
),
itg_hcp360_sfmc_open_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_open_data') }}
),
itg_hcp360_sfmc_click_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_click_data') }}
),
itg_hcp360_sfmc_unsubscribe_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_unsubscribe_data') }}
),
itg_hcp360_sfmc_forward_data as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_forward_data') }}
),
cte as
(
SELECT 'SENT' AS ACTIVITY_TYPE,
    'INDIA' AS COUNTRY,
    SENT.SUBSCRIBER_KEY as SUBSCRIBER_KEY,
    SENT.JOB_ID as JOB_ID,
    SENT.EVENT_DATE AS ACTIVITY_DATE,
    SENT.EVENT_DATE AS SENT_ACTIVITY_DATE,
    SENT.EMAIL_ID,
    trim(SENT.EMAIL_NAME) as EMAIL_NAME,
    SENT.EMAIL_SUBJECT,
    CASE 
        WHEN BOUNCE.SUBSCRIBER_KEY IS NULL
            THEN 'Y'
        ELSE 'N'
        END AS EMAIL_DELIVERED_FLAG,
    SENT.TRIGGERER_SEND_DEFINITION_OBJECT_ID,
    NULL AS IS_UNIQUE,
    NULL AS URL,
    NULL AS LINK_NAME,
    NULL AS LINK_CONTENT,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
FROM ITG_HCP360_SFMC_SENT_DATA SENT
LEFT JOIN ITG_HCP360_SFMC_BOUNCE_DATA BOUNCE ON SENT.SUBSCRIBER_KEY = BOUNCE.SUBSCRIBER_KEY
    AND trim(SENT.EMAIL_NAME) = trim(BOUNCE.EMAIL_NAME)
    AND SENT.JOB_ID = BOUNCE.JOB_ID
),
cte1 as 
(
SELECT 'OPEN' AS ACTIVITY_TYPE,
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
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
FROM ITG_HCP360_SFMC_OPEN_DATA O
LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = O.SUBSCRIBER_KEY
    AND SENT.EMAIL_NAME = O.EMAIL_NAME
    AND SENT.JOB_ID = O.JOB_ID
),
cte2 as 
(
SELECT 'CLICK' AS ACTIVITY_TYPE,
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
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
FROM ITG_HCP360_SFMC_CLICK_DATA CLICK
LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = CLICK.SUBSCRIBER_KEY
    AND SENT.EMAIL_NAME = CLICK.EMAIL_NAME
    AND SENT.JOB_ID = CLICK.JOB_ID
),
cte3 as 
(
SELECT 'UNSUBSCRIBE' AS ACTIVITY_TYPE,
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
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
FROM ITG_HCP360_SFMC_UNSUBSCRIBE_DATA unsub
LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = unsub.SUBSCRIBER_KEY
    AND SENT.EMAIL_NAME = unsub.EMAIL_NAME
    AND SENT.JOB_ID = unsub.JOB_ID
),
cte4 as 
(
SELECT 'FORWARD' AS ACTIVITY_TYPE,
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
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS CRT_DTTM,
    convert_timezone('UTC',current_timestamp())::timestamp_ntz AS UPDT_DTTM
FROM ITG_HCP360_SFMC_FORWARD_DATA fwd
LEFT JOIN ITG_HCP360_SFMC_SENT_DATA SENT ON SENT.SUBSCRIBER_KEY = fwd.SUBSCRIBER_KEY
    AND SENT.EMAIL_NAME = fwd.EMAIL_NAME
    AND SENT.JOB_ID = fwd.JOB_ID
),
transformed as 
(
    select * from cte
    union all
    select * from cte1
    union all
    select * from cte2
    union all
    select * from cte3
    union all
    select * from cte4
),
final as 
(   
    select
        activity_type::varchar(20) as activity_type,
        'INDIA' as cntry_cd,
        subscriber_key::varchar(100) as subscriber_key,
        job_id::varchar(20) as job_id,
        activity_date::timestamp_ntz(9) as activity_date,
        email_id::varchar(20) as email_id,
        email_name::varchar(100) as email_name,
        email_subject::varchar(200) as email_subject,
        email_delivered_flag::varchar(1) as email_delivered_flag,
        triggerer_send_definition_object_id::varchar(50) as triggerer_send_definition_object_id,
        is_unique::varchar(10) as is_unique,
        url::varchar(1000) as url,
        link_name::varchar(200) as link_name,
        link_content::varchar(1000) as link_content,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as crt_dttm,
        convert_timezone('UTC',current_timestamp())::timestamp_ntz as updt_dttm,
        sent_activity_date::timestamp_ntz(9) as sent_activity_date
    from transformed 
)
select * from final
