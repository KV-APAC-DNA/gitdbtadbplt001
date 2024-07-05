with cust_customer as
(
    select * from {{ source('aspsdl_raw', 'cust_customer') }}
),
edw_vw_store_master_rex_pop6 as
(
    select * from {{ ref('aspedw_integration__edw_vw_store_master_rex_pop6') }}
),
v_mrchr_merchandisingresponse as
(
    select * from {{ ref('aspitg_integration__v_mrchr_merchandisingresponse') }}
),
v_mrchr_responses as
(
    select * from {{ ref('aspitg_integration__v_mrchr_responses') }}
),
v_vst_visit as
(
    select * from {{ ref('aspitg_integration__v_vst_visit') }}
),
v_prod_product as
(
    select * from {{ ref('aspitg_integration__v_prod_product') }}
),
edw_vw_pop6_salesperson as
(
    select * from {{ ref('aspedw_integration__edw_vw_pop6_salesperson') }}
),
sdl_rex_pop6_usr_map as
(
    select * from {{ source('ntasdl_raw', 'sdl_rex_pop6_usr_map') }}
),
kpi2data_mapping as
(
    select * from {{ source('aspitg_integration', 'kpi2data_mapping') }}
),
v_ms_mastersurvey as
(
    select * from {{ ref('aspitg_integration__v_ms_mastersurvey') }}
),
edw_calendar_dim as
(
    select * from {{ ref('aspedw_integration__edw_calendar_dim') }}
),
edw_product_attr_dim as
(
    select * from {{ ref('aspedw_integration__edw_product_attr_dim') }}
),
v_slsp_salesperson as
(
    select * from {{ ref('aspitg_integration__v_slsp_salesperson') }}
),
v_bi_survey_response_values as
(
    select * from {{ ref('aspedw_integration__v_bi_survey_response_values') }}
),
merchandising_response_msl as
(   
    SELECT 
        'Merchandising_Response' AS dataset,
        mrch_resp_vst.merchandisingresponseid,
        null AS surveyresponseid,
        mrch_resp_vst.customerid,
        mrch_resp_vst.salespersonid,
        mrch_resp_vst.visitid,
        mrch_resp_vst.mrch_resp_startdt,
        mrch_resp_vst.mrch_resp_enddt,
        mrch_resp_vst.mrch_resp_status,
        null AS mastersurveyid,
        null AS survey_status,
        null AS survey_enddate,
        null AS questionkey,
        null AS questiontext,
        null AS valuekey,
        null AS value,
        mrch_resp_vst.productid,
        mrch_resp_vst.mustcarryitem,
        mrch_resp_vst.presence,
        (mrch_resp_vst.outofstock)::character varying AS outofstock,
        null AS mastersurveyname,
        mrch_resp_vst.kpi,
        null AS category,
        null AS segment,
        mrch_resp_vst.vst_visitid,
        mrch_resp_vst.scheduleddate,
        mrch_resp_vst.scheduledtime,
        mrch_resp_vst.duration,
        mrch_resp_vst.vst_status,
        cal.fisc_yr,
        cal.fisc_per,
        slsp.firstname,
        slsp.lastname,
        (cust.remotekey)::character varying AS cust_remotekey,
        cust.customername,
        cust.country,
        cust.county,
        cust.district,
        cust.city,
        CASE
            WHEN (
                (
                    upper((cust.country)::text) = ('TW'::character varying)::text
                )
                OR (
                    upper((cust.country)::text) = ('TAIWAN'::character varying)::text
                )
            ) THEN cust.salesgroup
            ELSE cust.storereference
        END AS storereference,
        cust.storetype,
        (cust.channel)::character varying AS channel,
        cust.salesgroup,
        (cust.soldtoparty)::character varying AS soldtoparty,
        prod.productname,
        prod.eannumber,
        prod_hier.sap_matl_num AS matl_num,
        prod_hier.prod_hier_l1,
        prod_hier.prod_hier_l2,
        prod_hier.prod_hier_l3,
        prod_hier.prod_hier_l4,
        prod_hier.prod_hier_l5,
        prod_hier.prod_hier_l6,
        prod_hier.prod_hier_l7,
        prod_hier.prod_hier_l8,
        prod_hier.prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        null AS mkt_share,
        null AS ques_desc,
        null AS "y/n_flag",
        null AS rej_reason,
        null AS response,
        null AS response_score,
        null AS acc_rej_reason
    FROM 
        (
            (
                SELECT DISTINCT 
                    rex.customerid,
                    trim((rex.remotekey)::text) AS remotekey,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.pop_name
                        ELSE rex.customername
                    END AS customername,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.country
                        ELSE (
                            CASE
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HK'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HK' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HONGKONG'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HONGKONG' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('KR'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('KR' IS NULL)
                                    )
                                ) THEN 'Korea'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('TW'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('TW' IS NULL)
                                    )
                                ) THEN 'Taiwan'::character varying
                                ELSE rex.country
                            END
                        )::character varying(30)
                    END AS country,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.county
                    END AS county,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.district
                    END AS district,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.city
                    END AS city,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.customer
                        ELSE rex.storereference
                    END AS storereference,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.retail_environment_ps
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('deptstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('deptstore' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('discounter/dollor/wholes'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('discounter/dollor/wholes' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('h/s/m'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('h/s/m' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('indepsuper/lka'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('indepsuper/lka' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('grocery'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('grocery' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wh club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wh club' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                            ELSE CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                        END
                    END AS storetype,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (pop6_store.channel)::text
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN trim(
                                (
                                    CASE
                                        WHEN (
                                            (
                                                trim((rex.channel)::text) = ('GT'::character varying)::text
                                            )
                                            OR (
                                                (trim((rex.channel)::text) IS NULL)
                                                AND ('GT' IS NULL)
                                            )
                                        ) THEN 'NH'::character varying
                                        ELSE rex.channel
                                    END
                                )::text
                            )
                            ELSE trim((rex.channel)::text)
                        END
                    END AS channel,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.sales_group_name
                        ELSE rex.salesgroup
                    END AS salesgroup,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (NULL::character varying)::text
                        ELSE trim((rex.soldtoparty)::text)
                    END AS soldtoparty
                FROM 
                    (
                        (
                            SELECT v_cust_customer.rank,
                                V_CUST_CUSTOMER.region,
                                v_cust_customer.fetcheddatetime,
                                v_cust_customer.fetchedsequence,
                                v_cust_customer.azurefile,
                                v_cust_customer.azuredatetime,
                                v_cust_customer.customerid,
                                v_cust_customer.remotekey,
                                v_cust_customer.customername,
                                v_cust_customer.country,
                                v_cust_customer.county,
                                v_cust_customer.district,
                                v_cust_customer.city,
                                v_cust_customer.postcode,
                                v_cust_customer.streetname,
                                v_cust_customer.streetnumber,
                                v_cust_customer.storereference,
                                v_cust_customer.email,
                                v_cust_customer.phonenumber,
                                v_cust_customer.storetype,
                                v_cust_customer.website,
                                v_cust_customer.ecommerceflag,
                                v_cust_customer.marketingpermission,
                                v_cust_customer.channel,
                                v_cust_customer.salesgroup,
                                v_cust_customer.secondarytradecode,
                                v_cust_customer.secondarytradename,
                                v_cust_customer.soldtoparty,
                                v_cust_customer.cdl_datetime,
                                v_cust_customer.cdl_source_file,
                                v_cust_customer.load_key
                            FROM 
                                (
                                    SELECT rank() OVER(
                                            PARTITION BY t1.region,
                                            t1.customerid
                                            ORDER BY t1.azuredatetime DESC
                                        ) AS rank,
                                        t1.region,
                                        t1.fetcheddatetime,
                                        t1.fetchedsequence,
                                        t1.azurefile,
                                        t1.azuredatetime,
                                        t1.customerid,
                                        t1.remotekey,
                                        t1.customername,
                                        t1.country,
                                        t1.county,
                                        t1.district,
                                        t1.city,
                                        t1.postcode,
                                        t1.streetname,
                                        t1.streetnumber,
                                        t1.storereference,
                                        t1.email,
                                        t1.phonenumber,
                                        t1.storetype,
                                        t1.website,
                                        t1.ecommerceflag,
                                        t1.marketingpermission,
                                        t1.channel,
                                        t1.salesgroup,
                                        t1.secondarytradecode,
                                        t1.secondarytradename,
                                        t1.soldtoparty,
                                        t1.cdl_datetime,
                                        t1.cdl_source_file,
                                        t1.load_key
                                    FROM cust_customer t1
                                ) v_cust_customer
                            WHERE 
                                (
                                    (v_cust_customer.rank = 1)
                                    AND (v_cust_customer.country IS NOT NULL)
                                    AND (trim(v_cust_customer.country::text) <> '')
                                    AND (v_cust_customer.channel IS NOT NULL)
                                    AND (trim(v_cust_customer.channel::text) <> '')
                                    AND (v_cust_customer.storetype IS NOT NULL)
                                    AND (trim(v_cust_customer.storetype::text) <> '')
                                )

                        ) rex
                        LEFT JOIN (
                            SELECT DISTINCT vw.cntry_cd,
                                vw.src_file_date,
                                vw.status,
                                vw.popdb_id,
                                vw.pop_code,
                                vw.pop_name,
                                vw.address,
                                vw.longitude,
                                vw.latitude,
                                vw.country,
                                vw.channel,
                                vw.retail_environment_ps,
                                vw.customer,
                                vw.sales_group_code,
                                vw.sales_group_name,
                                vw.customer_grade,
                                vw.external_pop_code
                            FROM edw_vw_store_master_rex_pop6 vw
                        ) pop6_store ON (
                            (
                                (rex.remotekey)::text = (pop6_store.external_pop_code)::text
                            )
                        )
                    )
            ) cust
            LEFT JOIN 
            (
                SELECT mrch_resp.merchandisingresponseid,
                    mrch_resp.customerid,
                    mrch_resp.salespersonid,
                    mrch_resp.visitid,
                    to_date
                    (
                        (
                            CASE
                                WHEN (
                                    (mrch_resp.startdate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mrch_resp.startdate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS mrch_resp_startdt,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (mrch_resp.enddate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mrch_resp.enddate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS mrch_resp_enddt,
                    mrch_resp.status AS mrch_resp_status,
                    resp.productid,
                    resp.mustcarryitem,
                    resp.presence,
                    resp.outofstock,
                    vst.visitid AS vst_visitid,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (vst.scheduleddate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE vst.scheduleddate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS scheduleddate,
                    vst.scheduledtime,
                    vst.duration,
                    vst.status AS vst_status,
                    (
                        CASE
                            WHEN (
                                (
                                    upper((resp.mustcarryitem)::text) = ('TRUE'::character varying)::text
                                )
                                AND (
                                    upper((vst.status)::text) = ('COMPLETED'::character varying)::text
                                )
                            ) THEN 'MSL Compliance'::character varying
                            ELSE NULL::character varying
                        END
                    )::character varying(20) AS kpi
                FROM 
                    (
                        (
                            SELECT v_mrchr_merchandisingresponse.rank,
                                V_MRCHR_MERCHANDISINGRESPONSE.region,
                                v_mrchr_merchandisingresponse.fetcheddatetime,
                                v_mrchr_merchandisingresponse.fetchedsequence,
                                v_mrchr_merchandisingresponse.azurefile,
                                v_mrchr_merchandisingresponse.azuredatetime,
                                v_mrchr_merchandisingresponse.merchandisingresponseid,
                                v_mrchr_merchandisingresponse.customerid,
                                v_mrchr_merchandisingresponse.salespersonid,
                                v_mrchr_merchandisingresponse.salescampaignid,
                                v_mrchr_merchandisingresponse.visitid,
                                v_mrchr_merchandisingresponse.taskid,
                                v_mrchr_merchandisingresponse.mastertaskid,
                                v_mrchr_merchandisingresponse.merchandisingid,
                                v_mrchr_merchandisingresponse.businessunitid,
                                v_mrchr_merchandisingresponse.startdate,
                                v_mrchr_merchandisingresponse.starttime,
                                v_mrchr_merchandisingresponse.enddate,
                                v_mrchr_merchandisingresponse.endtime,
                                v_mrchr_merchandisingresponse.status,
                                v_mrchr_merchandisingresponse.cdl_datetime,
                                v_mrchr_merchandisingresponse.cdl_source_file,
                                v_mrchr_merchandisingresponse.load_key
                            FROM v_mrchr_merchandisingresponse
                            WHERE (v_mrchr_merchandisingresponse.rank = 1)
                        ) mrch_resp
                        JOIN 
                        (
                            SELECT v_mrchr_responses.merchandisingresponseid,
                                v_mrchr_responses.productid,
                                v_mrchr_responses.primaryhierarchynodeid,
                                v_mrchr_responses.mustcarryitem,
                                v_mrchr_responses.presence,
                                trim((v_mrchr_responses.outofstock)::text) AS outofstock
                            FROM v_mrchr_responses
                            WHERE (v_mrchr_responses.rank = 1)
                        ) resp ON 
                        (
                            (
                                (resp.merchandisingresponseid)::text = (mrch_resp.merchandisingresponseid)::text
                            )
                        )
                        LEFT JOIN 
                        (
                            SELECT v_vst_visit.visitid,
                                v_vst_visit.scheduleddate,
                                v_vst_visit.scheduledtime,
                                v_vst_visit.duration,
                                v_vst_visit.status
                            FROM v_vst_visit
                            WHERE (v_vst_visit.rank = 1)
                        ) vst ON (
                            ((mrch_resp.visitid)::text = (vst.visitid)::text)
                        )
                    )
            ) mrch_resp_vst ON (
                (
                    (mrch_resp_vst.customerid)::text = (cust.customerid)::text
                )
            )
            LEFT JOIN edw_calendar_dim cal ON ((cal.cal_day = mrch_resp_vst.scheduleddate))
            LEFT JOIN (
                SELECT v_prod_product.productid,
                    v_prod_product.remotekey,
                    v_prod_product.productname,
                    v_prod_product.eannumber
                FROM v_prod_product
                WHERE (v_prod_product.rank = 1)
            ) prod ON (
                (
                    (mrch_resp_vst.productid)::text = (prod.productid)::text
                )
            )
            LEFT JOIN edw_product_attr_dim prod_hier ON 
            (
                (
                    ((prod.eannumber)::text = (prod_hier.ean)::text)
                    AND (
                        CASE
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('HONG KONG'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('HONG KONG' IS NULL)
                                )
                            ) THEN ('HK'::character varying)::text
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('KOREA'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('KOREA' IS NULL)
                                )
                            ) THEN ('KR'::character varying)::text
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('TAIWAN'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('TAIWAN' IS NULL)
                                )
                            ) THEN ('TW'::character varying)::text
                            ELSE upper((cust.country)::text)
                        END = upper((prod_hier.cntry)::text)
                    )
                )
            )
            LEFT JOIN 
            (
                SELECT slsp.salespersonid,
                    slsp.remotekey,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.first_name
                        ELSE slsp.firstname
                    END AS firstname,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.last_name
                        ELSE slsp.lastname
                    END AS lastname
                FROM 
                    v_slsp_salesperson slsp
                    LEFT JOIN 
                    (
                        SELECT pop6.username,
                            pop6.first_name,
                            pop6.last_name,
                            pop6.team,
                            pop6.superior_name,
                            "map".rex_sales_rep_id
                        FROM edw_vw_pop6_salesperson pop6,
                            sdl_rex_pop6_usr_map "map"
                        WHERE (
                                upper((pop6.username)::text) = upper(("map".prod_user_name)::text)
                            )
                    ) usr_map ON (
                        (
                            (slsp.remotekey)::text = (usr_map.rex_sales_rep_id)::text
                        )
                    )
                WHERE (slsp.rank = 1)
            ) slsp ON (
                (
                    (mrch_resp_vst.salespersonid)::text = (slsp.salespersonid)::text
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS channel,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.store_type,
                    (kpi2data_mapping.value)::double precision AS weight
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Channel Weightage'::character varying)::text
                    )
            ) kpi_wt ON (
                (
                    (
                        (
                            (
                                (kpi_wt.kpi_name)::text = (mrch_resp_vst.kpi)::text
                            )
                            AND ((kpi_wt.ctry)::text = (cust.country)::text)
                        )
                        AND ((kpi_wt.channel)::text = cust.channel)
                    )
                    AND (
                        (kpi_wt.store_type)::text = (cust.storetype)::text
                    )
                )
            )
        )
    WHERE 
        (
            (
                mrch_resp_vst.scheduleddate >= '2019-05-01'::date
            )
            AND (
                date_part(
                    year,
                    (mrch_resp_vst.scheduleddate)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                       convert_timezone('UTC', current_timestamp())::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
merchandising_response_oos as
(   
    SELECT 
        'Merchandising_Response' AS dataset,
        mrch_resp_vst.merchandisingresponseid,
        null AS surveyresponseid,
        mrch_resp_vst.customerid,
        mrch_resp_vst.salespersonid,
        mrch_resp_vst.visitid,
        mrch_resp_vst.mrch_resp_startdt,
        mrch_resp_vst.mrch_resp_enddt,
        mrch_resp_vst.mrch_resp_status,
        null AS mastersurveyid,
        null AS survey_status,
        null AS survey_enddate,
        null AS questionkey,
        null AS questiontext,
        null AS valuekey,
        null AS value,
        mrch_resp_vst.productid,
        mrch_resp_vst.mustcarryitem,
        mrch_resp_vst.presence,
        (mrch_resp_vst.outofstock)::character varying AS outofstock,
        null AS mastersurveyname,
        mrch_resp_vst.kpi,
        null AS category,
        null AS segment,
        mrch_resp_vst.vst_visitid,
        mrch_resp_vst.scheduleddate,
        mrch_resp_vst.scheduledtime,
        mrch_resp_vst.duration,
        mrch_resp_vst.vst_status,
        cal.fisc_yr,
        cal.fisc_per,
        slsp.firstname,
        slsp.lastname,
        (cust.remotekey)::character varying AS cust_remotekey,
        cust.customername,
        cust.country,
        cust.county,
        cust.district,
        cust.city,
        CASE
            WHEN (
                (
                    upper((cust.country)::text) = ('TW'::character varying)::text
                )
                OR (
                    upper((cust.country)::text) = ('TAIWAN'::character varying)::text
                )
            ) THEN cust.salesgroup
            ELSE cust.storereference
        END AS storereference,
        cust.storetype,
        (cust.channel)::character varying AS channel,
        cust.salesgroup,
        (cust.soldtoparty)::character varying AS soldtoparty,
        prod.productname,
        prod.eannumber,
        prod_hier.sap_matl_num AS matl_num,
        prod_hier.prod_hier_l1,
        prod_hier.prod_hier_l2,
        prod_hier.prod_hier_l3,
        prod_hier.prod_hier_l4,
        prod_hier.prod_hier_l5,
        prod_hier.prod_hier_l6,
        prod_hier.prod_hier_l7,
        prod_hier.prod_hier_l8,
        prod_hier.prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        null AS mkt_share,
        null AS ques_desc,
        null AS "y/n_flag",
        null AS rej_reason,
        null AS response,
        null AS response_score,
        null AS acc_rej_reason
    FROM 
        (
            (
                SELECT 
                    DISTINCT rex.customerid,
                    trim((rex.remotekey)::text) AS remotekey,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.pop_name
                        ELSE rex.customername
                    END AS customername,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.country
                        ELSE (
                            CASE
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HK'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HK' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HONGKONG'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HONGKONG' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('KR'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('KR' IS NULL)
                                    )
                                ) THEN 'Korea'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('TW'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('TW' IS NULL)
                                    )
                                ) THEN 'Taiwan'::character varying
                                ELSE rex.country
                            END
                        )::character varying(30)
                    END AS country,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.county
                    END AS county,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.district
                    END AS district,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.city
                    END AS city,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.customer
                        ELSE rex.storereference
                    END AS storereference,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.retail_environment_ps
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('deptstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('deptstore' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('discounter/dollor/wholes'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('discounter/dollor/wholes' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('h/s/m'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('h/s/m' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('indepsuper/lka'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('indepsuper/lka' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('grocery'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('grocery' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wh club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wh club' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                            ELSE CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                        END
                    END AS storetype,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (pop6_store.channel)::text
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN trim(
                                (
                                    CASE
                                        WHEN (
                                            (
                                                trim((rex.channel)::text) = ('GT'::character varying)::text
                                            )
                                            OR (
                                                (trim((rex.channel)::text) IS NULL)
                                                AND ('GT' IS NULL)
                                            )
                                        ) THEN 'NH'::character varying
                                        ELSE rex.channel
                                    END
                                )::text
                            )
                            ELSE trim((rex.channel)::text)
                        END
                    END AS channel,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.sales_group_name
                        ELSE rex.salesgroup
                    END AS salesgroup,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (NULL::character varying)::text
                        ELSE trim((rex.soldtoparty)::text)
                    END AS soldtoparty
                FROM 
                    (
                        (
                            SELECT 
                                v_cust_customer.rank,
                                V_CUST_CUSTOMER.region,
                                v_cust_customer.fetcheddatetime,
                                v_cust_customer.fetchedsequence,
                                v_cust_customer.azurefile,
                                v_cust_customer.azuredatetime,
                                v_cust_customer.customerid,
                                v_cust_customer.remotekey,
                                v_cust_customer.customername,
                                v_cust_customer.country,
                                v_cust_customer.county,
                                v_cust_customer.district,
                                v_cust_customer.city,
                                v_cust_customer.postcode,
                                v_cust_customer.streetname,
                                v_cust_customer.streetnumber,
                                v_cust_customer.storereference,
                                v_cust_customer.email,
                                v_cust_customer.phonenumber,
                                v_cust_customer.storetype,
                                v_cust_customer.website,
                                v_cust_customer.ecommerceflag,
                                v_cust_customer.marketingpermission,
                                v_cust_customer.channel,
                                v_cust_customer.salesgroup,
                                v_cust_customer.secondarytradecode,
                                v_cust_customer.secondarytradename,
                                v_cust_customer.soldtoparty,
                                v_cust_customer.cdl_datetime,
                                v_cust_customer.cdl_source_file,
                                v_cust_customer.load_key
                            FROM 
                                (
                                    SELECT rank() OVER(
                                            PARTITION BY t1.region,
                                            t1.customerid
                                            ORDER BY t1.azuredatetime DESC
                                        ) AS rank,
                                        t1.region,
                                        t1.fetcheddatetime,
                                        t1.fetchedsequence,
                                        t1.azurefile,
                                        t1.azuredatetime,
                                        t1.customerid,
                                        t1.remotekey,
                                        t1.customername,
                                        t1.country,
                                        t1.county,
                                        t1.district,
                                        t1.city,
                                        t1.postcode,
                                        t1.streetname,
                                        t1.streetnumber,
                                        t1.storereference,
                                        t1.email,
                                        t1.phonenumber,
                                        t1.storetype,
                                        t1.website,
                                        t1.ecommerceflag,
                                        t1.marketingpermission,
                                        t1.channel,
                                        t1.salesgroup,
                                        t1.secondarytradecode,
                                        t1.secondarytradename,
                                        t1.soldtoparty,
                                        t1.cdl_datetime,
                                        t1.cdl_source_file,
                                        t1.load_key
                                    FROM cust_customer t1
                                ) v_cust_customer
                            WHERE 
                                (
                                    (v_cust_customer.rank = 1)
                                    AND (v_cust_customer.country IS NOT NULL)
                                    AND (trim(v_cust_customer.country::text) <> '')
                                    AND (v_cust_customer.channel IS NOT NULL)
                                    AND (trim(v_cust_customer.channel::text) <> '')
                                    AND (v_cust_customer.storetype IS NOT NULL)
                                    AND (trim(v_cust_customer.storetype::text) <> '')
                                )

                        ) rex
                        LEFT JOIN 
                        (
                            SELECT DISTINCT vw.cntry_cd,
                                vw.src_file_date,
                                vw.status,
                                vw.popdb_id,
                                vw.pop_code,
                                vw.pop_name,
                                vw.address,
                                vw.longitude,
                                vw.latitude,
                                vw.country,
                                vw.channel,
                                vw.retail_environment_ps,
                                vw.customer,
                                vw.sales_group_code,
                                vw.sales_group_name,
                                vw.customer_grade,
                                vw.external_pop_code
                            FROM edw_vw_store_master_rex_pop6 vw
                        ) pop6_store ON (
                            (
                                (rex.remotekey)::text = (pop6_store.external_pop_code)::text
                            )
                        )
                    )
            ) cust
            LEFT JOIN 
            (
                SELECT 
                    mrch_resp.merchandisingresponseid,
                    mrch_resp.customerid,
                    mrch_resp.salespersonid,
                    mrch_resp.visitid,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (mrch_resp.startdate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mrch_resp.startdate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS mrch_resp_startdt,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (mrch_resp.enddate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE mrch_resp.enddate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS mrch_resp_enddt,
                    mrch_resp.status AS mrch_resp_status,
                    resp.productid,
                    resp.mustcarryitem,
                    resp.presence,
                    resp.outofstock,
                    vst.visitid AS vst_visitid,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (vst.scheduleddate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE vst.scheduleddate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS scheduleddate,
                    vst.scheduledtime,
                    vst.duration,
                    vst.status AS vst_status,
                    (
                        CASE
                            WHEN (
                                (
                                    upper((resp.mustcarryitem)::text) = ('TRUE'::character varying)::text
                                )
                                AND (
                                    upper((vst.status)::text) = ('COMPLETED'::character varying)::text
                                )
                            ) THEN 'OOS Compliance'::character varying
                            ELSE NULL::character varying
                        END
                    )::character varying(20) AS kpi
                FROM 
                    (
                        (
                            SELECT 
                                v_mrchr_merchandisingresponse.rank,
                                V_MRCHR_MERCHANDISINGRESPONSE.region,
                                v_mrchr_merchandisingresponse.fetcheddatetime,
                                v_mrchr_merchandisingresponse.fetchedsequence,
                                v_mrchr_merchandisingresponse.azurefile,
                                v_mrchr_merchandisingresponse.azuredatetime,
                                v_mrchr_merchandisingresponse.merchandisingresponseid,
                                v_mrchr_merchandisingresponse.customerid,
                                v_mrchr_merchandisingresponse.salespersonid,
                                v_mrchr_merchandisingresponse.salescampaignid,
                                v_mrchr_merchandisingresponse.visitid,
                                v_mrchr_merchandisingresponse.taskid,
                                v_mrchr_merchandisingresponse.mastertaskid,
                                v_mrchr_merchandisingresponse.merchandisingid,
                                v_mrchr_merchandisingresponse.businessunitid,
                                v_mrchr_merchandisingresponse.startdate,
                                v_mrchr_merchandisingresponse.starttime,
                                v_mrchr_merchandisingresponse.enddate,
                                v_mrchr_merchandisingresponse.endtime,
                                v_mrchr_merchandisingresponse.status,
                                v_mrchr_merchandisingresponse.cdl_datetime,
                                v_mrchr_merchandisingresponse.cdl_source_file,
                                v_mrchr_merchandisingresponse.load_key
                            FROM v_mrchr_merchandisingresponse
                            WHERE (v_mrchr_merchandisingresponse.rank = 1)
                        ) mrch_resp
                        JOIN 
                        (
                            SELECT v_mrchr_responses.merchandisingresponseid,
                                v_mrchr_responses.productid,
                                v_mrchr_responses.primaryhierarchynodeid,
                                v_mrchr_responses.mustcarryitem,
                                v_mrchr_responses.presence,
                                trim((v_mrchr_responses.outofstock)::text) AS outofstock
                            FROM v_mrchr_responses
                            WHERE (v_mrchr_responses.rank = 1)
                        ) resp ON (
                            (
                                (resp.merchandisingresponseid)::text = (mrch_resp.merchandisingresponseid)::text
                            )
                        )
                        LEFT JOIN (
                            SELECT v_vst_visit.visitid,
                                v_vst_visit.scheduleddate,
                                v_vst_visit.scheduledtime,
                                v_vst_visit.duration,
                                v_vst_visit.status
                            FROM v_vst_visit
                            WHERE (v_vst_visit.rank = 1)
                        ) vst ON (
                            ((mrch_resp.visitid)::text = (vst.visitid)::text)
                        )
                    )
            ) mrch_resp_vst ON (
                (
                    (mrch_resp_vst.customerid)::text = (cust.customerid)::text
                )
            )
            LEFT JOIN edw_calendar_dim cal ON ((cal.cal_day = mrch_resp_vst.scheduleddate))
            LEFT JOIN (
                SELECT v_prod_product.productid,
                    v_prod_product.remotekey,
                    v_prod_product.productname,
                    v_prod_product.eannumber
                FROM v_prod_product
                WHERE (v_prod_product.rank = 1)
            ) prod ON (
                (
                    (mrch_resp_vst.productid)::text = (prod.productid)::text
                )
            )
            LEFT JOIN edw_product_attr_dim prod_hier ON (
                (
                    ((prod.eannumber)::text = (prod_hier.ean)::text)
                    AND (
                        CASE
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('HONG KONG'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('HONG KONG' IS NULL)
                                )
                            ) THEN ('HK'::character varying)::text
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('KOREA'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('KOREA' IS NULL)
                                )
                            ) THEN ('KR'::character varying)::text
                            WHEN (
                                (
                                    upper((cust.country)::text) = ('TAIWAN'::character varying)::text
                                )
                                OR (
                                    (upper((cust.country)::text) IS NULL)
                                    AND ('TAIWAN' IS NULL)
                                )
                            ) THEN ('TW'::character varying)::text
                            ELSE upper((cust.country)::text)
                        END = upper((prod_hier.cntry)::text)
                    )
                )
            )
            LEFT JOIN (
                SELECT slsp.salespersonid,
                    slsp.remotekey,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.first_name
                        ELSE slsp.firstname
                    END AS firstname,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.last_name
                        ELSE slsp.lastname
                    END AS lastname
                FROM (
                        v_slsp_salesperson slsp
                        LEFT JOIN (
                            SELECT pop6.username,
                                pop6.first_name,
                                pop6.last_name,
                                pop6.team,
                                pop6.superior_name,
                                "map".rex_sales_rep_id
                            FROM edw_vw_pop6_salesperson pop6,
                                sdl_rex_pop6_usr_map "map"
                            WHERE (
                                    upper((pop6.username)::text) = upper(("map".prod_user_name)::text)
                                )
                        ) usr_map ON (
                            (
                                (slsp.remotekey)::text = (usr_map.rex_sales_rep_id)::text
                            )
                        )
                    )
                WHERE (slsp.rank = 1)
            ) slsp ON (
                (
                    (mrch_resp_vst.salespersonid)::text = (slsp.salespersonid)::text
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS channel,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.store_type,
                    (kpi2data_mapping.value)::double precision AS weight
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Channel Weightage'::character varying)::text
                    )
            ) kpi_wt ON (
                (
                    (
                        (
                            (
                                (kpi_wt.kpi_name)::text = (mrch_resp_vst.kpi)::text
                            )
                            AND ((kpi_wt.ctry)::text = (cust.country)::text)
                        )
                        AND ((kpi_wt.channel)::text = cust.channel)
                    )
                    AND (
                        (kpi_wt.store_type)::text = (cust.storetype)::text
                    )
                )
            )
        )
    WHERE 
        (
            (
                (
                    (
                        mrch_resp_vst.scheduleddate >= '2019-05-01'::date
                    )
                    AND (
                        date_part(
                            year,
                            (mrch_resp_vst.scheduleddate)::timestamp without time zone
                        ) >= (
                            date_part(
                                year,
                               convert_timezone('UTC', current_timestamp())::timestamp without time zone
                            ) - (2)::double precision
                        )
                    )
                )
                AND (
                    upper((mrch_resp_vst.mustcarryitem)::text) = ('TRUE'::character varying)::text
                )
            )
            AND (
                upper((mrch_resp_vst.vst_status)::text) = ('COMPLETED'::character varying)::text
            )
        )
),
survey_response as
(   
    SELECT 'Survey_Response' AS dataset,
        null AS merchandisingresponseid,
        srv_resp_val.surveyresponseid,
        srv_resp_val.customerid,
        srv_resp_val.salespersonid,
        srv_resp_val.visitid,
        null AS mrch_resp_startdt,
        null AS mrch_resp_enddt,
        null AS mrch_resp_status,
        srv_resp_val.mastersurveyid,
        srv_resp_val.status AS survey_status,
        srv_resp_val.enddate AS survey_enddate,
        srv_resp_val.questionkey,
        srv_resp_val.questiontext,
        srv_resp_val.valuekey,
        (
            CASE
                WHEN (
                    (
                        (
                            (srv_map.kpi_name)::text = ('Share of Assortment'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Share of Shelf'::character varying)::text
                        )
                    )
                    AND (
                        (cust.country)::text = ('Korea'::character varying)::text
                    )
                ) THEN split_part(
                    split_part(
                        split_part(
                            (srv_resp_val.value)::text,
                            ('('::character varying)::text,
                            1
                        ),
                        ('개'::character varying)::text,
                        1
                    ),
                    ('줄'::character varying)::text,
                    1
                )
                ELSE trim((srv_resp_val.value)::text)
            END
        )::character varying AS value,
        null AS productid,
        null AS mustcarryitem,
        null AS presence,
        null AS outofstock,
        mstr_srv.mastersurveyname,
        srv_map.kpi_name AS kpi,
        CASE
            WHEN (
                (
                    (mstr_srv.mastersurveyname)::text like (
                        (
                            ('%'::character varying)::text || (srv_map.survey_prefix)::text
                        ) || ('%'::character varying)::text
                    )
                )
                AND ((cust.country)::text = (srv_map.ctry)::text)
            ) THEN (mstr_srv.category)::character varying
            ELSE NULL::character varying
        END AS category,
        CASE
            WHEN (
                (
                    (mstr_srv.mastersurveyname)::text like (
                        (
                            ('%'::character varying)::text || (srv_map.survey_prefix)::text
                        ) || ('%'::character varying)::text
                    )
                )
                AND ((cust.country)::text = (srv_map.ctry)::text)
            ) THEN (mstr_srv.segment)::character varying
            ELSE NULL::character varying
        END AS segment,
        vst.visitid AS vst_visitid,
        vst.scheduleddate,
        vst.scheduledtime,
        vst.duration,
        vst.status AS vst_status,
        cal.fisc_yr,
        cal.fisc_per,
        slsp.firstname,
        slsp.lastname,
        (cust.remotekey)::character varying AS cust_remotekey,
        cust.customername,
        cust.country,
        cust.county,
        cust.district,
        cust.city,
        CASE
            WHEN (
                (
                    upper((cust.country)::text) = ('TW'::character varying)::text
                )
                OR (
                    upper((cust.country)::text) = ('TAIWAN'::character varying)::text
                )
            ) THEN cust.salesgroup
            ELSE cust.storereference
        END AS storereference,
        cust.storetype,
        (cust.channel)::character varying AS channel,
        cust.salesgroup,
        (cust.soldtoparty)::character varying AS soldtoparty,
        null AS productname,
        null AS eannumber,
        null AS matl_num,
        null AS prod_hier_l1,
        null AS prod_hier_l2,
        null AS prod_hier_l3,
        null AS prod_hier_l4,
        null AS prod_hier_l5,
        null AS prod_hier_l6,
        null AS prod_hier_l7,
        null AS prod_hier_l8,
        null AS prod_hier_l9,
        kpi_wt.weight AS kpi_chnl_wt,
        mkt_shr.mkt_share,
        ques.value AS ques_desc,
        flag.value AS "y/n_flag",
        CASE
            WHEN (
                (
                    (
                        (cust.country)::text = ('Korea'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('NO'::character varying)::text
                )
            ) THEN (
                trim(
                    "replace"(
                        (srv_resp_val.value)::text,
                        split_part(
                            trim((srv_resp_val.value)::text),
                            (' '::character varying)::text,
                            1
                        ),
                        (''::character varying)::text
                    )
                )
            )::character varying
            WHEN (
                (
                    (
                        (cust.country)::text <> ('Korea'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('NO'::character varying)::text
                )
            ) THEN (
                trim(
                    split_part(
                        trim((srv_resp_val.value)::text),
                        (','::character varying)::text,
                        2
                    )
                )
            )::character varying
            WHEN (
                (
                    (
                        (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                    )
                    OR (
                        (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                    )
                )
                AND (
                    (
                        (flag.value)::text = (''::character varying)::text
                    )
                    OR (flag.value IS NULL)
                )
            ) THEN (trim((srv_resp_val.value)::text))::character varying
            ELSE ' '::character varying
        END AS rej_reason,
        CASE
            WHEN (
                (
                    (cust.country)::text = ('Taiwan'::character varying)::text
                )
                AND (
                    (
                        (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                    )
                    OR (
                        (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                    )
                )
            ) THEN (
                trim(
                    split_part(
                        trim((srv_resp_val.value)::text),
                        (','::character varying)::text,
                        1
                    )
                )
            )::character varying
            ELSE ' '::character varying
        END AS response,
        CASE
            WHEN (
                (
                    (
                        (cust.country)::text = ('Taiwan'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('YES'::character varying)::text
                )
            ) THEN (
                trim(
                    split_part(
                        trim((srv_resp_val.value)::text),
                        (','::character varying)::text,
                        2
                    )
                )
            )::character varying
            WHEN (
                (
                    (
                        (cust.country)::text = ('Taiwan'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('NO'::character varying)::text
                )
            ) THEN '0'::character varying
            ELSE ' '::character varying
        END AS response_score,
        CASE
            WHEN (
                (
                    (
                        (cust.country)::text = ('Taiwan'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('YES'::character varying)::text
                )
            ) THEN (
                trim(
                    split_part(
                        trim((srv_resp_val.value)::text),
                        (','::character varying)::text,
                        3
                    )
                )
            )::character varying
            WHEN (
                (
                    (
                        (cust.country)::text = ('Taiwan'::character varying)::text
                    )
                    AND (
                        (
                            (srv_map.kpi_name)::text = ('Display Compliance'::character varying)::text
                        )
                        OR (
                            (srv_map.kpi_name)::text = ('Promo Compliance'::character varying)::text
                        )
                    )
                )
                AND (
                    (flag.value)::text = ('NO'::character varying)::text
                )
            ) THEN (
                trim(
                    split_part(
                        trim((srv_resp_val.value)::text),
                        (','::character varying)::text,
                        2
                    )
                )
            )::character varying
            ELSE ' '::character varying
        END AS acc_rej_reason
    FROM 
        (
            (
                SELECT DISTINCT rex.customerid,
                    trim((rex.remotekey)::text) AS remotekey,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.pop_name
                        ELSE rex.customername
                    END AS customername,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.country
                        ELSE (
                            CASE
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HK'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HK' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('HONGKONG'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('HONGKONG' IS NULL)
                                    )
                                ) THEN 'Hong Kong'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('KR'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('KR' IS NULL)
                                    )
                                ) THEN 'Korea'::character varying
                                WHEN (
                                    (
                                        upper((rex.country)::text) = ('TW'::character varying)::text
                                    )
                                    OR (
                                        (upper((rex.country)::text) IS NULL)
                                        AND ('TW' IS NULL)
                                    )
                                ) THEN 'Taiwan'::character varying
                                ELSE rex.country
                            END
                        )::character varying(30)
                    END AS country,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.county
                    END AS county,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.district
                    END AS district,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN ''::character varying
                        ELSE rex.city
                    END AS city,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.customer
                        ELSE rex.storereference
                    END AS storereference,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.retail_environment_ps
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('deptstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('deptstore' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('discounter/dollor/wholes'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('discounter/dollor/wholes' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('h/s/m'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('h/s/m' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('indepsuper/lka'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('indepsuper/lka' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('grocery'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('grocery' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wh club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wh club' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                            ELSE CASE
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('wholesaler'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('wholesaler' IS NULL)
                                    )
                                ) THEN 'Wholesaler'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('warehouse'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('warehouse' IS NULL)
                                    )
                                ) THEN 'Warehouse'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('supermarket'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('supermarket' IS NULL)
                                    )
                                ) THEN 'Supermarket'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super' IS NULL)
                                    )
                                ) THEN 'Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pcs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pcs' IS NULL)
                                    )
                                ) THEN 'PCS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('otc'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('otc' IS NULL)
                                    )
                                ) THEN 'OTC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('nacf'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('nacf' IS NULL)
                                    )
                                ) THEN 'NACF'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hyper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hyper' IS NULL)
                                    )
                                ) THEN 'Hyper'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('headquarters'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('headquarters' IS NULL)
                                    )
                                ) THEN 'Headquarters'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('gtdistributor'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('gtdistributor' IS NULL)
                                    )
                                ) THEN 'GT Distributor'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drugstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drugstore' IS NULL)
                                    )
                                ) THEN 'Drug Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('drug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('drug' IS NULL)
                                    )
                                ) THEN 'Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('department'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('department' IS NULL)
                                    )
                                ) THEN 'Department'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('daiso'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('daiso' IS NULL)
                                    )
                                ) THEN 'Daiso'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('cvs'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('cvs' IS NULL)
                                    )
                                ) THEN 'CVS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('club'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('club' IS NULL)
                                    )
                                ) THEN 'Club'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chainsuper'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chainsuper' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chain super'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chain super' IS NULL)
                                    )
                                ) THEN 'Chain Super'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('chaindrug'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('chaindrug' IS NULL)
                                    )
                                ) THEN 'Chain Drug'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('basicstore'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('basicstore' IS NULL)
                                    )
                                ) THEN 'Basic Store'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('hds'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('hds' IS NULL)
                                    )
                                ) THEN 'HDS'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('pic'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('pic' IS NULL)
                                    )
                                ) THEN 'PIC'::character varying
                                WHEN (
                                    (
                                        lower(trim((rex.storetype)::text)) = ('super store'::character varying)::text
                                    )
                                    OR (
                                        (lower(trim((rex.storetype)::text)) IS NULL)
                                        AND ('super store' IS NULL)
                                    )
                                ) THEN 'Super store'::character varying
                                ELSE rex.storetype
                            END
                        END
                    END AS storetype,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (pop6_store.channel)::text
                        ELSE CASE
                            WHEN (
                                (
                                    upper((rex.country)::text) = ('KR'::character varying)::text
                                )
                                OR (
                                    upper((rex.country)::text) = ('KOREA'::character varying)::text
                                )
                            ) THEN trim(
                                (
                                    CASE
                                        WHEN (
                                            (
                                                trim((rex.channel)::text) = ('GT'::character varying)::text
                                            )
                                            OR (
                                                (trim((rex.channel)::text) IS NULL)
                                                AND ('GT' IS NULL)
                                            )
                                        ) THEN 'NH'::character varying
                                        ELSE rex.channel
                                    END
                                )::text
                            )
                            ELSE trim((rex.channel)::text)
                        END
                    END AS channel,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN pop6_store.sales_group_name
                        ELSE rex.salesgroup
                    END AS salesgroup,
                    CASE
                        WHEN (pop6_store.pop_name IS NOT NULL) THEN (NULL::character varying)::text
                        ELSE trim((rex.soldtoparty)::text)
                    END AS soldtoparty
                FROM 
                    (
                        (
                            SELECT v_cust_customer.rank,
                                V_CUST_CUSTOMER.region,
                                v_cust_customer.fetcheddatetime,
                                v_cust_customer.fetchedsequence,
                                v_cust_customer.azurefile,
                                v_cust_customer.azuredatetime,
                                v_cust_customer.customerid,
                                v_cust_customer.remotekey,
                                v_cust_customer.customername,
                                v_cust_customer.country,
                                v_cust_customer.county,
                                v_cust_customer.district,
                                v_cust_customer.city,
                                v_cust_customer.postcode,
                                v_cust_customer.streetname,
                                v_cust_customer.streetnumber,
                                v_cust_customer.storereference,
                                v_cust_customer.email,
                                v_cust_customer.phonenumber,
                                v_cust_customer.storetype,
                                v_cust_customer.website,
                                v_cust_customer.ecommerceflag,
                                v_cust_customer.marketingpermission,
                                v_cust_customer.channel,
                                v_cust_customer.salesgroup,
                                v_cust_customer.secondarytradecode,
                                v_cust_customer.secondarytradename,
                                v_cust_customer.soldtoparty,
                                v_cust_customer.cdl_datetime,
                                v_cust_customer.cdl_source_file,
                                v_cust_customer.load_key
                            FROM 
                                (
                                    SELECT rank() OVER(
                                            PARTITION BY t1.region,
                                            t1.customerid
                                            ORDER BY t1.azuredatetime DESC
                                        ) AS rank,
                                        t1.region,
                                        t1.fetcheddatetime,
                                        t1.fetchedsequence,
                                        t1.azurefile,
                                        t1.azuredatetime,
                                        t1.customerid,
                                        t1.remotekey,
                                        t1.customername,
                                        t1.country,
                                        t1.county,
                                        t1.district,
                                        t1.city,
                                        t1.postcode,
                                        t1.streetname,
                                        t1.streetnumber,
                                        t1.storereference,
                                        t1.email,
                                        t1.phonenumber,
                                        t1.storetype,
                                        t1.website,
                                        t1.ecommerceflag,
                                        t1.marketingpermission,
                                        t1.channel,
                                        t1.salesgroup,
                                        t1.secondarytradecode,
                                        t1.secondarytradename,
                                        t1.soldtoparty,
                                        t1.cdl_datetime,
                                        t1.cdl_source_file,
                                        t1.load_key
                                    FROM cust_customer t1
                                ) v_cust_customer
                            WHERE 
                                (
                                    (v_cust_customer.rank = 1)
                                    AND (v_cust_customer.country IS NOT NULL)
                                    AND (trim(v_cust_customer.country::text) <> '')
                                    AND (v_cust_customer.channel IS NOT NULL)
                                    AND (trim(v_cust_customer.channel::text) <> '')
                                    AND (v_cust_customer.storetype IS NOT NULL)
                                    AND (trim(v_cust_customer.storetype::text) <> '')
                                )

                        ) rex
                        LEFT JOIN 
                        (
                            SELECT DISTINCT vw.cntry_cd,
                                vw.src_file_date,
                                vw.status,
                                vw.popdb_id,
                                vw.pop_code,
                                vw.pop_name,
                                vw.address,
                                vw.longitude,
                                vw.latitude,
                                vw.country,
                                vw.channel,
                                vw.retail_environment_ps,
                                vw.customer,
                                vw.sales_group_code,
                                vw.sales_group_name,
                                vw.customer_grade,
                                vw.external_pop_code
                            FROM edw_vw_store_master_rex_pop6 vw
                        ) pop6_store ON (
                            (
                                (rex.remotekey)::text = (pop6_store.external_pop_code)::text
                            )
                        )
                    )
            ) cust
            LEFT JOIN v_bi_survey_response_values srv_resp_val ON (
                (
                    (srv_resp_val.customerid)::text = (cust.customerid)::text
                )
            )
            LEFT JOIN (
                SELECT v_ms_mastersurvey.mastersurveyid,
                    v_ms_mastersurvey.mastersurveyname,
                    split_part(
                        (v_ms_mastersurvey.mastersurveyname)::text,
                        ('_'::character varying)::text,
                        3
                    ) AS category,
                    split_part(
                        split_part(
                            (v_ms_mastersurvey.mastersurveyname)::text,
                            ('_'::character varying)::text,
                            4
                        ),
                        (']'::character varying)::text,
                        1
                    ) AS segment
                FROM v_ms_mastersurvey
                WHERE (v_ms_mastersurvey.rank = 1)
            ) mstr_srv ON (
                (
                    (srv_resp_val.mastersurveyid)::text = (mstr_srv.mastersurveyid)::text
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS survey_prefix,
                    kpi2data_mapping.kpi_name
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('KPI'::character varying)::text
                    )
            ) srv_map ON (
                (
                    (
                        (mstr_srv.mastersurveyname)::text like (
                            (
                                ('%'::character varying)::text || (srv_map.survey_prefix)::text
                            ) || ('%'::character varying)::text
                        )
                    )
                    AND ((cust.country)::text = (srv_map.ctry)::text)
                )
            )
            LEFT JOIN (
                SELECT v_vst_visit.visitid,
                    to_date(
                        (
                            CASE
                                WHEN (
                                    (v_vst_visit.scheduleddate)::text = (''::character varying)::text
                                ) THEN NULL::character varying
                                ELSE v_vst_visit.scheduleddate
                            END
                        )::text,
                        ('YYYY-MM-DD'::character varying)::text
                    ) AS scheduleddate,
                    v_vst_visit.scheduledtime,
                    v_vst_visit.duration,
                    v_vst_visit.status
                FROM v_vst_visit
                WHERE (v_vst_visit.rank = 1)
            ) vst ON (
                (
                    (srv_resp_val.visitid)::text = (vst.visitid)::text
                )
            )
            LEFT JOIN edw_calendar_dim cal ON ((cal.cal_day = vst.scheduleddate))
            LEFT JOIN (
                SELECT slsp.salespersonid,
                    slsp.remotekey,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.first_name
                        ELSE slsp.firstname
                    END AS firstname,
                    CASE
                        WHEN (usr_map.username IS NOT NULL) THEN usr_map.last_name
                        ELSE slsp.lastname
                    END AS lastname
                FROM (
                        v_slsp_salesperson slsp
                        LEFT JOIN (
                            SELECT pop6.username,
                                pop6.first_name,
                                pop6.last_name,
                                pop6.team,
                                pop6.superior_name,
                                "map".rex_sales_rep_id
                            FROM edw_vw_pop6_salesperson pop6,
                                sdl_rex_pop6_usr_map "map"
                            WHERE (
                                    upper((pop6.username)::text) = upper(("map".prod_user_name)::text)
                                )
                        ) usr_map ON (
                            (
                                (slsp.remotekey)::text = (usr_map.rex_sales_rep_id)::text
                            )
                        )
                    )
                WHERE (slsp.rank = 1)
            ) slsp ON (
                (
                    (srv_resp_val.salespersonid)::text = (slsp.salespersonid)::text
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS ques_identifier,
                    kpi2data_mapping.value,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.start_date,
                    kpi2data_mapping.end_date
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Question'::character varying)::text
                    )
            ) ques ON (
                (
                    (
                        (
                            (
                                ((ques.ctry)::text = (cust.country)::text)
                                AND ((ques.kpi_name)::text = (srv_map.kpi_name)::text)
                            )
                            AND (
                                (srv_resp_val.questiontext)::text like (
                                    (
                                        ('%'::character varying)::text || (ques.ques_identifier)::text
                                    ) || ('%'::character varying)::text
                                )
                            )
                        )
                        AND (vst.scheduleddate >= ques.start_date)
                    )
                    AND (vst.scheduleddate <= ques.end_date)
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier,
                    kpi2data_mapping.value,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.start_date,
                    kpi2data_mapping.end_date
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Yes/No Flag'::character varying)::text
                    )
            ) flag ON (
                (
                    (
                        (
                            (
                                ((flag.ctry)::text = (cust.country)::text)
                                AND ((flag.kpi_name)::text = (srv_map.kpi_name)::text)
                            )
                            AND (
                                (srv_resp_val.value)::text like (
                                    (
                                        ('%'::character varying)::text || (flag.identifier)::text
                                    ) || ('%'::character varying)::text
                                )
                            )
                        )
                        AND (vst.scheduleddate >= flag.start_date)
                    )
                    AND (vst.scheduleddate <= flag.end_date)
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS channel,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.store_type,
                    (kpi2data_mapping.value)::double precision AS weight
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Channel Weightage'::character varying)::text
                    )
            ) kpi_wt ON (
                (
                    (
                        (
                            (
                                (kpi_wt.kpi_name)::text = (srv_map.kpi_name)::text
                            )
                            AND ((kpi_wt.ctry)::text = (cust.country)::text)
                        )
                        AND ((kpi_wt.channel)::text = cust.channel)
                    )
                    AND (
                        (kpi_wt.store_type)::text = (cust.storetype)::text
                    )
                )
            )
            LEFT JOIN (
                SELECT kpi2data_mapping.ctry,
                    kpi2data_mapping.data_type,
                    kpi2data_mapping.identifier AS channel,
                    kpi2data_mapping.kpi_name,
                    kpi2data_mapping.store_type,
                    kpi2data_mapping.category,
                    kpi2data_mapping.segment,
                    (kpi2data_mapping.value)::double precision AS mkt_share
                FROM kpi2data_mapping
                WHERE (
                        (kpi2data_mapping.data_type)::text = ('Market Share'::character varying)::text
                    )
            ) mkt_shr ON (
                (
                    (
                        (
                            (
                                (
                                    (
                                        (mkt_shr.kpi_name)::text = (srv_map.kpi_name)::text
                                    )
                                    AND ((mkt_shr.ctry)::text = (cust.country)::text)
                                )
                                AND ((mkt_shr.channel)::text = cust.channel)
                            )
                            AND (
                                upper((mkt_shr.store_type)::text) = upper((cust.storetype)::text)
                            )
                        )
                        AND (
                            upper((mkt_shr.category)::text) = upper(mstr_srv.category)
                        )
                    )
                    AND (
                        upper((mkt_shr.segment)::text) = upper(mstr_srv.segment)
                    )
                )
            )
        )
    WHERE 
        (
            (vst.scheduleddate >= '2019-05-01'::date)
            AND (
                date_part(
                    year,
                    (vst.scheduleddate)::timestamp without time zone
                ) >= (
                    date_part(
                        year,
                       convert_timezone('UTC', current_timestamp())::timestamp without time zone
                    ) - (2)::double precision
                )
            )
        )
),
final as
(
    select * from merchandising_response_msl
    union all
    select * from merchandising_response_oos
    union all
    select * from survey_response
)
select * from final