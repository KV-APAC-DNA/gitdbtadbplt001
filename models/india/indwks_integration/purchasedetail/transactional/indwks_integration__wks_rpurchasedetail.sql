with sdl_rpurchasedetail as
(
    select * from {{ source('indsdl_raw', 'sdl_rpurchasedetail') }}
),
edw_retailer_calendar_dim as
(
    select * from {{ ref('indedw_integration__edw_retailer_calendar_dim') }}
),
itg_mds_month_end_dates as
(
    select * from {{ ref('inditg_integration__itg_mds_month_end_dates') }}
),
final as
(
    SELECT distcode,
            compinvno,
            compinvdate,
            salesorderno,
            netvalue,
            totaltax,
            totaldiscount,
            totalschemeamount,
            totaloctroi,
            suppliercode,
            cfacode,
            companyname,
            transportercode,
            lorryrecno,
            lorryrecdate,
            posgrnno,
            posgrndate,
            productcode,
            indentqtycs,
            indentqtypcs,
            uomcode,
            cashdiscrs,
            cashdiscper,
            octroi,
            linelevelamt,
            batchno,
            mnfgdate,
            expdate,
            mrp,
            lsp,
            purtax,
            purdisc,
            purrate,
            sellrate,
            sellrateat,
            sellrateavat,
            vattaxvalue,
            status,
            freescheme,
            schemerefrno,
            waybillno,
            designcode,
            bundledeal,
            createduserid,
            createddate,
            migrationflag,
            mproductcode,
            taxablevalue,
            cgstper,
            sgstper,
            utgstper,
            igstper,
            cessper,
            othertax1per,
            othertax2per,
            othertax3per,
            cgstvalue,
            sgstvalue,
            utgstvalue,
            igstvalue,
            cessvalue,
            othertax1value,
            othertax2value,
            othertax3value,
            reversecharges,
            taxvalueother1,
            taxvalueother2,
            taxvalueother3,
            taxtype,
            createddt,
            downloadflag
        FROM sdl_rpurchasedetail
        WHERE (POSGrnNo IS NULL OR (TO_DATE(POSGrnDate) >(SELECT DISTINCT MAX(TO_DATE(caldate)) AS todate
                                                        FROM edw_retailer_calendar_dim
                                                        WHERE mth_mm IN (SELECT (year||lpad (MONTH,2,0))
                                                                        FROM itg_mds_month_end_dates
                                                                        WHERE TO_DATE(convert_timezone('Asia/Kolkata',current_timestamp())) <= TO_DATE(pathfinder_month_end))) AND POSGrnNo <> ''))
)
select * from final