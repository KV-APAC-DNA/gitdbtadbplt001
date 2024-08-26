{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['distributorid','arcode']
    )
}}

WITH source
AS (
    SELECT *,
        dense_rank() OVER (
            PARTITION BY distributorid,
            arcode ORDER BY file_name DESC
            ) AS rnk
    FROM {{ source('thasdl_raw', 'sdl_th_dms_customer_dim') }} source
    WHERE file_name NOT IN (
            SELECT DISTINCT file_name
            FROM {{ source('thawks_integration', 'TRATBL_sdl_th_dms_customer_dim__duplicate_test') }}
            ) qualify rnk=1
    ),
final
AS (
    SELECT distributorid::VARCHAR(10) AS distributorid,
        arcode::VARCHAR(20) AS arcode,
        arname::VARCHAR(500) AS arname,
        araddress::VARCHAR(500) AS araddress,
        telephone::VARCHAR(150) AS telephone,
        fax::VARCHAR(150) AS fax,
        city::VARCHAR(500) AS city,
        region::VARCHAR(20) AS region,
        saledistrict::VARCHAR(200) AS saledistrict,
        saleoffice::VARCHAR(10) AS saleoffice,
        salegroup::VARCHAR(10) AS salegroup,
        artypecode::VARCHAR(10) AS artypecode,
        saleemployee::VARCHAR(150) AS saleemployee,
        salename::VARCHAR(250) AS salename,
        billno::VARCHAR(500) AS billno,
        billmoo::VARCHAR(250) AS billmoo,
        billsoi::VARCHAR(255) AS billsoi,
        billroad::VARCHAR(255) AS billroad,
        billsubdist::VARCHAR(30) AS billsubdist,
        billdistrict::VARCHAR(30) AS billdistrict,
        billprovince::VARCHAR(30) AS billprovince,
        billzipcode::VARCHAR(50) AS billzipcode,
        activestatus::number(18, 0) AS activestatus,
        routestep1::VARCHAR(10) AS routestep1,
        routestep2::VARCHAR(10) AS routestep2,
        routestep3::VARCHAR(10) AS routestep3,
        routestep4::VARCHAR(10) AS routestep4,
        routestep5::VARCHAR(10) AS routestep5,
        routestep6::VARCHAR(10) AS routestep6,
        routestep7::VARCHAR(10) AS routestep7,
        routestep8::VARCHAR(20) AS routestep8,
        routestep9::VARCHAR(20) AS routestep9,
        routestep10::VARCHAR(10) AS routestep10,
        store::VARCHAR(200) AS store,
        sourcefile::VARCHAR(255) AS sourcefile,
        old_custid::VARCHAR(500) AS old_custid,
        try_to_timestamp(modifydate) AS modifydate,
        current_timestamp()::timestamp_ntz(9) AS curr_date,
        run_id::number(18, 0) AS run_id,
        file_name as file_name
    FROM source
    )
SELECT *
FROM final
