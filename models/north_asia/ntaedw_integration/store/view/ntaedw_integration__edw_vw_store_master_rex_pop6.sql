with edw_vw_pop6_store as (
select * from DEV_DNA_CORE.SNAPNTAEDW_INTEGRATION.EDW_VW_POP6_STORE
),
cust_customer as (
select * from DEV_DNA_LOAD.SNAPASPSDL_RAW.CUST_CUSTOMER
),
final as (
SELECT 
  store_master_set1.remotekey, 
  store_master_set1.cntry_cd, 
  store_master_set1.src_file_date, 
  store_master_set1.status, 
  store_master_set1.popdb_id, 
  store_master_set1.pop_code, 
  store_master_set1.pop_name, 
  store_master_set1.address, 
  store_master_set1.longitude, 
  store_master_set1.latitude, 
  store_master_set1.country, 
  store_master_set1.channel, 
  store_master_set1.retail_environment_ps, 
  store_master_set1.customer, 
  store_master_set1.sales_group_code, 
  store_master_set1.sales_group_name, 
  store_master_set1.customer_grade, 
  store_master_set1.external_pop_code 
FROM 
  (
    SELECT 
      DISTINCT rex.remotekey, 
      pop6_store.cntry_cd, 
      pop6_store.src_file_date, 
      pop6_store.status, 
      pop6_store.popdb_id, 
      pop6_store.pop_code, 
      pop6_store.pop_name, 
      pop6_store.address, 
      pop6_store.longitude, 
      pop6_store.latitude, 
      pop6_store.country, 
      pop6_store.channel, 
      pop6_store.retail_environment_ps, 
      pop6_store.customer, 
      pop6_store.sales_group_code, 
      pop6_store.sales_group_name, 
      pop6_store.customer_grade, 
      pop6_store.external_pop_code 
    FROM 
      (
        SELECT 
          DISTINCT vw.cntry_cd, 
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
        FROM 
          edw_vw_pop6_store vw 
        WHERE 
          (
            (vw.external_pop_code IS NOT NULL) 
            AND (
              vw.external_pop_code IN (
                SELECT 
                  edw_vw_pop6_store.external_pop_code 
                FROM 
                  edw_vw_pop6_store 
                WHERE 
                  (
                    edw_vw_pop6_store.external_pop_code IS NOT NULL
                  ) 
                GROUP BY 
                  edw_vw_pop6_store.external_pop_code 
                HAVING 
                  (
                    count(*) = 1
                  )
              )
            )
          )
      ) pop6_store, 
      (
        SELECT 
          v_cust_customer.rank, 
          v_cust_customer.region, 
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
            SELECT 
              rank() OVER(
                PARTITION BY t1.region, 
                t1.customerid 
                ORDER BY 
                  t1.azuredatetime DESC
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
            FROM 
              cust_customer t1
          ) v_cust_customer 
        WHERE 
          (
            (
              (v_cust_customer.rank = 1) 
              AND (
                v_cust_customer.channel IS NOT NULL
              )
            ) 
            AND (
              (v_cust_customer.channel):: text <> ('' :: character varying):: text
            )
          )
      ) rex 
    WHERE 
      (
        (
          upper(
            trim(
              (pop6_store.external_pop_code):: text
            )
          ) = upper(
            trim(
              (rex.remotekey):: text
            )
          )
        ) 
        AND (
          trim(
            (pop6_store.cntry_cd):: text
          ) = (
            CASE WHEN (
              (
                (rex.country):: text = ('tw' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('tw' IS NULL)
              )
            ) THEN 'TW' :: character varying WHEN (
              (
                (rex.country):: text = ('hk' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('hk' IS NULL)
              )
            ) THEN 'HK' :: character varying WHEN (
              (
                (rex.country):: text = ('Taiwan' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('Taiwan' IS NULL)
              )
            ) THEN 'TW' :: character varying WHEN (
              (
                (rex.country):: text = ('kr' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('kr' IS NULL)
              )
            ) THEN 'KR' :: character varying WHEN (
              (
                (rex.country):: text = ('Korea' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('Korea' IS NULL)
              )
            ) THEN 'KR' :: character varying ELSE rex.country END
          ):: text
        )
      )
  ) store_master_set1 
UNION ALL 
SELECT 
  store_master_set2.remotekey, 
  store_master_set2.cntry_cd, 
  store_master_set2.src_file_date, 
  store_master_set2.status, 
  store_master_set2.popdb_id, 
  store_master_set2.pop_code, 
  store_master_set2.pop_name, 
  store_master_set2.address, 
  store_master_set2.longitude, 
  store_master_set2.latitude, 
  store_master_set2.country, 
  store_master_set2.channel, 
  store_master_set2.retail_environment_ps, 
  store_master_set2.customer, 
  store_master_set2.sales_group_code, 
  store_master_set2.sales_group_name, 
  store_master_set2.customer_grade, 
  store_master_set2.external_pop_code 
FROM 
  (
    SELECT 
      DISTINCT rex.remotekey, 
      pop6_store.cntry_cd, 
      pop6_store.src_file_date, 
      pop6_store.status, 
      pop6_store.popdb_id, 
      pop6_store.pop_code, 
      pop6_store.pop_name, 
      pop6_store.address, 
      pop6_store.longitude, 
      pop6_store.latitude, 
      pop6_store.country, 
      pop6_store.channel, 
      pop6_store.retail_environment_ps, 
      pop6_store.customer, 
      pop6_store.sales_group_code, 
      pop6_store.sales_group_name, 
      pop6_store.customer_grade, 
      pop6_store.external_pop_code 
    FROM 
      (
        SELECT 
          DISTINCT vw.cntry_cd, 
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
        FROM 
          edw_vw_pop6_store vw 
        WHERE 
          (
            (
              (vw.external_pop_code IS NOT NULL) 
              AND (
                 (
                  vw.external_pop_code NOT IN (
                    SELECT 
                      DISTINCT store_master_set1.external_pop_code 
                    FROM 
                      (
                        SELECT 
                          DISTINCT rex.remotekey, 
                          pop6_store.cntry_cd, 
                          pop6_store.src_file_date, 
                          pop6_store.status, 
                          pop6_store.popdb_id, 
                          pop6_store.pop_code, 
                          pop6_store.pop_name, 
                          pop6_store.address, 
                          pop6_store.longitude, 
                          pop6_store.latitude, 
                          pop6_store.country, 
                          pop6_store.channel, 
                          pop6_store.retail_environment_ps, 
                          pop6_store.customer, 
                          pop6_store.sales_group_code, 
                          pop6_store.sales_group_name, 
                          pop6_store.customer_grade, 
                          pop6_store.external_pop_code 
                        FROM 
                          (
                            SELECT 
                              DISTINCT vw.cntry_cd, 
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
                            FROM 
                              edw_vw_pop6_store vw 
                            WHERE 
                              (
                                (vw.external_pop_code IS NOT NULL) 
                                AND (
                                  vw.external_pop_code IN (
                                    SELECT 
                                      edw_vw_pop6_store.external_pop_code 
                                    FROM 
                                      edw_vw_pop6_store 
                                    WHERE 
                                      (
                                        edw_vw_pop6_store.external_pop_code IS NOT NULL
                                      ) 
                                    GROUP BY 
                                      edw_vw_pop6_store.external_pop_code 
                                    HAVING 
                                      (
                                        count(*) = 1
                                      )
                                  )
                                )
                              )
                          ) pop6_store, 
                          (
                            SELECT 
                              v_cust_customer.rank, 
                              v_cust_customer.region, 
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
                                SELECT 
                                  rank() OVER(
                                    PARTITION BY t1.region, 
                                    t1.customerid 
                                    ORDER BY 
                                      t1.azuredatetime DESC
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
                                FROM 
                                  cust_customer t1
                              ) v_cust_customer 
                            WHERE 
                              (
                                (
                                  (v_cust_customer.rank = 1) 
                                  AND (
                                    v_cust_customer.channel IS NOT NULL
                                  )
                                ) 
                                AND (
                                  (v_cust_customer.channel):: text <> ('' :: character varying):: text
                                )
                              )
                          ) rex 
                        WHERE 
                          (
                            (
                              upper(
                                trim(
                                  (pop6_store.external_pop_code):: text
                                )
                              ) = upper(
                                trim(
                                  (rex.remotekey):: text
                                )
                              )
                            ) 
                            AND (
                              trim(
                                (pop6_store.cntry_cd):: text
                              ) = (
                                CASE WHEN (
                                  (
                                    (rex.country):: text = ('tw' :: character varying):: text
                                  ) 
                                  OR (
                                    (rex.country IS NULL) 
                                    AND ('tw' IS NULL)
                                  )
                                ) THEN 'TW' :: character varying WHEN (
                                  (
                                    (rex.country):: text = ('hk' :: character varying):: text
                                  ) 
                                  OR (
                                    (rex.country IS NULL) 
                                    AND ('hk' IS NULL)
                                  )
                                ) THEN 'HK' :: character varying WHEN (
                                  (
                                    (rex.country):: text = ('Taiwan' :: character varying):: text
                                  ) 
                                  OR (
                                    (rex.country IS NULL) 
                                    AND ('Taiwan' IS NULL)
                                  )
                                ) THEN 'TW' :: character varying WHEN (
                                  (
                                    (rex.country):: text = ('kr' :: character varying):: text
                                  ) 
                                  OR (
                                    (rex.country IS NULL) 
                                    AND ('kr' IS NULL)
                                  )
                                ) THEN 'KR' :: character varying WHEN (
                                  (
                                    (rex.country):: text = ('Korea' :: character varying):: text
                                  ) 
                                  OR (
                                    (rex.country IS NULL) 
                                    AND ('Korea' IS NULL)
                                  )
                                ) THEN 'KR' :: character varying ELSE rex.country END
                              ):: text
                            )
                          )
                      ) store_master_set1
                  )
                )
              )
            ) 
            AND (
              vw.external_pop_code IN (
                SELECT 
                  edw_vw_pop6_store.external_pop_code 
                FROM 
                  edw_vw_pop6_store 
                WHERE 
                  (
                    edw_vw_pop6_store.external_pop_code IS NOT NULL
                  ) 
                GROUP BY 
                  edw_vw_pop6_store.external_pop_code 
                HAVING 
                  (
                    count(*) = 1
                  )
              )
            )
          )
      ) pop6_store, 
      (
        SELECT 
          v_cust_customer.rank, 
          v_cust_customer.region, 
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
            SELECT 
              rank() OVER(
                PARTITION BY t1.region, 
                t1.customerid 
                ORDER BY 
                  t1.azuredatetime DESC
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
            FROM 
              cust_customer t1
          ) v_cust_customer 
        WHERE 
          (
            (
              (v_cust_customer.rank = 1) 
              AND (
                v_cust_customer.channel IS NOT NULL
              )
            ) 
            AND (
              (v_cust_customer.channel):: text <> ('' :: character varying):: text
            )
          )
      ) rex 
    WHERE 
      (
        (
          upper(
            trim(
              (pop6_store.external_pop_code):: text
            )
          ) = "substring"(
            trim(
              (rex.remotekey):: text
            ), 
            regexp_instr(
              trim(
                (rex.remotekey):: text
              ), 
              ('[0-9]' :: character varying):: text
            ), 
            length(
              trim(
                (rex.remotekey):: text
              )
            )
          )
        ) 
        AND (
          trim(
            (pop6_store.cntry_cd):: text
          ) = (
            CASE WHEN (
              (
                (rex.country):: text = ('tw' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('tw' IS NULL)
              )
            ) THEN 'TW' :: character varying WHEN (
              (
                (rex.country):: text = ('hk' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('hk' IS NULL)
              )
            ) THEN 'HK' :: character varying WHEN (
              (
                (rex.country):: text = ('Taiwan' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('Taiwan' IS NULL)
              )
            ) THEN 'TW' :: character varying WHEN (
              (
                (rex.country):: text = ('kr' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('kr' IS NULL)
              )
            ) THEN 'KR' :: character varying WHEN (
              (
                (rex.country):: text = ('Korea' :: character varying):: text
              ) 
              OR (
                (rex.country IS NULL) 
                AND ('Korea' IS NULL)
              )
            ) THEN 'KR' :: character varying ELSE rex.country END
          ):: text
        )
      )
  ) store_master_set2
)
select * from final 
