with itg_customer_inventory as
(
   select * from {{ref('pcfitg_integration__itg_customer_inventory')}}
),
itg_customer_grocery_inventory as
(
   select * from {{ref('pcfitg_integration__itg_customer_grocery_inventory')}}
),
itg_dstr_coles_sap_mapping as
(
   select * from {{ref('pcfitg_integration__itg_dstr_coles_sap_mapping')}}
),
itg_dstr_woolworth_sap_mapping as
(
   select * from {{ref('pcfitg_integration__itg_dstr_woolworth_sap_mapping')}}
),
edw_perenso_prod_dim as
(
   select * from {{ref('pcfedw_integration__edw_perenso_prod_dim')}}
),
edw_list_price as
(
   select * from {{ref('aspedw_integration__edw_list_price')}}
),
itg_parameter_reg_inventory as
(
   select * from {{ source('aspitg_integration', 'itg_parameter_reg_inventory') }}
),
edw_calendar_dim as
(
   select * from {{ref('aspedw_integration__edw_calendar_dim')}}
),
a as
(
   (
      (
         (
               SELECT 
                    itg_customer_inventory.sap_parent_customer_key,
                    itg_customer_inventory.sap_parent_customer_desc,
                    itg_customer_inventory.dstr_prod_cd,
                    itg_customer_inventory.dstr_product_desc,
                    itg_customer_inventory.matl_num,
                    itg_customer_inventory.inv_date,
                    itg_customer_inventory.inventory_qty,
                    (
                        (
                            itg_customer_inventory.inventory_qty * itg_customer_inventory.std_cost
                        )
                    )::numeric(16, 4) AS inventory_amount,
                    itg_customer_inventory.inventory_amt AS distributor_inventory_amount,
                    itg_customer_inventory.std_cost,
                    itg_customer_inventory.crt_dttm
               FROM itg_customer_inventory
               WHERE (
                     (
                           (itg_customer_inventory.sap_parent_customer_desc)::text <> ('METCASH'::character varying)::text
                     )
                     AND (
                           (itg_customer_inventory.sap_parent_customer_desc)::text <> ('CHEMIST WAREHOUSE'::character varying)::text
                     )
                  )
               UNION ALL
                SELECT 
                    cwh.sap_parent_customer_key,
                    cwh.sap_parent_customer_desc,
                    cwh.dstr_prod_cd,
                    cwh.dstr_product_desc,
                    cwh.matl_num,
                    cwh.inv_date,
                    cwh.inventory_qty,
                    ((cwh.inventory_qty * lp.amount))::numeric(16, 4) AS inventory_amount,
                    cwh.inventory_amt AS distributor_inventory_amount,
                    lp.amount AS std_cost,
                    cwh.crt_dttm
               FROM 
                  (
                    itg_customer_inventory cwh
                    LEFT JOIN 
                    (
                        SELECT lp.material,
                            lp.list_price,
                            b.parameter_value,
                            (
                                (
                                    lp.list_price * (b.parameter_value)::numeric(10, 4)
                                )
                            )::numeric(10, 4) AS amount
                        FROM 
                            (
                                SELECT ltrim(
                                        (edw_list_price.material)::text,
                                        ((0)::character varying)::text
                                    ) AS material,
                                    edw_list_price.amount AS list_price,
                                    row_number() OVER(
                                        PARTITION BY ltrim(
                                            (edw_list_price.material)::text,
                                            ((0)::character varying)::text
                                        )
                                        ORDER BY to_date(
                                                (edw_list_price.valid_to)::text,
                                                ('YYYYMMDD'::character varying)::text
                                            ) DESC,
                                            to_date(
                                                (edw_list_price.dt_from)::text,
                                                ('YYYYMMDD'::character varying)::text
                                            ) DESC
                                    ) AS rn
                                FROM edw_list_price
                                WHERE (
                                        (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                                    )
                            ) lp,
                            (
                                SELECT itg_parameter_reg_inventory.country_name,
                                    itg_parameter_reg_inventory.parameter_name,
                                    itg_parameter_reg_inventory.parameter_value
                                FROM itg_parameter_reg_inventory
                                WHERE (
                                        (
                                            (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
                                        )
                                        AND (
                                            (itg_parameter_reg_inventory.parameter_name)::text = ('listprice'::character varying)::text
                                        )
                                    )
                            ) b
                        WHERE (lp.rn = 1)
                     ) lp ON (
                           (
                              ltrim(
                                 (cwh.matl_num)::text,
                                 ((0)::character varying)::text
                              ) = lp.material
                           )
                     )
                  )
               WHERE (
                     (cwh.sap_parent_customer_desc)::text = ('CHEMIST WAREHOUSE'::character varying)::text
                  )
         )
         UNION ALL
         SELECT inv.sap_prnt_cust_key,
               inv.sap_prnt_cust_desc,
               inv.article_code,
               inv.article_desc,
               "map".sap_code AS matl_id,
               inv.inv_date,
               inv.soh_qty,
               ((inv.soh_qty * lp.amount))::numeric(16, 4) AS inventory_amount,
               inv.soh_price,
               lp.amount AS std_cost,
               inv.crt_dttm
         FROM 
            (
                (
                    (
                        SELECT itg_customer_grocery_inventory.sap_prnt_cust_key,
                            itg_customer_grocery_inventory.sap_prnt_cust_desc,
                            itg_customer_grocery_inventory.inv_date,
                            itg_customer_grocery_inventory.article_code,
                            itg_customer_grocery_inventory.article_desc,
                            itg_customer_grocery_inventory.soh_qty,
                            itg_customer_grocery_inventory.soh_price,
                            itg_customer_grocery_inventory.crt_dttm
                        FROM itg_customer_grocery_inventory
                        WHERE (
                                (
                                    itg_customer_grocery_inventory.sap_prnt_cust_desc
                                )::text = ('COLES'::character varying)::text
                            )
                     ) inv
                     LEFT JOIN
                    (
                        SELECT DISTINCT itg_dstr_coles_sap_mapping.sap_code,
                            itg_dstr_coles_sap_mapping.item_idnt
                        FROM itg_dstr_coles_sap_mapping
                        WHERE (
                                (itg_dstr_coles_sap_mapping.sap_code)::text <> (''::character varying)::text
                              )
                    ) "map" ON 
                    (
                        (
                            ltrim(
                                (inv.article_code)::text,
                                ((0)::character varying)::text
                            ) = ltrim(
                                ("map".item_idnt)::text,
                                ((0)::character varying)::text
                            )
                        )
                    )
                  )
                  LEFT JOIN (
                     SELECT lp.material,
                           lp.list_price,
                           b.parameter_value,
                           (
                              (
                                 lp.list_price * (b.parameter_value)::numeric(10, 4)
                              )
                           )::numeric(10, 4) AS amount
                     FROM (
                              SELECT ltrim(
                                       (edw_list_price.material)::text,
                                       ((0)::character varying)::text
                                 ) AS material,
                                 edw_list_price.amount AS list_price,
                                 row_number() OVER(
                                       PARTITION BY ltrim(
                                          (edw_list_price.material)::text,
                                          ((0)::character varying)::text
                                       )
                                       ORDER BY to_date(
                                             (edw_list_price.valid_to)::text,
                                             ('YYYYMMDD'::character varying)::text
                                          ) DESC,
                                          to_date(
                                             (edw_list_price.dt_from)::text,
                                             ('YYYYMMDD'::character varying)::text
                                          ) DESC
                                 ) AS rn
                              FROM edw_list_price
                              WHERE (
                                       (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                                 )
                           ) lp,
                           (
                              SELECT itg_parameter_reg_inventory.country_name,
                                 itg_parameter_reg_inventory.parameter_name,
                                 itg_parameter_reg_inventory.parameter_value
                              FROM itg_parameter_reg_inventory
                              WHERE (
                                       (
                                          (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
                                       )
                                       AND (
                                          (itg_parameter_reg_inventory.parameter_name)::text = ('listprice'::character varying)::text
                                       )
                                 )
                           ) b
                     WHERE (lp.rn = 1)
                  ) lp ON (
                     (
                           ltrim(
                              ("map".sap_code)::text,
                              ((0)::character varying)::text
                           ) = lp.material
                     )
                  )
               )
      )
      UNION ALL
      SELECT inv.sap_prnt_cust_key,
         inv.sap_prnt_cust_desc,
         inv.article_code,
         inv.article_desc,
         "map".sap_code AS matl_id,
         inv.inv_date,
         inv.soh_qty,
         ((inv.soh_qty * lp.amount))::numeric(16, 4) AS inventory_amount,
         inv.soh_price,
         lp.amount AS std_cost,
         inv.crt_dttm
      FROM 
        (
            (
                (
                    SELECT itg_customer_grocery_inventory.sap_prnt_cust_key,
                        itg_customer_grocery_inventory.sap_prnt_cust_desc,
                        itg_customer_grocery_inventory.inv_date,
                        itg_customer_grocery_inventory.article_code,
                        itg_customer_grocery_inventory.article_desc,
                        itg_customer_grocery_inventory.soh_qty,
                        itg_customer_grocery_inventory.soh_price,
                        itg_customer_grocery_inventory.crt_dttm
                    FROM itg_customer_grocery_inventory
                    WHERE (
                            (
                                itg_customer_grocery_inventory.sap_prnt_cust_desc
                            )::text = ('WOOLWORTHS'::character varying)::text
                        )
                ) inv
                LEFT JOIN (
                    SELECT DISTINCT itg_dstr_woolworth_sap_mapping.sap_code,
                        itg_dstr_woolworth_sap_mapping.article_code
                    FROM itg_dstr_woolworth_sap_mapping
                    WHERE (
                            (itg_dstr_woolworth_sap_mapping.sap_code)::text <> (''::character varying)::text
                        )
                ) "map" ON (
                    (
                        ltrim(
                            (inv.article_code)::text,
                            ((0)::character varying)::text
                        ) = ltrim(
                            ("map".article_code)::text,
                            ((0)::character varying)::text
                        )
                    )
                )
            )
            LEFT JOIN (
                SELECT lp.material,
                    lp.list_price,
                    b.parameter_value,
                    (
                        (
                            lp.list_price * (b.parameter_value)::numeric(10, 4)
                        )
                    )::numeric(10, 4) AS amount
                FROM (
                        SELECT ltrim(
                                (edw_list_price.material)::text,
                                ((0)::character varying)::text
                            ) AS material,
                            edw_list_price.amount AS list_price,
                            row_number() OVER(
                                PARTITION BY ltrim(
                                    (edw_list_price.material)::text,
                                    ((0)::character varying)::text
                                )
                                ORDER BY to_date(
                                        (edw_list_price.valid_to)::text,
                                        ('YYYYMMDD'::character varying)::text
                                    ) DESC,
                                    to_date(
                                        (edw_list_price.dt_from)::text,
                                        ('YYYYMMDD'::character varying)::text
                                    ) DESC
                            ) AS rn
                        FROM edw_list_price
                        WHERE (
                                (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                            )
                    ) lp,
                    (
                        SELECT itg_parameter_reg_inventory.country_name,
                            itg_parameter_reg_inventory.parameter_name,
                            itg_parameter_reg_inventory.parameter_value
                        FROM itg_parameter_reg_inventory
                        WHERE (
                                (
                                    (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
                                )
                                AND (
                                    (itg_parameter_reg_inventory.parameter_name)::text = ('listprice'::character varying)::text
                                )
                            )
                    ) b
                WHERE (lp.rn = 1)
            ) lp ON (
                (
                    ltrim(
                        ("map".sap_code)::text,
                        ((0)::character varying)::text
                    ) = lp.material
                )
            )
        )
   )
   UNION ALL
    SELECT inv.sap_parent_customer_key,
        inv.sap_parent_customer_desc,
        inv.dstr_prod_cd,
        inv.dstr_product_desc,
        "map".prod_id AS matl_num,
        inv.inv_date,
        inv.inventory_qty,
        ((inv.inventory_qty * lp.std_cost))::numeric(16, 4) AS inventory_amount,
        inv.inventory_amt AS distributor_inventory_amount,
        lp.std_cost,
        inv.crt_dttm
    FROM 
    (
            (
                (
                    SELECT 
                        itg_customer_inventory.sap_parent_customer_key,
                        itg_customer_inventory.sap_parent_customer_desc,
                        itg_customer_inventory.dstr_prod_cd,
                        itg_customer_inventory.dstr_product_desc,
                        itg_customer_inventory.matl_num,
                        itg_customer_inventory.ean,
                        itg_customer_inventory.inv_date,
                        itg_customer_inventory.inventory_qty,
                        itg_customer_inventory.inventory_amt,
                        itg_customer_inventory.back_order_qty,
                        itg_customer_inventory.std_cost,
                        itg_customer_inventory.crt_dttm
                    FROM itg_customer_inventory
                    WHERE (
                            (itg_customer_inventory.sap_parent_customer_desc)::text = ('METCASH'::character varying)::text
                        )
                ) inv
                LEFT JOIN 
                (
                    SELECT 
                        edw_perenso_prod_dim.prod_key,
                        edw_perenso_prod_dim.prod_desc,
                        edw_perenso_prod_dim.prod_id,
                        edw_perenso_prod_dim.prod_ean,
                        edw_perenso_prod_dim.prod_jj_franchise,
                        edw_perenso_prod_dim.prod_jj_category,
                        edw_perenso_prod_dim.prod_jj_brand,
                        edw_perenso_prod_dim.prod_sap_franchise,
                        edw_perenso_prod_dim.prod_sap_profit_centre,
                        edw_perenso_prod_dim.prod_sap_product_major,
                        edw_perenso_prod_dim.prod_grocery_franchise,
                        edw_perenso_prod_dim.prod_grocery_category,
                        edw_perenso_prod_dim.prod_grocery_brand,
                        edw_perenso_prod_dim.prod_active_nz_pharma,
                        edw_perenso_prod_dim.prod_active_au_grocery,
                        edw_perenso_prod_dim.prod_active_metcash,
                        edw_perenso_prod_dim.prod_active_nz_grocery,
                        edw_perenso_prod_dim.prod_active_au_pharma,
                        edw_perenso_prod_dim.prod_pbs,
                        edw_perenso_prod_dim.prod_ims_brand,
                        edw_perenso_prod_dim.prod_nz_code,
                        edw_perenso_prod_dim.prod_metcash_code,
                        edw_perenso_prod_dim.prod_old_id,
                        edw_perenso_prod_dim.prod_old_ean,
                        edw_perenso_prod_dim.prod_tax,
                        edw_perenso_prod_dim.prod_bwp_aud,
                        edw_perenso_prod_dim.prod_bwp_nzd
                    FROM edw_perenso_prod_dim
                WHERE (
                            (edw_perenso_prod_dim.prod_metcash_code)::text <> ('NOT ASSIGNED'::character varying)::text
                    )
                ) "map" ON 
                (
                (
                    ltrim(
                            (inv.dstr_prod_cd)::text,
                            ((0)::character varying)::text
                    ) = ltrim(
                            ("map".prod_metcash_code)::text,
                            ((0)::character varying)::text
                    )
                )
                )
            )
        LEFT JOIN 
        (
               SELECT lp.material,
                  lp.list_price,
                  b.parameter_value,
                  (
                     (
                           lp.list_price * (b.parameter_value)
                     )
                  )::numeric(10, 4) AS std_cost
               FROM 
                (
                    SELECT ltrim(
                            (edw_list_price.material)::text,
                            ((0)::character varying)::text
                        ) AS material,
                        edw_list_price.amount AS list_price,
                        row_number() OVER(
                            PARTITION BY ltrim(
                                (edw_list_price.material)::text,
                                ((0)::character varying)::text
                            )
                            ORDER BY to_date(
                                    (edw_list_price.valid_to)::text,
                                    ('YYYYMMDD'::character varying)::text
                                ) DESC,
                                to_date(
                                    (edw_list_price.dt_from)::text,
                                    ('YYYYMMDD'::character varying)::text
                                ) DESC
                        ) AS rn
                    FROM edw_list_price
                    WHERE (
                            (edw_list_price.sls_org)::text = ('3300'::character varying)::text
                        )
                ) lp,
                (
                    SELECT itg_parameter_reg_inventory.country_name,
                        itg_parameter_reg_inventory.parameter_name,
                        itg_parameter_reg_inventory.parameter_value
                    FROM itg_parameter_reg_inventory
                    WHERE (
                            (
                                (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
                            )
                            AND (
                                (itg_parameter_reg_inventory.parameter_name)::text = ('listprice'::character varying)::text
                            )
                        )
                ) b
               WHERE (lp.rn = 1)
        ) lp ON 
        (
               (
                  ltrim(
                     ("map".prod_id)::text,
                     ((0)::character varying)::text
                  ) = ltrim(lp.material, ((0)::character varying)::text)
               )
        )
    )
),
final as
(
    SELECT 
        b.channel_desc,
        a.sap_parent_customer_key,
        a.sap_parent_customer_desc,
        a.dstr_prod_cd,
        a.dstr_product_desc,
        a.matl_num,
        a.inv_date,
        c.jj_mnth,
        a.inventory_qty,
        a.inventory_amount,
        a.distributor_inventory_amount,
        a.std_cost,
        a.crt_dttm
    FROM a
    LEFT JOIN 
    (
        SELECT DISTINCT itg_parameter_reg_inventory.parameter_value AS channel_desc,
            itg_parameter_reg_inventory.parameter_name
        FROM itg_parameter_reg_inventory
        WHERE (
                (itg_parameter_reg_inventory.country_name)::text = ('AUSTRALIA'::character varying)::text
            )
    ) b ON 
    (
        (
            upper(trim((a.sap_parent_customer_desc)::text)) = upper(trim((b.parameter_name)::text))
        )
    )
    LEFT JOIN 
    (
        SELECT DISTINCT edw_calendar_dim.cal_day,
            (
                "substring"(
                    ((edw_calendar_dim.fisc_per)::character varying)::text,
                    1,
                    4
                ) || "substring"(
                    ((edw_calendar_dim.fisc_per)::character varying)::text,
                    6,
                    2
                )
            ) AS jj_mnth
        FROM edw_calendar_dim
    ) c ON 
    (
        (
            to_date((a.inv_date)::timestamp without time zone) = to_date((c.cal_day)::timestamp without time zone)
        )
    )
)
select * from final