
(
  (
    (
      SELECT
        'MY' AS cntry_key,
        'MALAYSIA' AS cntry_nm,
        b.plant,
        b.cnty,
        b.item_cd,
        b.item_desc,
        b.valid_from,
        b.valid_to,
        b.rate,
        b.currency,
        b.price_unit,
        b.uom,
        b.yearmo,
        b.mnth_type,
        b.snapshot_dt
      FROM snaposeitg_integration.itg_my_listprice_daily AS b, (
        SELECT
          MAX(CAST((
            itg_my_listprice.yearmo
          ) AS TEXT)) AS max_hist
        FROM ASING012_WORKSPACE.itg_my_listprice
      ) AS a
      WHERE
        (
          (
            CAST((
              b.yearmo
            ) AS TEXT) > COALESCE(a.max_hist, CAST('' AS TEXT))
          )
          AND (
            CAST((
              b.mnth_type
            ) AS TEXT) = CAST('CAL' AS TEXT)
          )
        )
      UNION ALL
      SELECT
        'MY' AS cntry_key,
        'MALAYSIA' AS cntry_nm,
        b.plant,
        b.cnty,
        b.item_cd,
        b.item_desc,
        b.valid_from,
        b.valid_to,
        b.rate,
        b.currency,
        b.price_unit,
        b.uom,
        b.yearmo,
        b.mnth_type,
        b.snapshot_dt
      FROM snaposeitg_integration.itg_my_listprice_daily AS b, (
        SELECT
          MAX(CAST((
            itg_my_listprice.yearmo
          ) AS TEXT)) AS max_hist
        FROM ASING012_WORKSPACE.itg_my_listprice
      ) AS a
      WHERE
        (
          (
            CAST((
              b.yearmo
            ) AS TEXT) > COALESCE(a.max_hist, CAST('' AS TEXT))
          )
          AND (
            CAST((
              b.mnth_type
            ) AS TEXT) = CAST('JJ' AS TEXT)
          )
        )
    )
    UNION ALL
    SELECT
      'MY' AS cntry_key,
      'MALAYSIA' AS cntry_nm,
      itg_my_listprice.plant,
      itg_my_listprice.cnty,
      itg_my_listprice.item_cd,
      itg_my_listprice.item_desc,
      itg_my_listprice.valid_from,
      itg_my_listprice.valid_to,
      itg_my_listprice.rate,
      itg_my_listprice.currency,
      itg_my_listprice.field9 AS price_unit,
      itg_my_listprice.uom,
      itg_my_listprice.yearmo,
      'CAL' AS mnth_type,
      NULL AS snapshot_dt
    FROM ASING012_WORKSPACE.itg_my_listprice
  )
  UNION ALL
  SELECT
    'MY' AS cntry_key,
    'MALAYSIA' AS cntry_nm,
    itg_my_listprice.plant,
    itg_my_listprice.cnty,
    itg_my_listprice.item_cd,
    itg_my_listprice.item_desc,
    itg_my_listprice.valid_from,
    itg_my_listprice.valid_to,
    itg_my_listprice.rate,
    itg_my_listprice.currency,
    itg_my_listprice.field9 AS price_unit,
    itg_my_listprice.uom,
    itg_my_listprice.yearmo,
    'JJ' AS mnth_type,
    NULL AS snapshot_dt
  FROM ASING012_WORKSPACE.itg_my_listprice
);