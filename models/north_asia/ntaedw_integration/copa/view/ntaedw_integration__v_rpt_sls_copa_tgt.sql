with v_intrm_copa_tgt AS (
  SELECT 
    * 
  FROM 
    {{ ref('ntaedw_integration__v_intrm_copa_tgt') }}
), 
v_intrm_internal_tgt AS (
  SELECT 
    * 
  FROM 
    {{ ref('ntaedw_integration__v_intrm_internal_tgt') }}
), 
v_intrm_crncy_exch AS (
  SELECT 
    * 
  FROM 
    {{ ref('ntaedw_integration__v_intrm_crncy_exch') }}
), 
derived_table1 AS (
  SELECT 
    x.co_cd, 
    x.cust_num, 
    x.ctry_nm, 
    x.ctry_key, 
    x.sls_grp, 
    x.rgn, 
    x.fisc_yr_per, 
    g.from_crncy, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    CASE WHEN x.acct_hier_shrt_desc :: TEXT = 'TP' :: CHARACTER VARYING :: TEXT THEN x.amt_obj_crncy * (
      -1 :: NUMERIC :: NUMERIC(18, 0)
    ) ELSE x.amt_obj_crncy END AS amt_obj_crncy, 
    x.obj_crncy_co_obj, 
    x.sls_trgt, 
    x.cust_sls_grp, 
    x.sls_grp_desc, 
    x.cust_sls_ofc, 
    x.sls_ofc_desc, 
    x.channel, 
    x.edw_cust_nm, 
    x.acct_hier_desc, 
    x.acct_hier_shrt_desc, 
    x.company_nm 
  FROM 
    (
      SELECT 
        CASE WHEN a.co_cd IS NULL THEN b.co_cd ELSE a.co_cd END AS co_cd, 
        CASE WHEN a.cust_num IS NULL THEN b.cust_num :: CHARACTER VARYING(40) ELSE a.cust_num END AS cust_num, 
        CASE WHEN a.ctry_nm IS NULL THEN b.ctry_nm ELSE a.ctry_nm END AS ctry_nm, 
        CASE WHEN a.ctry_key IS NULL THEN b.ctry_key ELSE a.ctry_key END AS ctry_key, 
        CASE WHEN a.sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_grp END AS sls_grp, 
        CASE WHEN a.rgn IS NULL THEN b.rgn ELSE a.rgn END AS rgn, 
        CASE WHEN a.fisc_yr_per IS NULL THEN b.fisc_yr_per :: INTEGER ELSE a.fisc_yr_per END AS fisc_yr_per, 
        COALESCE(a.amt_obj_crncy, 0.0) AS amt_obj_crncy, 
        CASE WHEN a.obj_crncy_co_obj IS NULL THEN b.crncy ELSE a.obj_crncy_co_obj END AS obj_crncy_co_obj, 
        b.sls_trgt, 
        CASE WHEN a.cust_sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_grp END AS cust_sls_grp, 
        CASE WHEN a.sls_grp_desc IS NULL THEN b.sls_grp_desc ELSE a.sls_grp_desc END AS sls_grp_desc, 
        CASE WHEN a.cust_sls_ofc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_ofc END AS cust_sls_ofc, 
        CASE WHEN a.sls_ofc_desc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_ofc_desc END AS sls_ofc_desc, 
        CASE WHEN a.channel IS NULL THEN b.channel ELSE a.channel END AS channel, 
        CASE WHEN a.edw_cust_nm IS NULL THEN b.edw_cust_nm ELSE a.edw_cust_nm END AS edw_cust_nm, 
        CASE WHEN a.acct_hier_desc IS NULL THEN 'NA' :: CHARACTER VARYING ELSE a.acct_hier_desc END AS acct_hier_desc, 
        CASE WHEN a.acct_hier_shrt_desc IS NULL THEN 'NA' :: CHARACTER VARYING ELSE a.acct_hier_shrt_desc END AS acct_hier_shrt_desc, 
        CASE WHEN a.company_nm IS NULL THEN 'J&J Taiwan Ltd' :: CHARACTER VARYING ELSE a.company_nm END AS company_nm 
      FROM 
        (
          SELECT 
            v_intrm_copa_tgt.co_cd, 
            v_intrm_copa_tgt.cust_num, 
            v_intrm_copa_tgt.ctry_nm, 
            v_intrm_copa_tgt.ctry_key, 
            v_intrm_copa_tgt.sls_grp, 
            v_intrm_copa_tgt.rgn, 
            v_intrm_copa_tgt.fisc_yr_per, 
            v_intrm_copa_tgt.amt_obj_crncy, 
            v_intrm_copa_tgt.obj_crncy_co_obj, 
            v_intrm_copa_tgt.cust_sls_grp, 
            v_intrm_copa_tgt.sls_grp_desc, 
            v_intrm_copa_tgt.cust_sls_ofc, 
            v_intrm_copa_tgt.sls_ofc_desc, 
            v_intrm_copa_tgt.channel, 
            v_intrm_copa_tgt.edw_cust_nm, 
            v_intrm_copa_tgt.acct_hier_desc, 
            v_intrm_copa_tgt.acct_hier_shrt_desc, 
            v_intrm_copa_tgt.company_nm 
          FROM 
            v_intrm_copa_tgt 
          WHERE 
            v_intrm_copa_tgt.ctry_key :: TEXT = 'TW' :: CHARACTER VARYING :: TEXT
        ) a 
        LEFT JOIN v_intrm_internal_tgt b ON a.fisc_yr_per :: CHARACTER VARYING :: TEXT = b.fisc_yr_per :: TEXT 
        AND a.co_cd :: TEXT = b.co_cd :: TEXT 
        AND a.obj_crncy_co_obj :: TEXT = b.crncy :: TEXT 
        AND ltrim(
          a.cust_num :: TEXT, 0 :: CHARACTER VARYING :: TEXT
        ) = ltrim(
          b.cust_num :: CHARACTER VARYING :: TEXT, 
          0 :: CHARACTER VARYING :: TEXT
        )
    ) x 
    LEFT JOIN v_intrm_crncy_exch g ON x.obj_crncy_co_obj :: TEXT = g.from_crncy :: TEXT
), 
derived_table2 AS (
  SELECT 
    x.co_cd, 
    x.cust_num, 
    x.ctry_nm, 
    x.ctry_key, 
    x.sls_grp, 
    x.rgn, 
    x.fisc_yr_per, 
    g.from_crncy, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    CASE WHEN x.acct_hier_shrt_desc :: TEXT = 'TP' :: CHARACTER VARYING :: TEXT THEN x.amt_obj_crncy * (
      -1 :: NUMERIC :: NUMERIC(18, 0)
    ) ELSE x.amt_obj_crncy END AS amt_obj_crncy, 
    x.obj_crncy_co_obj, 
    x.sls_trgt, 
    x.cust_sls_grp, 
    x.sls_grp_desc, 
    x.cust_sls_ofc, 
    x.sls_ofc_desc, 
    x.channel, 
    x.edw_cust_nm, 
    x.acct_hier_desc, 
    x.acct_hier_shrt_desc, 
    x.company_nm 
  FROM 
    (
      SELECT 
        CASE WHEN a.co_cd IS NULL THEN b.co_cd ELSE a.co_cd END AS co_cd, 
        CASE WHEN a.cust_num IS NULL THEN b.cust_num :: CHARACTER VARYING(40) ELSE a.cust_num END AS cust_num, 
        CASE WHEN a.ctry_nm IS NULL THEN b.ctry_nm ELSE a.ctry_nm END AS ctry_nm, 
        CASE WHEN a.ctry_key IS NULL THEN b.ctry_key ELSE a.ctry_key END AS ctry_key, 
        CASE WHEN a.sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_grp END AS sls_grp, 
        CASE WHEN a.rgn IS NULL THEN b.rgn ELSE a.rgn END AS rgn, 
        CASE WHEN a.fisc_yr_per IS NULL THEN b.fisc_yr_per :: INTEGER ELSE a.fisc_yr_per END AS fisc_yr_per, 
        COALESCE(a.amt_obj_crncy, 0.0) AS amt_obj_crncy, 
        CASE WHEN a.obj_crncy_co_obj IS NULL THEN b.crncy ELSE a.obj_crncy_co_obj END AS obj_crncy_co_obj, 
        b.sls_trgt, 
        CASE WHEN a.cust_sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_grp END AS cust_sls_grp, 
        CASE WHEN a.sls_grp_desc IS NULL THEN b.sls_grp_desc ELSE a.sls_grp_desc END AS sls_grp_desc, 
        CASE WHEN a.cust_sls_ofc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_ofc END AS cust_sls_ofc, 
        CASE WHEN a.sls_ofc_desc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_ofc_desc END AS sls_ofc_desc, 
        CASE WHEN a.channel IS NULL THEN b.channel ELSE a.channel END AS channel, 
        CASE WHEN a.edw_cust_nm IS NULL THEN b.edw_cust_nm ELSE a.edw_cust_nm END AS edw_cust_nm, 
        CASE WHEN a.acct_hier_desc IS NULL THEN 'NTS' :: CHARACTER VARYING ELSE a.acct_hier_desc END AS acct_hier_desc, 
        CASE WHEN a.acct_hier_shrt_desc IS NULL THEN 'NTS' :: CHARACTER VARYING ELSE a.acct_hier_shrt_desc END AS acct_hier_shrt_desc, 
        CASE WHEN a.company_nm IS NULL THEN 'J&J Taiwan Ltd' :: CHARACTER VARYING ELSE a.company_nm END AS company_nm 
      FROM 
        (
          SELECT 
            v_intrm_copa_tgt.co_cd, 
            v_intrm_copa_tgt.cust_num, 
            v_intrm_copa_tgt.ctry_nm, 
            v_intrm_copa_tgt.ctry_key, 
            v_intrm_copa_tgt.sls_grp, 
            v_intrm_copa_tgt.rgn, 
            v_intrm_copa_tgt.fisc_yr_per, 
            v_intrm_copa_tgt.amt_obj_crncy, 
            v_intrm_copa_tgt.obj_crncy_co_obj, 
            v_intrm_copa_tgt.cust_sls_grp, 
            v_intrm_copa_tgt.sls_grp_desc, 
            v_intrm_copa_tgt.cust_sls_ofc, 
            v_intrm_copa_tgt.sls_ofc_desc, 
            v_intrm_copa_tgt.channel, 
            v_intrm_copa_tgt.edw_cust_nm, 
            v_intrm_copa_tgt.acct_hier_desc, 
            v_intrm_copa_tgt.acct_hier_shrt_desc, 
            v_intrm_copa_tgt.company_nm 
          FROM 
            v_intrm_copa_tgt 
          WHERE 
            v_intrm_copa_tgt.ctry_key :: TEXT = 'TW' :: CHARACTER VARYING :: TEXT 
            AND v_intrm_copa_tgt.acct_hier_shrt_desc :: TEXT <> 'NTS' :: CHARACTER VARYING :: TEXT
        ) a FULL 
        JOIN v_intrm_internal_tgt b ON a.fisc_yr_per :: CHARACTER VARYING :: TEXT = b.fisc_yr_per :: TEXT 
        AND a.co_cd :: TEXT = b.co_cd :: TEXT 
        AND a.obj_crncy_co_obj :: TEXT = b.crncy :: TEXT 
        AND ltrim(
          a.cust_num :: TEXT, 0 :: CHARACTER VARYING :: TEXT
        ) = ltrim(
          b.cust_num :: CHARACTER VARYING :: TEXT, 
          0 :: CHARACTER VARYING :: TEXT
        )
    ) x 
    LEFT JOIN v_intrm_crncy_exch g ON x.obj_crncy_co_obj :: TEXT = g.from_crncy :: TEXT
), 
derived_table3 AS (
  SELECT 
    x.co_cd, 
    x.cust_num, 
    x.ctry_nm, 
    x.ctry_key, 
    x.sls_grp, 
    x.rgn, 
    x.fisc_yr_per, 
    g.from_crncy, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    CASE WHEN x.acct_hier_shrt_desc :: TEXT = 'TP' :: CHARACTER VARYING :: TEXT THEN x.amt_obj_crncy * (
      -1 :: NUMERIC :: NUMERIC(18, 0)
    ) ELSE x.amt_obj_crncy END AS amt_obj_crncy, 
    x.obj_crncy_co_obj, 
    x.sls_trgt, 
    x.cust_sls_grp, 
    x.sls_grp_desc, 
    x.cust_sls_ofc, 
    x.sls_ofc_desc, 
    x.channel, 
    x.edw_cust_nm, 
    x.acct_hier_desc, 
    x.acct_hier_shrt_desc, 
    x.company_nm 
  FROM 
    (
      SELECT 
        CASE WHEN a.co_cd IS NULL THEN b.co_cd ELSE a.co_cd END AS co_cd, 
        CASE WHEN a.cust_num IS NULL THEN b.cust_num :: CHARACTER VARYING(40) ELSE a.cust_num END AS cust_num, 
        CASE WHEN a.ctry_nm IS NULL THEN b.ctry_nm ELSE a.ctry_nm END AS ctry_nm, 
        CASE WHEN a.ctry_key IS NULL THEN b.ctry_key ELSE a.ctry_key END AS ctry_key, 
        CASE WHEN a.sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_grp END AS sls_grp, 
        CASE WHEN a.rgn IS NULL THEN b.rgn ELSE a.rgn END AS rgn, 
        CASE WHEN a.fisc_yr_per IS NULL THEN b.fisc_yr_per :: INTEGER ELSE a.fisc_yr_per END AS fisc_yr_per, 
        COALESCE(a.amt_obj_crncy, 0.0) AS amt_obj_crncy, 
        CASE WHEN a.obj_crncy_co_obj IS NULL THEN b.crncy ELSE a.obj_crncy_co_obj END AS obj_crncy_co_obj, 
        b.sls_trgt, 
        CASE WHEN a.cust_sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_grp END AS cust_sls_grp, 
        CASE WHEN a.sls_grp_desc IS NULL THEN b.sls_grp_desc ELSE a.sls_grp_desc END AS sls_grp_desc, 
        CASE WHEN a.cust_sls_ofc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_ofc END AS cust_sls_ofc, 
        CASE WHEN a.sls_ofc_desc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_ofc_desc END AS sls_ofc_desc, 
        CASE WHEN a.channel IS NULL THEN b.channel ELSE a.channel END AS channel, 
        CASE WHEN a.edw_cust_nm IS NULL THEN b.edw_cust_nm ELSE a.edw_cust_nm END AS edw_cust_nm, 
        CASE WHEN a.acct_hier_desc IS NULL THEN 'GTS' :: CHARACTER VARYING ELSE a.acct_hier_desc END AS acct_hier_desc, 
        CASE WHEN a.acct_hier_shrt_desc IS NULL THEN 'GTS' :: CHARACTER VARYING ELSE a.acct_hier_shrt_desc END AS acct_hier_shrt_desc, 
        CASE WHEN a.company_nm IS NULL THEN 'J&J Taiwan Ltd' :: CHARACTER VARYING ELSE a.company_nm END AS company_nm 
      FROM 
        (
          SELECT 
            v_intrm_copa_tgt.co_cd, 
            v_intrm_copa_tgt.cust_num, 
            v_intrm_copa_tgt.ctry_nm, 
            v_intrm_copa_tgt.ctry_key, 
            v_intrm_copa_tgt.sls_grp, 
            v_intrm_copa_tgt.rgn, 
            v_intrm_copa_tgt.fisc_yr_per, 
            v_intrm_copa_tgt.amt_obj_crncy, 
            v_intrm_copa_tgt.obj_crncy_co_obj, 
            v_intrm_copa_tgt.cust_sls_grp, 
            v_intrm_copa_tgt.sls_grp_desc, 
            v_intrm_copa_tgt.cust_sls_ofc, 
            v_intrm_copa_tgt.sls_ofc_desc, 
            v_intrm_copa_tgt.channel, 
            v_intrm_copa_tgt.edw_cust_nm, 
            v_intrm_copa_tgt.acct_hier_desc, 
            v_intrm_copa_tgt.acct_hier_shrt_desc, 
            v_intrm_copa_tgt.company_nm 
          FROM 
            v_intrm_copa_tgt 
          WHERE 
            v_intrm_copa_tgt.ctry_key :: TEXT = 'TW' :: CHARACTER VARYING :: TEXT 
            AND v_intrm_copa_tgt.acct_hier_shrt_desc :: TEXT <> 'GTS' :: CHARACTER VARYING :: TEXT
        ) a FULL 
        JOIN v_intrm_internal_tgt b ON a.fisc_yr_per :: CHARACTER VARYING :: TEXT = b.fisc_yr_per :: TEXT 
        AND a.co_cd :: TEXT = b.co_cd :: TEXT 
        AND a.obj_crncy_co_obj :: TEXT = b.crncy :: TEXT 
        AND ltrim(
          a.cust_num :: TEXT, 0 :: CHARACTER VARYING :: TEXT
        ) = ltrim(
          b.cust_num :: CHARACTER VARYING :: TEXT, 
          0 :: CHARACTER VARYING :: TEXT
        )
    ) x 
    LEFT JOIN v_intrm_crncy_exch g ON x.obj_crncy_co_obj :: TEXT = g.from_crncy :: TEXT
), 
derived_table4 AS (
  SELECT 
    x.co_cd, 
    x.cust_num, 
    x.ctry_nm, 
    x.ctry_key, 
    x.sls_grp, 
    x.rgn, 
    x.fisc_yr_per, 
    g.from_crncy, 
    g.to_crncy, 
    g.ex_rt_typ, 
    g.ex_rt, 
    CASE WHEN x.acct_hier_shrt_desc :: TEXT = 'TP' :: CHARACTER VARYING :: TEXT THEN x.amt_obj_crncy * (
      -1 :: NUMERIC :: NUMERIC(18, 0)
    ) ELSE x.amt_obj_crncy END AS amt_obj_crncy, 
    x.obj_crncy_co_obj, 
    x.sls_trgt, 
    x.cust_sls_grp, 
    x.sls_grp_desc, 
    x.cust_sls_ofc, 
    x.sls_ofc_desc, 
    x.channel, 
    x.edw_cust_nm, 
    x.acct_hier_desc, 
    x.acct_hier_shrt_desc, 
    x.company_nm 
  FROM 
    (
      SELECT 
        CASE WHEN a.co_cd IS NULL THEN b.co_cd ELSE a.co_cd END AS co_cd, 
        CASE WHEN a.cust_num IS NULL THEN b.cust_num :: CHARACTER VARYING(40) ELSE a.cust_num END AS cust_num, 
        CASE WHEN a.ctry_nm IS NULL THEN b.ctry_nm ELSE a.ctry_nm END AS ctry_nm, 
        CASE WHEN a.ctry_key IS NULL THEN b.ctry_key ELSE a.ctry_key END AS ctry_key, 
        CASE WHEN a.sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_grp END AS sls_grp, 
        CASE WHEN a.rgn IS NULL THEN b.rgn ELSE a.rgn END AS rgn, 
        CASE WHEN a.fisc_yr_per IS NULL THEN b.fisc_yr_per :: INTEGER ELSE a.fisc_yr_per END AS fisc_yr_per, 
        COALESCE(a.amt_obj_crncy, 0.0) AS amt_obj_crncy, 
        CASE WHEN a.obj_crncy_co_obj IS NULL THEN b.crncy ELSE a.obj_crncy_co_obj END AS obj_crncy_co_obj, 
        b.sls_trgt, 
        CASE WHEN a.cust_sls_grp IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_grp END AS cust_sls_grp, 
        CASE WHEN a.sls_grp_desc IS NULL THEN b.sls_grp_desc ELSE a.sls_grp_desc END AS sls_grp_desc, 
        CASE WHEN a.cust_sls_ofc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.cust_sls_ofc END AS cust_sls_ofc, 
        CASE WHEN a.sls_ofc_desc IS NULL THEN 'N/A' :: CHARACTER VARYING ELSE a.sls_ofc_desc END AS sls_ofc_desc, 
        CASE WHEN a.channel IS NULL THEN b.channel ELSE a.channel END AS channel, 
        CASE WHEN a.edw_cust_nm IS NULL THEN b.edw_cust_nm ELSE a.edw_cust_nm END AS edw_cust_nm, 
        CASE WHEN a.acct_hier_desc IS NULL THEN 'NA' :: CHARACTER VARYING ELSE a.acct_hier_desc END AS acct_hier_desc, 
        CASE WHEN a.acct_hier_shrt_desc IS NULL THEN 'NA' :: CHARACTER VARYING ELSE a.acct_hier_shrt_desc END AS acct_hier_shrt_desc, 
        CASE WHEN a.company_nm IS NULL THEN 'J&J Taiwan Ltd' :: CHARACTER VARYING ELSE a.company_nm END AS company_nm 
      FROM 
        (
          SELECT 
            v_intrm_copa_tgt.co_cd, 
            v_intrm_copa_tgt.cust_num, 
            v_intrm_copa_tgt.ctry_nm, 
            v_intrm_copa_tgt.ctry_key, 
            v_intrm_copa_tgt.sls_grp, 
            v_intrm_copa_tgt.rgn, 
            v_intrm_copa_tgt.fisc_yr_per, 
            v_intrm_copa_tgt.amt_obj_crncy, 
            v_intrm_copa_tgt.obj_crncy_co_obj, 
            v_intrm_copa_tgt.cust_sls_grp, 
            v_intrm_copa_tgt.sls_grp_desc, 
            v_intrm_copa_tgt.cust_sls_ofc, 
            v_intrm_copa_tgt.sls_ofc_desc, 
            v_intrm_copa_tgt.channel, 
            v_intrm_copa_tgt.edw_cust_nm, 
            v_intrm_copa_tgt.acct_hier_desc, 
            v_intrm_copa_tgt.acct_hier_shrt_desc, 
            v_intrm_copa_tgt.company_nm 
          FROM 
            v_intrm_copa_tgt 
          WHERE 
            v_intrm_copa_tgt.ctry_key :: TEXT = 'TW' :: CHARACTER VARYING :: TEXT
        ) a FULL 
        JOIN v_intrm_internal_tgt b ON a.fisc_yr_per :: CHARACTER VARYING :: TEXT = b.fisc_yr_per :: TEXT 
        AND a.co_cd :: TEXT = b.co_cd :: TEXT 
        AND a.obj_crncy_co_obj :: TEXT = b.crncy :: TEXT 
        AND ltrim(
          a.cust_num :: TEXT, 0 :: CHARACTER VARYING :: TEXT
        ) = ltrim(
          b.cust_num :: CHARACTER VARYING :: TEXT, 
          0 :: CHARACTER VARYING :: TEXT
        )
    ) x 
    LEFT JOIN v_intrm_crncy_exch g ON x.obj_crncy_co_obj :: TEXT = g.from_crncy :: TEXT
), 
transformed AS (
  (
    (
      SELECT 
        derived_table1.co_cd, 
        derived_table1.cust_num, 
        derived_table1.ctry_nm, 
        derived_table1.ctry_key, 
        derived_table1.sls_grp, 
        derived_table1.rgn, 
        derived_table1.fisc_yr_per, 
        derived_table1.from_crncy, 
        derived_table1.to_crncy, 
        derived_table1.ex_rt_typ, 
        derived_table1.ex_rt, 
        derived_table1.amt_obj_crncy, 
        derived_table1.obj_crncy_co_obj, 
        derived_table1.sls_trgt, 
        derived_table1.cust_sls_grp, 
        derived_table1.sls_grp_desc, 
        derived_table1.cust_sls_ofc, 
        derived_table1.sls_ofc_desc, 
        derived_table1.channel, 
        derived_table1.edw_cust_nm, 
        derived_table1.acct_hier_desc, 
        derived_table1.acct_hier_shrt_desc, 
        derived_table1.company_nm 
      FROM 
        derived_table1 
      WHERE 
        derived_table1.acct_hier_shrt_desc :: TEXT <> 'NA' :: CHARACTER VARYING :: TEXT 
      UNION ALL 
      SELECT 
        derived_table2.co_cd, 
        derived_table2.cust_num, 
        derived_table2.ctry_nm, 
        derived_table2.ctry_key, 
        derived_table2.sls_grp, 
        derived_table2.rgn, 
        derived_table2.fisc_yr_per, 
        derived_table2.from_crncy, 
        derived_table2.to_crncy, 
        derived_table2.ex_rt_typ, 
        derived_table2.ex_rt, 
        derived_table2.amt_obj_crncy, 
        derived_table2.obj_crncy_co_obj, 
        derived_table2.sls_trgt, 
        derived_table2.cust_sls_grp, 
        derived_table2.sls_grp_desc, 
        derived_table2.cust_sls_ofc, 
        derived_table2.sls_ofc_desc, 
        derived_table2.channel, 
        derived_table2.edw_cust_nm, 
        derived_table2.acct_hier_desc, 
        derived_table2.acct_hier_shrt_desc, 
        derived_table2.company_nm 
      FROM 
        derived_table2 
      WHERE 
        derived_table2.acct_hier_shrt_desc :: TEXT = 'NTS' :: CHARACTER VARYING :: TEXT
    ) 
    UNION ALL 
    SELECT 
      derived_table3.co_cd, 
      derived_table3.cust_num, 
      derived_table3.ctry_nm, 
      derived_table3.ctry_key, 
      derived_table3.sls_grp, 
      derived_table3.rgn, 
      derived_table3.fisc_yr_per, 
      derived_table3.from_crncy, 
      derived_table3.to_crncy, 
      derived_table3.ex_rt_typ, 
      derived_table3.ex_rt, 
      derived_table3.amt_obj_crncy, 
      derived_table3.obj_crncy_co_obj, 
      derived_table3.sls_trgt, 
      derived_table3.cust_sls_grp, 
      derived_table3.sls_grp_desc, 
      derived_table3.cust_sls_ofc, 
      derived_table3.sls_ofc_desc, 
      derived_table3.channel, 
      derived_table3.edw_cust_nm, 
      derived_table3.acct_hier_desc, 
      derived_table3.acct_hier_shrt_desc, 
      derived_table3.company_nm 
    FROM 
      derived_table3 
    WHERE 
      derived_table3.acct_hier_shrt_desc :: TEXT = 'GTS' :: CHARACTER VARYING :: TEXT
  ) 
  UNION ALL 
  SELECT 
    derived_table4.co_cd, 
    derived_table4.cust_num, 
    derived_table4.ctry_nm, 
    derived_table4.ctry_key, 
    derived_table4.sls_grp, 
    derived_table4.rgn, 
    derived_table4.fisc_yr_per, 
    derived_table4.from_crncy, 
    derived_table4.to_crncy, 
    derived_table4.ex_rt_typ, 
    derived_table4.ex_rt, 
    derived_table4.amt_obj_crncy, 
    derived_table4.obj_crncy_co_obj, 
    derived_table4.sls_trgt, 
    derived_table4.cust_sls_grp, 
    derived_table4.sls_grp_desc, 
    derived_table4.cust_sls_ofc, 
    derived_table4.sls_ofc_desc, 
    derived_table4.channel, 
    derived_table4.edw_cust_nm, 
    derived_table4.acct_hier_desc, 
    derived_table4.acct_hier_shrt_desc, 
    derived_table4.company_nm 
  FROM 
    derived_table4 
  WHERE 
    derived_table4.acct_hier_shrt_desc :: TEXT = 'NA' :: CHARACTER VARYING :: TEXT
), 
final AS (
  SELECT 
    transformed.co_cd, 
    transformed.cust_num, 
    transformed.ctry_nm, 
    transformed.ctry_key, 
    transformed.sls_grp, 
    transformed.rgn, 
    transformed.fisc_yr_per, 
    transformed.from_crncy, 
    transformed.to_crncy, 
    transformed.ex_rt_typ, 
    transformed.ex_rt, 
    transformed.amt_obj_crncy, 
    transformed.obj_crncy_co_obj, 
    transformed.sls_trgt, 
    transformed.cust_sls_grp, 
    transformed.sls_grp_desc, 
    transformed.cust_sls_ofc, 
    transformed.sls_ofc_desc, 
    transformed.channel, 
    transformed.edw_cust_nm, 
    CASE WHEN transformed.acct_hier_desc :: TEXT = 'NA' :: CHARACTER VARYING :: TEXT THEN 'TP' :: CHARACTER VARYING ELSE transformed.acct_hier_desc END AS acct_hier_desc, 
    CASE WHEN transformed.acct_hier_shrt_desc :: TEXT = 'NA' :: CHARACTER VARYING :: TEXT THEN 'TP' :: CHARACTER VARYING ELSE transformed.acct_hier_shrt_desc END AS acct_hier_shrt_desc, 
    transformed.company_nm 
  FROM 
    transformed
) 
SELECT 
  * 
FROM 
  final
