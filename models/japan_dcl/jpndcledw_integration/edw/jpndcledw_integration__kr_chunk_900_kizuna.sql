WITH tbusrpram
AS (
  SELECT *
  FROM dev_dna_core.jpdclitg_integration.tbusrpram
  ),
idokeido
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.idokeido
  ),
kr_chunk_002_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_002_kizuna
  ),
kr_chunk_003_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_003_kizuna
  ),
kr_chunk_004_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_004_kizuna
  ),
kr_chunk_005_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_005_kizuna
  ),
kr_chunk_006_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_006_kizuna
  ),
kr_chunk_007_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_007_kizuna
  ),
kr_chunk_100_kizuna
AS (
  SELECT *
  FROM dev_dna_core.jpdcledw_integration.kr_chunk_100_kizuna
  ),
transformed
AS (
  SELECT
        LPAD(USR.DIUSRID,10,0) CUSTOMERNO
        ,USR.DSDAT42 REGISTRATIONDATE
        ,USR.DSDAT44 REGISTRATIONCHANNEL
        ,CASE 
            WHEN USR.DSSEIBETSU = 0 THEN '男性'
            WHEN USR.DSSEIBETSU = 1 THEN '女性'
            WHEN USR.DSSEIBETSU = 2 THEN '不明'
            ELSE 'その他'
        END  SEX
        ,TO_CHAR(current_timestamp(),'YYYYMMDD')::integer as today
        ,TO_CHAR(TO_DATE(DSBIRTHDAY,'YYYY-MM-DD HH24:MI:SS'),'YYYYMMDD')::integer as dob
        ,{{ getnenrei("today", "dob") }} as AGE
        ,USR.DSPREF PREFECTURE
        ,USR.DSZIP POSTALCODE
        ,GEO.LATITUDE LATITUDE
        ,GEO.LONGITUDE LONGITUDE
        ,USR.DSDAT10 CUSTOMERSTATUS1
        ,USR.DSDAT11 CUSTOMERSTATUS2
        ,USR.DSDAT12 CUSTOMERSTATUS3
        ,CASE WHEN USR.DSCTRLCD IS NOT NULL THEN 'Y' 
              ELSE 'N' 
         END ECREGISTRATION
        ,USR.DSCTRLCD ECNO
        ,CASE WHEN USR.DSDAT7   IS NOT NULL THEN 'Y' 
              ELSE 'N' 
         END STOREREGISTRATION
        ,USR.DSDAT7 STORENO
        ,USR.DISECESSIONFLG DELETEACOUNT
        ,USR.DSDAT40 NOTPAYAMOUNT
        ,USR.DSDAT41 DEMANDPAYMENTCNT
        ,USR.DSDAT4 PHONETYPE
        ,USR.DSDAT9 OCCUPATION
        ,USR.DSDAT48 EMAILPERMISSION 
		,USR.DSDAT46 KAIINGENTEIMAIL
        ,USR.DSDAT60 CCDMPERMISSION
        ,USR.DSDAT61 STOREDMPERMISSION
        ,USR.DSDAT62 CCCALLPERMISSION
        ,USR.DSDAT63 STORECALLPERMISSION
        ,USR.DSDAT64 MAGAGINEPERMISSION
        ,USR.DIPOINT OWNINGPOINT
        ,CHUNK_002.MINJUCHDATE MINJUCHDATE
        ,CHUNK_003.MINJUCHDATE1 MINJUCHDATE1
        ,CHUNK_003.MAXJUCHDATE1 MAXJUCHDATE1
        ,CHUNK_003.JUCHCNTTSUHAN JUCHCNTTSUHAN
        ,CHUNK_003.JUCHCNTWEB JUCHCNTWEB
        ,CHUNK_003.JUCHCNTSTORE JUCHCNTSTORE
        ,CHUNK_003.JUCHSUMTSUHAN JUCHSUMTSUHAN
        ,CHUNK_003.JUCHSUMWEB JUCHSUMWEB
        ,CHUNK_003.JUCHSUMSTORE JUCHSUMSTORE
        ,CHUNK_003.AMOUNTAVG AMOUNTAVG
        ,CHUNK_004.AMOUNTSUMALL AMOUNTSUMALL
        ,CHUNK_004.AMOUNTSUM30 AMOUNTSUM30
        ,CHUNK_004.AMOUNTSUM25 AMOUNTSUM25
        ,CHUNK_004.AMOUNTSUM20 AMOUNTSUM20
        ,CHUNK_004.AMOUNTSUM15 AMOUNTSUM15
        ,CHUNK_004.AMOUNTSUM10 AMOUNTSUM10
        ,CHUNK_004.AMOUNTSUM05 AMOUNTSUM05
        ,CHUNK_004.SHIPPINGFEECNT SHIPPINGFEECNT
        ,CASE WHEN CHUNK_007.CONTRACT_ENDYM IS NOT NULL THEN 'N' ELSE CHUNK_005.CONTRACT END CONTRACT
        ,CHUNK_006.CONTRACT_FSTYM CONTRACT_FSTYM
        ,CHUNK_007.CONTRACT_ENDYM CONTRACT_ENDYM
        ,CHUNK_100.CNT_ATTR001 CNT_ATTR001
        ,CHUNK_100.FST_ATTR001 FST_ATTR001
        ,CASE WHEN CHUNK_100.LST_ATTR001 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR001 END LST_ATTR001
        ,CHUNK_100.CNT_ATTR002 CNT_ATTR002
        ,CHUNK_100.FST_ATTR002 FST_ATTR002
        ,CASE WHEN CHUNK_100.LST_ATTR002 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR002 END LST_ATTR002
        ,CHUNK_100.CNT_ATTR003 CNT_ATTR003
        ,CHUNK_100.FST_ATTR003 FST_ATTR003
        ,CASE WHEN CHUNK_100.LST_ATTR003 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR003 END LST_ATTR003
        ,CHUNK_100.CNT_ATTR004 CNT_ATTR004
        ,CHUNK_100.FST_ATTR004 FST_ATTR004
        ,CASE WHEN CHUNK_100.LST_ATTR004 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR004 END LST_ATTR004
        ,CHUNK_100.CNT_ATTR005 CNT_ATTR005
        ,CHUNK_100.FST_ATTR005 FST_ATTR005
        ,CASE WHEN CHUNK_100.LST_ATTR005 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR005 END LST_ATTR005
        ,CHUNK_100.CNT_ATTR006 CNT_ATTR006
        ,CHUNK_100.FST_ATTR006 FST_ATTR006
        ,CASE WHEN CHUNK_100.LST_ATTR006 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR006 END LST_ATTR006
        ,CHUNK_100.CNT_ATTR007 CNT_ATTR007
        ,CHUNK_100.FST_ATTR007 FST_ATTR007
        ,CASE WHEN CHUNK_100.LST_ATTR007 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR007 END LST_ATTR007
        ,CHUNK_100.CNT_ATTR008 CNT_ATTR008
        ,CHUNK_100.FST_ATTR008 FST_ATTR008
        ,CASE WHEN CHUNK_100.LST_ATTR008 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR008 END LST_ATTR008
        ,CHUNK_100.CNT_ATTR009 CNT_ATTR009
        ,CHUNK_100.FST_ATTR009 FST_ATTR009
        ,CASE WHEN CHUNK_100.LST_ATTR009 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR009 END LST_ATTR009
        ,CHUNK_100.CNT_ATTR010 CNT_ATTR010
        ,CHUNK_100.FST_ATTR010 FST_ATTR010
        ,CASE WHEN CHUNK_100.LST_ATTR010 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR010 END LST_ATTR010
        ,CHUNK_100.CNT_ATTR011 CNT_ATTR011
        ,CHUNK_100.FST_ATTR011 FST_ATTR011
        ,CASE WHEN CHUNK_100.LST_ATTR011 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR011 END LST_ATTR011
        ,CHUNK_100.CNT_ATTR012 CNT_ATTR012
        ,CHUNK_100.FST_ATTR012 FST_ATTR012
        ,CASE WHEN CHUNK_100.LST_ATTR012 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR012 END LST_ATTR012
        ,CHUNK_100.CNT_ATTR013 CNT_ATTR013
        ,CHUNK_100.FST_ATTR013 FST_ATTR013
        ,CASE WHEN CHUNK_100.LST_ATTR013 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR013 END LST_ATTR013
        ,CHUNK_100.CNT_ATTR014 CNT_ATTR014
        ,CHUNK_100.FST_ATTR014 FST_ATTR014
        ,CASE WHEN CHUNK_100.LST_ATTR014 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR014 END LST_ATTR014
        ,CHUNK_100.CNT_ATTR015 CNT_ATTR015
        ,CHUNK_100.FST_ATTR015 FST_ATTR015
        ,CASE WHEN CHUNK_100.LST_ATTR015 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR015 END LST_ATTR015
        ,CHUNK_100.CNT_ATTR016 CNT_ATTR016
        ,CHUNK_100.FST_ATTR016 FST_ATTR016
        ,CASE WHEN CHUNK_100.LST_ATTR016 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR016 END LST_ATTR016
        ,CHUNK_100.CNT_ATTR017 CNT_ATTR017
        ,CHUNK_100.FST_ATTR017 FST_ATTR017
        ,CASE WHEN CHUNK_100.LST_ATTR017 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR017 END LST_ATTR017
        ,CHUNK_100.CNT_ATTR018 CNT_ATTR018
        ,CHUNK_100.FST_ATTR018 FST_ATTR018
        ,CASE WHEN CHUNK_100.LST_ATTR018 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR018 END LST_ATTR018
        ,CHUNK_100.CNT_ATTR019 CNT_ATTR019
        ,CHUNK_100.FST_ATTR019 FST_ATTR019
        ,CASE WHEN CHUNK_100.LST_ATTR019 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR019 END LST_ATTR019
        ,CHUNK_100.CNT_ATTR020 CNT_ATTR020
        ,CHUNK_100.FST_ATTR020 FST_ATTR020
        ,CASE WHEN CHUNK_100.LST_ATTR020 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR020 END LST_ATTR020
        ,CHUNK_100.CNT_ATTR021 CNT_ATTR021
        ,CHUNK_100.FST_ATTR021 FST_ATTR021
        ,CASE WHEN CHUNK_100.LST_ATTR021 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR021 END LST_ATTR021
        ,CHUNK_100.CNT_ATTR022 CNT_ATTR022
        ,CHUNK_100.FST_ATTR022 FST_ATTR022
        ,CASE WHEN CHUNK_100.LST_ATTR022 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR022 END LST_ATTR022
        ,CHUNK_100.CNT_ATTR023 CNT_ATTR023
        ,CHUNK_100.FST_ATTR023 FST_ATTR023
        ,CASE WHEN CHUNK_100.LST_ATTR023 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR023 END LST_ATTR023
        ,CHUNK_100.CNT_ATTR024 CNT_ATTR024
        ,CHUNK_100.FST_ATTR024 FST_ATTR024
        ,CASE WHEN CHUNK_100.LST_ATTR024 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR024 END LST_ATTR024
        ,CHUNK_100.CNT_ATTR025 CNT_ATTR025
        ,CHUNK_100.FST_ATTR025 FST_ATTR025
        ,CASE WHEN CHUNK_100.LST_ATTR025 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR025 END LST_ATTR025
        ,CHUNK_100.CNT_ATTR026 CNT_ATTR026
        ,CHUNK_100.FST_ATTR026 FST_ATTR026
        ,CASE WHEN CHUNK_100.LST_ATTR026 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR026 END LST_ATTR026
        ,CHUNK_100.CNT_ATTR027 CNT_ATTR027
        ,CHUNK_100.FST_ATTR027 FST_ATTR027
        ,CASE WHEN CHUNK_100.LST_ATTR027 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR027 END LST_ATTR027
        ,CHUNK_100.CNT_ATTR028 CNT_ATTR028
        ,CHUNK_100.FST_ATTR028 FST_ATTR028
        ,CASE WHEN CHUNK_100.LST_ATTR028 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR028 END LST_ATTR028
        ,CHUNK_100.CNT_ATTR029 CNT_ATTR029
        ,CHUNK_100.FST_ATTR029 FST_ATTR029
        ,CASE WHEN CHUNK_100.LST_ATTR029 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR029 END LST_ATTR029
        ,CHUNK_100.CNT_ATTR030 CNT_ATTR030
        ,CHUNK_100.FST_ATTR030 FST_ATTR030
        ,CASE WHEN CHUNK_100.LST_ATTR030 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR030 END LST_ATTR030
        ,CHUNK_100.CNT_ATTR031 CNT_ATTR031
        ,CHUNK_100.FST_ATTR031 FST_ATTR031
        ,CASE WHEN CHUNK_100.LST_ATTR031 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR031 END LST_ATTR031
        ,CHUNK_100.CNT_ATTR032 CNT_ATTR032
        ,CHUNK_100.FST_ATTR032 FST_ATTR032
        ,CASE WHEN CHUNK_100.LST_ATTR032 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR032 END LST_ATTR032
        ,CHUNK_100.CNT_ATTR033 CNT_ATTR033
        ,CHUNK_100.FST_ATTR033 FST_ATTR033
        ,CASE WHEN CHUNK_100.LST_ATTR033 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR033 END LST_ATTR033
        ,CHUNK_100.CNT_ATTR034 CNT_ATTR034
        ,CHUNK_100.FST_ATTR034 FST_ATTR034
        ,CASE WHEN CHUNK_100.LST_ATTR034 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR034 END LST_ATTR034
        ,CHUNK_100.CNT_ATTR035 CNT_ATTR035
        ,CHUNK_100.FST_ATTR035 FST_ATTR035
        ,CASE WHEN CHUNK_100.LST_ATTR035 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR035 END LST_ATTR035
        ,CHUNK_100.CNT_ATTR036 CNT_ATTR036
        ,CHUNK_100.FST_ATTR036 FST_ATTR036
        ,CASE WHEN CHUNK_100.LST_ATTR036 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR036 END LST_ATTR036
        ,CHUNK_100.CNT_ATTR037 CNT_ATTR037
        ,CHUNK_100.FST_ATTR037 FST_ATTR037
        ,CASE WHEN CHUNK_100.LST_ATTR037 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR037 END LST_ATTR037
        ,CHUNK_100.CNT_ATTR038 CNT_ATTR038
        ,CHUNK_100.FST_ATTR038 FST_ATTR038
        ,CASE WHEN CHUNK_100.LST_ATTR038 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR038 END LST_ATTR038
        ,CHUNK_100.CNT_ATTR039 CNT_ATTR039
        ,CHUNK_100.FST_ATTR039 FST_ATTR039
        ,CASE WHEN CHUNK_100.LST_ATTR039 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR039 END LST_ATTR039
        ,CHUNK_100.CNT_ATTR040 CNT_ATTR040
        ,CHUNK_100.FST_ATTR040 FST_ATTR040
        ,CASE WHEN CHUNK_100.LST_ATTR040 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR040 END LST_ATTR040
        ,CHUNK_100.CNT_ATTR041 CNT_ATTR041
        ,CHUNK_100.FST_ATTR041 FST_ATTR041
        ,CASE WHEN CHUNK_100.LST_ATTR041 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR041 END LST_ATTR041
        ,CHUNK_100.CNT_ATTR042 CNT_ATTR042
        ,CHUNK_100.FST_ATTR042 FST_ATTR042
        ,CASE WHEN CHUNK_100.LST_ATTR042 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR042 END LST_ATTR042
        ,CHUNK_100.CNT_ATTR043 CNT_ATTR043
        ,CHUNK_100.FST_ATTR043 FST_ATTR043
        ,CASE WHEN CHUNK_100.LST_ATTR043 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR043 END LST_ATTR043
        ,CHUNK_100.CNT_ATTR044 CNT_ATTR044
        ,CHUNK_100.FST_ATTR044 FST_ATTR044
        ,CASE WHEN CHUNK_100.LST_ATTR044 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR044 END LST_ATTR044
        ,CHUNK_100.CNT_ATTR045 CNT_ATTR045
        ,CHUNK_100.FST_ATTR045 FST_ATTR045
        ,CASE WHEN CHUNK_100.LST_ATTR045 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR045 END LST_ATTR045
        ,CHUNK_100.CNT_ATTR046 CNT_ATTR046
        ,CHUNK_100.FST_ATTR046 FST_ATTR046
        ,CASE WHEN CHUNK_100.LST_ATTR046 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR046 END LST_ATTR046
        ,CHUNK_100.CNT_ATTR047 CNT_ATTR047
        ,CHUNK_100.FST_ATTR047 FST_ATTR047
        ,CASE WHEN CHUNK_100.LST_ATTR047 = 0 THEN NULL ELSE CHUNK_100.LST_ATTR047 END LST_ATTR047
        ,NULL INSERTED_BY
        ,NULL UPDATED_BY
FROM   tbusrpram USR
       LEFT JOIN (
                  SELECT
                         ZIPCODE
                        ,SUBSTRING(IDOKEIDO, 1, REGEXP_INSTR(IDOKEIDO, ',', 1, 1) -1) AS LATITUDE
                        ,SUBSTRING(IDOKEIDO, REGEXP_INSTR(IDOKEIDO, ',', 1, 1) + 1)   AS LONGITUDE
                  FROM idokeido
                  WHERE IDOKEIDO.IDOKEIDO IS NOT NULL
                 ) GEO
              ON GEO.ZIPCODE = USR.DSZIP
       LEFT JOIN kr_chunk_002_kizuna CHUNK_002
              ON LPAD(DIUSRID,10,0) = CHUNK_002.KOKYANO
       LEFT JOIN kr_chunk_003_kizuna CHUNK_003
              ON LPAD(DIUSRID,10,0) = CHUNK_003.KOKYANO
       LEFT JOIN kr_chunk_004_kizuna CHUNK_004
              ON LPAD(DIUSRID,10,0) = CHUNK_004.KOKYANO
       LEFT JOIN kr_chunk_005_kizuna CHUNK_005
              ON LPAD(DIUSRID,10,0) = CHUNK_005.KOKYANO
       LEFT JOIN kr_chunk_006_kizuna CHUNK_006
              ON LPAD(DIUSRID,10,0) = CHUNK_006.KOKYANO
       LEFT JOIN kr_chunk_007_kizuna CHUNK_007
              ON LPAD(DIUSRID,10,0) = CHUNK_007.KOKYANO
       LEFT JOIN kr_chunk_100_kizuna CHUNK_100
              ON LPAD(DIUSRID,10,0) = CHUNK_100.KOKYANO
WHERE USR.DSDAT93 = '通常ユーザ'
  ),
final
AS (
  SELECT customerno::varchar(30) as customerno,
registrationdate::varchar(6000) as registrationdate,
registrationchannel::varchar(6000) as registrationchannel,
sex::varchar(9) as sex,
age::number(38,18) as age,
prefecture::varchar(15) as prefecture,
postalcode::varchar(15) as postalcode,
latitude::varchar(90) as latitude,
longitude::varchar(90) as longitude,
customerstatus1::varchar(6000) as customerstatus1,
customerstatus2::varchar(6000) as customerstatus2,
customerstatus3::varchar(6000) as customerstatus3,
ecregistration::varchar(1) as ecregistration,
ecno::varchar(24) as ecno,
storeregistration::varchar(1) as storeregistration,
storeno::varchar(6000) as storeno,
deleteacount::varchar(1) as deleteacount,
notpayamount::varchar(6000) as notpayamount,
demandpaymentcnt::varchar(6000) as demandpaymentcnt,
phonetype::varchar(6000) as phonetype,
occupation::varchar(6000) as occupation,
emailpermission::varchar(6000) as emailpermission,
kaiingenteimail::varchar(6000) as kaiingenteimail,
ccdmpermission::varchar(6000) as ccdmpermission,
storedmpermission::varchar(6000) as storedmpermission,
cccallpermission::varchar(6000) as cccallpermission,
storecallpermission::varchar(6000) as storecallpermission,
magaginepermission::varchar(6000) as magaginepermission,
owningpoint::number(38,0) as owningpoint,
minjuchdate::number(38,18) as minjuchdate,
minjuchdate1::number(38,18) as minjuchdate1,
maxjuchdate1::number(38,18) as maxjuchdate1,
juchcnttsuhan::number(38,18) as juchcnttsuhan,
juchcntweb::number(38,18) as juchcntweb,
juchcntstore::number(38,18) as juchcntstore,
juchsumtsuhan::number(38,18) as juchsumtsuhan,
juchsumweb::number(38,18) as juchsumweb,
juchsumstore::number(38,18) as juchsumstore,
amountavg::number(38,18) as amountavg,
amountsumall::number(38,18) as amountsumall,
amountsum30::number(38,18) as amountsum30,
amountsum25::number(38,18) as amountsum25,
amountsum20::number(38,18) as amountsum20,
amountsum15::number(38,18) as amountsum15,
amountsum10::number(38,18) as amountsum10,
amountsum05::number(38,18) as amountsum05,
shippingfeecnt::number(38,18) as shippingfeecnt,
contract::varchar(1) as contract,
contract_fstym::varchar(9) as contract_fstym,
contract_endym::varchar(9) as contract_endym,
cnt_attr001::number(38,18) as cnt_attr001,
fst_attr001::number(38,18) as fst_attr001,
lst_attr001::number(38,18) as lst_attr001,
cnt_attr002::number(38,18) as cnt_attr002,
fst_attr002::number(38,18) as fst_attr002,
lst_attr002::number(38,18) as lst_attr002,
cnt_attr003::number(38,18) as cnt_attr003,
fst_attr003::number(38,18) as fst_attr003,
lst_attr003::number(38,18) as lst_attr003,
cnt_attr004::number(38,18) as cnt_attr004,
fst_attr004::number(38,18) as fst_attr004,
lst_attr004::number(38,18) as lst_attr004,
cnt_attr005::number(38,18) as cnt_attr005,
fst_attr005::number(38,18) as fst_attr005,
lst_attr005::number(38,18) as lst_attr005,
cnt_attr006::number(38,18) as cnt_attr006,
fst_attr006::number(38,18) as fst_attr006,
lst_attr006::number(38,18) as lst_attr006,
cnt_attr007::number(38,18) as cnt_attr007,
fst_attr007::number(38,18) as fst_attr007,
lst_attr007::number(38,18) as lst_attr007,
cnt_attr008::number(38,18) as cnt_attr008,
fst_attr008::number(38,18) as fst_attr008,
lst_attr008::number(38,18) as lst_attr008,
cnt_attr009::number(38,18) as cnt_attr009,
fst_attr009::number(38,18) as fst_attr009,
lst_attr009::number(38,18) as lst_attr009,
cnt_attr010::number(38,18) as cnt_attr010,
fst_attr010::number(38,18) as fst_attr010,
lst_attr010::number(38,18) as lst_attr010,
cnt_attr011::number(38,18) as cnt_attr011,
fst_attr011::number(38,18) as fst_attr011,
lst_attr011::number(38,18) as lst_attr011,
cnt_attr012::number(38,18) as cnt_attr012,
fst_attr012::number(38,18) as fst_attr012,
lst_attr012::number(38,18) as lst_attr012,
cnt_attr013::number(38,18) as cnt_attr013,
fst_attr013::number(38,18) as fst_attr013,
lst_attr013::number(38,18) as lst_attr013,
cnt_attr014::number(38,18) as cnt_attr014,
fst_attr014::number(38,18) as fst_attr014,
lst_attr014::number(38,18) as lst_attr014,
cnt_attr015::number(38,18) as cnt_attr015,
fst_attr015::number(38,18) as fst_attr015,
lst_attr015::number(38,18) as lst_attr015,
cnt_attr016::number(38,18) as cnt_attr016,
fst_attr016::number(38,18) as fst_attr016,
lst_attr016::number(38,18) as lst_attr016,
cnt_attr017::number(38,18) as cnt_attr017,
fst_attr017::number(38,18) as fst_attr017,
lst_attr017::number(38,18) as lst_attr017,
cnt_attr018::number(38,18) as cnt_attr018,
fst_attr018::number(38,18) as fst_attr018,
lst_attr018::number(38,18) as lst_attr018,
cnt_attr019::number(38,18) as cnt_attr019,
fst_attr019::number(38,18) as fst_attr019,
lst_attr019::number(38,18) as lst_attr019,
cnt_attr020::number(38,18) as cnt_attr020,
fst_attr020::number(38,18) as fst_attr020,
lst_attr020::number(38,18) as lst_attr020,
cnt_attr021::number(38,18) as cnt_attr021,
fst_attr021::number(38,18) as fst_attr021,
lst_attr021::number(38,18) as lst_attr021,
cnt_attr022::number(38,18) as cnt_attr022,
fst_attr022::number(38,18) as fst_attr022,
lst_attr022::number(38,18) as lst_attr022,
cnt_attr023::number(38,18) as cnt_attr023,
fst_attr023::number(38,18) as fst_attr023,
lst_attr023::number(38,18) as lst_attr023,
cnt_attr024::number(38,18) as cnt_attr024,
fst_attr024::number(38,18) as fst_attr024,
lst_attr024::number(38,18) as lst_attr024,
cnt_attr025::number(38,18) as cnt_attr025,
fst_attr025::number(38,18) as fst_attr025,
lst_attr025::number(38,18) as lst_attr025,
cnt_attr026::number(38,18) as cnt_attr026,
fst_attr026::number(38,18) as fst_attr026,
lst_attr026::number(38,18) as lst_attr026,
cnt_attr027::number(38,18) as cnt_attr027,
fst_attr027::number(38,18) as fst_attr027,
lst_attr027::number(38,18) as lst_attr027,
cnt_attr028::number(38,18) as cnt_attr028,
fst_attr028::number(38,18) as fst_attr028,
lst_attr028::number(38,18) as lst_attr028,
cnt_attr029::number(38,18) as cnt_attr029,
fst_attr029::number(38,18) as fst_attr029,
lst_attr029::number(38,18) as lst_attr029,
cnt_attr030::number(38,18) as cnt_attr030,
fst_attr030::number(38,18) as fst_attr030,
lst_attr030::number(38,18) as lst_attr030,
cnt_attr031::number(38,18) as cnt_attr031,
fst_attr031::number(38,18) as fst_attr031,
lst_attr031::number(38,18) as lst_attr031,
cnt_attr032::number(38,18) as cnt_attr032,
fst_attr032::number(38,18) as fst_attr032,
lst_attr032::number(38,18) as lst_attr032,
cnt_attr033::number(38,18) as cnt_attr033,
fst_attr033::number(38,18) as fst_attr033,
lst_attr033::number(38,18) as lst_attr033,
cnt_attr034::number(38,18) as cnt_attr034,
fst_attr034::number(38,18) as fst_attr034,
lst_attr034::number(38,18) as lst_attr034,
cnt_attr035::number(38,18) as cnt_attr035,
fst_attr035::number(38,18) as fst_attr035,
lst_attr035::number(38,18) as lst_attr035,
cnt_attr036::number(38,18) as cnt_attr036,
fst_attr036::number(38,18) as fst_attr036,
lst_attr036::number(38,18) as lst_attr036,
cnt_attr037::number(38,18) as cnt_attr037,
fst_attr037::number(38,18) as fst_attr037,
lst_attr037::number(38,18) as lst_attr037,
cnt_attr038::number(38,18) as cnt_attr038,
fst_attr038::number(38,18) as fst_attr038,
lst_attr038::number(38,18) as lst_attr038,
cnt_attr039::number(38,18) as cnt_attr039,
fst_attr039::number(38,18) as fst_attr039,
lst_attr039::number(38,18) as lst_attr039,
cnt_attr040::number(38,18) as cnt_attr040,
fst_attr040::number(38,18) as fst_attr040,
lst_attr040::number(38,18) as lst_attr040,
cnt_attr041::number(38,18) as cnt_attr041,
fst_attr041::number(38,18) as fst_attr041,
lst_attr041::number(38,18) as lst_attr041,
cnt_attr042::number(38,18) as cnt_attr042,
fst_attr042::number(38,18) as fst_attr042,
lst_attr042::number(38,18) as lst_attr042,
cnt_attr043::number(38,18) as cnt_attr043,
fst_attr043::number(38,18) as fst_attr043,
lst_attr043::number(38,18) as lst_attr043,
cnt_attr044::number(38,18) as cnt_attr044,
fst_attr044::number(38,18) as fst_attr044,
lst_attr044::number(38,18) as lst_attr044,
cnt_attr045::number(38,18) as cnt_attr045,
fst_attr045::number(38,18) as fst_attr045,
lst_attr045::number(38,18) as lst_attr045,
cnt_attr046::number(38,18) as cnt_attr046,
fst_attr046::number(38,18) as fst_attr046,
lst_attr046::number(38,18) as lst_attr046,
cnt_attr047::number(38,18) as cnt_attr047,
fst_attr047::number(38,18) as fst_attr047,
lst_attr047::number(38,18) as lst_attr047,
current_timestamp()::timestamp_ntz(9) AS inserted_date,
inserted_by::varchar(100) as inserted_by,
current_timestamp()::timestamp_ntz(9) AS updated_date,
updated_by::varchar(100) as updated_by
FROM transformed
)
SELECT *
FROM final