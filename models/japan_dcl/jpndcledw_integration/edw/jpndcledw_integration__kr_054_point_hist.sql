

with KR_054_ALLHIST
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_ALLHIST
)

,


TBECORDER 
as
(
    select * from dev_dna_core.jpdclitg_integration.TBECORDER
)

,

KR_054_ALLADM 
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_ALLADM
)

,

KR_054_V_PTRGSTDIVMST
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_V_PTRGSTDIVMST
)

,

TBUSRPRAM 
as
(
    select * from dev_dna_core.jpdclitg_integration.TBUSRPRAM
)

,

KR_054_PFUYO_MEISAI
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_PFUYO_MEISAI
)

,

KR_054_PUSE_MEISAI
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_PUSE_MEISAI
)

,

KR_054_PLOST_MEISAI --snap and og are greater than 19th 
as
(
    select * from dev_dna_core.jpdcledw_integration.KR_054_PLOST_MEISAI
)

,

raw_transformed as(
 SELECT
       HIS.DIECUSRID as DIECUSRID,
       ODR.DIORDERID as DIORDERID,
       CASE WHEN HIS.DIPOINTCODE='2' THEN TO_CHAR(NVL(ODR.DSURIAGEDT,HIS.DSREN),'YYYYMMDD')ELSE TO_CHAR(HIS.DSREN,'YYYYMMDD')END as P_DATE,
       HIS.DIREGISTDIVCODE as DIREGISTDIVCODE,
       NVL(ODR.DIROUTEID,'99') as DIROUTEID,
       HIS.DIPOINTCODE as DIPOINTCODE,
       HIS.DIPOINT as DIPOINT,
       TO_CHAR(ADM.DSPREP,'YYYYMM') as DSPREP,
       MAS.C_DSREGISTDIVNAME as DIVNAME_MAE,
       HIS.c_diPointIssueID as POINT_ID,
       HIS.diPointHistId as HIST_ID
 FROM
       KR_054_ALLHIST HIS
 LEFT JOIN 
    TBECORDER ODR
  ON
       ODR.DIORDERID=HIS.DIORDERID AND 
       ODR.DICANCEL=0 AND 
       ODR.DIELIMFLG=0
 LEFT JOIN 
    KR_054_ALLADM ADM
  ON
       ADM.C_DIPOINTISSUEID=HIS.C_DIPOINTISSUEID AND 
       ADM.DIELIMFLG=0
 INNER JOIN 
   KR_054_V_PTRGSTDIVMST MAS
  ON
       MAS.DIREGISTDIVCODE=ADM.DIREGISTDIVCODE
 INNER JOIN 
    TBUSRPRAM USR
  ON
       HIS.DIECUSRID=USR.DIUSRID
 WHERE   
       1=1 AND  
       USR.DIELIMFLG='0' AND  
       USR.DISECESSIONFLG='0' AND  
       USR.DSDAT93='通常ユーザ' AND  
       HIS.DIELIMFLG=0 AND  
       HIS.DIPOINTCODE<>0 AND  
       HIS.DIVALIDFLG<>0
)

,

union_1 as
(
    select * from raw_transformed where DIVNAME_MAE != 'バースデー特典'
)

,

union_2 
as
(

 SELECT
       HIS.DIECUSRID as DIECUSRID,
       ODR.DIORDERID as DIORDERID,
       FUYO.POINT_YM|| '01' as P_DATE,
       HIS.DIREGISTDIVCODE as DIREGISTDIVCODE,
       NVL(ODR.DIROUTEID,'99') as DIROUTEID,
       HIS.DIPOINTCODE as DIPOINTCODE,
       HIS.DIPOINT as DIPOINT,
       FUYO.POINT_YM as DSPREP,
       MAS.C_DSREGISTDIVNAME as DIVNAME_MAE,
       HIS.c_diPointIssueID as POINT_ID,
       HIS.diPointHistId as HIST_ID
 FROM
       KR_054_ALLHIST HIS
 LEFT JOIN 
    TBECORDER ODR
  ON
       ODR.DIORDERID=HIS.DIORDERID AND 
       ODR.DICANCEL=0 AND 
       ODR.DIELIMFLG=0
 LEFT JOIN 
    KR_054_ALLADM ADM
  ON
       ADM.C_DIPOINTISSUEID=HIS.C_DIPOINTISSUEID AND 
       ADM.DIELIMFLG=0
 INNER JOIN 
    KR_054_V_PTRGSTDIVMST MAS
  ON
       MAS.DIREGISTDIVCODE=ADM.DIREGISTDIVCODE
 INNER JOIN 
    KR_054_PFUYO_MEISAI FUYO
  ON
       HIS.DIECUSRID=FUYO.DIECUSRID AND 
       HIS.DIPOINTHISTID=FUYO.HIST_ID

)

,

union_3
as
(
    SELECT
       HIS.DIECUSRID as DIECUSRID,
       ODR.DIORDERID as DIORDERID,
       TO_CHAR(PUSE.DSREN,'YYYYMMDD') as P_DATE,
       HIS.DIREGISTDIVCODE as DIREGISTDIVCODE,
       NVL(ODR.DIROUTEID,'99') as DIROUTEID,
       HIS.DIPOINTCODE as DIPOINTCODE,
       HIS.DIPOINT as DIPOINT,
       TO_CHAR(PUSE.DSREN,'YYYYMM') as DSPREP,
       MAS.C_DSREGISTDIVNAME as DIVNAME_MAE,
       HIS.c_diPointIssueID as POINT_ID,
       HIS.diPointHistId as HIST_ID
 FROM
       KR_054_ALLHIST HIS
 LEFT JOIN 
    TBECORDER ODR
  ON
       ODR.DIORDERID=HIS.DIORDERID AND 
       ODR.DICANCEL=0 AND 
       ODR.DIELIMFLG=0
 LEFT JOIN 
    KR_054_ALLADM ADM
  ON
       ADM.C_DIPOINTISSUEID=HIS.C_DIPOINTISSUEID AND 
       ADM.DIELIMFLG=0
 INNER JOIN 
    KR_054_V_PTRGSTDIVMST MAS
  ON
       MAS.DIREGISTDIVCODE=ADM.DIREGISTDIVCODE
 INNER JOIN 
    TBUSRPRAM USR
  ON
       HIS.DIECUSRID=USR.DIUSRID
 INNER JOIN 
    KR_054_PUSE_MEISAI PUSE
  ON
       HIS.DIECUSRID=PUSE.DIECUSRID AND 
       HIS.DIPOINTHISTID=PUSE.HIST_ID
)

,

union_4
as
(
     SELECT
       HIS.DIECUSRID as DIECUSRID,
       ODR.DIORDERID as DIORDERID,
       PLOST.LPOINT_LIMIT as P_DATE,
       HIS.DIREGISTDIVCODE as DIREGISTDIVCODE,
       NVL(ODR.DIROUTEID,'99') as DIROUTEID,
       HIS.DIPOINTCODE as DIPOINTCODE,
       HIS.DIPOINT as DIPOINT,
       PLOST.LOST_YM as DSPREP,
       MAS.C_DSREGISTDIVNAME as DIVNAME_MAE,
       HIS.c_diPointIssueID as POINT_ID,
       HIS.diPointHistId as HIST_ID
 FROM
       KR_054_ALLHIST HIS
 LEFT JOIN 
    TBECORDER ODR
  ON
       ODR.DIORDERID=HIS.DIORDERID AND 
       ODR.DICANCEL=0 AND 
       ODR.DIELIMFLG=0
 LEFT JOIN 
    KR_054_ALLADM ADM
  ON
       ADM.C_DIPOINTISSUEID=HIS.C_DIPOINTISSUEID AND 
       ADM.DIELIMFLG=0
 INNER JOIN 
    KR_054_V_PTRGSTDIVMST MAS
  ON
       MAS.DIREGISTDIVCODE=ADM.DIREGISTDIVCODE
 INNER JOIN 
    TBUSRPRAM USR
  ON
       HIS.DIECUSRID=USR.DIUSRID
 INNER JOIN 
    KR_054_PLOST_MEISAI PLOST
  ON
       HIS.DIECUSRID=PLOST.DIECUSRID AND 
       HIS.DIPOINTHISTID=PLOST.HIST_ID
 )

,

transformed 
as
(
    select * from union_1
    union all
    select * from union_2
    union all
    select * from union_3
    union all
    select * from union_4

)
,

final
as
(
    select * from transformed 
)

select * from final

          
