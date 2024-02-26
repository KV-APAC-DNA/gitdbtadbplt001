/*
This model is an exception for now as data from sap coming directly in wks table.
Once the cdl table is avaialable the definition of this model will change.
For now we are hard coding the database and schema name.
*/
--Import CTE
with apc_mara as (
    select * from {{ source('apc_access', 'apc_mara') }} where _deleted_='F'
),
apc_ztranfran as (
    select * from {{ source('apc_access', 'apc_ztranfran') }} where _deleted_='F'
),
apc_ztranbrand as (
    select * from {{ source('apc_access', 'apc_ztranbrand') }} where _deleted_='F'
),
apc_ztransbrand as (
    select * from {{ source('apc_access', 'apc_ztransbrand') }} where _deleted_='F' 
),
apc_ztranvariant as (
    select * from {{ source('apc_access', 'apc_ztranvariant') }} where _deleted_='F'
),
apc_ztransvariant as (
    select * from {{ source('apc_access', 'apc_ztransvariant') }} where _deleted_='F'
),
apc_ztranflav as (
    select * from {{ source('apc_access', 'apc_ztranflav') }} where _deleted_='F'
),
apc_ztraningr as (
    select * from {{ source('apc_access', 'apc_ztraningr') }} where _deleted_='F'
),
apc_ztranappl as (
    select * from {{ source('apc_access', 'apc_ztranappl') }} where _deleted_='F'
),
apc_ztranleng as (
    select * from {{ source('apc_access', 'apc_ztranleng') }} where _deleted_='F'
),
apc_ztranshape as (
    select * from {{ source('apc_access', 'apc_ztranshape') }} where _deleted_='F'
),
apc_ztranspf as (
    select * from {{ source('apc_access', 'apc_ztranspf') }} where _deleted_='F'
),
apc_ztrancover as (
    select * from {{ source('apc_access', 'apc_ztrancover') }} where _deleted_='F'
),
apc_ztranform as (
    select * from {{ source('apc_access', 'apc_ztranform') }} where _deleted_='F'
),
apc_ztransize as (
    select * from {{ source('apc_access', 'apc_ztransize') }}  where _deleted_='F'
),
apc_ztranchar as (
    select * from {{ source('apc_access', 'apc_ztranchar') }} where _deleted_='F'
),
apc_ztranpack as (
    select * from {{ source('apc_access', 'apc_ztranpack') }} where _deleted_='F'
),
apc_ztranattrib13 as (
    select * from {{ source('apc_access', 'apc_ztranattrib13') }} where _deleted_='F' 
),
apc_ztranattrib14 as (
    select * from {{ source('apc_access', 'apc_ztranattrib14') }} where _deleted_='F' 
),
apc_ztransku as (
    select * from {{ source('apc_access', 'apc_ztransku') }} where _deleted_='F'
),
apc_ztranrelabel as (
    select * from {{ source('apc_access', 'apc_ztranrelabel') }} where _deleted_='F'
),
--Logical CTE
final as (
    select 
            mara.mandt,
            mara.matnr,
            mara.ersda,
            mara.ernam,
            IFF(replace(coalesce(mara.LAEDA,''),' ','')::number=0,'',mara.LAEDA) as LAEDA,
            IFF(coalesce(mara.AENAM,'')=' ','',coalesce(mara.AENAM,''))  as AENAM, 
	        IFF(coalesce(mara.VPSTA,'')=' ','',coalesce(mara.VPSTA,''))  as VPSTA, 
	        IFF(coalesce(mara.PSTAT,'')=' ','',coalesce(mara.PSTAT,''))  as PSTAT,
            IFF(coalesce(mara.LVORM,'')=' ','',coalesce(mara.LVORM,'')) as LVORM,
	        IFF(coalesce(mara.MTART,'')=' ','',coalesce(mara.MTART,''))   as MTART,
	        IFF(coalesce(mara.MBRSH,'')=' ','',coalesce(mara.MBRSH,''))   as MBRSH,
	        IFF(coalesce(mara.MATKL,'')=' ','',coalesce(mara.MATKL,''))   as MATKL,
	        IFF(coalesce(mara.BISMT,'')=' ','',coalesce(mara.BISMT,''))   as BISMT,
            IFF(coalesce(mara.MEINS,'')=' ','',coalesce(mara.MEINS,''))   as MEINS,
	        IFF(coalesce(mara.BSTME,'')=' ','',coalesce(mara.BSTME,''))   as BSTME,
            IFF(coalesce(mara.ZEINR,'')=' ','',coalesce(mara.ZEINR,''))   as ZEINR,
            IFF(coalesce(mara.ZEIAR,'')=' ','',coalesce(mara.ZEIAR,'')) as ZEIAR, 
            IFF(coalesce(mara.ZEIVR,'')=' ','',coalesce(mara.ZEIVR,'')) as ZEIVR, 
            IFF(coalesce(mara.ZEIFO,'')=' ','',coalesce(mara.ZEIFO,'')) as ZEIFO, 
            IFF(coalesce(mara.AESZN,'')=' ','',coalesce(mara.AESZN,'')) as AESZN, 
            IFF(coalesce(mara.BLATT,'')=' ','',coalesce(mara.BLATT,'')) as BLATT, 
            IFF(coalesce(mara.BLANZ,'')=' ','',coalesce(mara.BLANZ,'')) as BLANZ, 
            IFF(coalesce(mara.FERTH,'')=' ','',coalesce(mara.FERTH,'')) as FERTH, 
            IFF(coalesce(mara.FORMT,'')=' ','',coalesce(mara.FORMT,'')) as FORMT, 
            IFF(coalesce(mara.GROES,'')=' ','',coalesce(mara.GROES,'')) as GROES, 
            IFF(coalesce(mara.WRKST,'')=' ','',coalesce(mara.WRKST,'')) as WRKST, 
            IFF(coalesce(mara.NORMT,'')=' ','',coalesce(mara.NORMT,'')) as NORMT, 
            IFF(coalesce(mara.LABOR,'')=' ','',coalesce(mara.LABOR,'')) as LABOR, 
            IFF(coalesce(mara.EKWSL,'')=' ','',coalesce(mara.EKWSL,'')) as EKWSL, 
            IFF(coalesce(mara.BRGEW,'')=' ','',coalesce(mara.BRGEW,'')) as BRGEW, 
            IFF(coalesce(mara.NTGEW,'')=' ','',coalesce(mara.NTGEW,'')) as NTGEW, 
            IFF(coalesce(mara.GEWEI,'')=' ','',coalesce(mara.GEWEI,'')) as GEWEI, 
            IFF(coalesce(mara.VOLUM,'')=' ','',coalesce(mara.VOLUM,'')) as VOLUM, 
            IFF(coalesce(mara.VOLEH,'')=' ','',coalesce(mara.VOLEH,'')) as VOLEH, 
            IFF(coalesce(mara.BEHVO,'')=' ','',coalesce(mara.BEHVO,'')) as BEHVO, 
            IFF(coalesce(mara.RAUBE,'')=' ','',coalesce(mara.RAUBE,'')) as RAUBE, 
            IFF(coalesce(mara.TEMPB,'')=' ','',coalesce(mara.TEMPB,'')) as TEMPB, 
            IFF(coalesce(mara.DISST,'')=' ','',coalesce(mara.DISST,'')) as DISST, 
            IFF(coalesce(mara.TRAGR,'')=' ','',coalesce(mara.TRAGR,'')) as TRAGR, 
            IFF(coalesce(mara.STOFF,'')=' ','',coalesce(mara.STOFF,'')) as STOFF, 
            IFF(coalesce(mara.SPART,'')=' ','',coalesce(mara.SPART,'')) as SPART, 
            IFF(coalesce(mara.KUNNR,'')=' ','',coalesce(mara.KUNNR,'')) as KUNNR, 
            IFF(coalesce(mara.EANNR,'')=' ','',coalesce(mara.EANNR,'')) as EANNR, 
            IFF(coalesce(mara.WESCH,'')=' ','',coalesce(mara.WESCH,'')) as WESCH, 
            IFF(coalesce(mara.BWVOR,'')=' ','',coalesce(mara.BWVOR,'')) as BWVOR, 
            IFF(coalesce(mara.BWSCL,'')=' ','',coalesce(mara.BWSCL,'')) as BWSCL, 
            IFF(coalesce(mara.SAISO,'')=' ','',coalesce(mara.SAISO,'')) as SAISO, 
            IFF(coalesce(mara.ETIAR,'')=' ','',coalesce(mara.ETIAR,'')) as ETIAR, 
            IFF(coalesce(mara.ETIFO,'')=' ','',coalesce(mara.ETIFO,'')) as ETIFO, 
            IFF(coalesce(mara.ENTAR,'')=' ','',coalesce(mara.ENTAR,'')) as ENTAR, 
            IFF(coalesce(mara.EAN11,'')=' ','',coalesce(mara.EAN11,'')) as EAN11, 
            IFF(coalesce(mara.NUMTP,'')=' ','',coalesce(mara.NUMTP,'')) as NUMTP, 
            IFF(coalesce(mara.LAENG,'')=' ','',coalesce(mara.LAENG,'')) as LAENG, 
            IFF(coalesce(mara.BREIT,'')=' ','',coalesce(mara.BREIT,'')) as BREIT, 
            IFF(coalesce(mara.HOEHE,'')=' ','',coalesce(mara.HOEHE,'')) as HOEHE, 
            IFF(coalesce(mara.MEABM,'')=' ','',coalesce(mara.MEABM,'')) as MEABM, 
            IFF(coalesce(mara.PRDHA,'')=' ','',coalesce(mara.PRDHA,'')) as PRDHA, 
            IFF(coalesce(mara.AEKLK,'')=' ','',coalesce(mara.AEKLK,'')) as AEKLK, 
            IFF(coalesce(mara.CADKZ,'')=' ','',coalesce(mara.CADKZ,'')) as CADKZ, 
            IFF(coalesce(mara.QMPUR,'')=' ','',coalesce(mara.QMPUR,'')) as QMPUR, 
            IFF(coalesce(mara.ERGEW,'')=' ','',coalesce(mara.ERGEW,'')) as ERGEW, 
            IFF(coalesce(mara.ERGEI,'')=' ','',coalesce(mara.ERGEI,'')) as ERGEI, 
            IFF(coalesce(mara.ERVOL,'')=' ','',coalesce(mara.ERVOL,'')) as ERVOL, 
            IFF(coalesce(mara.ERVOE,'')=' ','',coalesce(mara.ERVOE,'')) as ERVOE, 
            IFF(coalesce(mara.GEWTO,'')=' ','',coalesce(mara.GEWTO,'')) as GEWTO, 
            IFF(coalesce(mara.VOLTO,'')=' ','',coalesce(mara.VOLTO,'')) as VOLTO, 
            IFF(coalesce(mara.VABME,'')=' ','',coalesce(mara.VABME,'')) as VABME, 
            IFF(coalesce(mara.KZREV,'')=' ','',coalesce(mara.KZREV,'')) as KZREV, 
            IFF(coalesce(mara.KZKFG,'')=' ','',coalesce(mara.KZKFG,'')) as KZKFG, 
            IFF(coalesce(mara.XCHPF,'')=' ','',coalesce(mara.XCHPF,'')) as XCHPF, 
            IFF(coalesce(mara.VHART,'')=' ','',coalesce(mara.VHART,'')) as VHART, 
            IFF(coalesce(mara.FUELG,'')=' ','',coalesce(mara.FUELG,'')) as FUELG, 
            IFF(coalesce(mara.STFAK,'')=' ','',coalesce(mara.STFAK,'')) as STFAK, 
            IFF(coalesce(mara.MAGRV,'')=' ','',coalesce(mara.MAGRV,'')) as MAGRV, 
            IFF(coalesce(mara.BEGRU,'')=' ','',coalesce(mara.BEGRU,'')) as BEGRU, 
            IFF(replace(coalesce(mara.DATAB,''),' ','')::number=0,'',mara.DATAB) as DATAB,
            IFF(replace(coalesce(mara.LIQDT,''),' ','')::number=0,'',mara.LIQDT) as LIQDT,
            IFF(coalesce(mara.SAISJ,'')=' ','',coalesce(mara.SAISJ,'')) as SAISJ,
            IFF(coalesce(mara.PLGTP,'')=' ','',coalesce(mara.PLGTP,'')) as PLGTP, 
            IFF(coalesce(mara.MLGUT,'')=' ','',coalesce(mara.MLGUT,'')) as MLGUT, 
            IFF(coalesce(mara.EXTWG,'')=' ','',coalesce(mara.EXTWG,'')) as EXTWG,
            IFF(coalesce(mara.SATNR,'')=' ','',coalesce(mara.SATNR,'')) as SATNR, 
            IFF(coalesce(mara.ATTYP,'')=' ','',coalesce(mara.ATTYP,'')) as ATTYP, 
            IFF(coalesce(mara.KZKUP,'')=' ','',coalesce(mara.KZKUP,'')) as KZKUP, 
            IFF(coalesce(mara.KZNFM,'')=' ','',coalesce(mara.KZNFM,'')) as KZNFM, 
            IFF(coalesce(mara.PMATA,'')=' ','',coalesce(mara.PMATA,'')) as PMATA, 
            IFF(coalesce(mara.MSTAE,'')=' ','',coalesce(mara.MSTAE,'')) as MSTAE, 
            IFF(coalesce(mara.MSTAV,'')=' ','',coalesce(mara.MSTAV,'')) as MSTAV,
            IFF(replace(coalesce(mara.MSTDE,''),' ','')::number=0,'',mara.MSTDE) as MSTDE,
            IFF(replace(coalesce(mara.MSTDV,''),' ','')::number=0,'',mara.MSTDV) as MSTDV,
            IFF(coalesce(mara.TAKLV,'')=' ','',coalesce(mara.TAKLV,'')) as TAKLV,
            IFF(coalesce(mara.RBNRM,'')=' ','',coalesce(mara.RBNRM,'')) as RBNRM,
            IFF(coalesce(mara.MHDRZ,'')=' ','',coalesce(mara.MHDRZ,'')) as MHDRZ,
            IFF(coalesce(mara.MHDHB,'')=' ','',coalesce(mara.MHDHB,'')) as MHDHB,
            IFF(coalesce(mara.MHDLP,'')=' ','',coalesce(mara.MHDLP,'')) as MHDLP,
            IFF(coalesce(mara.INHME,'')=' ','',coalesce(mara.INHME,'')) as INHME,
            IFF(coalesce(mara.INHAL,'')=' ','',coalesce(mara.INHAL,'')) as INHAL,
            IFF(coalesce(mara.VPREH,'')=' ','',coalesce(mara.VPREH,'')) as VPREH,
            IFF(coalesce(mara.ETIAG,'')=' ','',coalesce(mara.ETIAG,'')) as ETIAG,
            IFF(coalesce(mara.INHBR,'')=' ','',coalesce(mara.INHBR,'')) as INHBR,
            IFF(coalesce(mara.CMETH,'')=' ','',coalesce(mara.CMETH,'')) as CMETH,
            IFF(coalesce(mara.CUOBF,'')=' ','',coalesce(mara.CUOBF,'')) as CUOBF,
            IFF(coalesce(mara.KZUMW,'')=' ','',coalesce(mara.KZUMW,'')) as KZUMW,
            IFF(coalesce(mara.KOSCH,'')=' ','',coalesce(mara.KOSCH,'')) as KOSCH,
            IFF(coalesce(mara.SPROF,'')=' ','',coalesce(mara.SPROF,'')) as SPROF,
            IFF(coalesce(mara.NRFHG,'')=' ','',coalesce(mara.NRFHG,'')) as NRFHG,
            IFF(coalesce(mara.MFRPN,'')=' ','',coalesce(mara.MFRPN,'')) as MFRPN,
            IFF(coalesce(mara.MFRNR,'')=' ','',coalesce(mara.MFRNR,'')) as MFRNR,
            IFF(coalesce(mara.BMATN,'')=' ','',coalesce(mara.BMATN,'')) as BMATN,
            IFF(coalesce(mara.MPROF,'')=' ','',coalesce(mara.MPROF,'')) as MPROF,
            IFF(coalesce(mara.KZWSM,'')=' ','',coalesce(mara.KZWSM,'')) as KZWSM,
            IFF(coalesce(mara.SAITY,'')=' ','',coalesce(mara.SAITY,'')) as SAITY,
            IFF(coalesce(mara.PROFL,'')=' ','',coalesce(mara.PROFL,'')) as PROFL,
            IFF(coalesce(mara.IHIVI,'')=' ','',coalesce(mara.IHIVI,'')) as IHIVI,
            IFF(coalesce(mara.ILOOS,'')=' ','',coalesce(mara.ILOOS,'')) as ILOOS,
            IFF(coalesce(mara.SERLV,'')=' ','',coalesce(mara.SERLV,'')) as SERLV,
            IFF(coalesce(mara.KZGVH,'')=' ','',coalesce(mara.KZGVH,'')) as KZGVH,
            IFF(coalesce(mara.XGCHP,'')=' ','',coalesce(mara.XGCHP,'')) as XGCHP,
            IFF(coalesce(mara.KZEFF,'')=' ','',coalesce(mara.KZEFF,'')) as KZEFF,
            IFF(coalesce(mara.COMPL,'')=' ','',coalesce(mara.COMPL,'')) as COMPL,
            IFF(coalesce(mara.IPRKZ,'')=' ','',coalesce(mara.IPRKZ,'')) as IPRKZ,
            IFF(coalesce(mara.RDMHD,'')=' ','',coalesce(mara.RDMHD,'')) as RDMHD,
            IFF(coalesce(mara.PRZUS,'')=' ','',coalesce(mara.PRZUS,'')) as PRZUS,
            IFF(coalesce(mara.MTPOS_MARA,'')=' ','',coalesce(mara.MTPOS_MARA,'')) as MTPOS_MARA,
            IFF(coalesce(mara.BFLME,'')=' ','',coalesce(mara.BFLME,'')) as BFLME,
            IFF(coalesce(mara.ZZ_MVGR4,'')=' ','',coalesce(mara.ZZ_MVGR4,'')) as ZZ_MVGR4,
            IFF(coalesce(mara.ZZ_MVGR5,'')=' ','',coalesce(mara.ZZ_MVGR5,'')) as ZZ_MVGR5,
            IFF(coalesce(mara.ZZ_MVGR1,'')=' ','',coalesce(mara.ZZ_MVGR1,'')) as ZZ_MVGR1,
            IFF(coalesce(mara.ZZ_MVGR2,'')=' ','',coalesce(mara.ZZ_MVGR2,'')) as ZZ_MVGR2,
            IFF(coalesce(mara.ZZ_MVGR3,'')=' ','',coalesce(mara.ZZ_MVGR3,'')) as ZZ_MVGR3,
            IFF(coalesce(mara.ZZ_TECHNOLOGY,'')=' ','',coalesce(mara.ZZ_TECHNOLOGY,'')) as ZZ_TECHNOLOGY,
            IFF(coalesce(mara.ZZ_COLOR,'')=' ','',coalesce(mara.ZZ_COLOR,'')) as ZZ_COLOR,
            IFF(coalesce(mara.ZZ_STACK_FACTOR,'')=' ','',coalesce(mara.ZZ_STACK_FACTOR,'')) as ZZ_STACK_FACTOR,
            IFF(coalesce(mara.ZZ_MFGSOURCE,'')=' ','',coalesce(mara.ZZ_MFGSOURCE,'')) as ZZ_MFGSOURCE,
            IFF(coalesce(mara.ZZ_FORNUM,'')=' ','',coalesce(mara.ZZ_FORNUM,'')) as ZZ_FORNUM,
            IFF(coalesce(mara.ZZ_FRAN,'')=' ','',coalesce(mara.ZZ_FRAN,'')) as ZZ_FRAN,
            IFF(coalesce(mara.ZZ_BRAND,'')=' ','',coalesce(mara.ZZ_BRAND,'')) as ZZ_BRAND,
            IFF(coalesce(mara.ZZ_SBRAND,'')=' ','',coalesce(mara.ZZ_SBRAND,'')) as ZZ_SBRAND,
            IFF(coalesce(mara.ZZ_VARIANT,'')=' ','',coalesce(mara.ZZ_VARIANT,'')) as ZZ_VARIANT,
            IFF(coalesce(mara.ZZ_SVARIANT,'')=' ','',coalesce(mara.ZZ_SVARIANT,'')) as ZZ_SVARIANT,
            IFF(coalesce(mara.ZZ_FLAV,'')=' ','',coalesce(mara.ZZ_FLAV,'')) as ZZ_FLAV,
            IFF(coalesce(mara.ZZ_INGR,'')=' ','',coalesce(mara.ZZ_INGR,'')) as ZZ_INGR,
            IFF(coalesce(mara.ZZ_APPL,'')=' ','',coalesce(mara.ZZ_APPL,'')) as ZZ_APPL,
            IFF(coalesce(mara.ZZ_LENG,'')=' ','',coalesce(mara.ZZ_LENG,'')) as ZZ_LENG,
            IFF(coalesce(mara.ZZ_SHAPE,'')=' ','',coalesce(mara.ZZ_SHAPE,'')) as ZZ_SHAPE,
            IFF(coalesce(mara.ZZ_SPF,'')=' ','',coalesce(mara.ZZ_SPF,'')) as ZZ_SPF,
            IFF(coalesce(mara.ZZ_COVER,'')=' ','',coalesce(mara.ZZ_COVER,'')) as ZZ_COVER,
            IFF(coalesce(mara.ZZ_FORM,'')=' ','',coalesce(mara.ZZ_FORM,'')) as ZZ_FORM,
            IFF(coalesce(mara.ZZ_SIZE,'')=' ','',coalesce(mara.ZZ_SIZE,'')) as ZZ_SIZE,
            IFF(coalesce(mara.ZZ_CHAR,'')=' ','',coalesce(mara.ZZ_CHAR,'')) as ZZ_CHAR,
            IFF(coalesce(mara.ZZ_PACK,'')=' ','',coalesce(mara.ZZ_PACK,'')) as ZZ_PACK,
            IFF(coalesce(mara.ZZ_ATTRIB13,'')=' ','',coalesce(mara.ZZ_ATTRIB13,'')) as ZZ_ATTRIB13,
            IFF(coalesce(mara.ZZ_ATTRIB14,'')=' ','',coalesce(mara.ZZ_ATTRIB14,'')) as ZZ_ATTRIB14,
            IFF(coalesce(mara.ZZ_SKU,'')=' ','',coalesce(mara.ZZ_SKU,'')) as ZZ_SKU,
            IFF(coalesce(mara.ZZ_RELABEL,'')=' ','',coalesce(mara.ZZ_RELABEL,'')) as ZZ_RELABEL,
            IFF(coalesce(mara.ZZ_PRODKEY,'')=' ','',coalesce(mara.ZZ_PRODKEY,'')) as ZZ_PRODKEY,
            IFF(coalesce(mara.zz_rootcode,'')=' ','',coalesce(mara.zz_rootcode,'')) as zz_rootcode,
            IFF(coalesce(mara.zz_rootdesc1,'')=' ','',coalesce(mara.zz_rootdesc1,'')) as zz_rootdesc1,
            IFF(coalesce(mara.zz_rootdesc2,'')=' ','',coalesce(mara.zz_rootdesc2,'')) as zz_rootdesc2,
            IFF(coalesce(mara.zz_proddesc1,'')=' ','',coalesce(mara.zz_proddesc1,'')) as zz_proddesc1,
            IFF(coalesce(mara.zz_proddesc2,'')=' ','',coalesce(mara.zz_proddesc2,'')) as zz_proddesc2,
            IFF(coalesce(ztranfran.zfrandesc,'')=' ','',coalesce(ztranfran.zfrandesc,'')) as ztranfran_zfrandesc,
            IFF(coalesce(ztranfran.flag,'')=' ','',coalesce(ztranfran.flag,'')) as ztranfran_flag,
            IFF(coalesce(ztranfran.dflag,'')=' ','',coalesce(ztranfran.dflag,'')) as ztranfran_dflag,
            IFF(coalesce(ztranfran.crdat,'')=' ','',coalesce(ztranfran.crdat,'')) as ztranfran_crdat,
            IFF(coalesce(ztranfran.crname,'')=' ','',coalesce(ztranfran.crname,'')) as ztranfran_crname,
            IFF(coalesce(ztranfran.aedtm,'')=' ','',coalesce(ztranfran.aedtm,'')) as ztranfran_aedtm,
            IFF(coalesce(ztranfran.uname,'')=' ','',coalesce(ztranfran.uname,'')) as ztranfran_uname,
            IFF(coalesce(ztranbrand.zbrandesc,'')=' ','',coalesce(ztranbrand.zbrandesc,'')) as ztranbrand_zbrandesc,
            IFF(coalesce(ztranbrand.flag,'')=' ','',coalesce(ztranbrand.flag,'')) as ztranbrand_flag,
            IFF(coalesce(ztranbrand.dflag,'')=' ','',coalesce(ztranbrand.dflag,'')) as ztranbrand_dflag,
            IFF(coalesce(ztranbrand.crdat,'')=' ','',coalesce(ztranbrand.crdat,'')) as ztranbrand_crdat,
            IFF(coalesce(ztranbrand.crname,'')=' ','',coalesce(ztranbrand.crname,'')) as ztranbrand_crname,
            IFF(coalesce(ztranbrand.aedtm,'')=' ','',coalesce(ztranbrand.aedtm,'')) as ztranbrand_aedtm,
            IFF(coalesce(ztranbrand.uname,'')=' ','',coalesce(ztranbrand.uname,'')) as ztranbrand_uname,
            IFF(coalesce(ztransbrand.zsbrandesc,'')=' ','',coalesce(ztransbrand.zsbrandesc,'')) as ztransbrand_zsbrandesc,
            IFF(coalesce(ztransbrand.flag,'')=' ','',coalesce(ztransbrand.flag,'')) as ztransbrand_flag,
            IFF(coalesce(ztransbrand.dflag,'')=' ','',coalesce(ztransbrand.dflag,'')) as ztransbrand_dflag,
            IFF(coalesce(ztransbrand.crdat,'')=' ','',coalesce(ztransbrand.crdat,'')) as ztransbrand_crdat,
            IFF(coalesce(ztransbrand.crname,'')=' ','',coalesce(ztransbrand.crname,'')) as ztransbrand_crname,
            IFF(coalesce(ztransbrand.aedtm,'')=' ','',coalesce(ztransbrand.aedtm,'')) as ztransbrand_aedtm,
            IFF(coalesce(ztransbrand.uname,'')=' ','',coalesce(ztransbrand.uname,'')) as ztransbrand_uname,
            IFF(coalesce(ztranvariant.zvardesc,'')=' ','',coalesce(ztranvariant.zvardesc,'')) as ztranvariant_zvardesc,
            IFF(coalesce(ztranvariant.flag,'')=' ','',coalesce(ztranvariant.flag,'')) as ztranvariant_flag,
            IFF(coalesce(ztranvariant.dflag,'')=' ','',coalesce(ztranvariant.dflag,'')) as ztranvariant_dflag,
            IFF(coalesce(ztranvariant.crdat,'')=' ','',coalesce(ztranvariant.crdat,'')) as ztranvariant_crdat,
            IFF(coalesce(ztranvariant.crname,'')=' ','',coalesce(ztranvariant.crname,'')) as ztranvariant_crname,
            IFF(coalesce(ztranvariant.aedtm,'')=' ','',coalesce(ztranvariant.aedtm,'')) as ztranvariant_aedtm,
            IFF(coalesce(ztranvariant.uname,'')=' ','',coalesce(ztranvariant.uname,'')) as ztranvariant_uname,
            IFF(coalesce(ztransvariant.zsvardesc,'')=' ','',coalesce(ztransvariant.zsvardesc,'')) as ztransvariant_zsvardesc,
            IFF(coalesce(ztransvariant.flag,'')=' ','',coalesce(ztransvariant.flag,'')) as ztransvariant_flag,
            IFF(coalesce(ztransvariant.dflag,'')=' ','',coalesce(ztransvariant.dflag,'')) as ztransvariant_dflag,
            IFF(coalesce(ztransvariant.crdat,'')=' ','',coalesce(ztransvariant.crdat,'')) as ztransvariant_crdat,
            IFF(coalesce(ztransvariant.crname,'')=' ','',coalesce(ztransvariant.crname,'')) as ztransvariant_crname,
            IFF(coalesce(ztransvariant.aedtm,'')=' ','',coalesce(ztransvariant.aedtm,'')) as ztransvariant_aedtm,
            IFF(coalesce(ztransvariant.uname,'')=' ','',coalesce(ztransvariant.uname,'')) as ztransvariant_uname,
            IFF(coalesce(ztranflav.zflavdesc,'')=' ','',coalesce(ztranflav.zflavdesc,'')) as ztranflav_zflavdesc,
            IFF(coalesce(ztranflav.flag,'')=' ','',coalesce(ztranflav.flag,'')) as ztranflav_flag,
            IFF(coalesce(ztranflav.dflag,'')=' ','',coalesce(ztranflav.dflag,'')) as ztranflav_dflag,
            IFF(coalesce(ztranflav.crdat,'')=' ','',coalesce(ztranflav.crdat,'')) as ztranflav_crdat,
            IFF(coalesce(ztranflav.crname,'')=' ','',coalesce(ztranflav.crname,'')) as ztranflav_crname,
            IFF(coalesce(ztranflav.aedtm,'')=' ','',coalesce(ztranflav.aedtm,'')) as ztranflav_aedtm,
            IFF(coalesce(ztranflav.uname,'')=' ','',coalesce(ztranflav.uname,'')) as ztranflav_uname,
            IFF(coalesce(ztraningr.zingrdesc,'')=' ','',coalesce(ztraningr.zingrdesc,'')) as ztraningr_zingrdesc,
            IFF(coalesce(ztraningr.flag,'')=' ','',coalesce(ztraningr.flag,'')) as ztraningr_flag,
            IFF(coalesce(ztraningr.dflag,'')=' ','',coalesce(ztraningr.dflag,'')) as ztraningr_dflag,
            IFF(coalesce(ztraningr.crdat,'')=' ','',coalesce(ztraningr.crdat,'')) as ztraningr_crdat,
            IFF(coalesce(ztraningr.crname,'')=' ','',coalesce(ztraningr.crname,'')) as ztraningr_crname,
            IFF(coalesce(ztraningr.aedtm,'')=' ','',coalesce(ztraningr.aedtm,'')) as ztraningr_aedtm,
            IFF(coalesce(ztraningr.uname,'')=' ','',coalesce(ztraningr.uname,'')) as ztraningr_uname,
            IFF(coalesce(ztranappl.zappldesc,'')=' ','',coalesce(ztranappl.zappldesc,'')) as ztranappl_zappldesc,
            IFF(coalesce(ztranappl.flag,'')=' ','',coalesce(ztranappl.flag,'')) as ztranappl_flag,
            IFF(coalesce(ztranappl.dflag,'')=' ','',coalesce(ztranappl.dflag,'')) as ztranappl_dflag,
            IFF(coalesce(ztranappl.crdat,'')=' ','',coalesce(ztranappl.crdat,'')) as ztranappl_crdat,
            IFF(coalesce(ztranappl.crname,'')=' ','',coalesce(ztranappl.crname,'')) as ztranappl_crname,
            IFF(coalesce(ztranappl.aedtm,'')=' ','',coalesce(ztranappl.aedtm,'')) as ztranappl_aedtm,
            IFF(coalesce(ztranappl.uname,'')=' ','',coalesce(ztranappl.uname,'')) as ztranappl_uname,
            IFF(coalesce(ztranleng.zlengdesc,'')=' ','',coalesce(ztranleng.zlengdesc,'')) as ztranleng_zlengdesc,
            IFF(coalesce(ztranleng.flag,'')=' ','',coalesce(ztranleng.flag,'')) as ztranleng_flag,
            IFF(coalesce(ztranleng.dflag,'')=' ','',coalesce(ztranleng.dflag,'')) as ztranleng_dflag,
            IFF(coalesce(ztranleng.crdat,'')=' ','',coalesce(ztranleng.crdat,'')) as ztranleng_crdat,
            IFF(coalesce(ztranleng.crname,'')=' ','',coalesce(ztranleng.crname,'')) as ztranleng_crname,
            IFF(coalesce(ztranleng.aedtm,'')=' ','',coalesce(ztranleng.aedtm,'')) as ztranleng_aedtm,
            IFF(coalesce(ztranleng.uname,'')=' ','',coalesce(ztranleng.uname,'')) as ztranleng_uname,
            IFF(coalesce(ztranshape.zzshapedesc,'')=' ','',coalesce(ztranshape.zzshapedesc,'')) as ztranshape_zzshapedesc,
            IFF(coalesce(ztranshape.flag,'')=' ','',coalesce(ztranshape.flag,'')) as ztranshape_flag,
            IFF(coalesce(ztranshape.dflag,'')=' ','',coalesce(ztranshape.dflag,'')) as ztranshape_dflag,
            IFF(coalesce(ztranshape.crdat,'')=' ','',coalesce(ztranshape.crdat,'')) as ztranshape_crdat,
            IFF(coalesce(ztranshape.crname,'')=' ','',coalesce(ztranshape.crname,'')) as ztranshape_crname,
            IFF(coalesce(ztranshape.aedtm,'')=' ','',coalesce(ztranshape.aedtm,'')) as ztranshape_aedtm,
            IFF(coalesce(ztranshape.uname,'')=' ','',coalesce(ztranshape.uname,'')) as ztranshape_uname,
            IFF(coalesce(ztranspf.zzspfdesc,'')=' ','',coalesce(ztranspf.zzspfdesc,'')) as ztranspf_zzspfdesc,
            IFF(coalesce(ztranspf.flag,'')=' ','',coalesce(ztranspf.flag,'')) as ztranspf_flag,
            IFF(coalesce(ztranspf.dflag,'')=' ','',coalesce(ztranspf.dflag,'')) as ztranspf_dflag,
            IFF(coalesce(ztranspf.crdat,'')=' ','',coalesce(ztranspf.crdat,'')) as ztranspf_crdat,
            IFF(coalesce(ztranspf.crname,'')=' ','',coalesce(ztranspf.crname,'')) as ztranspf_crname,
            IFF(coalesce(ztranspf.aedtm,'')=' ','',coalesce(ztranspf.aedtm,'')) as ztranspf_aedtm,
            IFF(coalesce(ztranspf.uname,'')=' ','',coalesce(ztranspf.uname,'')) as ztranspf_uname,
            IFF(coalesce(ztrancover.zcoverdesc,'')=' ','',coalesce(ztrancover.zcoverdesc,'')) as ztrancover_zcoverdesc,
            IFF(coalesce(ztrancover.flag,'')=' ','',coalesce(ztrancover.flag,'')) as ztrancover_flag,
            IFF(coalesce(ztrancover.dflag,'')=' ','',coalesce(ztrancover.dflag,'')) as ztrancover_dflag,
            IFF(coalesce(ztrancover.crdat,'')=' ','',coalesce(ztrancover.crdat,'')) as ztrancover_crdat,
            IFF(coalesce(ztrancover.crname,'')=' ','',coalesce(ztrancover.crname,'')) as ztrancover_crname,
            IFF(coalesce(ztrancover.aedtm,'')=' ','',coalesce(ztrancover.aedtm,'')) as ztrancover_aedtm,
            IFF(coalesce(ztrancover.uname,'')=' ','',coalesce(ztrancover.uname,'')) as ztrancover_uname,
            IFF(coalesce(ztranform.zzformdesc,'')=' ','',coalesce(ztranform.zzformdesc,'')) as ztranform_zzformdesc,
            IFF(coalesce(ztranform.flag,'')=' ','',coalesce(ztranform.flag,'')) as ztranform_flag,
            IFF(coalesce(ztranform.dflag,'')=' ','',coalesce(ztranform.dflag,'')) as ztranform_dflag,
            IFF(coalesce(ztranform.crdat,'')=' ','',coalesce(ztranform.crdat,'')) as ztranform_crdat,
            IFF(coalesce(ztranform.crname,'')=' ','',coalesce(ztranform.crname,'')) as ztranform_crname,
            IFF(coalesce(ztranform.aedtm,'')=' ','',coalesce(ztranform.aedtm,'')) as ztranform_aedtm,
            IFF(coalesce(ztranform.uname,'')=' ','',coalesce(ztranform.uname,'')) as ztranform_uname,
            IFF(coalesce(ztransize.zsizedesc,'')=' ','',coalesce(ztransize.zsizedesc,'')) as ztransize_zsizedesc,
            IFF(coalesce(ztransize.flag,'')=' ','',coalesce(ztransize.flag,'')) as ztransize_flag,
            IFF(coalesce(ztransize.dflag,'')=' ','',coalesce(ztransize.dflag,'')) as ztransize_dflag,
            IFF(coalesce(ztransize.crdat,'')=' ','',coalesce(ztransize.crdat,'')) as ztransize_crdat,
            IFF(coalesce(ztransize.crname,'')=' ','',coalesce(ztransize.crname,'')) as ztransize_crname,
            IFF(coalesce(ztransize.aedtm,'')=' ','',coalesce(ztransize.aedtm,'')) as ztransize_aedtm,
            IFF(coalesce(ztransize.uname,'')=' ','',coalesce(ztransize.uname,'')) as ztransize_uname,
            IFF(coalesce(ztranchar.zchardesc,'')=' ','',coalesce(ztranchar.zchardesc,'')) as ztranchar_zchardesc,
            IFF(coalesce(ztranchar.flag,'')=' ','',coalesce(ztranchar.flag,'')) as ztranchar_flag,
            IFF(coalesce(ztranchar.dflag,'')=' ','',coalesce(ztranchar.dflag,'')) as ztranchar_dflag,
            IFF(coalesce(ztranchar.crdat,'')=' ','',coalesce(ztranchar.crdat,'')) as ztranchar_crdat,
            IFF(coalesce(ztranchar.crname,'')=' ','',coalesce(ztranchar.crname,'')) as ztranchar_crname,
            IFF(coalesce(ztranchar.aedtm,'')=' ','',coalesce(ztranchar.aedtm,'')) as ztranchar_aedtm,
            IFF(coalesce(ztranchar.uname,'')=' ','',coalesce(ztranchar.uname,'')) as ztranchar_uname,
            IFF(coalesce(ztranpack.zpackdesc,'')=' ','',coalesce(ztranpack.zpackdesc,'')) as ztranpack_zpackdesc,
            IFF(coalesce(ztranpack.flag,'')=' ','',coalesce(ztranpack.flag,'')) as ztranpack_flag,
            IFF(coalesce(ztranpack.dflag,'')=' ','',coalesce(ztranpack.dflag,'')) as ztranpack_dflag,
            IFF(coalesce(ztranpack.crdat,'')=' ','',coalesce(ztranpack.crdat,'')) as ztranpack_crdat,
            IFF(coalesce(ztranpack.crname,'')=' ','',coalesce(ztranpack.crname,'')) as ztranpack_crname,
            IFF(coalesce(ztranpack.aedtm,'')=' ','',coalesce(ztranpack.aedtm,'')) as ztranpack_aedtm,
            IFF(coalesce(ztranpack.uname,'')=' ','',coalesce(ztranpack.uname,'')) as ztranpack_uname,
            IFF(coalesce(ztranattrib13.zattrib13desc,'')=' ','',coalesce(ztranattrib13.zattrib13desc,'')) as ztranattrib13_zattrib13desc,
            IFF(coalesce(ztranattrib13.flag,'')=' ','',coalesce(ztranattrib13.flag,'')) as ztranattrib13_flag,
            IFF(coalesce(ztranattrib13.dflag,'')=' ','',coalesce(ztranattrib13.dflag,'')) as ztranattrib13_dflag,
            IFF(coalesce(ztranattrib13.crdat,'')=' ','',coalesce(ztranattrib13.crdat,'')) as ztranattrib13_crdat,
            IFF(coalesce(ztranattrib13.crname,'')=' ','',coalesce(ztranattrib13.crname,'')) as ztranattrib13_crname,
            IFF(coalesce(ztranattrib13.aedtm,'')=' ','',coalesce(ztranattrib13.aedtm,'')) as ztranattrib13_aedtm,
            IFF(coalesce(ztranattrib13.uname,'')=' ','',coalesce(ztranattrib13.uname,'')) as ztranattrib13_uname,
            IFF(coalesce(ztranattrib14.zattrib14desc,'')=' ','',coalesce(ztranattrib14.zattrib14desc,'')) as ztranattrib14_zattrib14desc,
            IFF(coalesce(ztranattrib14.flag,'')=' ','',coalesce(ztranattrib14.flag,'')) as ztranattrib14_flag,
            IFF(coalesce(ztranattrib14.dflag,'')=' ','',coalesce(ztranattrib14.dflag,'')) as ztranattrib14_dflag,
            IFF(coalesce(ztranattrib14.crdat,'')=' ','',coalesce(ztranattrib14.crdat,'')) as ztranattrib14_crdat,
            IFF(coalesce(ztranattrib14.crname,'')=' ','',coalesce(ztranattrib14.crname,'')) as ztranattrib14_crname,
            IFF(coalesce(ztranattrib14.aedtm,'')=' ','',coalesce(ztranattrib14.aedtm,'')) as ztranattrib14_aedtm,
            IFF(coalesce(ztranattrib14.uname,'')=' ','',coalesce(ztranattrib14.uname,'')) as ztranattrib14_uname,
            IFF(coalesce(ztransku.zskudesc,'')=' ','',coalesce(ztransku.zskudesc,'')) as ztransku_zskudesc,
            IFF(coalesce(ztransku.flag,'')=' ','',coalesce(ztransku.flag,'')) as ztransku_flag,
            IFF(coalesce(ztransku.dflag,'')=' ','',coalesce(ztransku.dflag,'')) as ztransku_dflag,
            IFF(coalesce(ztransku.crdat,'')=' ','',coalesce(ztransku.crdat,'')) as ztransku_crdat,
            IFF(coalesce(ztransku.crname,'')=' ','',coalesce(ztransku.crname,'')) as ztransku_crname,
            IFF(coalesce(ztransku.aedtm,'')=' ','',coalesce(ztransku.aedtm,'')) as ztransku_aedtm,
            IFF(coalesce(ztransku.uname,'')=' ','',coalesce(ztransku.uname,'')) as ztransku_uname,
            IFF(coalesce(ztranrelabel.zrelabeldesc,'')=' ','',coalesce(ztranrelabel.zrelabeldesc,'')) as ztranrelabel_zrelabeldesc,
            IFF(coalesce(ztranrelabel.flag,'')=' ','',coalesce(ztranrelabel.flag,'')) as ztranrelabel_flag,
            IFF(coalesce(ztranrelabel.dflag,'')=' ','',coalesce(ztranrelabel.dflag,'')) as ztranrelabel_dflag,
            IFF(coalesce(ztranrelabel.crdat,'')=' ','',coalesce(ztranrelabel.crdat,'')) as ztranrelabel_crdat,
            IFF(coalesce(ztranrelabel.crname,'')=' ','',coalesce(ztranrelabel.crname,'')) as ztranrelabel_crname,
            IFF(coalesce(ztranrelabel.aedtm,'')=' ','',coalesce(ztranrelabel.aedtm,'')) as ztranrelabel_aedtm,
            IFF(coalesce(ztranrelabel.uname,'')=' ','',coalesce(ztranrelabel.uname,'')) as ztranrelabel_uname,
            current_timestamp()::timestamp_ntz(9) as crt_dttm,
            current_timestamp()::timestamp_ntz(9) as updt_dttm
    from apc_mara as mara
            left join apc_ztranfran as ztranfran on mara.mandt = ztranfran.mandt
            and ztranfran.zfran = mara.zz_fran
            left join apc_ztranbrand as ztranbrand on mara.mandt = ztranbrand.mandt
            and ztranbrand.zbrand = mara.zz_brand
            left join apc_ztransbrand as ztransbrand on mara.mandt = ztransbrand.mandt
            and ztransbrand.zsbrand = mara.zz_sbrand
            left join apc_ztranvariant as ztranvariant on mara.mandt = ztranvariant.mandt
            and ztranvariant.zvariant = mara.zz_variant
            left join apc_ztransvariant as ztransvariant on mara.mandt = ztransvariant.mandt
            and ztransvariant.zsvariant = mara.zz_svariant
            left join apc_ztranflav as ztranflav on mara.mandt = ztranflav.mandt
            and ztranflav.zflav = mara.zz_flav
            left join apc_ztraningr as ztraningr on mara.mandt = ztraningr.mandt
            and ztraningr.zingr = mara.zz_ingr
            left join apc_ztranappl as ztranappl on mara.mandt = ztranappl.mandt
            and ztranappl.zappl = mara.zz_appl
            left join apc_ztranleng as ztranleng on mara.mandt = ztranleng.mandt
            and ztranleng.zleng = mara.zz_leng
            left join apc_ztranshape as ztranshape on mara.mandt = ztranshape.mandt
            and ztranshape.zzshape = mara.zz_shape
            left join apc_ztranspf as ztranspf on mara.mandt = ztranspf.mandt
            and ztranspf.zzspf = mara.zz_spf
            left join apc_ztrancover as ztrancover on mara.mandt = ztrancover.mandt
            and ztrancover.zcover = mara.zz_cover
            left join apc_ztranform as ztranform on mara.mandt = ztranform.mandt
            and ztranform.zzform = mara.zz_form
            left join apc_ztransize as ztransize on mara.mandt = ztransize.mandt
            and ztransize.zsize = mara.zz_size
            left join apc_ztranchar as ztranchar on mara.mandt = ztranchar.mandt
            and ztranchar.zchar = mara.zz_char
            left join apc_ztranpack as ztranpack on mara.mandt = ztranpack.mandt
            and ztranpack.zpack = mara.zz_pack
            left join apc_ztranattrib13 as ztranattrib13 on mara.mandt = ztranattrib13.mandt
            and ztranattrib13.zattrib13 = mara.zz_attrib13
            left join apc_ztranattrib14 as ztranattrib14 on mara.mandt = ztranattrib14.mandt
            and ztranattrib14.zattrib14 = mara.zz_attrib14
            left join apc_ztransku as ztransku on mara.mandt = ztransku.mandt
            and ztransku.zsku = mara.zz_sku
            left join apc_ztranrelabel as ztranrelabel on mara.mandt = ztranrelabel.mandt
            and ztranrelabel.zrelabel = mara.zz_relabel
)

--Final select
select * from final