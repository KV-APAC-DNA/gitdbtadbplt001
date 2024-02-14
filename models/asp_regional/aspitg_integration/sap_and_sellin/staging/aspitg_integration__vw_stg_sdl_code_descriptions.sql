--import cte
with bwa_tzpcust as (
    select * from {{ source('bwa_access', 'bwa_tzpcust') }}
),
bwa_tzchannel as (
    select * from {{ source('bwa_access', 'bwa_tzchannel') }}
),
bwa_tzschannel as (
    select * from {{ source('bwa_access', 'bwa_tzschannel') }}
),
bwa_tzbannerf as (
    select * from {{ source('bwa_access', 'bwa_tzbannerf') }}
),
bwa_tzbanner as (
    select * from {{ source('bwa_access', 'bwa_tzbanner') }}
),
bwa_tzgotomod as (
    select * from {{ source('bwa_access', 'bwa_tzgotomod') }}
),
bwa_tzlcus_gr1 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr1') }}
),
bwa_tzlcus_gr2 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr2') }}
),
bwa_tzlcus_gr3 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr3') }}
),
bwa_tzlcus_gr4 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr4') }}
),
bwa_tzlcus_gr5 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr5') }}
),
bwa_tzlcus_gr6 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr6') }}
),
bwa_tzlcus_gr7 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr7') }}
),
bwa_tzlcus_gr8 as (
    select * from {{ source('bwa_access', 'bwa_tzlcus_gr8') }}
),
--Logical CTE

transformed as (
select 'SAP BW' as source_type,
           'Parent Customer Key' as code_type,
           BIC_ZPCUST as Code,
           TXTMD as code_desc
    from BWA_TZPCUST where _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Channel Key' as code_type,
           BIC_ZCHANNEL as Code,
           TXTMD as code_desc
    from BWA_TZCHANNEL where _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Sub Channel Key' as code_type,
           BIC_ZSCHANNEL as Code,
           TXTMD as code_desc
    from BWA_TZSCHANNEL where _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Banner Format Key' as code_type,
           BIC_ZBANNERF as Code,
           TXTMD as code_desc
    from BWA_TZBANNERF where _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Banner Key' as code_type,
           BIC_ZBANNER as Code,
           TXTMD as code_desc
    from BWA_TZBANNER where _deleted_='F'


    union all
   
    select 'SAP BW' as source_type,
           'Go To Model Key' as code_type,
           BIC_ZGOTOMOD as Code,
           TXTMD as code_desc
    from BWA_TZGOTOMOD where _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Local Customer Group 1' as code_type,
           BIC_ZLCUS_GR1 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR1 where LANGU = 'E' and  _deleted_='F'

    union all
   
    select 'SAP BW' as source_type,
           'Local Customer Group 2' as code_type,
           BIC_ZLCUS_GR2 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR2 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 3' as code_type,
           BIC_ZLCUS_GR3 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR3 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 4' as code_type,
           BIC_ZLCUS_GR4 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR4 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 5' as code_type,
           BIC_ZLCUS_GR5 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR5 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 6' as code_type,
           BIC_ZLCUS_GR6 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR6 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 7' as code_type,
           BIC_ZLCUS_GR7 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR7 where LANGU = 'E' and  _deleted_='F'

    union all
   
select 'SAP BW' as source_type,
           'Local Customer Group 8' as code_type,
           BIC_ZLCUS_GR8 as Code,
           TXTLG as code_desc
    from BWA_TZLCUS_GR8 where LANGU = 'E' and  _deleted_='F'

    
),
--Final CTE
final as (
    select 
        source_type,
        code_type,
        code,
        code_desc
    from transformed
)
--Final select
select * from final