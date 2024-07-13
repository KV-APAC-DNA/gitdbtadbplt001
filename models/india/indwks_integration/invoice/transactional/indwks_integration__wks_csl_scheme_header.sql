with 
sdl_csl_scheme_header as 
(
    select * from {{ source('indsdl_raw', 'sdl_csl_scheme_header') }}
),
final as 
(
    select 
        schid,
        schcode,
        schdsc,
        claimable,
        clmamton,
        cmpschcode,
        schlevel_id,
        schtype,
        flexisch,
        flexischtype,
        combisch,
        range,
        prorata,
        qps,
        qpsreset,
        schvalidfrom,
        schvalidtill,
        schstatus,
        budget,
        adjwindisponlyonce,
        purofevery,
        apyqpssch,
        setwindowdisp,
        editscheme,
        schlvlmode,
        createduserid,
        createddate,
        modifiedby,
        modifieddate,
        versionno,
        serialno,
        claimgrpcode,
        fbm,
        combitype,
        allowuncheck,
        settlementtype,
        consumerpromo,
        wdsbillscount,
        wdscapamount,
        wdsminpurqty,
    from sdl_csl_scheme_header
)
select * from final