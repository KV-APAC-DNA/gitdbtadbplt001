with itg_hcp360_sfmc_hcp_details as 
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_sfmc_hcp_details') }}
),
itg_hcp360_in_ventasys_hcp_master as
(
    select * from {{ ref('hcpitg_integration__itg_hcp360_in_ventasys_hcp_master') }}
),
block1 as
(
    ---ventasys and sfmc mobile match check ------
    select team_name as brand,md5(team_name||mobile_number||nvl(sfmc.email,nvl(ventasys.ventasys_email,'xx')) ) as master_hcp_key, mobile_number, 
    nvl(sfmc.email,ventasys.ventasys_email) as email, null, team_name, v_custid, subscriber_key 
    from
        (
        select cell_phone, 'Ventasys' as source, team_name, v_custid ,lower(email) as ventasys_email
        from itg_hcp360_in_ventasys_hcp_master 
        where cell_phone <> '0' 
            and length(cell_phone) = 10
            and length(cell_phone) = 10
            and team_name = 'ORSL' 
            and is_active ='Y'
            and cell_phone not in (select cell_phone 
                            from itg_hcp360_in_ventasys_hcp_master 
                            where cell_phone <> '0' 
                                and length(cell_phone) = 10
                                and team_name = 'ORSL' 
                                and is_active ='Y'
                            group by cell_phone
                            having count(*) > 1 )
        ) Ventasys,
        (select cast(mobile_number as varchar) as mobile_number , 'sfmc' as source, subscriber_key , lower(email) as email
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'ORSL' 
        and  mobile_number is not null and length(mobile_number) = 10   
        ) sfmc
    where Ventasys.cell_phone = sfmc.mobile_number
),
block2 as
(
     ---ventasys and sfmc email match check-----
    select team_name as brand, md5(team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||nvl(lower(sfmc.email),'xx')) as master_hcp_key,  
    nvl(sfmc.mobile_number,ventasys.cell_phone) as mobile_phone, lower(sfmc.email) as email ,null, team_name, v_custid, subscriber_key
    from
        (
        select lower(email) as email, 'Ventasys' as source, team_name, v_custid, cell_phone --,email
        from itg_hcp360_in_ventasys_hcp_master 
        where email is not null
            and team_name = 'ORSL' 
            and is_active ='Y' 
            and lower(email) not in (
                select lower(email) 
                    from itg_hcp360_in_ventasys_hcp_master 
                where email is not null
                    and team_name = 'ORSL' 
                    and is_active ='Y'
                group by lower(email)
                having count(*) > 1
                    )
            and not exists (select lower(email) from block1 
                                    where brand ='ORSL'
                                        and lower(email) = nvl(email,'xx') )          
        ) Ventasys,
        (select lower(email) as email, 'sfmc' as source, subscriber_key ,mobile_number
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'ORSL'  
            and  email is not null 
            and  not exists (select lower(email) 
                                    from block1 t 
                                    where brand = 'ORSL' 
                                    and  subscriber_key is not null
                                    and nvl(s.mobile_number,'x') = nvl(t.mobile_number,'x')
                                    and lower(s.email)           = nvl(lower(t.email),'xx')
                                )      
        ) sfmc
        where Ventasys.email = sfmc.email 
        and not exists ( select  1
                            from block1 
                        where  md5(ventasys.team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||sfmc.email) =  master_hcp_key )
),
block1_2 as
(
    select * from block1
    UNION ALL
    select * from block2
),
block3 as
(
    -----------------JBABY--------------ventasys and sfmc mobile match check JB-----
    select team_name as brand, md5(team_name||mobile_number||nvl(sfmc.email,nvl(ventasys.ventasys_email,'xx')) ) as master_hcp_key, mobile_number, 
    nvl(sfmc.email,ventasys.ventasys_email) as email, null, team_name, v_custid, subscriber_key 
    from
        (
        select cell_phone, 'Ventasys' as source, team_name, v_custid ,lower(email) as ventasys_email
        from itg_hcp360_in_ventasys_hcp_master 
        where cell_phone <> '0' 
            and length(cell_phone) = 10
            and length(cell_phone) = 10
            and team_name = 'JB' 
            and is_active ='Y' 
            and cell_phone not in (select cell_phone 
                            from itg_hcp360_in_ventasys_hcp_master 
                            where cell_phone <> '0' 
                                and length(cell_phone) = 10
                                and team_name = 'JB' 
                                and is_active ='Y'
                            group by cell_phone
                            having count(*) > 1 )
            and not exists (select 1 
                            from block1_2 
                            where brand ='JB'
                                and cell_phone = nvl(mobile_number,'x') 
                                )                   
        ) Ventasys,
        (select cast(mobile_number as varchar) as mobile_number , 'sfmc' as source, subscriber_key , lower(email) as email
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'JB' 
        and  mobile_number is not null and length(mobile_number) = 10  
        and  not exists (select mobile_number 
                                    from block1_2 t 
                                    where brand = 'JB' 
                                    and subscriber_key is not null
                                    and s.mobile_number           = nvl(t.mobile_number,'x')
                                    and lower(nvl(s.email,'xx'))  = lower(nvl(t.email,'xx'))
                                )
        
        ) sfmc
    where Ventasys.cell_phone = sfmc.mobile_number 
        and not exists ( select  1
                            from block1_2 
                        where  md5(ventasys.team_name||ventasys.cell_phone||nvl(sfmc.email,nvl(ventasys.ventasys_email,'xx'))) =  master_hcp_key )
),
block1_3 as
(
    select * from block1_2
    UNION ALL
    select * from block3 
),
block4 as
(
        ---------Ventasys and SFMC Email Match JB -----------------
    select team_name as brand, md5(team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||nvl(lower(sfmc.email),'xx')) as master_hcp_key,  
    nvl(sfmc.mobile_number,ventasys.cell_phone) as mobile_phone, lower(sfmc.email) as email ,null, team_name, v_custid, subscriber_key
    from
        (
        select lower(email) as email, 'Ventasys' as source, team_name, v_custid, cell_phone --,email
        from itg_hcp360_in_ventasys_hcp_master 
        where email is not null
            and team_name = 'JB' 
            and is_active ='Y' 
            and lower(email) not in (
                select lower(email) 
                    from itg_hcp360_in_ventasys_hcp_master 
                where email is not null
                    and team_name = 'JB' 
                    and is_active ='Y'
                group by lower(email)
                having count(*) > 1
                    )
            and not exists (select lower(email) from block1_3 
                                    where brand ='JB'
                                        and lower(email) = nvl(email,'xx') )          
        ) Ventasys,
        (select lower(email) as email, 'sfmc' as source, subscriber_key ,mobile_number
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'JB'  
            and  email is not null 
            and  not exists (select lower(email) 
                                    from block1_3 t 
                                    where brand = 'JB' 
                                    and  subscriber_key is not null
                                    and nvl(s.mobile_number,'x') = nvl(t.mobile_number,'x')
                                    and lower(s.email)           = nvl(lower(t.email),'xx')
                                )      
        ) sfmc
        where Ventasys.email = sfmc.email 
        and not exists ( select  1
                            from block1_3 
                        where  md5(ventasys.team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||sfmc.email) =  master_hcp_key )
),
block1_4 as
(
    select * from block1_3
    UNION ALL
    select * from block4
),
block5 as
(
        ---------Ventasys and SFMC Mobile number Match Derma------
    select team_name as brand, md5(team_name||mobile_number||nvl(sfmc.email,nvl(ventasys.ventasys_email,'xx')) ) as master_hcp_key, mobile_number, 
    nvl(sfmc.email,ventasys.ventasys_email) as email, null, team_name, v_custid, subscriber_key 
    from
        (
        select cell_phone, 'Ventasys' as source, team_name, v_custid ,lower(email) as ventasys_email
        from itg_hcp360_in_ventasys_hcp_master 
        where cell_phone <> '0' 
            and length(cell_phone) = 10
            and length(cell_phone) = 10
            and team_name = 'DERMA' 
            and is_active ='Y' 
            and cell_phone not in (select cell_phone 
                            from itg_hcp360_in_ventasys_hcp_master 
                            where cell_phone <> '0' 
                                and length(cell_phone) = 10
                                and team_name = 'DERMA' 
                                and is_active ='Y'
                            group by cell_phone
                            having count(*) > 1 )
            and not exists (select 1 
                            from block1_4 
                            where brand ='DERMA'
                                and cell_phone = nvl(mobile_number,'x') 
                                )                   
        ) Ventasys,
        (select cast(mobile_number as varchar) as mobile_number , 'sfmc' as source, subscriber_key , lower(email) as email
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'Aveeno' 
        and  mobile_number is not null and length(mobile_number) = 10  
        and  not exists (select mobile_number 
                                    from block1_4 t 
                                    where brand = 'DERMA' 
                                    and subscriber_key is not null
                                    and s.mobile_number           = nvl(t.mobile_number,'x')
                                    and lower(nvl(s.email,'xx'))  = lower(nvl(t.email,'xx'))
                                )
        
        ) sfmc
    where Ventasys.cell_phone = sfmc.mobile_number 
        and not exists ( select  1
                            from block1_4 
                        where  md5(ventasys.team_name||ventasys.cell_phone||nvl(sfmc.email,nvl(ventasys.ventasys_email,'xx'))) =  master_hcp_key )
),
block1_5 as
(
    select * from block1_4
    UNION ALL
    select * from block5
),
block6 as
(
        ----------------Ventasys and SFMC Email Match------------
    select team_name as brand, md5(team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||nvl(lower(sfmc.email),'xx')) as master_hcp_key,  nvl(sfmc.mobile_number,ventasys.cell_phone) as mobile_phone, lower(sfmc.email) as email ,null, team_name, v_custid, subscriber_key
    from
        (
        select lower(email) as email, 'Ventasys' as source, team_name, v_custid, cell_phone 
        from itg_hcp360_in_ventasys_hcp_master 
        where email is not null
            and team_name = 'DERMA' 
            and is_active ='Y' 
            and lower(email) not in (
                select lower(email) 
                    from itg_hcp360_in_ventasys_hcp_master 
                where email is not null
                    and team_name = 'DERMA' 
                    and is_active ='Y'
                group by lower(email)
                having count(*) > 1
                    )
            and not exists (select lower(email) from block1_5 
                                    where brand ='DERMA'
                                        and lower(email) = nvl(email,'xx') )          
        ) Ventasys,
        (select lower(email) as email, 'sfmc' as source, subscriber_key ,mobile_number
        from itg_hcp360_sfmc_hcp_details s
        where split_part(subscriber_key,'_',1) = 'Aveeno'  
            and  email is not null 
            and  not exists (select lower(email) 
                                    from block1_5 t 
                                    where brand = 'DERMA' 
                                    and  subscriber_key is not null
                                    and nvl(s.mobile_number,'x') = nvl(t.mobile_number,'x')
                                    and lower(s.email)           = nvl(lower(t.email),'xx')
                                )      
        ) sfmc
        where Ventasys.email = sfmc.email 
        and not exists ( select  1
                            from block1_5 
                        where  md5(ventasys.team_name||nvl(sfmc.mobile_number,ventasys.cell_phone)||sfmc.email) =  master_hcp_key )
),
final as
(
    select * from block1_5
    UNION ALL
    select * from block6
)
select brand::varchar(20) as brand,
    master_hcp_key::varchar(32) as master_hcp_key,
    mobile_number::varchar(40) as mobile_number,
    email::varchar(180) as email,
    null::varchar(18) as account_source_id,
    team_name::varchar(20) as team_name,
    v_custid::varchar(20) as v_custid,
    subscriber_key::varchar(100) as subscriber_key
 from final