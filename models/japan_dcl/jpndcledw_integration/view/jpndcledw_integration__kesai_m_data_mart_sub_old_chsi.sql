with kesai_m_data_mart_sub_old_chsi_tbl as
(
    select * from {{source('jpdcledw_integration', 'kesai_m_data_mart_sub_old_chsi_tbl')}}
),

final as
(
    SELECT 
        kesai_m_data_mart_sub_old_chsi_tbl.saleno_key, 
        kesai_m_data_mart_sub_old_chsi_tbl.saleno, 
        kesai_m_data_mart_sub_old_chsi_tbl.kingaku, 
        kesai_m_data_mart_sub_old_chsi_tbl.meisainukikingaku, 
        kesai_m_data_mart_sub_old_chsi_tbl.kesaiid, 
        kesai_m_data_mart_sub_old_chsi_tbl.saleno_trim, 
        kesai_m_data_mart_sub_old_chsi_tbl.inserted_date, 
        kesai_m_data_mart_sub_old_chsi_tbl.inserted_by, 
        kesai_m_data_mart_sub_old_chsi_tbl.updated_date, 
        kesai_m_data_mart_sub_old_chsi_tbl.updated_by 
    FROM kesai_m_data_mart_sub_old_chsi_tbl
)

select * from final