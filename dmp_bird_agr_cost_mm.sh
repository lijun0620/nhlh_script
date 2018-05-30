#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_agr_cost_mm.sh                               
# 创建时间: 2018年05月22日                                            
# 创 建 者: lj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 农业制造费用
# 修改说明:     3504001004   鸭养殖合格蛋  3503001004   鸡养殖合格蛋                                                    
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_agr_cost_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 从zq12取出所有费用
DMP_BIRD_AGR_COST_MM_01='DMP_BIRD_AGR_COST_MM_01'
CREATE_DMP_BIRD_AGR_COST_MM_01="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_AGR_COST_MM_01(
    period_id                 string  
   ,org_id                    string  
   ,currency_code             string          --本位币
   ,rate                      string          --汇率
   ,dept_code                 string          --7级id
   ,dept_desc                 string          --7级描述
   ,account_code              string          --科目编码
   ,fixed_salary_cost         string          --职工薪酬(元)                                           '农业制造费用_固定_职工薪酬'
   ,change_office_cost        string          --办公费(元)                                             '农业制造费用_变动_办公费'
   ,change_travel_cost        string          --差旅费(元)                                             '农业制造费用_变动_差旅费'
   ,change_post_cost          string          --邮电费(元)                                             '农业制造费用_变动_邮电费'
   ,change_used_mtl_cost      string          --机物料消耗(元)                                         '农业制造费用_变动_机物料消耗'
   ,low_valued_cal_cost       string          --低值易耗品摊销(元)                                     '农业制造费用_变动_低值易耗品摊销'
   ,fixed_depreciated_cost    string          --折旧费(元)                                             '农业制造费用_固定_折旧费'
   ,fixed_rental_cost         string          --租赁费(元)                                             '农业制造费用_固定_租赁费'
   ,change_protect_work_cost  string          --劳动保护费(元)                                         '农业制造费用_变动_劳动保护费'
   ,change_carriage_cost      string          --运输费(元)                                             '农业制造费用_变动_运输费'
   ,change_check_cost         string          --试验检验费(元)                                         '农业制造费用_变动_试验检验费'
   ,fixed_trun_mtl_cal_cost   string          --周转材料摊销(元)                                       '农业制造费用_固定_周转材料摊销'
   ,fixed_long_wait_cal_cost  string          --长期待摊费用摊销(元)                                   '农业制造费用_固定_长期待摊费用摊销'
   ,water_power_cost          string          --水费                                                   '生产成本_辅助生产成本_水'
   ,electric_cost             string          --电                                                     '生产成本_辅助生产成本_电'
   ,coal_cost                 string          --煤                                                     '生产成本_辅助生产成本_燃煤' 
   ,fuel_cost                 string          --油                                                     '生产成本_辅助生产成本_燃油'
   ,Gas_cost                  string          --汽                                                     '生产成本_辅助生产成本_燃汽'
   ,change_padding_mtl_cost   string          --垫料(元)                                               '农业制造费用_变动_垫料'      
   ,fumigating_cost           string          --栋舍熏蒸费                                             '农业制造费用_变动_栋舍熏蒸费'
   ,eliminate_cost            string          --                                                       '农业制造费用_变动_淘汰残值'
   ,other_cost                string          --                                                       '农业制造费用_变动_其他'
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>从ZQ12分类取出所有费用>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_AGR_COST_MM_01="
INSERT OVERWRITE TABLE $DMP_BIRD_AGR_COST_MM_01 PARTITION(op_month='$OP_MONTH')
select
  regexp_replace(t1.period_name,'-','')
 ,t1.org_id                    
 ,t1.currency_code             
 ,case when t1.currency_code ='CNY' then 1
  else t3.conversion_rate end
 ,t1.dept_code                 
 ,t1.dept_desc                 
 ,t1.account_code
 ,case when account_desc  = '农业制造费用_固定_职工薪酬'          then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_办公费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_差旅费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_邮电费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_机物料消耗'        then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_低值易耗品摊销'    then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_固定_折旧费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_固定_租赁费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_劳动保护费'        then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_运输费'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_试验检验费'        then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_固定_周转材料摊销'      then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_固定_长期待摊费用摊销'  then nvl(account_net,0) else 0 end
 ,case when account_desc  = '生产成本_辅助生产成本_水'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '生产成本_辅助生产成本_电'            then nvl(account_net,0) else 0 end
 ,case when account_desc  = '生产成本_辅助生产成本_燃煤'          then nvl(account_net,0) else 0 end
 ,case when account_desc  = '生产成本_辅助生产成本_燃油'          then nvl(account_net,0) else 0 end
 ,case when account_desc  = '生产成本_辅助生产成本_燃气'          then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_垫料'              then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_栋舍熏蒸费'        then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_淘汰残值'          then nvl(account_net,0) else 0 end
 ,case when account_desc  = '农业制造费用_变动_其他'              then nvl(account_net,0) else 0 end
from (select * from MREPORT_POULTRY.DWU_QW_FARM_WIP_EXPENSE 
where op_day = '$OP_DAY' and dept_desc not like '%猪%') t1 
 LEFT join 
   (  SELECT
                    from_currency,
                    to_currency,
                    conversion_rate,
                    conversion_period
                FROM
                    mreport_global.dmd_fin_period_currency_rate_mm
                WHERE to_currency='CNY') t3
    on t1.currency_code =t3.from_currency  
   and t3.conversion_period=regexp_replace(t1.period_name,'-','')
inner join (select * from MREPORT_GLOBAL.DWU_DIM_ZQ_OU) t4
   on t1.org_id = t4.org_id
  and t1.org_code = t4.org_code
;
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_AGR_COST_MM='DMP_BIRD_AGR_COST_MM'

CREATE_DMP_BIRD_AGR_COST_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_AGR_COST_MM(
       month_id                     string    --期间(月份)          
       ,day_id                       string    --期间(日)            
       ,level1_org_id                string    --组织1级(股份)       
       ,level1_org_descr             string    --组织1级(股份)       
       ,level2_org_id                string    --组织2级(片联)       
       ,level2_org_descr             string    --组织2级(片联)       
       ,level3_org_id                string    --组织3级(片区)       
       ,level3_org_descr             string    --组织3级(片区)       
       ,level4_org_id                string    --组织4级(小片)       
       ,level4_org_descr             string    --组织4级(小片)       
       ,level5_org_id                string    --组织5级(公司)       
       ,level5_org_descr             string    --组织5级(公司)       
       ,level6_org_id                string    --组织6级(OU)         
       ,level6_org_descr             string    --组织6级(OU)         
       ,level7_org_id                string    --组织7级(库存组织)   
       ,level7_org_descr             string    --组织7级(库存组织)   
       ,level1_businesstype_id       string    --业态1级             
       ,level1_businesstype_name     string    --业态1级             
       ,level2_businesstype_id       string    --业态2级             
       ,level2_businesstype_name     string    --业态2级             
       ,level3_businesstype_id       string    --业态3级             
       ,level3_businesstype_name     string    --业态3级             
       ,level4_businesstype_id       string    --业态4级             
       ,level4_businesstype_name     string    --业态4级             
       ,production_line_id           string    --产线                
       ,production_line_descr        string    --产线                
       ,kpi_type_id                  string    --指标类型            
       ,kpi_type_descr               string    --指标类型            
       ,sales_qty                    string    --总销售/产量(只)     
       ,fixed_salary_cost            string    --职工薪酬(元)        
       ,change_office_cost           string    --办公费(元)          
       ,change_travel_cost           string    --差旅费(元)          
       ,change_post_cost             string    --邮电费(元)          
       ,change_used_mtl_cost         string    --机物料消耗(元)      
       ,low_valued_cal_cost          string    --低值易耗品摊销(元)  
       ,fixed_depreciated_cost       string    --折旧费(元)          
       ,fixed_rental_cost            string    --租赁费(元)          
       ,change_protect_work_cost     string    --劳动保护费(元)      
       ,change_carriage_cost         string    --运输费(元)          
       ,change_check_cost            string    --试验检验费(元)      
       ,fixed_trun_mtl_cal_cost      string    --周转材料摊销(元)    
       ,fixed_long_wait_cal_cost     string    --长期待摊费用摊销(元)
       ,water_power_cost             string    --水电(元)            
       ,consum_cost                  string    --能耗费(元)          
       ,change_padding_mtl_cost      string    --垫料(元)            
       ,eliminate_cost               string    --农业制造费用-淘汰残值
       ,change_other_cost            string    --其他(元)            
       ,manufacture_change_cost      string    --制造费用-变动(元)   
       ,manufacture_fixed_cost       string    --制造费用-固定(元)   
       ,create_time                  string    --数据推送时间        
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_AGR_COST_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_AGR_COST_MM PARTITION(op_month='$OP_MONTH')
 SELECT
        t1.period_id       as   month_id         --month_id
       ,''    as  day_id                           
       ,case when t6.level1_org_id    is null then coalesce(t7.level1_org_id,'-1')      else coalesce(t6.level1_org_id,'-1')       end as level1_org_id      --一级组织编码
       ,case when t6.level1_org_descr is null then coalesce(t7.level1_org_descr,'缺失') else coalesce(t6.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
       ,case when t6.level2_org_id    is null then coalesce(t7.level2_org_id,'-1')      else coalesce(t6.level2_org_id,'-1')       end as level2_org_id      --二级组织编码
       ,case when t6.level2_org_descr is null then coalesce(t7.level2_org_descr,'缺失') else coalesce(t6.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
       ,case when t6.level3_org_id    is null then coalesce(t7.level3_org_id,'-1')      else coalesce(t6.level3_org_id,'-1')       end as level3_org_id      --三级组织编码
       ,case when t6.level3_org_descr is null then coalesce(t7.level3_org_descr,'缺失') else coalesce(t6.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
       ,case when t6.level4_org_id    is null then coalesce(t7.level4_org_id,'-1')      else coalesce(t6.level4_org_id,'-1')       end as level4_org_id      --四级组织编码
       ,case when t6.level4_org_descr is null then coalesce(t7.level4_org_descr,'缺失') else coalesce(t6.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
       ,case when t6.level5_org_id    is null then coalesce(t7.level5_org_id,'-1')      else coalesce(t6.level5_org_id,'-1')       end as level5_org_id      --五级组织编码
       ,case when t6.level5_org_descr is null then coalesce(t7.level5_org_descr,'缺失') else coalesce(t6.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
       ,case when t6.level6_org_id    is null then coalesce(t7.level6_org_id,'-1')      else coalesce(t6.level6_org_id,'-1')       end as level6_org_id      --六级组织编码
       ,case when t6.level6_org_descr is null then coalesce(t7.level6_org_descr,'缺失') else coalesce(t6.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
       ,t1.dept_code                                --7级id
       ,t1.dept_desc                                --7级描述
       ,'' --level1_businesstype_id                 --业态1级             
       ,'' --level1_businesstype_name               --业态1级             
       ,'' --level2_businesstype_id                 --业态2级             
       ,'' --level2_businesstype_name               --业态2级             
       ,'' --level3_businesstype_id                 --业态3级             
       ,'' --level3_businesstype_name               --业态3级             
       ,'' --level4_businesstype_id                 --业态4级             
       ,'' --level4_businesstype_name               --业态4级             
       ,'' --production_line_id                     --产线                
       ,'' --production_line_descr                  --产线                
       ,'1' --kpi_type_id                           --指标类型            
       ,'' --kpi_type_descr                         --指标类型                                 
       ,'' --sales_qty                              --总销售/产量(只)       没取呢
       ,t1.fixed_salary_cost                                 *rate
       ,t1.change_office_cost                                *rate
       ,t1.change_travel_cost                                *rate
       ,t1.change_post_cost                                  *rate
       ,t1.change_used_mtl_cost                              *rate
       ,t1.low_valued_cal_cost                               *rate
       ,t1.fixed_depreciated_cost                            *rate
       ,t1.fixed_rental_cost                                 *rate
       ,t1.change_protect_work_cost                          *rate
       ,t1.change_carriage_cost                              *rate
       ,t1.change_check_cost                                 *rate
       ,t1.fixed_trun_mtl_cal_cost                           *rate
       ,t1.fixed_long_wait_cal_cost                          *rate
       ,(t1.water_power_cost+t1.electric_cost)               *rate
       ,(t1.coal_cost+t1.fuel_cost+t1.Gas_cost)              *rate
       ,t1.change_padding_mtl_cost                           *rate
       ,t1.eliminate_cost                                    *rate
       ,(t1.fumigating_cost+t1.other_cost) * rate 
       ,(t1.change_office_cost+t1.change_travel_cost+t1.change_post_cost+t1.change_used_mtl_cost+t1.low_valued_cal_cost
       +t1.change_protect_work_cost+t1.change_carriage_cost+t1.change_check_cost+t1.change_padding_mtl_cost
       +t1.fumigating_cost+t1.eliminate_cost+t1.other_cost+t1.water_power_cost+t1.electric_cost+t1.coal_cost+t1.fuel_cost+t1.Gas_cost) * rate   --制造费用-变动(元)   
       ,(t1.fixed_salary_cost+t1.fixed_rental_cost+t1.fixed_depreciated_cost+t1.fixed_trun_mtl_cal_cost+t1.fixed_long_wait_cal_cost) * rate      --制造费用-固定(元)   
       ,'$CREATE_TIME'                                                    --数据推送时间
    FROM
        (SELECT * FROM DMP_BIRD_AGR_COST_MM_01
            WHERE op_month='$OP_MONTH') t1
left join mreport_global.dim_org_management t6 
     on t1.org_id=t6.org_id  
     and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
     on t1.org_id=t7.org_id 
--     and t1.bus_type=t7.bus_type_id 
     and t7.attribute5='2'
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DMP_BIRD_AGR_COST_MM_01;
    $INSERT_DMP_BIRD_AGR_COST_MM_01;
    $CREATE_DMP_BIRD_AGR_COST_MM;
    $INSERT_DMP_BIRD_AGR_COST_MM;
"  -v