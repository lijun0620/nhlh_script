#!/bin/bash
######################################################################
#                                                                    
# 程    序: dmp_bird_drugs_comp_mm.sh                               
# 创建时间: 2018年04月27日                                            
# 创 建 者: lijun                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度耗用药品类别对比表
# 修改说明:                                                          
######################################################################




OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_drugs_comp_mm.sh $OP_MONTH01"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


###########################################################################################
## 统计五类数据消耗
TMP_DMP_BIRD_DRUGS_COMP_MM_01='TMP_DMP_BIRD_DRUGS_COMP_MM_01'
CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_01="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_DRUGS_COMP_MM_01(
           period_id             string                            --期间                                     
          ,material_id          string                             --物料id                                   
          ,org_id               string                             --ouid                                    
          ,organization_id      string                             --库存组织id                                     
          ,bus_type             string                             --业态 
          ,product_line         string                             --产线
          ,nutrition            string                             --营养类耗用
          ,disinfect            string                             --消毒类耗用               
          ,vaccine              string                             --疫苗类耗用                 
          ,treat                string                             --治疗类耗用                 
          ,other                string                             --其它类耗用
  )
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC;
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>取出要统计的五类数据消耗>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_01="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_DRUGS_COMP_MM_01 PARTITION(op_month='$OP_MONTH')
select
     t1.period_id                              --期间                   
    ,t1.material_id                            --物料id                 
    ,t1.org_id                                 --ouid                   
    ,t1.organization_id                        --库存组织id             
    ,t1.bus_type                               --业态    
    ,'' --t2.product_line                      --产线
    ,case when t2.material_segment2_id = '01' then coalesce(t1.primary_quantity,0)  else '0' end       --营养类耗用
    ,case when t2.material_segment2_id = '04' then coalesce(t1.primary_quantity,0)  else '0' end       --消毒类耗用    
    ,case when t2.material_segment2_id = '03' then coalesce(t1.primary_quantity,0)  else '0' end       --疫苗类耗用    
    ,case when t2.material_segment2_id = '02' then coalesce(t1.primary_quantity,0)  else '0' end       --治疗类耗用    
    ,case when (t2.material_segment2_id = '05' or t2.material_segment2_id = '99') then coalesce(t1.primary_quantity,0) else '0' end --其它类耗用      
  from (
  select  *  from 
  mreport_poultry.dwu_zq_kc01_dd  
  where op_day = '$OP_DAY'
  and in_out_flag = '出库'
  and org_type='YZ'
  and (bus_type = '132011'  or bus_type = '132012') --限制ou是种禽的公司，目前测试限制之后没有数据，暂时注释
  )t1 
  inner join (
  select * from mreport_global.dwu_dim_material_new 
  where material_segment1_id = '65'
  and material_segment2_id in ('01','02','03','04','05','99')                                     --（只取营养类，消毒类，疫苗类，抗生素，其它） 
  and finance_segment1_desc = '兽药'
  ) t2 
  on t1.organization_id = t2.inv_org_id     
  and t1.material_id =t2.inventory_item_id
  ;
"
###########################################################################################
## 算出各月每类出库总和金额
TMP_DMP_BIRD_DRUGS_COMP_MM_02='TMP_DMP_BIRD_DRUGS_COMP_MM_02'
CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_02="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_DRUGS_COMP_MM_02(
    period_id                  string                        --期间                                                                  
   ,org_id                    string                         --ouid                                                                       
   ,bus_type                  string                         --业态   
   ,product_line              string                         --产线 
   ,mm_nutrition              string                         --营养类耗用
   ,mm_disinfect              string                         --消毒类耗用               
   ,mm_vaccine                string                         --疫苗类耗用                 
   ,mm_treat                  string                         --治疗类耗用                 
   ,mm_other                  string                         --其它类耗用
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>算出各月每类出库总和金额>>>>>>>>>>>>>>>>>>>>>加产线条件>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_02="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_DRUGS_COMP_MM_02 PARTITION(op_month='$OP_MONTH')

 select 
       t1.period_id                                            --期间                                
      ,t2.org_id                                              --ouid                             
      ,t2.bus_type                                            --业态   
      ,''-- t2.product_line                                        --产线         
      ,sum(t2.nutrition * nvl(t1.cost,1))                                 --月_营养类耗用
      ,sum(t2.disinfect * nvl(t1.cost,1))                                 --月_消毒类耗用       
      ,sum(t2.vaccine *   nvl(t1.cost,1))                                 --月_疫苗类耗用       
      ,sum(t2.treat *     nvl(t1.cost,1))                                 --月_治疗类耗用       
      ,sum(t2.other *     nvl(t1.cost,1))                                 --月_其它类耗用
   from
(
 select 
   m1.period_id
   ,m1.org_id
   ,m1.inventory_item_id
   ,case when m3.conversion_rate is null  then m1.cost_amount_t_loc  
   else cost_amount_t_loc * m3.conversion_rate end   cost
from dwu_finance_cost_pric m1
left  join 
(SELECT
     from_currency
     ,to_currency
     ,conversion_rate
     ,conversion_period
FROM
     mreport_global.dmd_fin_period_currency_rate_mm                     
WHERE to_currency='CNY') m3                            --汇率表
    on    m1.loc_currency_id =m3.from_currency  
    and   m1.period_id = m3.conversion_period
) t1  
   inner join  (
 select 
      substr(period_id,1,6) period_id                   --期间                                
      ,org_id                                           --ouid                              
      ,bus_type                                         --业态   
      ,material_id                                      --物料id
      ,case when sum(nutrition) < 0 then sum(nutrition)
            else 0 end       nutrition                      --月_营养类耗用
      ,case when sum(disinfect) < 0 then sum(disinfect)
        else 0 end      disinfect                           --月_消毒类耗用             
      ,case when sum(vaccine) < 0 then sum(vaccine)
        else 0 end      vaccine                             --月_疫苗类耗用      
      ,case when sum(treat) < 0 then sum(treat)
        else 0 end      treat                               --月_治疗类耗用   
      ,case when sum(other) < 0 then sum(other)
        else 0 end      other                               --月_其它类耗用
      from tmp_dmp_bird_drugs_comp_mm_01
  where op_month = '$OP_MONTH'
      group by 
      substr(period_id,1,6)                             --期间                   
      ,org_id                                           --ouid                     
      ,bus_type                                         --业态 
      ,material_id
      ) t2
  on  t2.period_id=t1.period_id                
  and t2.org_id = t1.org_id
  and t2.material_id = t1.inventory_item_id
 group by 
       t1.period_id                                        --期间                              
      ,t2.org_id                                           --ouid                             
      ,t2.bus_type                                         --业态    
 ;
"
###########################################################################################
## 关联时间维度表，出本月，上月，去年同期月消耗金额
TMP_DMP_BIRD_DRUGS_COMP_MM_03='TMP_DMP_BIRD_DRUGS_COMP_MM_03'
CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_03="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_DRUGS_COMP_MM_03(
       period_id                              string                           --期间         
      ,org_id                                 string                           --ouid         
      ,bus_type                               string                           --业态   
      ,product_line                           string                           --产线         
      ,mm_nutrition                           string                           --本月_营养类耗用
      ,mm_disinfect                           string                           --本月_消毒类耗用
      ,mm_vaccine                             string                           --本月_疫苗类耗用
      ,mm_treat                               string                           --本月_治疗类耗用
      ,mm_other                               string                           --本月_其它类耗用
      ,last_mm_nutrition                      string                           --上月_营养类耗用
      ,last_mm_disinfect                      string                           --上月_消毒类耗用
      ,last_mm_vaccine                        string                           --上月_疫苗类耗用
      ,last_mm_treat                          string                           --上月_治疗类耗用
      ,last_mm_other                          string                           --上月_其它类耗用
      ,last_yy_nutrition                      string                           --去年同期_营养类耗用
      ,last_yy_disinfect                      string                           --去年同期_消毒类耗用
      ,last_yy_vaccine                        string                           --去年同期_疫苗类耗用
      ,last_yy_treat                          string                           --去年同期_治疗类耗用
      ,last_yy_other                          string                           --去年同期_其它类耗用
)                                 
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>关联时间维度表，出本月，上月，去年同期月消耗金额>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_03="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_DRUGS_COMP_MM_03 PARTITION(op_month='$OP_MONTH')
  select
        t2.period_id                                                                         --期间          
       ,t2.org_id                                                                            --ouid         
       ,t2.bus_type                                                                          --业态   
       ,'' --t2.product_line                                                                      --产线         
       ,t2.mm_nutrition                                                                      --本月_营养类耗用
       ,t2.mm_disinfect                                                                      --本月_消毒类耗用
       ,t2.mm_vaccine                                                                        --本月_疫苗类耗用
       ,t2.mm_treat                                                                          --本月_治疗类耗用
       ,t2.mm_other                                                                          --本月_其它类耗用
       ,t3.mm_nutrition                                                                      --上月_营养类耗用
       ,t3.mm_disinfect                                                                      --上月_消毒类耗用
       ,t3.mm_vaccine                                                                        --上月_疫苗类耗用
       ,t3.mm_treat                                                                          --上月_治疗类耗用
       ,t3.mm_other                                                                          --上月_其它类耗用
       ,t4.mm_nutrition                                                                      --去年同期_营养类耗用
       ,t4.mm_disinfect                                                                      --去年同期_消毒类耗用
       ,t4.mm_vaccine                                                                        --去年同期_疫苗类耗用
       ,t4.mm_treat                                                                          --去年同期_治疗类耗用
       ,t4.mm_other                                                                          --去年同期_其它类耗用
    from ( 
      select 
         month_id
        ,last_month_id
        ,substr(day_in_last_year_id,1,6) month_in_last_year 
      from mreport_global.dim_day  
      where  month_id between '201512' and '$OP_MONTH'
      group by 
         month_id
         ,last_month_id
         ,substr(day_in_last_year_id,1,6)
) t1                                                   --时间维表
    inner join (select * from tmp_dmp_bird_drugs_comp_mm_02  
    where op_month = '$OP_MONTH') t2        --当月数据
       on t1.month_id = t2.period_id
    left join (select * from tmp_dmp_bird_drugs_comp_mm_02  
    where op_month = '$OP_MONTH') t3         --上月数据
       on t1.last_month_id = t3.period_id    
       and t2.org_id = t3.org_id 
       and t2.bus_type = t3.bus_type
   left join (select * from tmp_dmp_bird_drugs_comp_mm_02     
    where op_month = '$OP_MONTH') t4          --去年同期      
       on t1.month_in_last_year = t4.period_id                
       and t2.org_id = t4.org_id                              
       and t2.bus_type = t4.bus_type                          

;
"

###########################################################################################
## 计算年累计金额
TMP_DMP_BIRD_DRUGS_COMP_MM_04='TMP_DMP_BIRD_DRUGS_COMP_MM_04'
CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_04="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_DRUGS_COMP_MM_04(
       period_id                              string                           --期间         
      ,org_id                                 string                           --ouid         
      ,bus_type                               string                           --业态   
      ,product_line                           string                           --产线         
      ,yy_t_nutrition_cost                    string                           --年累计_营养类耗用金额
      ,yy_t_disinfect_cost                    string                           --年累计_消毒类耗用金额
      ,yy_t_vaccine_cost                      string                           --年累计_疫苗类耗用金额
      ,yy_t_treat_cost                        string                           --年累计_治疗类耗用金额
      ,yy_t_other_cost                        string                           --年累计_其它类耗用金额
)                                 
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 计算年累计金额>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_04="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_DRUGS_COMP_MM_04 PARTITION(op_month='$OP_MONTH')
SELECT
    t1.month_id
   ,t2.org_id               --ouid       
   ,t2.bus_type             --业态 
   ,''  --t2.product_line         --产线
   ,sum(t2.mm_nutrition)  mm_nutrition   --年累计_营养类耗用
   ,sum(t2.mm_disinfect)  mm_disinfect   --年累计_消毒类耗用 
   ,sum(t2.mm_vaccine)    mm_vaccine   --年累计_疫苗类耗用 
   ,sum(t2.mm_treat)      mm_treat     --年累计_治疗类耗用 
   ,sum(t2.mm_other)      mm_other   --年累计_其它类耗用
FROM
    ( SELECT month_id,year_id FROM mreport_global.dim_month ) t1
JOIN
    (
        SELECT
            *
        FROM
            tmp_dmp_bird_drugs_comp_mm_02
        WHERE
           op_month = '$OP_MONTH') t2
WHERE
    t1.month_id >= t2.period_id
AND t1.year_id =SUBSTR(t2.period_id,1,4)
group by 
    t1.month_id
   ,t2.org_id               --ouid       
   ,t2.bus_type    
;
"
###########################################################################################
## 计算本年累计和去年累计
TMP_DMP_BIRD_DRUGS_COMP_MM_05='TMP_DMP_BIRD_DRUGS_COMP_MM_05'
CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_05="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_DRUGS_COMP_MM_05(
       period_id                              string                           --期间             
      ,org_id                                 string                           --ouid         
      ,bus_type                               string                           --业态   
      ,product_line                              string                        --产线         
      ,yy_t_nutrition_cost                      string                          --年累计_营养类耗用金额
      ,yy_t_disinfect_cost                      string                          --年累计_消毒类耗用金额
      ,yy_t_vaccine_cost                      string                          --年累计_疫苗类耗用金额
      ,yy_t_treat_cost                          string                          --年累计_治疗类耗用金额
      ,yy_t_other_cost                          string                          --年累计_其它类耗用金额
      ,last_yy_t_nutrition_cost                  string                          --去年累计_营养类耗用金额
      ,last_yy_t_disinfect_cost                  string                          --去年累计_消毒类耗用金额
      ,last_yy_t_vaccine_cost                  string                          --去年累计_疫苗类耗用金额
      ,last_yy_t_treat_cost                      string                          --去年累计_治疗类耗用金额
      ,last_yy_t_other_cost                      string                          --去年累计_其它类耗用金额
)                                 
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>> 计算本年累计和去年累计>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_05="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_DRUGS_COMP_MM_05 PARTITION(op_month='$OP_MONTH')
select
       t1.month_id                                            --期间         
      ,t2.org_id                                               --ouid         
      ,t2.bus_type                                             --业态   
      ,t2.product_line                                         --产线           
      ,t2.yy_t_nutrition_cost                                  --年累计_营养类耗用金额
      ,t2.yy_t_disinfect_cost                                  --年累计_消毒类耗用金额
      ,t2.yy_t_vaccine_cost                                      --年累计_疫苗类耗用金额
      ,t2.yy_t_treat_cost                                      --年累计_治疗类耗用金额
      ,t2.yy_t_other_cost                                      --年累计_其它类耗用金额
      ,t3.yy_t_nutrition_cost                                 --去年累计_营养类耗用金额
      ,t3.yy_t_disinfect_cost                                 --去年累计_消毒类耗用金额
      ,t3.yy_t_vaccine_cost                                   --去年累计_疫苗类耗用金额
      ,t3.yy_t_treat_cost                                     --去年累计_治疗类耗用金额
      ,t3.yy_t_other_cost                                     --去年累计_其它类耗用金额
 from (                                                      
        select 
           month_id
          ,substr(day_in_last_year_id,1,6) last_year_id 
        from mreport_global.dim_day  
        group by 
            month_id
           ,substr(day_in_last_year_id,1,6)

) t1                                                   --时间维表
      inner join 
      (select * from tmp_dmp_bird_drugs_comp_mm_04 
      where op_month = '$OP_MONTH' ) t2        --当年数据
         on t1.month_id = t2.period_id
      left join (select * from tmp_dmp_bird_drugs_comp_mm_04 
      where op_month = '$OP_MONTH' ) t3        --上年数据
         on  t1.last_year_id = t3.period_id    
         and t2.org_id = t3.org_id
         and t2.bus_type = t3.bus_type
;
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_DRUGS_COMP_MM='DMP_BIRD_DRUGS_COMP_MM'
CREATE_DMP_BIRD_DRUGS_COMP_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_DRUGS_COMP_MM(
          month_id                           string            --期间(月份)
         ,day_id                             string            --期间(日)
         ,level1_org_id                      string            --组织1级(股份)
         ,level1_org_descr                   string            --组织1级(股份)
         ,level2_org_id                      string            --组织2级(片联)
         ,level2_org_descr                   string            --组织2级(片联)
         ,level3_org_id                      string            --组织3级(片区)
         ,level3_org_descr                   string            --组织3级(片区)
         ,level4_org_id                      string            --组织4级(小片)
         ,level4_org_descr                   string            --组织4级(小片)
         ,level5_org_id                      string            --组织5级(公司)
         ,level5_org_descr                   string            --组织5级(公司)
         ,level6_org_id                      string            --组织6级(ou)
         ,level6_org_descr                   string            --组织6级(ou)
         ,level7_org_id                      string            --组织7级(库存组织)
         ,level7_org_descr                   string            --组织7级(库存组织)
         ,level1_businesstype_id             string            --业态1级
         ,level1_businesstype_name           string            --业态1级
         ,level2_businesstype_id             string            --业态2级
         ,level2_businesstype_name           string            --业态2级
         ,level3_businesstype_id             string            --业态3级
         ,level3_businesstype_name           string            --业态3级
         ,level4_businesstype_id             string            --业态4级
         ,level4_businesstype_name           string            --业态4级
         ,production_line_id                 string            --产线
         ,production_line_descr              string            --产线
         ,mm_nutrition_cost                  string            --本月_营养类耗用金额
         ,mm_disinfect_cost                  string            --本月_消毒类耗用金额
         ,mm_vaccine_cost                    string            --本月_疫苗类耗用金额
         ,mm_treat_cost                      string            --本月_治疗类耗用金额
         ,mm_other_cost                      string            --本月_其它类耗用金额
         ,last_mm_nutrition_cost             string            --上月_营养类耗用金额
         ,last_mm_disinfect_cost             string            --上月_消毒类耗用金额
         ,last_mm_vaccine_cost               string            --上月_疫苗类耗用金额
         ,last_mm_treat_cost                 string            --上月_治疗类耗用金额
         ,last_mm_other_cost                 string            --上月_其它类耗用金额
         ,yy_t_nutrition_cost                string            --年累计_营养类耗用金额
         ,yy_t_disinfect_cost                string            --年累计_消毒类耗用金额
         ,yy_t_vaccine_cost                  string            --年累计_疫苗类耗用金额
         ,yy_t_treat_cost                    string            --年累计_治疗类耗用金额
         ,yy_t_other_cost                    string            --年累计_其它类耗用金额
         ,last_yy_nutrition_cost             string            --去年同期_营养类耗用金额
         ,last_yy_disinfect_cost             string            --去年同期_消毒类耗用金额
         ,last_yy_vaccine_cost               string            --去年同期_疫苗类耗用金额
         ,last_yy_treat_cost                 string            --去年同期_治疗类耗用金额
         ,last_yy_other_cost                 string            --去年同期_其它类耗用金额
         ,last_yy_t_nutrition_cost           string            --去年累计_营养类耗用金额
         ,last_yy_t_disinfect_cost           string            --去年累计_消毒类耗用金额
         ,last_yy_t_vaccine_cost             string            --去年累计_疫苗类耗用金额
         ,last_yy_t_treat_cost               string            --去年累计_治疗类耗用金额
         ,last_yy_t_other_cost               string            --去年累计_其它类耗用金额
         ,create_time                        string            --数据推送时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
;
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_DRUGS_COMP_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_DRUGS_COMP_MM PARTITION(op_month='$OP_MONTH')
select 
          t1.period_id                              --期间(月份)
         ,''                                       --期间(日)
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
         ,'' level7_org_id                         --组织7级(库存组织)
         ,'' level7_org_descr                      --组织7级(库存组织)
         ,t5.level1_businesstype_id                --业态1级
         ,t5.level1_businesstype_name              --业态1级
         ,t5.level2_businesstype_id                --业态2级
         ,t5.level2_businesstype_name              --业态2级
         ,t5.level3_businesstype_id                --业态3级
         ,t5.level3_businesstype_name              --业态3级
         ,t5.level4_businesstype_id                --业态4级
         ,t5.level4_businesstype_name              --业态4级
         ,t1.product_line                         --产线
         ,case when t1.product_line = '10' then '鸡线' 
               when t1.product_line = '20' then '鸭线' else '缺省' end     --产线
         ,abs(t1.mm_nutrition            )           --本月_营养类耗用
         ,abs(t1.mm_disinfect            )           --本月_消毒类耗用
         ,abs(t1.mm_vaccine              )           --本月_疫苗类耗用
         ,abs(t1.mm_treat                )           --本月_治疗类耗用
         ,abs(t1.mm_other                )           --本月_其它类耗用
         ,abs(t1.last_mm_nutrition       )           --上月_营养类耗用
         ,abs(t1.last_mm_disinfect       )           --上月_消毒类耗用
         ,abs(t1.last_mm_vaccine         )           --上月_疫苗类耗用
         ,abs(t1.last_mm_treat           )           --上月_治疗类耗用
         ,abs(t1.last_mm_other           )           --上月_其它类耗用
         ,abs(t2.yy_t_nutrition_cost     )           --年累计_营养类耗用金额
         ,abs(t2.yy_t_disinfect_cost     )           --年累计_消毒类耗用金额
         ,abs(t2.yy_t_vaccine_cost       )             --年累计_疫苗类耗用金额
         ,abs(t2.yy_t_treat_cost         )           --年累计_治疗类耗用金额
         ,abs(t2.yy_t_other_cost         )           --年累计_其它类耗用金额
         ,abs(t1.last_yy_nutrition       )             --去年同期_营养类耗用
         ,abs(t1.last_yy_disinfect       )             --去年同期_消毒类耗用
         ,abs(t1.last_yy_vaccine         )           --去年同期_疫苗类耗用
         ,abs(t1.last_yy_treat           )             --去年同期_治疗类耗用
         ,abs(t1.last_yy_other           )             --去年同期_其它类耗用
         ,abs(t2.last_yy_t_nutrition_cost)            --去年累计_营养类耗用金额
         ,abs(t2.last_yy_t_disinfect_cost)            --去年累计_消毒类耗用金额
         ,abs(t2.last_yy_t_vaccine_cost  )              --去年累计_疫苗类耗用金额
         ,abs(t2.last_yy_t_treat_cost    )            --去年累计_治疗类耗用金额
         ,abs(t2.last_yy_t_other_cost    )            --去年累计_其它类耗用金额
         ,'$CREATE_TIME'                          --数据推送时间
from 
 (select * from mreport_poultry.tmp_dmp_bird_drugs_comp_mm_03
 where op_month = '$OP_MONTH') t1 
left join 
 (select * from mreport_poultry.tmp_dmp_bird_drugs_comp_mm_05 
 where op_month = '$OP_MONTH') t2                   
    on t1.org_id = t2.org_id   
    and t1.bus_type = t2.bus_type 
    and t1.period_id = t2.period_id                          
left join mreport_global.dim_org_management t6 
    on t1.org_id=t6.org_id  
    and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
    on t1.org_id=t7.org_id 
    and t1.bus_type=t7.bus_type_id 
    and t7.attribute5='2'
left join                                                                                   
    (                                                                                       
        select * from mreport_global.dim_org_businesstype                                   
        where  level4_businesstype_name is not null) t5                                     --业态
     on  (t1.bus_type=t5.level4_businesstype_id)
;
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_01;
    $INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_01;
    $CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_02;
    $INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_02;
    $CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_03;
    $INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_03;
    $CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_04;
    $INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_04;
    $CREATE_TMP_DMP_BIRD_DRUGS_COMP_MM_05;
    $INSERT_TMP_DMP_BIRD_DRUGS_COMP_MM_05;
    $CREATE_DMP_BIRD_DRUGS_COMP_MM;
    $INSERT_DMP_BIRD_DRUGS_COMP_MM;
"  -v.sh                               
