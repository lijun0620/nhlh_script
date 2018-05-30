#!/bin/bash
######################################################################
#                                                                    
# 程    序: dmp_bird_prod_effi_mm.sh                               
# 创建时间: 2018年04月17日                                            
# 创 建 者: lj                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 生产效率表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_prod_effi_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 统计月总和蛋数，健雏
TMP_DMP_BIRD_PROD_EFFI_MM_01='TMP_DMP_BIRD_PROD_EFFI_MM_01'
CREATE_TMP_DMP_BIRD_PROD_EFFI_MM_01="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROD_EFFI_MM_01(
         period_id                   string --期间
        ,org_id                     string --ou组织
        ,bus_type                   string --业态
        ,product_line               string --生产线
        ,farm_id                    string --养殖场id
        ,farm_name                  string --养殖场
        ,big_batch_no               string --大批次no
        ,big_batch_name             string --大批次name
        ,qualified_egg              string --养殖合格蛋(月总和)
        ,fh_check                   string --fh验收商品蛋(月总和)
        ,standard_prod_qualified    string --合格蛋数量（标准）(月总和)
        ,real_eggs_qty              string --实际验收合格蛋数             
        ,real_healthy_chicks_qty    string --实际健雏数量
        ,big_good_a_standard        string --标准健雏
        ,std_eggs_cost              string --种蛋总成本
  )
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>统计月总和蛋数，健雏>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROD_EFFI_MM_01="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROD_EFFI_MM_01 PARTITION(op_month='$OP_MONTH')
select
    substr(t1.period_id,1,6)                                        --期间
    ,t1.org_id                                                      --ou组织
    ,t1.bus_type                                                    --业态
    ,t1.product_line                                                --生产线
    ,t1.farm_id                                                     --养殖场id   
    ,t1.farm_name                                                   --养殖场
    ,t1.big_batch_no                                                --大批次no
    ,t1.big_batch_name                                              --大批次name
    ,t1.qualified_egg                                               --养殖合格蛋 
    ,t1.fh_check                                                    --fh验收商品蛋 
    ,t1.standard_prod_qualified                                     --合格蛋数量（标准）
    ,t1.qualified_egg-t1.fh_check                                   --实际验收合格蛋数
    ,(t2.big_good_a+ t2.middle_good_a+t2.little_good_a+t2.good_b+t2.middle_goob_b+t2.big_parent_a+t2.little_parent_a)    --实际健雏数量
    ,t2.big_good_a_standard                                         --标准健雏
    ,t1.std_eggs_cost                                               --种蛋总成本(元)
from (
 select
     substr(m1.period_id,1,6)   period_id                        --期间
     ,m1.org_id                                                  --ou组织
     ,m1.bus_type                                                --业态
     ,m1.product_line                                            --生产线            
     ,substr(m1.big_batch_no,5,4) farm_id                        --养殖场id 
     ,m1.farm_name                                               --养殖场
     ,m1.big_batch_no                                            --大批次no
     ,m1.big_batch_name
     ,sum(coalesce (qualified_egg          ,0))  qualified_egg                                --养殖合格蛋 
     ,sum(coalesce (fh_check               ,0))       fh_check                                --fh验收商品蛋 
     ,sum(coalesce (standard_prod_qualified,0))  standard_prod_qualified                      --合格蛋数量（标准） 
     ,sum((coalesce (m1.qualified_egg,0) - coalesce (m1.fh_check,0))* coalesce (m2.cost_amount_t,0))  std_eggs_cost     --种蛋总成本(元)
 from (select * from mreport_poultry.dwu_qyz_nlbp_bird_prodfeed_head_dd  where op_day = '$OP_DAY') m1
 left join dwu_finance_cost_pric m2
   on m2.org_id = m1.org_id
   and m2.period_id = substr(m1.period_id,1,6) 
   and m2.material_item_id=m1.qualified_egg_code
 group by 
     substr(m1.period_id,1,6)                                    --期间
     ,m1.org_id                                                  --ou组织
     ,m1.bus_type                                                --业态
     ,m1.product_line                                            --生产线
     ,substr(m1.big_batch_no,5,4)                                --养殖场id 
     ,m1.farm_name                                               --养殖场
     ,m1.big_batch_no                                            --大批次no
     ,m1.big_batch_name
)  t1                 --zq01 
left join (
  select
      substr(period_id,1,6)  period_id                            --期间
      ,d2.org_id                                                  --ou组织
      ,d1.bus_type                                                --业态
      ,d1.product_line                                            --生产线
      ,d1.big_batch_no                                            --大批次no
      ,substr(d1.big_batch_no,5,4) farm_id                        --养殖场id 
      ,sum(coalesce (big_good_a     ,0))     big_good_a                      --商品代a大雏
      ,sum(coalesce (middle_good_a  ,0))     middle_good_a                   --商品代a中雏
      ,sum(coalesce (little_good_a  ,0))     little_good_a                   --商品代a小雏
      ,sum(coalesce (good_b         ,0))     good_b                          --商品代b雏  
      ,sum(coalesce (middle_goob_b  ,0))     middle_goob_b                   --商品代中b雏
      ,sum(coalesce (big_parent_a   ,0))     big_parent_a                    --父母代a大雏
      ,sum(coalesce (little_parent_a,0))     little_parent_a                 --父母代a小雏
      ,max(coalesce (big_good_a_standard,0))     big_good_a_standard         --标准健雏
  from (select * from mreport_poultry.dwu_zq_hatch_dd where op_day = '$OP_DAY') d1
  inner join mreport_global.dim_org_management d2
   on substr(d1.big_batch_no ,1,4) =d2.level6_org_id and d1.bus_type=d2.bus_type_id  
  group by 
      substr(period_id,1,6)                                       --期间
      ,d2.org_id                                                  --ou组织
      ,d1.bus_type                                                --业态
      ,d1.product_line                                            --生产线
      ,d1.big_batch_no                                            --大批次no
      ,substr(d1.big_batch_no,5,4)                                --养殖场id 
) t2                                                         
on t1.period_id = t2.period_id
and t1.org_id =t2.org_id
and t1.big_batch_no =t2.big_batch_no
and t1.bus_type = t2.bus_type
and t1.product_line = t2.product_line
and t1.farm_id = t2.farm_id
;
"

###########################################################################################
##将小批次号求和
TMP_DMP_BIRD_PROD_EFFI_MM_02='TMP_DMP_BIRD_PROD_EFFI_MM_02'
CREATE_TMP_DMP_BIRD_PROD_EFFI_MM_02="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROD_EFFI_MM_02(
         period_id                  string --期间
        ,org_id                     string --ou组织
        ,bus_type                   string --业态
        ,product_line               string --生产线
        ,farm_id                    string --养殖场id
        ,big_batch_no               string --大批次no
        ,day_end_num_fds            string --母定舍数
  )
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>计算大批次下的母定舍数总和>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROD_EFFI_MM_02="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROD_EFFI_MM_02 PARTITION(op_month='$OP_MONTH')
select  
      period_id                                               --期间
     ,org_id                                                  --ou组织
     ,bus_type                                                --业态
     ,product_line                                            --生产线
     ,farm_id                                                 --养殖场id 
     ,big_batch_no                                            --大批次no
     ,sum(day_end_num_fds) day_end_num_fds                    --母定舍数
from (
   select 
      day_end_num_fds
     ,batch_code
     ,substr(period_id,1,6) period_id                                    --期间
     ,org_id                                                  --ou组织
     ,bus_type                                                --业态
     ,product_line                                            --生产线
     ,substr(big_batch_no,5,4)  farm_id                       --养殖场id 
     ,big_batch_no                                            --大批次
   from mreport_poultry.dwu_qyz_nlbp_bird_prodfeed_head_dd 
   where op_day='$OP_DAY' 
   group by 
      substr(period_id,1,6)                                    --期间
     ,org_id                                                  --ou组织
     ,bus_type                                                --业态
     ,product_line                                            --生产线
     ,substr(big_batch_no,5,4)                                --养殖场id 
     ,big_batch_no                                            --大批次no
     ,batch_code
     ,day_end_num_fds) d1 
group by 
     period_id                                    --期间
     ,org_id                                                  --ou组织
     ,bus_type                                                --业态
     ,product_line                                            --生产线
     ,farm_id                                                 --养殖场id 
     ,big_batch_no                                            --大批次no
;
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PROD_EFFI_MM='DMP_BIRD_PROD_EFFI_MM'
CREATE_DMP_BIRD_PROD_EFFI_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PROD_EFFI_MM(
       month_id                   string         --期间(月份)
      ,day_id                     string         --期间(日)
      ,level1_org_id              string         --组织1级(股份)
      ,level1_org_descr           string         --组织1级(股份)
      ,level2_org_id              string         --组织2级(片联)
      ,level2_org_descr           string         --组织2级(片联)
      ,level3_org_id              string         --组织3级(片区)
      ,level3_org_descr           string         --组织3级(片区)
      ,level4_org_id              string         --组织4级(小片)
      ,level4_org_descr           string         --组织4级(小片)
      ,level5_org_id              string         --组织5级(公司)
      ,level5_org_descr           string         --组织5级(公司)
      ,level6_org_id              string         --组织6级(ou)
      ,level6_org_descr           string         --组织6级(ou)
      ,level7_org_id              string         --组织7级(库存组织)
      ,level7_org_descr           string         --组织7级(库存组织)
      ,level1_businesstype_id     string         --业态1级
      ,level1_businesstype_name   string         --业态1级
      ,level2_businesstype_id     string         --业态2级
      ,level2_businesstype_name   string         --业态2级
      ,level3_businesstype_id     string         --业态3级
      ,level3_businesstype_name   string         --业态3级
      ,level4_businesstype_id     string         --业态4级
      ,level4_businesstype_name   string         --业态4级
      ,production_line_id         string         --产线
      ,production_line_descr      string         --产线
      ,batch_id                   string         --批次号
      ,bird_house_qty             string         --转产母定舍数量(个)
      ,std_eggs_qty               string         --标准验收合格蛋数
      ,real_eggs_qty              string         --实际验收合格蛋数
      ,std_eggs_cost              string         --标准种蛋成本(元)
      ,real_eggs_cost             string         --实际种蛋成本(元)
      ,std_healthy_chicks_qty     string         --标准健雏数量
      ,real_healthy_chicks_qty    string         --实际健雏数量
      ,create_time                string         --数据推送时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
;
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PROD_EFFI_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_PROD_EFFI_MM PARTITION(op_month='$OP_MONTH')
select 
      t1.period_id                                --期间(月份)
      ,''                                         --期间(日)
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
      ,t1.farm_id                                 --level7_org_id--组织7级(库存组织)     
      ,t1.farm_name                               --level7_org_descr--组织7级(库存组织)  
      ,t5.level1_businesstype_id                  --业态1级
      ,t5.level1_businesstype_name                --业态1级
      ,t5.level2_businesstype_id                  --业态2级
      ,t5.level2_businesstype_name                --业态2级
      ,t5.level3_businesstype_id                  --业态3级
      ,t5.level3_businesstype_name                --业态3级
      ,t5.level4_businesstype_id                  --业态4级
      ,t5.level4_businesstype_name                --业态4级
      ,case when t1.product_line = '10' then '1'          
            when t1.product_line = '20' then  '2' 
            else '-1'     end       --产线ID
      ,case when t1.product_line = '10' then '鸡线'          
            when t1.product_line = '20' then  '鸭线' 
            else '缺省'     end     --产线  
      ,t1.big_batch_name                           --批次name
      ,t2.day_end_num_fds                         --转产母定舍数量(个)
      ,t1.standard_prod_qualified                 --标准验收合格蛋数
      ,t1.real_eggs_qty                           --实际验收合格蛋数
      ,t1.std_eggs_cost                           --标准种蛋总成本(元)
      ,t1.std_eggs_cost                           --实际种蛋总成本(元)
      ,t1.big_good_a_standard                     --标准健雏数量
      ,t1.real_healthy_chicks_qty                 --实际健雏数量
      ,'$CREATE_TIME'                             --数据推送时间
from 
(select * from mreport_poultry.tmp_dmp_bird_prod_effi_mm_01 
  where op_month = '$OP_MONTH') t1 
left join (select * from mreport_poultry.tmp_dmp_bird_prod_effi_mm_02
   where op_month = '$OP_MONTH') t2 
   on  t1.period_id       = t2.period_id     
   and t1.org_id          = t2.org_id        
   and t1.bus_type        = t2.bus_type      
   and t1.product_line    = t2.product_line  
   and t1.farm_id         = t2.farm_id       
   and t1.big_batch_no  = t2.big_batch_no
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
where  day_end_num_fds+standard_prod_qualified+real_eggs_qty+std_eggs_cost+big_good_a_standard+real_healthy_chicks_qty > 0 
;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PROD_EFFI_MM_01;
    $INSERT_TMP_DMP_BIRD_PROD_EFFI_MM_01;
    $CREATE_TMP_DMP_BIRD_PROD_EFFI_MM_02;
    $INSERT_TMP_DMP_BIRD_PROD_EFFI_MM_02;
    $CREATE_DMP_BIRD_PROD_EFFI_MM;
    $INSERT_DMP_BIRD_PROD_EFFI_MM;

"  -v.sh                               