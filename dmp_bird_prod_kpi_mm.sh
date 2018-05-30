#!/bin/bash
######################################################################
#                                                                    
# 程    序: dmp_bird_prod_kpi_mm.sh                               
# 创建时间: 2018年04月17日                                            
# 创 建 者: lj                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 生产指标表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_prod_kpi_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

#################################################################################################################
## 获取CW02表和物料的映射关系，取出苗种和项目单价
TMP_DMP_BIRD_PROD_KPI_MM_1='TMP_DMP_BIRD_PROD_KPI_MM_1'
CREATE_TMP_DMP_BIRD_PROD_KPI_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROD_KPI_MM_1(
    period_id              string                      --期间
   ,org_id                 string                      --ou_id 
   ,product_line           string                      --产线 
   ,bus_type               string                      --业态
   ,generation_name        string                      --系别
   ,seeds_cost             string                      --种苗成本总额
   ,used_eggs_cost         string                      --耗用种蛋成本总额
  )
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取CW02表和物料的映射关系，取出苗种和项目单价>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROD_KPI_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROD_KPI_MM_1 PARTITION(op_month='$OP_MONTH')
select   
d1.period_id,
d1.org_id,
d1.product_line,
d1.bus_type,
d1.level3_material_descr generation_name,
sum(nvl(d3.conversion_rate,1)*on_hatch_cost),
sum(nvl(loc_cost_amount01,0))
from 
(
select   
        coalesce(t1.org_id,m4.org_id,'') org_id
       ,coalesce(t1.period_id,m4.period_id,'') period_id
       ,coalesce(t1.product_line,m4.product_line,'') product_line
       ,coalesce(t1.bus_type,m4.bus_type,'') bus_type
       ,coalesce(t1.loc_currency_id,m4.CURRENCY_ID,'') loc_currency_id
       ,coalesce(t1.material_item_id,'') material_item_id
       ,(nvl(t1.loc_cost_amount01,0)+nvl(t1.loc_cost_amount02,0)+nvl(t1.loc_cost_amount03,0)+
         nvl(t1.loc_cost_amount04,0)+nvl(t1.loc_cost_amount05,0)+nvl(t1.loc_cost_amount06,0)+
         nvl(t1.loc_cost_amount07,0)+nvl(loc_cost_amount08,0)+nvl(loc_cost_amount09,0)+
         nvl(loc_cost_amount10,0)+nvl(loc_cost_amount11,0)+nvl(loc_cost_amount12,0)+
         nvl(loc_cost_amount13,0)+nvl(loc_cost_amount14,0)+nvl(loc_cost_amount15,0)+nvl(loc_cost_amount16,0)+nvl(loc_cost_amount17,0)+
         nvl(loc_cost_amount18,0)+nvl(loc_cost_amount19,0)+nvl(loc_cost_amount20,0)+nvl(loc_cost_amount21,0)+
         nvl(loc_cost_amount22,0)+nvl(loc_cost_amount23,0)+nvl(loc_cost_amount24,0)+nvl(loc_cost_amount25,0)+
         nvl(loc_cost_amount26,0)+nvl(loc_cost_amount27,0)+nvl(loc_cost_amount28,0)+nvl(loc_cost_amount29,0)+
         nvl(m4.seed_cost,0)+nvl(m4.packing_amount,0)+nvl(m4.direct_manual_amount,0)+nvl(m4.drugs_amount,0)+
         nvl(m4.depreciation_amount,0)+nvl(m4.expend_amount,0)+nvl(m4.water_electry_amount,0)+nvl(m4.indirect_amount,0)
         +nvl(m4.other_amount,0)+nvl(m4.byproduct_amount,0))       as on_hatch_cost            --种苗生产成本总额
        ,nvl(loc_cost_amount01,0)+nvl(m4.seed_cost,0)              as loc_cost_amount01
        ,coalesce(t2.level3_material_descr,m4.level3_material_descr) level3_material_descr
 from (select * from DWU_ZQ_REALITY_COST_SUBJECT_DD where op_day='$OP_DAY' 
 and substr(material_item_id,1,4) in (3506,3505))  t1 
 inner join 
     mreport_global.dim_material t2 
     on t1.material_item_id=t2.item_id 
     and t2.product_recordname='自产'
     and t2.level5_material_descr like '%雏%'
 full join (
 select
     period_id
    ,org_id
    ,org_name
    ,currency_id
    ,bus_type
    ,product_line
    ,category_id
    ,category_desc
    ,split(category_desc,'\\\\.')[2] level3_material_descr
    ,cost_type_id
    ,cost_type_desc
    ,seed_quantity
    ,seed_cost
    ,packing_amount
    ,direct_manual_amount
    ,drugs_amount
    ,depreciation_amount
    ,expend_amount
    ,water_electry_amount
    ,indirect_amount
    ,other_amount
    ,byproduct_amount
    ,manage_amount
    ,sale_amount
    ,financial_amount
 from DWU_ZQ_MANAGE_PRODUCT_COST_DD t1 where t1.op_day = '$OP_DAY' and cost_type_desc = '本月' 
 ) m4
         on t1.period_id = m4.period_id
        and t1.org_id = m4.org_id
        and t1.bus_type =m4.bus_type
        and t1.product_line = m4.product_line
        and t2.level3_material_descr=m4.level3_material_descr
) d1
left join   (  SELECT
                    from_currency,
                    to_currency,
                    conversion_rate,
                    conversion_period
                FROM
                    mreport_global.dmd_fin_period_currency_rate_mm
                WHERE to_currency='CNY') d3
    on d1.loc_currency_id =d3.from_currency 
   and d3.conversion_period=d1.period_id
 group by 
d1.org_id,
d1.period_id,
d1.product_line,
d1.bus_type,
d1.level3_material_descr
;
"
###########################################################################################
## 建立健雏数量表
## 变量声明
TMP_DMP_BIRD_PROD_KPI_MM_3='TMP_DMP_BIRD_PROD_KPI_MM_3'
CREATE_TMP_DMP_BIRD_PROD_KPI_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROD_KPI_MM_3(
     period_id              string                      --期间
   ,org_id                 string                      --ou_id 
   ,bus_type               string                      --业态
   ,product_line           string                      --产线 
   ,generation_name        string                      --系别描述
   ,big_good_a             string                      --商品代a大雏
   ,middle_good_a          string                      --商品代a中雏
   ,little_good_a          string                      --商品代a小雏
   ,good_b                 string                      --商品代b雏  
   ,middle_goob_b          string                      --商品代中b雏
   ,big_parent_a           string                      --父母代a大雏
   ,little_parent_a        string                      --父母代a小雏
   ,candling_fertile_egg   string                      --受精蛋
   ,hatch_egg_number       string                      --孵化合格蛋
   ,healthy_chicks_qty     string                      --健雏数量
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>分类对计算健雏，受精蛋数量求和>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROD_KPI_MM_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROD_KPI_MM_3 PARTITION(op_month='$OP_MONTH')
select 
     regexp_replace(substr(t1.nestling_date ,1,7),'-','') period_id
     ,t2.org_id
     ,t1.bus_type                
     ,t1.product_line                                                        
     ,t1.generation_name                                                     --系别描述
     ,sum(nvl(big_good_a,0)               ) big_good_a                                      --商品代a大雏
     ,sum(nvl(middle_good_a,0)            ) middle_good_a                                   --商品代a中雏
     ,sum(nvl(little_good_a,0)            ) little_good_a                                   --商品代a小雏
     ,sum(nvl(good_b,0)                   ) good_b                                          --商品代b雏  
     ,sum(nvl(middle_goob_b,0)            ) middle_goob_b                                   --商品代中b雏
     ,sum(nvl(big_parent_a,0)             ) big_parent_a                                    --父母代a大雏
     ,sum(nvl(little_parent_a,0)          ) little_parent_a                                 --父母代a小雏
     ,sum(nvl(candling_fertile_egg,0)     ) candling_fertile_egg                            --受精蛋
     ,sum(nvl(hatch_egg_number,0)         ) hatch_egg_number                                --孵化合格蛋
     ,sum(nvl(big_good_a,0)+nvl(middle_good_a,0)+nvl(little_good_a,0)+nvl(good_b,0)+nvl(middle_goob_b,0)+nvl(big_parent_a,0)+nvl(little_parent_a,0))  healthy_chicks_qty                  --健雏数量
  from (select * from dwu_zq_hatch_dd where op_day = '$OP_DAY')  t1
   inner join mreport_global.dim_org_management t2 
   on substr(t1.big_batch_no ,1,4) =t2.level6_org_id and t1.bus_type=t2.bus_type_id
  group by 
     regexp_replace(substr(t1.nestling_date ,1,7),'-','')
     ,t2.org_id
     ,t1.bus_type
     ,t1.product_line
     ,t1.generation_name
"

######################################################################################
## 月蛋数量统计
## 变量声明
TMP_DMP_BIRD_PROD_KPI_MM_7='TMP_DMP_BIRD_PROD_KPI_MM_7'
CREATE_TMP_DMP_BIRD_PROD_KPI_MM_7="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROD_KPI_MM_7(
           period_id                 string                             --期间    
          ,org_id                    string                             --ou_id 
          ,bus_type                  string                             --业态
          ,product_line              string                             --产线
          ,livestock_qty             string                             --存栏数量
          ,eggs_qty                  string                             --本月产蛋总数
          ,std_eggs_qty              string                             --产合格蛋数
          ,before_eggs_qty           string                             --产蛋期本月产蛋总数
          ,before_std_eggs_qty       string                             --产蛋期产合格蛋数
          ,std_eggs_cost             string                             --种蛋成本总额
          ,generation_name           string                             --系别
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>月蛋数量统计>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROD_KPI_MM_7="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROD_KPI_MM_7 PARTITION(op_month='$OP_MONTH')
select  
      substr(t1.period_id,1,6)                                --期间    
     ,t1.org_id                                               --ou_id 
     ,t1.bus_type                                             --业态
     ,t1.product_line                                         --产线
     ,sum(case when stage = '产蛋期' then nvl(day_end_num_f,0) else 0 end)                        --存栏                                                          
     ,sum(nvl(t1.qualified_egg,0) + nvl(t1.double_egg,0) + nvl(t1.broken_egg,0) + nvl(t1.deformity_egg,0))                                                        --本月产蛋总数
     ,sum(nvl(t1.qualified_egg,0) - nvl(t1.fh_check,0))                                                                                                           --产合格蛋数 
     ,sum(case when stage = '产蛋期' then nvl(t1.qualified_egg,0) + nvl(t1.double_egg,0) + nvl(t1.broken_egg,0) + nvl(t1.deformity_egg,0) else 0 end)           --产蛋期的产蛋总数
     ,sum(case when stage = '产蛋期' then nvl(t1.qualified_egg,0)-nvl(t1.fh_check,0) else   0 end)                                                              --产蛋期的合格蛋总数
     ,sum(case when stage = '产蛋期' then (nvl(t1.qualified_egg,0)-nvl(t1.fh_check,0)) * nvl(t2.cost_amount_t,0) else 0 end)                                     --种蛋成本总额 
     ,dict_item_name_xb generation_name
from (select * from DWU_QYZ_NLBP_BIRD_PRODFEED_HEAD_DD where op_day = '$OP_DAY')  t1 
left join dwu_finance_cost_pric t2 
  on t1.org_id = t2.org_id       
  and substr(t1.period_id,1,6) = t2.period_id     --加期间为null  
  and t1.qualified_egg_code = t2.MATERIAL_ITEM_ID
 group by 
      substr(t1.period_id,1,6)                                --期间    
     ,t1.org_id                                               --ou_id 
     ,t1.bus_type                                             --业态
     ,t1.product_line                                         --产线
     ,dict_item_name_xb
 ;
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PROD_KPI_MM='DMP_BIRD_PROD_KPI_MM'
CREATE_DMP_BIRD_PROD_KPI_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PROD_KPI_MM(
    month_id                    string    --期间(月)
   ,day_id                      string    --期间(日)
   ,level1_org_id               string    --组织1级(股份)  
   ,level1_org_descr            string    --组织1级(股份)  
   ,level2_org_id               string    --组织2级(片联)  
   ,level2_org_descr            string    --组织2级(片联)  
   ,level3_org_id               string    --组织3级(片区)  
   ,level3_org_descr            string    --组织3级(片区)  
   ,level4_org_id               string    --组织4级(小片)  
   ,level4_org_descr            string    --组织4级(小片)  
   ,level5_org_id               string    --组织5级(公司)  
   ,level5_org_descr            string    --组织5级(公司)  
   ,level6_org_id               string    --组织6级(ou)  
   ,level6_org_descr            string    --组织6级(ou)  
   ,level7_org_id               string    --组织7级(库存组织)
   ,level7_org_descr            string    --组织7级(库存组织)
   ,level1_businesstype_id      string    --业态1级
   ,level1_businesstype_name    string    --业态1级
   ,level2_businesstype_id      string    --业态2级
   ,level2_businesstype_name    string    --业态2级
   ,level3_businesstype_id      string    --业态3级
   ,level3_businesstype_name    string    --业态3级
   ,level4_businesstype_id      string    --业态4级
   ,level4_businesstype_name    string    --业态4级
   ,production_line_id          string    --产线id
   ,production_line_descr       string    --产线名称
   ,level1_material_id          string    --物料1级id
   ,level1_material_descr       string    --物料1级描述
   ,level2_material_id          string    --物料2级id
   ,level2_material_descr       string    --物料2级描述
   ,level3_material_id          string    --物料3级id
   ,level3_material_descr       string    --物料3级描述
   ,level4_material_id          string    --物料4级id
   ,level4_material_descr       string    --物料4级描述
   ,kpi_type_id                 string    --指标类型
   ,kpi_type_descr              string    --指标类型
   ,livestock_qty               string    --存栏数量
   ,eggs_qty                    string    --本月产蛋总数
   ,std_eggs_qty                string    --产合格蛋数
   ,before_eggs_qty             string    --产蛋期本月产蛋总数
   ,before_std_eggs_qty         string    --产蛋期产合格蛋数
   ,fertilized_eggs_qty         string    --受精蛋数
   ,used_aggs_qty               string    --耗用种蛋数量
   ,healthy_chicks_qty          string    --健雏数量
   ,std_eggs_cost               string    --种蛋成本总额
   ,used_eggs_cost              string    --耗用种蛋成本总额
   ,seeds_cost                  string    --种苗成本总额
   ,create_time                 string    --数据推送时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PROD_KPI_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_PROD_KPI_MM PARTITION(op_month='$OP_MONTH')
select                                        --zq01
        t1.month_id                           --month_id
       ,'' as  day_id  
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
       ,''                                    --组织7级
       ,''                                    --组织7级
       ,t10.level1_businesstype_id             --业态1级
       ,t10.level1_businesstype_name           --业态1级
       ,t10.level2_businesstype_id             --业态2级
       ,t10.level2_businesstype_name           --业态2级
       ,t10.level3_businesstype_id             --业态3级
       ,t10.level3_businesstype_name           --业态3级
       ,t10.level4_businesstype_id             --业态4级
       ,t10.level4_businesstype_name           --业态4级
       ,case when t1.product_line = '10' then  '1'          
            when t1.product_line = '20' then  '2' 
        else -1        end     
      ,case when t1.product_line = '10' then  '鸡线'          
            when t1.product_line = '20' then  '鸭线' 
        else '缺省'        end                           --产线  
       ,t1.level1_material_id                                     --物料1级id   
       ,t1.level1_material_descr                                  --物料1级描述
       ,t1.level2_material_id                                     --物料2级id
       ,t1.level2_material_descr                                  --物料2级描述
       ,t1.level3_material_id                                     --物料3级id
       ,t1.level3_material_descr                                  --物料3级描述
       ,''                                                        --物料4级id                   
       ,''                                                        --物料4级描述
       ,''                                                        --kpi_type_id   指标类型
       ,''                                                        --kpi_type_descr  指标类型
       ,t1.livestock_qty                                             --存栏
       ,case when t1.eggs_qty is null then 0 else  t1.eggs_qty end                                                 --本月产蛋总数
       ,case when t1.std_eggs_qty is null  then 0 else t1.std_eggs_qty end                                         --产合格蛋数 
       ,case when t1.before_eggs_qty  is null then 0 else  t1.before_eggs_qty end                                  --预产期本月产蛋总数
       ,case when t1.before_std_eggs_qty is null then 0 else  t1.before_std_eggs_qty end                           --预产期产合格蛋数
       ,case when t1.fertilized_eggs_qty  is null then 0  else t1.fertilized_eggs_qty end                          --受精蛋数
       ,case when t1.used_aggs_qty is null then 0 else t1.used_aggs_qty end                                        --耗用种蛋数量
       ,case when t1.healthy_chicks_qty  is null then 0 else t1.healthy_chicks_qty end                             --健雏数量
       ,case when t1.std_eggs_cost is null then 0 else t1.std_eggs_cost end                                        --种蛋成本总额
       ,case when t1.used_eggs_cost is null then 0 else t1.used_eggs_cost end                                      --耗用种蛋成本总额
       ,case when t1.seeds_cost  is null then 0 else t1.seeds_cost end                                             --种苗成本总额
      ,'$CREATE_TIME'       as  create_time                       --数据推送时间
from 
(  select 
     coalesce(m1.month_id,m3.period_id,m4.period_id) month_id
    ,coalesce(m3.org_id,m4.org_id,'') org_id
    ,coalesce(m3.bus_type,m4.bus_type,'') bus_type
    ,coalesce(m3.product_line,m4.product_line,'') product_line
    ,'' as level1_material_id                                 --物料1级id   
    ,'' as level1_material_descr                              --物料1级描述
    ,'' as level2_material_id                                 --物料2级id
    ,'' as level2_material_descr                              --物料2级描述
    ,'' as level3_material_id                                 --物料3级id
    ,coalesce(m3.generation_name,m4.generation_name)   as  level3_material_descr          -- 系别
    ,m3.livestock_qty    as  livestock_qty                    --存栏       
    ,m3.eggs_qty         as  eggs_qty                         --本月产蛋总数       
    ,m3.std_eggs_qty     as  std_eggs_qty                     --产合格蛋数  
    ,m3.before_eggs_qty       as   before_eggs_qty              --产蛋期本月产蛋总数
    ,m3.before_std_eggs_qty   as   before_std_eggs_qty          --产蛋期产合格蛋数
    ,m3.candling_fertile_egg  as  fertilized_eggs_qty              --受精蛋数
    ,m3.hatch_egg_number      as  used_aggs_qty                    --耗用种蛋数量
    ,m3.healthy_chicks_qty    as  healthy_chicks_qty               --健雏数量    
    ,m3.std_eggs_cost         as  std_eggs_cost                    --种蛋成本总额
    ,m4.used_eggs_cost        as  used_eggs_cost                   --耗用种蛋成本总额
    ,m4.seeds_cost            as  seeds_cost                       --种苗成本总额            
 from mreport_global.dim_month  m1
inner join (
    SELECT
         COALESCE(dm1.period_id,dm2.period_id)  period_id,
         COALESCE(dm1.org_id,dm2.org_id)             org_id,
         COALESCE(dm1.bus_type,dm2.bus_type)         bus_type,
         COALESCE(dm1.product_line,dm2.product_line) product_line,
         COALESCE(dm1.generation_name,dm2.generation_name) generation_name,
         COALESCE(dm1.livestock_qty,0)        livestock_qty,
         COALESCE(dm1.eggs_qty,0)             eggs_qty,
         COALESCE(dm1.std_eggs_qty,0)         std_eggs_qty,
         COALESCE(dm1.before_eggs_qty,0)      before_eggs_qty,
         COALESCE(dm1.before_std_eggs_qty,0)  before_std_eggs_qty,
         COALESCE(dm2.candling_fertile_egg,0) candling_fertile_egg,
         COALESCE(dm2.hatch_egg_number,0)     hatch_egg_number,
         COALESCE(dm2.healthy_chicks_qty,0)   healthy_chicks_qty,
         COALESCE(dm1.std_eggs_cost,0)        std_eggs_cost
     FROM
         (
             SELECT
                 *
             FROM
                 tmp_dmp_bird_prod_kpi_mm_7
             WHERE
                 op_month='$OP_MONTH') dm1
     FULL JOIN
         (
             SELECT
                 *
             FROM
                 tmp_dmp_bird_prod_kpi_mm_3
             WHERE
                 op_month='$OP_MONTH') dm2
     ON
         dm1.period_id = dm2.period_id
     AND dm1.org_id = dm2.org_id
     AND dm1.bus_type = dm2.bus_type
     AND dm1.product_line = dm2.product_line
     AND dm1.generation_name = dm2.generation_name
) m3  
 on  m1.month_id  = m3.period_id           
full join   
(select * from tmp_dmp_bird_prod_kpi_mm_1
    where op_month='$OP_MONTH') m4 
 on  m3.period_id = m4.period_id
 and m3.org_id = m4.org_id
 and m3.bus_type = m4.bus_type
 and m3.product_line = m4.product_line
 and m3.generation_name = m4.generation_name
) t1
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
        where  level4_businesstype_name is not null) t10
     on (t1.bus_type=t10.level4_businesstype_id) --业态
    ;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PROD_KPI_MM_1;
    $INSERT_TMP_DMP_BIRD_PROD_KPI_MM_1;
    $CREATE_TMP_DMP_BIRD_PROD_KPI_MM_3;
    $INSERT_TMP_DMP_BIRD_PROD_KPI_MM_3;
    $CREATE_TMP_DMP_BIRD_PROD_KPI_MM_7;
    $INSERT_TMP_DMP_BIRD_PROD_KPI_MM_7;
    $CREATE_DMP_BIRD_PROD_KPI_MM;
    $INSERT_DMP_BIRD_PROD_KPI_MM;

"  -v.sh                               