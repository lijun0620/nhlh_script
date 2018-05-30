#!/bin/bash

######################################################################
#                                                                    
# 创建时间: 2018年04月25日                                            
# 创 建 者: khz                                                      
# 参数:     dmp_bird_seeds_cost_mm.sh                                                         
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 种苗生产成本
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6} 

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_seeds_cost_mm.sh 20171201"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## ZQ02和物料,物料类别对应
TMP_DMP_BIRD_SEEDS_COST_MM_1='TMP_DMP_BIRD_SEEDS_COST_MM_1'

CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SEEDS_COST_MM_1(
    org_id                               string,            --OU_ID                                 
    bus_type                             string,
    product_line                         string,
    period_id                            string,            --时间
    level1_material_id                   string,   
    level1_material_descr                string,
    level2_material_id                   string,   
    level2_material_descr                string,
    level3_material_id                   string,   
    level3_material_descr                string,
    seeds_production_cost                string,    --种苗生产成本总额(元)
    par_seeds_cost                       string,    --苗种成本总额(元)
    packing_material_cost                string,    --包装材料(元)
    direct_labor_cost                    string,    --直接人工(元)
    drugs_cost                           string,    --药品成本(元)
    seeds_mf_rental_cost                 string,    --农业制造费用-折旧租赁费总额
    seeds_mf_consum_cost                 string,    --农业制造费用-能耗费总额
    seeds_mf_water_power_cost            string,    --农业制造费用- 水电费总额
    seeds_mf_indirect_labor_cost         string,    --农业制造费用-间接人工总额
    seeds_mf_other_cost                  string,    --农业制造费用-其他总额
    seeds_byproduct_income               string     --种苗成本-副产品收入总额    
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>将多种物料从行映射为列>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SEEDS_COST_MM_1 PARTITION(op_month='$OP_MONTH')
    SELECT
        m1.org_id,
        m1.bus_type,
        m1.product_line,
        m1.period_id,
         m2.material_segment1_id,    
        m2.material_segment1_desc, 
        m2.material_segment2_id,    
        m2.material_segment2_desc, 
        m2.material_segment3_id,    
        m2.material_segment3_desc,
        sum((coalesce(loc_cost_amount01,0)+coalesce(loc_cost_amount02,0) +coalesce(loc_cost_amount03,0)
        +coalesce(loc_cost_amount04,0) +coalesce(loc_cost_amount05,0)+coalesce(loc_cost_amount06,0) +
        coalesce(loc_cost_amount07,0)+coalesce(loc_cost_amount08,0) +coalesce(loc_cost_amount09,0)+
        coalesce(loc_cost_amount10,0) +coalesce(loc_cost_amount11,0)+coalesce(loc_cost_amount12,0) +
        coalesce(loc_cost_amount13,0)+coalesce(loc_cost_amount14,0) +coalesce(loc_cost_amount15,0)+
        coalesce(loc_cost_amount16,0) +coalesce(loc_cost_amount17,0)+coalesce(loc_cost_amount18,0) +
        coalesce(loc_cost_amount19,0)+coalesce(loc_cost_amount20,0) +coalesce(loc_cost_amount21,0)+
        coalesce(loc_cost_amount22,0) +coalesce(loc_cost_amount23,0)+coalesce(loc_cost_amount24,0) +
        coalesce(loc_cost_amount25,0)+coalesce(loc_cost_amount26,0) +coalesce(loc_cost_amount27,0)+
        coalesce(loc_cost_amount28,0) +coalesce(loc_cost_amount29,0))*coalesce(m3.conversion_rate,1)) seeds_production_cost,
        sum(coalesce(m3.conversion_rate,1)*coalesce(loc_cost_amount01,0)) par_seeds_cost,
        sum(coalesce(m3.conversion_rate,1)*coalesce(loc_cost_amount29,0)) packing_material_cost,
        sum(coalesce(m3.conversion_rate,1)*coalesce(loc_cost_amount02,0)) direct_labor_cost,
        sum(coalesce(m3.conversion_rate,1)*coalesce(loc_cost_amount04,0)) drugs_cost,
        sum(coalesce(m3.conversion_rate,1)*(coalesce(loc_cost_amount12,0)+coalesce(loc_cost_amount13,0))) seeds_mf_rental_cost,
        sum(coalesce(m3.conversion_rate,1)*(coalesce(loc_cost_amount23,0) + coalesce(loc_cost_amount25,0)+coalesce(loc_cost_amount27,0))) seeds_mf_consum_cost,
        sum (coalesce(m3.conversion_rate,1)*(coalesce(loc_cost_amount24,0)+coalesce(loc_cost_amount26,0)))   seeds_mf_water_power_cost,
        sum(coalesce(m3.conversion_rate,1)*coalesce(loc_cost_amount06,0)) seeds_mf_indirect_labor_cost,
        sum((coalesce(loc_cost_amount07,0)+coalesce(loc_cost_amount08,0) +coalesce(loc_cost_amount09,0)
        +coalesce(loc_cost_amount10,0) +coalesce(loc_cost_amount11,0)+ coalesce(loc_cost_amount14,0) +
        coalesce(loc_cost_amount15,0)+coalesce(loc_cost_amount16,0) + coalesce(loc_cost_amount17,0)+
        coalesce(loc_cost_amount18,0) +coalesce(loc_cost_amount19,0)+ coalesce(loc_cost_amount20,0) +
        coalesce(loc_cost_amount21,0)+coalesce(loc_cost_amount22,0)) * coalesce(m3.conversion_rate,1)) seeds_mf_other_cost,
        sum(coalesce(m3.conversion_rate,1)* coalesce(loc_cost_amount28,0)) seeds_byproduct_income
    FROM
        dwu_zq_reality_cost_subject_dd m1
   INNER JOIN
       mreport_global.dwu_dim_material_new m2
    ON
        m1.inventory_item_id =m2.inventory_item_id
    AND m2.material_segment5_desc LIKE '%雏%'
    AND m1.bus_type IN('132011','132012')
    AND m2.product_recordname in('自产')
    AND op_day='$OP_DAY'
    LEFT JOIN
        (
            SELECT
                from_currency,
                to_currency,
                conversion_rate,
                conversion_period
            FROM
                mreport_global.dmd_fin_period_currency_rate_mm
            WHERE
                to_currency='CNY') m3
    ON
        m1.loc_currency_id =m3.from_currency
    AND m3.conversion_period=m1.period_id
    group by  
  m1.org_id,
        m1.bus_type,
        m1.product_line,
        m1.period_id,
        m2.material_segment1_id,    
        m2.material_segment1_desc, 
        m2.material_segment2_id,    
        m2.material_segment2_desc, 
        m2.material_segment3_id,    
        m2.material_segment3_desc
;
"

###########################################################################################
## 获取CW02表和物料的映射关系
TMP_DMP_BIRD_SEEDS_COST_MM_2='TMP_DMP_BIRD_SEEDS_COST_MM_2'

CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SEEDS_COST_MM_2(
    org_id                               string,            --OU_ID                                 
    bus_type                             string,
    period_id                            string,            --时间
    seeds_qty                            string,            --种苗产量(只)
    management_cost                      string,            --管理费用(元)
    sales_change_cost                    string,            --销售费用-变动(元)
    sales_fixed_cost                     string,            --销售费用-固定(元)
    financing_cost                       string,            --财务费用(元)
    level1_material_id                   string,            --物料1级id
    level2_material_id                   string,            --物料2级id
    level3_material_id                   string,             --物料3级id
	generation_name                      string             --系别
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>将多种物料从行映射为列>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SEEDS_COST_MM_2 PARTITION(op_month='$OP_MONTH')
 select 
  t2.org_id,             
  t2.bus_type,           
  t2.period_id,          
  t2.healthy_chicks_qty seeds_qty,          
  t1.management_cost,    
  t1.sales_change_cost,  
  t1.sales_fixed_cost,   
  t1.financing_cost,     
  t1.level1_material_id, 
  t1.level2_material_id, 
  t1.level3_material_id,
  t2.generation_name   
from 
    (
   select 
     regexp_replace(substr(dw1.nestling_date ,1,7),'-','') period_id
     ,dw2.org_id
     ,dw1.bus_type                                                                     
     ,dw1.generation_name                                                     --系别描述
     ,sum(coalesce(big_good_a,0)+coalesce(middle_good_a,0)+coalesce(little_good_a,0)+coalesce(good_b,0)+coalesce(middle_goob_b,0)+coalesce(big_parent_a,0)+coalesce(little_parent_a,0))  healthy_chicks_qty      --健雏数量
  from (select * from dwu_zq_hatch_dd where op_day = '$OP_DAY')  dw1
   inner join mreport_global.dim_org_management dw2 
   on substr(dw1.big_batch_no ,1,4) =dw2.level6_org_id and dw1.bus_type=dw2.bus_type_id
  group by 
     regexp_replace(substr(dw1.nestling_date ,1,7),'-','')
     ,dw2.org_id
     ,dw1.bus_type
     ,dw1.generation_name
) t2 
LEFT join
   (SELECT
       d1.org_id,
       d1.bus_type,
       substr(d1.period_id,1,6) period_id,
       sum(coalesce(d1.admini_expense,0))         management_cost,    --管理费用(元)  
       sum(coalesce(d1.selling_expense_change,0)) sales_change_cost,  --销售费用-变动(元)
       sum(coalesce(d1.selling_expense_fixed,0))  sales_fixed_cost,   --销售费用-固定(元)
       sum(coalesce(d1.fin_expense,0))            financing_cost,     --财务费用(元)
       d2.material_segment1_id level1_material_id,  
	   d2.material_segment2_id level2_material_id,  
	   d2.material_segment3_id level3_material_id,  
	   d2.material_segment3_desc level3_material_descr
   FROM
       (
           SELECT
               *
           FROM
               dmd_fin_exps_profits
           WHERE
               1=1
           AND currency_type='3' and bus_type IN('132011','132012')) d1
   INNER JOIN
    mreport_global.dwu_dim_material_new d2
    ON
        d1.inventory_item_id =d2.inventory_item_id
    AND d2.material_segment5_desc LIKE '%雏%'
    AND d1.bus_type IN('132011','132012')
    AND d2.product_recordname in('自产')
   GROUP BY
       d1.org_id,
       d1.bus_type,
       substr(d1.period_id,1,6),
	   d2.material_segment1_id,  
	   d2.material_segment2_id,  
	   d2.material_segment3_id,  
	   d2.material_segment3_desc
	   ) t1
 on t1.org_id=t2.org_id and t1.bus_type=t2.bus_type 
 and t1.level3_material_descr=t2.generation_name
 and t1.period_id=t2.period_id
"
###########################################################################################
## 获取ZQ08表和ZQ09的映射关系
TMP_DMP_BIRD_SEEDS_COST_MM_3='TMP_DMP_BIRD_SEEDS_COST_MM_3'

CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SEEDS_COST_MM_3(
    org_id                               string,            --OU_ID                                 
    bus_type                             string,
    period_id                            string,            --时间
    seeds_qty                            string,            --种苗产量(只)
    seeds_production_cost                string,            --种苗生产成本总额(元)
    par_seeds_cost                       string,            --苗种成本总额(元)
    packing_material_cost                string,            --包装材料(元)
    direct_labor_cost                    string,            --直接人工(元)
    drugs_cost                           string,            --药品成本(元)
    seeds_mf_rental_cost                 string,            --农业制造费用-折旧租赁费总额
    seeds_mf_consum_cost                 string,            --农业制造费用-能耗费总额 
    seeds_mf_water_power_cost            string,            --农业制造费用- 水电费总额
    seeds_mf_indirect_labor_cost         string,            --农业制造费用-间接人工总额
    seeds_mf_other_cost                  string,            --农业制造费用-其他总额
    seeds_byproduct_income               string,            --种苗成本-副产品收入总额
    level1_material_id                   string,            --物料1级id
    level1_material_descr                string,            --物料1级描述
    level2_material_id                   string,            --物料2级id
    level2_material_descr                string,            --物料2级描述
    level3_material_id                   string,            --物料3级id
    level3_material_descr                string,            --物料3级描述
    org_code                             string,
    product_line                         string             --产线    
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>ZQ08表和ZQ09>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SEEDS_COST_MM_3 PARTITION(op_month='$OP_MONTH')
 SELECT
      d1.org_id,                        
      d1.bus_type,                      
      d1.period_id,                     
      sum(d1.qty) seeds_qty,                     
      sum(d2.seeds_production_cost),         
      sum(d2.par_seeds_cost),                
      sum(d2.packing_material_cost),         
      sum(d2.direct_labor_cost),             
      sum(d2.drugs_cost),                    
      sum(d2.seeds_mf_rental_cost),          
      sum(d2.seeds_mf_consum_cost),          
      sum(d2.seeds_mf_water_power_cost),     
      sum(d2.seeds_mf_indirect_labor_cost),  
      sum(d2.seeds_mf_other_cost),           
      sum(d2.seeds_byproduct_income),        
      d3.material_segment1_id,  
      d3.material_segment1_desc,
      d3.material_segment2_id,  
      d3.material_segment2_desc,
      d3.material_segment3_id,  
      d3.material_segment3_desc, 
      d1.org_code,
      d2.product_line      
    FROM
       (SELECT
             product_item_code,
             org_id,
             bus_type,
             regexp_replace(period_name,'-','') period_id,
             SUM(plan_cubs) qty,
             org_code
         FROM
             DWU_ZQ_ZQ08_DD
         WHERE
             op_day='$OP_DAY'
         GROUP BY
             product_item_code,
             org_id,
             bus_type,
             period_name,
             org_code         
           ) d1
   INNER JOIN (
        SELECT
             item_code,
             org_id,
             bus_type,
             regexp_replace(period_id,'-','') period_id,
             product_line,
             SUM((coalesce(dep_cub_cost,0)+coalesce(dep_feed_cost,0)+coalesce(dep_drugs_cost,0)
             +coalesce(dep_direct_hr_cost,0)+coalesce(dep_utiltity_bill,0)+coalesce(dep_loan_fee,0)
             +coalesce(dep_base_feed_fee,0)++coalesce(dep_energy_cost,0)
             +coalesce(dep_indirect_hr_cost,0)+coalesce(dep_other,0)+coalesce(born_feed_cost,0)
             +coalesce(born_drugs_cost,0)+coalesce(born_direct_hr_cost,0)+coalesce(born_utiltity_bill,0)
             +coalesce(born_loan_fee,0)+coalesce(born_base_feed_fee,0)+coalesce(born_energy_cost,0)
             +coalesce(born_indirect_hr_cost,0)+coalesce(born_other,0)+coalesce(born_sub_product_cost,0)
             +coalesce(borning_drugs_cost,0)+coalesce(borning_direct_hr_cost,0)+coalesce(borning_utiltity_bill,0)
             +coalesce(borning_loan_fee,0)+coalesce(borning_base_feed_fee,0)+coalesce(borning_energy_cost,0)
             +coalesce(borning_indirect_hr_cost,0)+coalesce(borning_other,0)+coalesce(borning_packing,0)
             +coalesce(borning_sub_product_cost,0))*coalesce(rate_b,1)) seeds_production_cost,
             sum((coalesce(dep_cub_cost,0)+coalesce(dep_feed_cost,0)
                  +coalesce(dep_drugs_cost,0)+coalesce(dep_direct_hr_cost,0)
                  +coalesce(dep_utiltity_bill,0)+coalesce(dep_loan_fee,0)
                  +coalesce(dep_base_feed_fee,0)+coalesce(dep_energy_cost,0)
                  +coalesce(dep_indirect_hr_cost,0)+coalesce(dep_other,0)
                  +coalesce(born_feed_cost,0)+coalesce(born_drugs_cost,0)
                  +coalesce(born_direct_hr_cost,0)+coalesce(born_utiltity_bill,0)
                  +coalesce(born_loan_fee,0)+coalesce(born_base_feed_fee,0)
                  +coalesce(born_energy_cost,0)+coalesce(born_indirect_hr_cost,0)
                  +coalesce(born_other,0)+coalesce(born_sub_product_cost,0))*coalesce(rate_b,1)) par_seeds_cost,
            sum(coalesce(borning_packing,0)*coalesce(rate_b,1))    packing_material_cost,
            sum(coalesce(borning_direct_hr_cost,0)*coalesce(rate_b,1))    direct_labor_cost,
            sum(coalesce(borning_drugs_cost,0)*coalesce(rate_b,1))    drugs_cost,
            sum(coalesce(borning_loan_fee,0)*coalesce(rate_b,1))            seeds_mf_rental_cost,        
            sum(coalesce(borning_energy_cost,0)*coalesce(rate_b,1))            seeds_mf_consum_cost,        
            sum(coalesce(borning_utiltity_bill,0)*coalesce(rate_b,1))        seeds_mf_water_power_cost,   
            sum(coalesce(borning_indirect_hr_cost,0)*coalesce(rate_b,1))    seeds_mf_indirect_labor_cost,
            sum(coalesce(borning_other,0)*coalesce(rate_b,1))               seeds_mf_other_cost,         
            sum(coalesce(borning_sub_product_cost,0)*coalesce(rate_b,1))    seeds_byproduct_income                            
         FROM
             DWU_ZQ_ZQ09_DD
         WHERE
             op_day='$OP_DAY'
         GROUP BY
             item_code,
             org_id,
             bus_type,
             period_id,
             product_line
   )d2
   on d1.org_id=d2.org_id and d1.bus_type=d2.bus_type 
     and d1.period_id=d2.period_id and d1.product_item_code=d2.item_code 
     and d1.bus_type IN('132011','132012')
   INNER JOIN
        mreport_global.dwu_dim_material_new d3
   ON
       d2.item_code =d3.inventory_item_code and d3.material_segment5_desc LIKE '%雏%'
	   AND d3.product_recordname in('自产')
   GROUP BY
       d1.org_id,
       d1.bus_type,
       d1.period_id,
       d3.material_segment1_id,  
       d3.material_segment1_desc,
       d3.material_segment2_id,  
       d3.material_segment2_desc,
       d3.material_segment3_id,  
       d3.material_segment3_desc, 
       d1.org_code,
       d2.product_line
;
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_SEEDS_COST_MM='DMP_BIRD_SEEDS_COST_MM'

CREATE_DMP_BIRD_SEEDS_COST_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SEEDS_COST_MM(
   month_id                      string    --期间(月)
  ,day_id                        string    --期间(日)
  ,level1_org_id                 string    --组织1级(股份)  
  ,level1_org_descr              string    --组织1级(股份)  
  ,level2_org_id                 string    --组织2级(片联)  
  ,level2_org_descr              string    --组织2级(片联)  
  ,level3_org_id                 string    --组织3级(片区)  
  ,level3_org_descr              string    --组织3级(片区)  
  ,level4_org_id                 string    --组织4级(小片)  
  ,level4_org_descr              string    --组织4级(小片)  
  ,level5_org_id                 string    --组织5级(公司)  
  ,level5_org_descr              string    --组织5级(公司)  
  ,level6_org_id                 string    --组织6级(OU)  
  ,level6_org_descr              string    --组织6级(OU)  
  ,level7_org_id                 string    --组织7级(库存组织)
  ,level7_org_descr              string    --组织7级(库存组织)
  ,level1_businesstype_id        string    --业态1级
  ,level1_businesstype_name      string    --业态1级
  ,level2_businesstype_id        string    --业态2级
  ,level2_businesstype_name      string    --业态2级
  ,level3_businesstype_id        string    --业态3级
  ,level3_businesstype_name      string    --业态3级
  ,level4_businesstype_id        string    --业态4级
  ,level4_businesstype_name      string    --业态4级
  ,production_line_id            string    --产线ID
  ,production_line_descr         string    --产线名称
  ,currency_type_id              string    --币种
  ,currency_type_descr           string    --币种描述
  ,level1_material_id            string    --物料1级id
  ,level1_material_descr         string    --物料1级描述
  ,level2_material_id            string    --物料2级id
  ,level2_material_descr         string    --物料2级描述
  ,level3_material_id            string    --物料3级id
  ,level3_material_descr         string    --物料3级描述
  ,level4_material_id            string    --物料4级id
  ,level4_material_descr         string    --物料4级描述
  ,kpi_type_id                   string    --0-预算指标  1-正常计算指标
  ,kpi_type_descr                string    --预算指标,正常计算指标
  ,seeds_qty                     string    --种苗产量(只)
  ,seeds_production_cost         string    --种苗生产成本总额(元)
  ,par_seeds_cost                string    --苗种成本总额(元)
  ,packing_material_cost         string    --包装材料(元)
  ,direct_labor_cost             string    --直接人工(元)
  ,drugs_cost                    string    --药品成本(元)
  ,seeds_mf_rental_cost          string    --农业制造费用-折旧租赁费总额
  ,seeds_mf_consum_cost          string    --农业制造费用-能耗费总额
  ,seeds_mf_water_power_cost     string    --农业制造费用- 水电费总额
  ,seeds_mf_indirect_labor_cost  string    --农业制造费用-间接人工总额
  ,seeds_mf_other_cost           string    --农业制造费用-其他总额
  ,seeds_byproduct_income        string    --种苗成本-副产品收入总额
  ,management_cost               string    --管理费用(元)
  ,sales_change_cost             string    --销售费用-变动(元)
  ,sales_fixed_cost              string    --销售费用-固定(元)
  ,financing_cost                string    --财务费用(元)
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SEEDS_COST_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_SEEDS_COST_MM PARTITION(op_month='$OP_MONTH')
SELECT
       t1.period_id                           --month_id
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
       ,'' as level7_org_id                   --组织7级
       ,'' as level7_org_descr                --组织7级
       ,t5.level1_businesstype_id             --业态1级
       ,t5.level1_businesstype_name           --业态1级
       ,t5.level2_businesstype_id             --业态2级
       ,t5.level2_businesstype_name           --业态2级
       ,t5.level3_businesstype_id             --业态3级
       ,t5.level3_businesstype_name           --业态3级
       ,t5.level4_businesstype_id             --业态4级
       ,t5.level4_businesstype_name           --业态4级
       ,'' product_line                       --产线ID
       ,'' as production_line_descr           --产线名称
       ,'CNY'  currency_type_id               --币种
       ,'母币' currency_type_descr            --币种描述
       ,t1.level1_material_id                 --物料1级id
       ,t1.level1_material_descr              --物料1级描述
       ,t1.level2_material_id                 --物料2级id
       ,t1.level2_material_descr              --物料2级描述
       ,t1.level3_material_id                 --物料3级id
       ,t1.level3_material_descr              --物料3级描述
       ,'' level4_material_id                 --物料4级id
       ,'' level4_material_descr              --物料4级描述
       ,t1.kpi_type_id                        --0-预算指标  1-正常计算指标
       ,t1.kpi_type_descr                     --预算指标,正常计算指标 
       ,t1.seeds_qty                          --种苗产量(只)
       ,t1.seeds_production_cost              --种苗生产成本总额
       ,t1.par_seeds_cost                     --苗种成本总额(元)
       ,t1.packing_material_cost              --包装材料(元)
       ,t1.direct_labor_cost                  --直接人工(元)
       ,t1.drugs_cost                         --药品成本(元)
       ,t1.seeds_mf_rental_cost               --农业制造费用-折旧租赁费总额
       ,t1.seeds_mf_consum_cost               --农业制造费用-能耗费总额
       ,t1.seeds_mf_water_power_cost          --农业制造费用- 水电费总额
       ,t1.seeds_mf_indirect_labor_cost       --农业制造费用-间接人工总额
       ,t1.seeds_mf_other_cost                --农业制造费用-其他总额
       ,t1.seeds_byproduct_income             --种苗成本-副产品收入总额
       ,t1.management_cost                    --管理费用(元)
       ,t1.sales_change_cost                  --销售费用-变动(元)
       ,t1.sales_fixed_cost                   --销售费用-固定(元)
       ,t1.financing_cost                     --财务费用(元)
       ,'$CREATE_TIME'                        --创建日期
 from  
    (
select 
    d1.org_id,                       
    d1.bus_type,                     
    d1.period_id,                    
    d1.level1_material_id,           
    d1.level1_material_descr,        
    d1.level2_material_id,           
    d1.level2_material_descr,        
    d1.level3_material_id,           
    d1.level3_material_descr,
    '1' kpi_type_id,                  
    '正常计算指标' kpi_type_descr,
    nvl(d2.seeds_qty,0)               seeds_qty,                      --种苗产量(只)
    d1.seeds_production_cost,          --种苗生产成本总额
    d1.par_seeds_cost,                 --苗种成本总额(元)
    d1.packing_material_cost,          --包装材料(元)
    d1.direct_labor_cost,              --直接人工(元)
    d1.drugs_cost,                         --药品成本(元)
    d1.seeds_mf_rental_cost,           --农业制造费用-折旧租赁费总额
    d1. seeds_mf_consum_cost,              --农业制造费用-能耗费总额
    d1.seeds_mf_water_power_cost,  --农业制造费用- 水电费总额
    d1.seeds_mf_indirect_labor_cost,    --农业制造费用-间接人工总额
    d1.seeds_mf_other_cost,                --农业制造费用-其他总额
    d1.seeds_byproduct_income,         --种苗成本-副产品收入总额
    nvl(d2.management_cost,0)+nvl(d1.management_cost,0)    management_cost,          --管理费用(元)
    nvl(d2.sales_change_cost,0) sales_change_cost,                                   --销售费用-变动(元)
    nvl(d2.sales_fixed_cost,0)+nvl(d1.sales_fixed_cost,0)   sales_fixed_cost,         --销售费用-固定(元)
	nvl(d2.financing_cost,0)+nvl(d1.financing_cost,0)    financing_cost                  --财务费用(元)
from 
  (select 
        coalesce(m1.org_id,m4.org_id)   org_id,                       
        coalesce(m1.bus_type,m4.bus_type)  bus_type,                     
        coalesce(m1.period_id,m4.period_id)  period_id,                    
        coalesce(m1.level1_material_id,'')   level1_material_id,           
        coalesce(m1.level1_material_descr,'')   level1_material_descr,        
        coalesce(m1.level2_material_id,'')   level2_material_id,           
        coalesce(m1.level2_material_descr,'') level2_material_descr,        
        coalesce(m1.level3_material_id,'')   level3_material_id,           
        coalesce(m1.level3_material_descr,m4.level3_material_descr)  level3_material_descr, 
        (nvl(m1.seeds_production_cost,0)+nvl(m4.seed_cost,0)+nvl(m4.packing_amount,0)+nvl(m4.direct_manual_amount,0)+nvl(m4.drugs_amount,0)+nvl(m4.depreciation_amount,0)+nvl(m4.expend_amount,0)+nvl(m4.water_electry_amount,0)+nvl(m4.indirect_amount,0)+nvl(m4.other_amount,0)+nvl(m4.byproduct_amount,0))      seeds_production_cost,          --种苗生产成本总额
        nvl(m1.par_seeds_cost,0)+nvl(m4.seed_cost,0)             par_seeds_cost,                 --苗种成本总额(元)
        nvl(m1.packing_material_cost,0)+nvl(m4.packing_amount,0)   packing_material_cost,          --包装材料(元)
        nvl(m1.direct_labor_cost,0)+nvl(m4.direct_manual_amount,0)   direct_labor_cost,              --直接人工(元)
        nvl(m1.drugs_cost,0)+nvl(m4.drugs_amount,0)   drugs_cost,                         --药品成本(元)
        nvl(m1.seeds_mf_rental_cost,0)+nvl(m4.depreciation_amount,0)   seeds_mf_rental_cost,           --农业制造费用-折旧租赁费总额
        nvl(m1.seeds_mf_consum_cost,0)+nvl(m4.expend_amount,0)    seeds_mf_consum_cost,              --农业制造费用-能耗费总额
        nvl(m1.seeds_mf_water_power_cost,0)+nvl(m4.water_electry_amount,0)  seeds_mf_water_power_cost,  --农业制造费用- 水电费总额
        nvl(m1.seeds_mf_indirect_labor_cost,0)+nvl(m4.indirect_amount,0)    seeds_mf_indirect_labor_cost,    --农业制造费用-间接人工总额
        nvl(m1.seeds_mf_other_cost,0)+nvl(m4.other_amount,0)    seeds_mf_other_cost,                --农业制造费用-其他总额
        nvl(m1.seeds_byproduct_income,0)+nvl(m4.byproduct_amount,0)    seeds_byproduct_income,         --种苗成本-副产品收入总额
        nvl(m4.manage_amount,0)    management_cost,                   --管理费用(元)
        nvl(m4.sale_amount,0)   sales_fixed_cost,                     --销售费用-固定(元)
        nvl(m4.financial_amount,0)    financing_cost                  --财务费用(元)
from 
 (select * from TMP_DMP_BIRD_SEEDS_COST_MM_1 where op_month='$OP_MONTH')  m1
 full join (
    select
         period_id
        ,org_id
        ,org_name
        ,currency_id
        ,bus_type
        ,product_line
        ,category_id
        ,split(category_desc,'\.')[2] level3_material_descr
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
     from DWU_ZQ_MANAGE_PRODUCT_COST_DD where op_day = '$OP_DAY' and cost_type_desc = '本月'
 ) m4
   on  m1.period_id = m4.period_id
   and m1.org_id = m4.org_id
   and m1.level3_material_descr =m4.level3_material_descr
   and m1.product_line = m4.product_line) d1
left JOIN
       TMP_DMP_BIRD_SEEDS_COST_MM_2 d2
    ON d2.op_month='$OP_MONTH'
    AND d1.org_id=d2.org_id
    AND d1.bus_type=d2.bus_type
    AND d1.period_id=d2.period_id
   where (d1.level3_material_id=d2.level3_material_id
	or d1.level3_material_descr=d2.generation_name)
union all 
  SELECT
    d1.org_id,
    d1.bus_type,
    d1.period_id,
    d1.level1_material_id,
    d1.level1_material_descr,
    d1.level2_material_id,
    d1.level2_material_descr,
    d1.level3_material_id,
    d1.level3_material_descr,
    '0'    kpi_type_id,
    '预算指标' kpi_type_descr,
    d1.seeds_qty, --种苗产量(只)
    d1.seeds_production_cost, --种苗生产成本总额
    d1.par_seeds_cost, --苗种成本总额(元)
    d1.packing_material_cost, --包装材料(元)
    d1.direct_labor_cost, --直接人工(元)
    d1.drugs_cost, --药品成本(元)
    d1.seeds_mf_rental_cost, --农业制造费用-折旧租赁费总额
    d1.seeds_mf_consum_cost, --农业制造费用-能耗费总额
    d1.seeds_mf_water_power_cost, --农业制造费用- 水电费总额
    d1.seeds_mf_indirect_labor_cost, --农业制造费用-间接人工总额
    d1.seeds_mf_other_cost, --农业制造费用-其他总额
    d1.seeds_byproduct_income, --种苗成本-副产品收入总额
    NVL(d2.management_cost,0)+NVL(d1.management_cost,0) management_cost, --管理费用(元)
    NVL(d2.sales_change_cost,0)                         sales_change_cost, --销售费用-变动(元)
    NVL(d1.sales_fixed_cost,0)                          sales_fixed_cost, --销售费用-固定(元)
    NVL(d2.financing_cost,0)+NVL(d1.financing_cost,0)   financing_cost
FROM
    (
  SELECT
        COALESCE(m1.org_id,m4.org_id)                               org_id,
        COALESCE(m1.bus_type,m4.bus_type)                           bus_type,
        COALESCE(m1.period_id,m4.period_id)                         period_id,
        COALESCE(m1.level1_material_id,'')                          level1_material_id,
        COALESCE(m1.level1_material_descr,'')                       level1_material_descr,
        COALESCE(m1.level2_material_id,'')                          level2_material_id,
        COALESCE(m1.level2_material_descr,'')                       level2_material_descr,
        COALESCE(m1.level3_material_id,'')                          level3_material_id,
        COALESCE(m1.level3_material_descr,m4.level3_material_descr) level3_material_descr,
        '0'                                                         kpi_type_id,
        '预算指标'                                                      kpi_type_descr,
        NVL(m1.seeds_qty,0)+NVL(m4.seed_quantity,0)                 seeds_qty, --种苗产量(只)
        (NVL(m1.seeds_production_cost,0)+NVL(m4.seed_cost,0)+NVL(m4.packing_amount,0)+NVL
        (m4.direct_manual_amount,0)+NVL(m4.drugs_amount,0)+NVL(m4.depreciation_amount,0)+NVL
        (m4.expend_amount,0)+NVL(m4.water_electry_amount,0)+NVL(m4.indirect_amount,0)+NVL
        (m4.other_amount,0)+NVL(m4.byproduct_amount,0))        seeds_production_cost, --种苗生产成本总额
        NVL(m1.par_seeds_cost,0)+NVL(m4.seed_cost,0)             par_seeds_cost, --苗种成本总额(元)
        NVL(m1.packing_material_cost,0)+NVL(m4.packing_amount,0) packing_material_cost, --包装材料(元)
        NVL(m1.direct_labor_cost,0)+NVL(m4.direct_manual_amount,0)  direct_labor_cost, --直接人工(元)
        NVL(m1.drugs_cost,0)+NVL(m4.drugs_amount,0)                  drugs_cost, --药品成本(元)
        NVL(m1.seeds_mf_rental_cost,0)+NVL(m4.depreciation_amount,0) seeds_mf_rental_cost, --农业制造费用-折旧租赁费总额
        NVL(m1.seeds_mf_consum_cost,0)+NVL(m4.expend_amount,0) seeds_mf_consum_cost, --农业制造费用-能耗费总额
        NVL(m1.seeds_mf_water_power_cost,0)+NVL(m4.water_electry_amount,0)
        seeds_mf_water_power_cost, --农业制造费用- 水电费总额
        NVL(m1.seeds_mf_indirect_labor_cost,0)+NVL(m4.indirect_amount,0)seeds_mf_indirect_labor_cost, --农业制造费用-间接人工总额
        NVL(m1.seeds_mf_other_cost,0)+NVL(m4.other_amount,0)  seeds_mf_other_cost, --农业制造费用-其他总额
        NVL(m1.seeds_byproduct_income,0)+NVL(m4.byproduct_amount,0) seeds_byproduct_income, --种苗成本-副产品收入总额
        COALESCE(m4.manage_amount,0)              management_cost, --管理费用
        NVL(m4.sale_amount,0)                     sales_fixed_cost, --销售费用-固定(元)
        NVL(m4.financial_amount,0)                financing_cost, --财务费用
        COALESCE(m1.org_code,m4.org_code)         org_code,
        COALESCE(m1.product_line,m4.product_line) product_line
    FROM
        (
            SELECT
                *
            FROM
                TMP_DMP_BIRD_SEEDS_COST_MM_3
            WHERE
                op_month='$OP_MONTH') m1
    FULL JOIN
        (
            SELECT
                period_id ,
                dw1.org_id org_id ,
                org_name ,
                currency_id ,
                bus_type ,
                product_line ,
                category_id ,
                split(category_desc,'\.')[2] level3_material_descr ,
                cost_type_id ,
                cost_type_desc ,
                seed_quantity ,
                seed_cost ,
                packing_amount ,
                direct_manual_amount ,
                drugs_amount ,
                depreciation_amount ,
                expend_amount ,
                water_electry_amount ,
                indirect_amount ,
                other_amount ,
                byproduct_amount ,
                manage_amount ,
                sale_amount ,
                financial_amount,
                dw2.org_code org_code
            FROM
                DWU_ZQ_MANAGE_PRODUCT_COST_DD dw1
            INNER JOIN
                (
                    SELECT
                        org_id,
                        level6_org_id org_code
                    FROM
                        mreport_global.dim_org_management
                    GROUP BY
                        org_id,
                        level6_org_id )dw2
            ON
                dw1.org_id=dw2.org_id
            AND dw1.op_day = '$OP_DAY'
            AND dw1.cost_type_desc = '预算' ) m4
    ON
        m1.period_id = m4.period_id
    AND m1.org_id = m4.org_id
    AND m1.level3_material_descr =m4.level3_material_descr
    AND m1.product_line = m4.product_line) d1
  LEFT JOIN
    (
        SELECT
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6601'
                    THEN fee_j_amount
                    ELSE 0
                END) sales_change_cost,
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6602'
                    THEN fee_j_amount
                    ELSE 0
                END) management_cost,
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6603'
                    THEN fee_j_amount
                    ELSE 0
                END) financing_cost,
            short_code,
            '10'                               product_line,
            regexp_replace(period_code,'-','') period_id
        FROM
            DWU_ZQ_ZQ13_DD
        WHERE
            op_day='$OP_DAY'
        AND SUBSTR(fee_segment,1,4) IN(6601,6602,6603)
        GROUP BY
            short_code,
            period_code
        UNION ALL
        SELECT
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6601'
                    THEN fee_y_amount
                    ELSE 0
                END) sales_change_cost,
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6602'
                    THEN fee_y_amount
                    ELSE 0
                END) management_cost,
            SUM(
                CASE
                    WHEN SUBSTR(fee_segment,1,4)='6603'
                    THEN fee_y_amount
                    ELSE 0
                END) financing_cost,
            short_code,
            '20'                               product_line,
            regexp_replace(period_code,'-','') period_id
        FROM
            DWU_ZQ_ZQ13_DD
        WHERE
            op_day='$OP_DAY'
        AND SUBSTR(fee_segment,1,4) IN(6601,6602,6603)
        GROUP BY
            short_code,
            period_code ) d2
  ON
      d1.org_code=d2.short_code
  AND d1.product_line=d2.product_line
  AND d1.period_id=d2.period_id
  )t1
left join mreport_global.dim_org_management t6 
     on t1.org_id=t6.org_id  
     and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
     on t1.org_id=t7.org_id 
     and t1.bus_type=t7.bus_type_id 
     and t7.attribute5='2'
 LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_businesstype
        WHERE  level4_businesstype_name IS NOT NULL) t5
     ON  (t1.bus_type=t5.level4_businesstype_id)
;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_1;
    $INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_1;
    $CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_2;
    $INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_2;
    $CREATE_TMP_DMP_BIRD_SEEDS_COST_MM_3;
    $INSERT_TMP_DMP_BIRD_SEEDS_COST_MM_3;
    $CREATE_DMP_BIRD_SEEDS_COST_MM;
    $INSERT_DMP_BIRD_SEEDS_COST_MM;
"  -v