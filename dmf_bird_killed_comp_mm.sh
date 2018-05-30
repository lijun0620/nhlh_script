#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_killed_comp_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰出成分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_killed_comp_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_KILLED_COMP_MM_0='TMP_DMF_BIRD_KILLED_COMP_MM_0'

CREATE_TMP_DMF_BIRD_KILLED_COMP_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_KILLED_COMP_MM_0(
  month_id                         string        --期间(月份)  
  ,level1_org_id                   string        --组织1级(股份)  
  ,level1_org_descr                string        --组织1级(股份)  
  ,level2_org_id                   string        --组织2级(片联)  
  ,level2_org_descr                string        --组织2级(片联)  
  ,level3_org_id                   string        --组织3级(片区)  
  ,level3_org_descr                string        --组织3级(片区)  
  ,level4_org_id                   string        --组织4级(小片)  
  ,level4_org_descr                string        --组织4级(小片)  
  ,level5_org_id                   string        --组织5级(公司)  
  ,level5_org_descr                string        --组织5级(公司)  
  ,level6_org_id                   string        --组织6级(OU)  
  ,level6_org_descr                string        --组织6级(OU)  
  ,level7_org_id                   string        --组织7级(库存组织)
  ,level7_org_descr                string        --组织7级(库存组织)
  ,level1_businesstype_id          string        --业态1级      
  ,level1_businesstype_name        string        --业态1级      
  ,level2_businesstype_id          string        --业态2级      
  ,level2_businesstype_name        string        --业态2级      
  ,level3_businesstype_id          string        --业态3级      
  ,level3_businesstype_name        string        --业态3级      
  ,level4_businesstype_id          string        --业态4级      
  ,level4_businesstype_name        string        --业态4级      
  ,production_line_id              string        --产线        
  ,production_line_descr           string        --产线    
  ,carcass_weight                  string        --胴体重量(kg)  
  ,settlement_weight               string        --结算重量(kg)  
  ,production_qty                  string        --产量(kg)    
  ,defective_products_weight       string        --残次品产量(kg)
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_KILLED_COMP_MM_0="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_KILLED_COMP_MM_0 PARTITION(op_month='$OP_MONTH')
SELECT month_id                                 --期间(月份)  
       ,level1_org_id                           --组织1级(股份)  
       ,level1_org_descr                        --组织1级(股份)  
       ,level2_org_id                           --组织2级(片联)  
       ,level2_org_descr                        --组织2级(片联)  
       ,level3_org_id                           --组织3级(片区)  
       ,level3_org_descr                        --组织3级(片区)  
       ,level4_org_id                           --组织4级(小片)  
       ,level4_org_descr                        --组织4级(小片)  
       ,level5_org_id                           --组织5级(公司)  
       ,level5_org_descr                        --组织5级(公司)  
       ,level6_org_id                           --组织6级(OU)  
       ,level6_org_descr                        --组织6级(OU)  
       ,level7_org_id                           --组织7级(库存组织)
       ,level7_org_descr                        --组织7级(库存组织)
       ,level1_businesstype_id                  --业态1级      
       ,level1_businesstype_name                --业态1级      
       ,level2_businesstype_id                  --业态2级      
       ,level2_businesstype_name                --业态2级      
       ,level3_businesstype_id                  --业态3级      
       ,level3_businesstype_name                --业态3级      
       ,level4_businesstype_id                  --业态4级      
       ,level4_businesstype_name                --业态4级      
       ,production_line_id                      --产线        
       ,production_line_descr                   --产线    
       ,sum(nvl(carcass_weight,0))                     --胴体重量(kg)  
       ,sum(nvl(settlement_weight,0))                  --结算重量(kg)  
       ,sum(nvl(production_qty,0))                     --产量(kg)    
       ,sum(nvl(defective_products_weight,0))          --残次品产量(kg)
  FROM (SELECT substr(recycle_date,1,6) month_id        --期间(月份)  
               ,level1_org_id                           --组织1级(股份)  
               ,level1_org_descr                        --组织1级(股份)  
               ,level2_org_id                           --组织2级(片联)  
               ,level2_org_descr                        --组织2级(片联)  
               ,level3_org_id                           --组织3级(片区)  
               ,level3_org_descr                        --组织3级(片区)  
               ,level4_org_id                           --组织4级(小片)  
               ,level4_org_descr                        --组织4级(小片)  
               ,level5_org_id                           --组织5级(公司)  
               ,level5_org_descr                        --组织5级(公司)  
               ,level6_org_id                           --组织6级(OU)  
               ,level6_org_descr                        --组织6级(OU)  
               ,level7_org_id                           --组织7级(库存组织)
               ,level7_org_descr                        --组织7级(库存组织)
               ,level1_businesstype_id                  --业态1级      
               ,level1_businesstype_name                --业态1级      
               ,level2_businesstype_id                  --业态2级      
               ,level2_businesstype_name                --业态2级      
               ,level3_businesstype_id                  --业态3级      
               ,level3_businesstype_name                --业态3级      
               ,level4_businesstype_id                  --业态4级      
               ,level4_businesstype_name                --业态4级      
               ,production_line_id                      --产线        
               ,production_line_descr                   --产线    
               ,carcass_weight                          --胴体重量(kg)  
               ,recycle_weight settlement_weight        --结算重量(kg)  
               ,0 production_qty                        --产量(kg)    
               ,0 defective_products_weight             --残次品产量(kg)
          FROM dwf_bird_killed_comp_dd
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT t1.month_id                                --期间(月)
               ,t2.level1_org_id                          --组织1级
               ,t2.level1_org_descr                       --组织1级
               ,t2.level2_org_id                          --组织2级
               ,t2.level2_org_descr                       --组织2级
               ,t2.level3_org_id                          --组织3级
               ,t2.level3_org_descr                       --组织3级
               ,t2.level4_org_id                          --组织4级
               ,t2.level4_org_descr                       --组织4级
               ,t2.level5_org_id                          --组织5级
               ,t2.level5_org_descr                       --组织5级
               ,t2.level6_org_id                          --组织6级
               ,t2.level6_org_descr                       --组织6级
               ,t3.level7_org_id                          --组织7级
               ,t3.level7_org_descr                       --组织7级
               ,t4.level1_businesstype_id                 --业态1级
               ,t4.level1_businesstype_name               --业态1级
               ,t4.level2_businesstype_id                 --业态2级
               ,t4.level2_businesstype_name               --业态2级
               ,t4.level3_businesstype_id                 --业态3级
               ,t4.level3_businesstype_name               --业态3级
               ,t4.level4_businesstype_id                 --业态4级
               ,t4.level4_businesstype_name               --业态4级
               ,t1.production_line_id                     --产线
               ,t1.production_line_descr                  --产线
               ,0 carcass_weight                          --胴体重量(kg)  
               ,0 settlement_weight                       --结算重量(kg)  
               ,t1.production_qty                         --产量(kg)    
               ,0 defective_products_weight               --残次品产量(kg)
          FROM (SELECT substr(period_id,1,6) month_id
                       ,org_id                                    --组织ID
                       ,organization_id                           --库存组织
                       ,case when product_line='20' then '2'
                             when product_line='10' then '1'
                             else null end production_line_id     --产线
                       ,case when product_line='20' then '鸭线'
                             when product_line='10' then '鸡线'
                             else null end production_line_descr  --产线
                       ,bus_type                                  --业态
                       ,sum(coalesce(primary_quantity,0)) production_qty   --自产产量(屠宰产品)
                  FROM dwu_tz_storage_transation02_dd a1
                  LEFT JOIN (SELECT item_code,
                                    prd_line_cate_id
                               FROM mreport_global.dim_crm_item
                              WHERE prd_line_cate_id = '1-16BW2M'
                              GROUP BY item_code
                                    ,prd_line_cate_id) a2
                    ON (a1.item_code=a2.item_code)
                 WHERE a1.op_day='$OP_DAY'
                   AND a2.item_code is null
                 GROUP BY substr(period_id,1,6)
                       ,org_id
                       ,organization_id
                       ,product_line
                       ,bus_type) t1
          LEFT JOIN (SELECT level1_org_id,
                            level1_org_descr,
                            level2_org_id,
                            level2_org_descr,
                            level3_org_id,
                            level3_org_descr,
                            level4_org_id,
                            level4_org_descr,
                            level5_org_id,
                            level5_org_descr,
                            level6_org_id,
                            level6_org_descr,
                            org_id
                       FROM mreport_global.dim_org_management
                      WHERE org_id is not null
                      GROUP BY level1_org_id,
                            level1_org_descr,
                            level2_org_id,
                            level2_org_descr,
                            level3_org_id,
                            level3_org_descr,
                            level4_org_id,
                            level4_org_descr,
                            level5_org_id,
                            level5_org_descr,
                            level6_org_id,
                            level6_org_descr,
                            org_id) t2
            ON (t1.org_id=t2.org_id)
          LEFT JOIN (SELECT *
                       FROM mreport_global.dim_org_inv_management
                      WHERE inv_org_id is not null) t3
            ON (t1.organization_id=t3.inv_org_id)
          LEFT JOIN (SELECT *
                       FROM mreport_global.dim_org_businesstype
                      WHERE level4_businesstype_name is not null) t4
            ON (t1.bus_type=t4.level4_businesstype_id)
        UNION ALL
        SELECT t1.month_id                                --期间(月)
               ,t2.level1_org_id                          --组织1级
               ,t2.level1_org_descr                       --组织1级
               ,t2.level2_org_id                          --组织2级
               ,t2.level2_org_descr                       --组织2级
               ,t2.level3_org_id                          --组织3级
               ,t2.level3_org_descr                       --组织3级
               ,t2.level4_org_id                          --组织4级
               ,t2.level4_org_descr                       --组织4级
               ,t2.level5_org_id                          --组织5级
               ,t2.level5_org_descr                       --组织5级
               ,t2.level6_org_id                          --组织6级
               ,t2.level6_org_descr                       --组织6级
               ,t3.level7_org_id                          --组织7级
               ,t3.level7_org_descr                       --组织7级
               ,t4.level1_businesstype_id                 --业态1级
               ,t4.level1_businesstype_name               --业态1级
               ,t4.level2_businesstype_id                 --业态2级
               ,t4.level2_businesstype_name               --业态2级
               ,t4.level3_businesstype_id                 --业态3级
               ,t4.level3_businesstype_name               --业态3级
               ,t4.level4_businesstype_id                 --业态4级
               ,t4.level4_businesstype_name               --业态4级
               ,t1.production_line_id                     --产线
               ,t1.production_line_descr                  --产线
               ,0 carcass_weight                          --胴体重量(kg)  
               ,0 settlement_weight                       --结算重量(kg)  
               ,0 production_qty                          --产量(kg)    
               ,t1.defective_products_weight              --残次品产量(kg)
          FROM (SELECT substr(period_id,1,6) month_id
                       ,org_id                                    --组织ID
                       ,organization_id inv_org_id                --库存组织
                       ,case when product_line='20' then '2'
                             when product_line='10' then '1'
                             else null end production_line_id     --产线
                       ,case when product_line='20' then '鸭线'
                             when product_line='10' then '鸡线'
                             else null end production_line_descr  --产线
                       ,bus_type                                  --业态
                       ,coalesce(transaction_quantity,0) defective_products_weight       --残次品产量(kg)
                  FROM dwu_tz_storage_transation02_dd a1
                 INNER JOIN (SELECT inventory_item_id
                               FROM mreport_global.dwu_dim_material_new
                              WHERE is_d_product='Y'
                              GROUP BY inventory_item_id) a2
                    ON (a1.item_id=a2.inventory_item_id)
                 WHERE a1.op_day='$OP_DAY') t1
          LEFT JOIN (SELECT level1_org_id,
                            level1_org_descr,
                            level2_org_id,
                            level2_org_descr,
                            level3_org_id,
                            level3_org_descr,
                            level4_org_id,
                            level4_org_descr,
                            level5_org_id,
                            level5_org_descr,
                            level6_org_id,
                            level6_org_descr,
                            org_id
                       FROM mreport_global.dim_org_management
                      WHERE org_id is not null
                      GROUP BY level1_org_id,
                            level1_org_descr,
                            level2_org_id,
                            level2_org_descr,
                            level3_org_id,
                            level3_org_descr,
                            level4_org_id,
                            level4_org_descr,
                            level5_org_id,
                            level5_org_descr,
                            level6_org_id,
                            level6_org_descr,
                            org_id) t2
            ON (t1.org_id=t2.org_id)
          LEFT JOIN (SELECT *
                       FROM mreport_global.dim_org_inv_management
                      WHERE inv_org_id is not null) t3
            ON (t1.inv_org_id=t3.inv_org_id)
          LEFT JOIN (SELECT *
                       FROM mreport_global.dim_org_businesstype
                      WHERE level4_businesstype_name is not null) t4
            ON (t1.bus_type=t4.level4_businesstype_id)
    ) a
 GROUP BY month_id                                 --期间(月份)  
       ,level1_org_id                           --组织1级(股份)  
       ,level1_org_descr                        --组织1级(股份)  
       ,level2_org_id                           --组织2级(片联)  
       ,level2_org_descr                        --组织2级(片联)  
       ,level3_org_id                           --组织3级(片区)  
       ,level3_org_descr                        --组织3级(片区)  
       ,level4_org_id                           --组织4级(小片)  
       ,level4_org_descr                        --组织4级(小片)  
       ,level5_org_id                           --组织5级(公司)  
       ,level5_org_descr                        --组织5级(公司)  
       ,level6_org_id                           --组织6级(OU)  
       ,level6_org_descr                        --组织6级(OU)  
       ,level7_org_id                           --组织7级(库存组织)
       ,level7_org_descr                        --组织7级(库存组织)
       ,level1_businesstype_id                  --业态1级      
       ,level1_businesstype_name                --业态1级      
       ,level2_businesstype_id                  --业态2级      
       ,level2_businesstype_name                --业态2级      
       ,level3_businesstype_id                  --业态3级      
       ,level3_businesstype_name                --业态3级      
       ,level4_businesstype_id                  --业态4级      
       ,level4_businesstype_name                --业态4级      
       ,production_line_id                      --产线        
       ,production_line_descr                   --产线
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_KILLED_COMP_MM='DMF_BIRD_KILLED_COMP_MM'

CREATE_DMF_BIRD_KILLED_COMP_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_KILLED_COMP_MM(
  month_id                         string        --期间(月份)
  ,day_id                          string        --期间(日期)
  ,level1_org_id                   string        --组织1级(股份)  
  ,level1_org_descr                string        --组织1级(股份)  
  ,level2_org_id                   string        --组织2级(片联)  
  ,level2_org_descr                string        --组织2级(片联)  
  ,level3_org_id                   string        --组织3级(片区)  
  ,level3_org_descr                string        --组织3级(片区)  
  ,level4_org_id                   string        --组织4级(小片)  
  ,level4_org_descr                string        --组织4级(小片)  
  ,level5_org_id                   string        --组织5级(公司)  
  ,level5_org_descr                string        --组织5级(公司)  
  ,level6_org_id                   string        --组织6级(OU)  
  ,level6_org_descr                string        --组织6级(OU)  
  ,level7_org_id                   string        --组织7级(库存组织)
  ,level7_org_descr                string        --组织7级(库存组织)
  ,level1_businesstype_id          string        --业态1级      
  ,level1_businesstype_name        string        --业态1级      
  ,level2_businesstype_id          string        --业态2级      
  ,level2_businesstype_name        string        --业态2级      
  ,level3_businesstype_id          string        --业态3级      
  ,level3_businesstype_name        string        --业态3级      
  ,level4_businesstype_id          string        --业态4级      
  ,level4_businesstype_name        string        --业态4级      
  ,production_line_id              string        --产线        
  ,production_line_descr           string        --产线    
  ,carcass_weight                  string        --胴体重量(kg)  
  ,settlement_weight               string        --结算重量(kg)  
  ,production_qty                  string        --产量(kg)    
  ,defective_products_weight       string        --残次品产量(kg)
  ,create_time                     string        --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_KILLED_COMP_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_KILLED_COMP_MM PARTITION(op_month='$OP_MONTH')
SELECT month_id                                 --期间(月份)  
       ,null day_id                             --期间(日期)
       ,level1_org_id                           --组织1级(股份)  
       ,level1_org_descr                        --组织1级(股份)  
       ,level2_org_id                           --组织2级(片联)  
       ,level2_org_descr                        --组织2级(片联)  
       ,level3_org_id                           --组织3级(片区)  
       ,level3_org_descr                        --组织3级(片区)  
       ,level4_org_id                           --组织4级(小片)  
       ,level4_org_descr                        --组织4级(小片)  
       ,level5_org_id                           --组织5级(公司)  
       ,level5_org_descr                        --组织5级(公司)  
       ,level6_org_id                           --组织6级(OU)  
       ,level6_org_descr                        --组织6级(OU)  
       ,level7_org_id                           --组织7级(库存组织)
       ,level7_org_descr                        --组织7级(库存组织)
       ,level1_businesstype_id                  --业态1级      
       ,level1_businesstype_name                --业态1级      
       ,level2_businesstype_id                  --业态2级      
       ,level2_businesstype_name                --业态2级      
       ,level3_businesstype_id                  --业态3级      
       ,level3_businesstype_name                --业态3级      
       ,level4_businesstype_id                  --业态4级      
       ,level4_businesstype_name                --业态4级      
       ,production_line_id                      --产线        
       ,production_line_descr                   --产线    
       ,coalesce(carcass_weight,0)              --胴体重量(kg)  
       ,coalesce(settlement_weight,0)           --结算重量(kg)  
       ,coalesce(production_qty,0)              --产量(kg)    
       ,coalesce(defective_products_weight,0)   --残次品产量(kg)
       ,'$CREATE_TIME' create_time              --创建时间
  FROM (SELECT *
          FROM $TMP_DMF_BIRD_KILLED_COMP_MM_0
         WHERE op_month='$OP_MONTH'
           AND level2_org_id NOT IN('1015')
           AND (coalesce(carcass_weight,0)+coalesce(settlement_weight,0)+coalesce(production_qty,0)+coalesce(defective_products_weight,0))>0) t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    
    $CREATE_TMP_DMF_BIRD_KILLED_COMP_MM_0;
    $INSERT_TMP_DMF_BIRD_KILLED_COMP_MM_0;
    $CREATE_DMF_BIRD_KILLED_COMP_MM;
    $INSERT_DMF_BIRD_KILLED_COMP_MM;
"  -v