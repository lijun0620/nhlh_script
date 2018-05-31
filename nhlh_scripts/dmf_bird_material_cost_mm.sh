#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_material_cost_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 养户档案信息
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_material_cost_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_MATERIAL_COST_MM_0='TMP_DMF_BIRD_MATERIAL_COST_MM_0'

CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_MATERIAL_COST_MM_0(
  month_id                       string    --期间(月份)       
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
  ,level4_businesstype_id        string    --业态4级          
  ,level4_businesstype_name      string    --业态4级          
  ,production_line_id            string    --产线             
  ,drugs_cost                    string    --兽药成本
  ,value_drugs_cost              string    --保值合同兽药金额
  ,mini_drugs_cost               string    --保底合同兽药金额
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_0="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_MATERIAL_COST_MM_0 PARTITION(op_month='$OP_MONTH')
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
       ,t4.level4_businesstype_id                 --业态4级
       ,t4.level4_businesstype_name               --业态4级
       ,t0.production_line_id                     --产线
       ,sum(t1.drugs_cost) drugs_cost             --兽药成本
       ,sum(case when t0.put_type_id='1' then t1.drugs_cost else '0' end) value_drugs_cost
       ,sum(case when t0.put_type_id='2' then t1.drugs_cost else '0' end) mini_drugs_cost
  FROM (SELECT contractnumber contract_no
               ,case when meaning='CHICHEN' then '1'
                     when meaning='DUCK' then '2'
                else null end production_line_id           --产线代码
               ,bus_type
               ,case when guarantees_market='保值' then '1'
                     when guarantees_market='保底' then '2'
                     when guarantees_market='市场' then '3'
                else null end put_type_id
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY'
           AND meaning IN('CHICHEN','DUCK')) t0
  LEFT JOIN (SELECT cust_po_num contract_no   --合同号
                    ,org_id
                    ,substr(period_id,1,6) month_id
                    ,sum(amount) drugs_cost
               FROM dwu_xs_other_sale_dd
              WHERE op_day='$OP_DAY'
                AND order_type like '%冷藏厂承担%'
              GROUP BY cust_po_num
                    ,org_id
                    ,substr(period_id,1,6)) t1
    ON (t0.contract_no=t1.contract_no)
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
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t4
    ON (t0.bus_type=t4.level4_businesstype_id)
 GROUP BY t1.month_id                                --期间(月)
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
       ,t4.level4_businesstype_id                 --业态4级
       ,t4.level4_businesstype_name               --业态4级
       ,t0.production_line_id                     --产线
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_MATERIAL_COST_MM_1='TMP_DMF_BIRD_MATERIAL_COST_MM_1'

CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_MATERIAL_COST_MM_1(
  month_id                       string    --期间(月份)        
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
  ,production_line_id            string    --产线            
  ,production_line_descr         string    --产线              
  ,production_qty                string    --自产产量(屠宰产品)
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_MATERIAL_COST_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                                --期间(月)
       ,null day_id                               --期间(日)
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
       ,t1.product_line production_line_id        --产线
       ,'' production_line_descr                  --产线
       ,t1.production_qty                         --自产产量(屠宰产品)
  FROM (SELECT substr(period_id,1,6) month_id
               ,org_id                 --组织ID
               ,organization_name      --库存组织(名称)
               ,case when product_line='20' then '2'
                     when product_line='10' then '1'
                     else null end product_line   --产线
               ,null bus_type          --业态
               ,sum(primary_quantity) production_qty  --自产产量(屠宰产品)
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
               ,organization_name
               ,product_line) t1
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
    ON (t1.organization_name=t3.inv_org_name)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t4
    ON (t1.bus_type=t4.level4_businesstype_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_MATERIAL_COST_MM_2='TMP_DMF_BIRD_MATERIAL_COST_MM_2'

CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_MATERIAL_COST_MM_2(
  month_id                       string    --期间(月份)        
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
  ,production_line_id            string    --产线            
  ,production_line_descr         string    --产线
  ,kpi_type                      string    --指标类型(PUT-投放，BUY_BACK-采购)        
  ,production_qty                string    --自产产量(kg)      
  ,stock_cost                    string    --原料成本(元)       
  ,carriage_cost                 string    --运费成本(元)       
  ,drugs_cost                    string    --兽药成本(元)       
  ,input_vat_cost                string    --进项税成本(元)      
  ,value_recyle_qty              string    --保值回收只数(只)     
  ,value_recyle_weight           string    --保值回收重量(kg)    
  ,value_recyle_cost             string    --保值回收金额(元)     
  ,value_recyle_carriage_cost    string    --保值回收运费        
  ,value_drugs_cost              string    --保值合同兽药金额      
  ,value_other_cost              string    --保值其他增减项       
  ,mini_recyle_qty               string    --保底回收只数(只)     
  ,mini_recyle_weight            string    --保底回收重量(kg)    
  ,mini_recyle_cost              string    --保底回收金额(元)     
  ,mini_recyle_carriage_cost     string    --保底回收运费        
  ,mini_drugs_cost               string    --保底合同兽药金额      
  ,mini_other_cost               string    --保底其他增减项       
  ,mkt_recyle_qty                string    --市场回收只数(只)     
  ,mkt_recyle_weight             string    --市场回收重量(kg)    
  ,mkt_recyle_cost               string    --市场回收金额(元)     
  ,mkt_recyle_carriage_cost      string    --市场回收运费(元)     
  ,near_recycle_qty              string    --近距离回收只数(只)    
  ,near_value_recycle_qty        string    --保值合同近距离回收只数(只)
  ,near_mini_recycle_qty         string    --保底合同近距离回收只数(只)
  ,value_put_qty                 string    --保值投放只数(只)     
  ,mini_put_qty                  string    --保底投放只数(只)     
  ,value_put_cost                string    --保值投放价格(元)     
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_2="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_MATERIAL_COST_MM_2 PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                            --期间(月份)        
       ,t1.day_id                             --期间(日)         
       ,t1.level1_org_id                      --组织1级(股份)      
       ,t1.level1_org_descr                   --组织1级(股份)      
       ,t1.level2_org_id                      --组织2级(片联)      
       ,t1.level2_org_descr                   --组织2级(片联)      
       ,t1.level3_org_id                      --组织3级(片区)      
       ,t1.level3_org_descr                   --组织3级(片区)      
       ,t1.level4_org_id                      --组织4级(小片)      
       ,t1.level4_org_descr                   --组织4级(小片)      
       ,t1.level5_org_id                      --组织5级(公司)      
       ,t1.level5_org_descr                   --组织5级(公司)      
       ,t1.level6_org_id                      --组织6级(OU)      
       ,t1.level6_org_descr                   --组织6级(OU)      
       ,t1.level7_org_id                      --组织7级(库存组织)    
       ,t1.level7_org_descr                   --组织7级(库存组织)    
       ,null level1_businesstype_id           --业态1级          
       ,null level1_businesstype_name         --业态1级          
       ,null level2_businesstype_id           --业态2级          
       ,null level2_businesstype_name         --业态2级          
       ,null level3_businesstype_id           --业态3级          
       ,null level3_businesstype_name         --业态3级          
       ,null level4_businesstype_id           --业态4级          
       ,null level4_businesstype_name         --业态4级          
       ,t1.production_line_id                 --产线            
       ,t1.production_line_descr              --产线
       ,t1.kpi_type                           --指标类型
       ,case when t1.kpi_type='BUY_BACK' then t2.production_qty
        else '0' end production_qty           --自产产量(kg)
       ,t1.stock_cost                         --原料成本(元)       
       ,t1.carriage_cost                      --运费成本(元)       
       ,t1.drugs_cost                         --兽药成本(元)       
       ,t1.input_vat_cost                     --进项税成本(元)      
       ,t1.value_recyle_qty                   --保值回收只数(只)     
       ,t1.value_recyle_weight                --保值回收重量(kg)    
       ,t1.value_recyle_cost                  --保值回收金额(元)     
       ,t1.value_recyle_carriage_cost         --保值回收运费        
       ,t1.value_drugs_cost                   --保值合同兽药金额      
       ,t1.value_other_cost                   --保值其他增减项       
       ,t1.mini_recyle_qty                    --保底回收只数(只)     
       ,t1.mini_recyle_weight                 --保底回收重量(kg)    
       ,t1.mini_recyle_cost                   --保底回收金额(元)     
       ,t1.mini_recyle_carriage_cost          --保底回收运费        
       ,t1.mini_drugs_cost                    --保底合同兽药金额      
       ,t1.mini_other_cost                    --保底其他增减项       
       ,t1.mkt_recyle_qty                     --市场回收只数(只)     
       ,t1.mkt_recyle_weight                  --市场回收重量(kg)    
       ,t1.mkt_recyle_cost                    --市场回收金额(元)     
       ,t1.mkt_recyle_carriage_cost           --市场回收运费(元)     
       ,t1.near_recycle_qty                   --近距离回收只数(只)    
       ,t1.near_value_recycle_qty             --保值合同近距离回收只数(只)
       ,t1.near_mini_recycle_qty              --保底合同近距离回收只数(只)
       ,t1.value_put_qty                      --保值投放只数(只)     
       ,t1.mini_put_qty                       --保底投放只数(只)     
       ,t1.value_put_cost                     --保值投放成本(元)  
  FROM (SELECT substr(recycle_date,1,6) month_id       --期间(月)
               ,null day_id                    --期间(日)
               ,level1_org_id                          --组织1级
               ,level1_org_descr                       --组织1级
               ,level2_org_id                          --组织2级
               ,level2_org_descr                       --组织2级
               ,level3_org_id                          --组织3级
               ,level3_org_descr                       --组织3级
               ,level4_org_id                          --组织4级
               ,level4_org_descr                       --组织4级
               ,level5_org_id                          --组织5级
               ,level5_org_descr                       --组织5级
               ,level6_org_id                          --组织6级
               ,level6_org_descr                       --组织6级
               ,level7_org_id                          --组织7级
               ,level7_org_descr                       --组织7级
               ,production_line_id                     --产线代码
               ,production_line_descr                  --产线描述
               ,kpi_type                               --指标类型
               
               ,sum(recycle_cost) stock_cost                --回收成本(元)       
               ,sum(recyle_carriage_cost) carriage_cost     --运费成本(元)       
               ,sum(drugs_cost) drugs_cost                  --兽药成本(元)       
               ,sum('0') input_vat_cost                     --进项税成本(元)   

               ,sum(case when recycle_type_id='1' then recycle_qty else '0' end) value_recyle_qty    --保值回收只数(只)
               ,sum(case when recycle_type_id='1' then recycle_weight else '0' end) value_recyle_weight  --保值回收重量(kg)
               ,sum(case when recycle_type_id='1' then recycle_cost else '0' end) value_recyle_cost  --保值回收金额(元)
               ,sum(case when recycle_type_id='1' then recyle_carriage_cost else '0' end) value_recyle_carriage_cost  --保值回收运费
               ,sum(case when recycle_type_id='1' then drugs_cost else '0' end) value_drugs_cost     --保值合同兽药金额
               ,sum(case when recycle_type_id='1' then '0' else '0' end) value_other_cost            --保值其他增减项

               ,sum(case when recycle_type_id='2' then recycle_qty else '0' end) mini_recyle_qty    --保底回收只数(只)
               ,sum(case when recycle_type_id='2' then recycle_weight else '0' end) mini_recyle_weight  --保底回收重量(kg)
               ,sum(case when recycle_type_id='2' then recycle_cost else '0' end) mini_recyle_cost  --保底回收金额(元)
               ,sum(case when recycle_type_id='2' then recyle_carriage_cost else '0' end) mini_recyle_carriage_cost  --保底回收运费
               ,sum(case when recycle_type_id='2' then drugs_cost else '0' end) mini_drugs_cost     --保底合同兽药金额
               ,sum(case when recycle_type_id='2' then '0' else '0' end) mini_other_cost            --保底其他增减项

               ,sum(case when recycle_type_id='3' then recycle_qty else '0' end) mkt_recyle_qty    --市场回收只数(只)
               ,sum(case when recycle_type_id='3' then recycle_weight else '0' end) mkt_recyle_weight  --市场回收重量(kg)
               ,sum(case when recycle_type_id='3' then recycle_cost else '0' end) mkt_recyle_cost  --市场回收金额(元)
               ,sum(case when recycle_type_id='3' then recyle_carriage_cost else '0' end) mkt_recyle_carriage_cost  --市场回收运费

               ,sum(distance) near_recycle_qty                                                    --近距离回收只数(只)
               ,sum(case when recycle_type_id='1' then distance else '0' end) near_value_recycle_qty --保值合同近距离回收只数(只)
               ,sum(case when recycle_type_id='2' then distance else '0' end) near_mini_recycle_qty  --保底合同近距离回收只数(只)
               
               ,sum(case when recycle_type_id='1' and kpi_type='PUT' then contract_qty else '0' end) value_put_qty    --保值投放只数(只)
               ,sum(case when recycle_type_id='2' and kpi_type='PUT' then contract_qty else '0' end) mini_put_qty     --保底投放只数(只)
               ,sum(case when recycle_type_id='1' and kpi_type='PUT' then put_cost else '0' end) value_put_cost       --保值投放成本
          FROM dwf_bird_material_cost_dd a1
         WHERE op_day='$OP_DAY'
         GROUP BY substr(recycle_date,1,6)            
               ,level1_org_id             
               ,level1_org_descr          
               ,level2_org_id             
               ,level2_org_descr          
               ,level3_org_id             
               ,level3_org_descr          
               ,level4_org_id             
               ,level4_org_descr          
               ,level5_org_id             
               ,level5_org_descr          
               ,level6_org_id             
               ,level6_org_descr          
               ,level7_org_id             
               ,level7_org_descr
               ,production_line_id        
               ,production_line_descr
               ,kpi_type     
        ) t1
  LEFT JOIN (SELECT month_id                           --期间(月份)        
                    ,level1_org_id                     --组织1级(股份)    
                    ,level2_org_id                     --组织2级(片联)     
                    ,level3_org_id                     --组织3级(片区)      
                    ,level4_org_id                     --组织4级(小片)      
                    ,level5_org_id                     --组织5级(公司)     
                    ,level6_org_id                     --组织6级(OU)      
                    ,level7_org_id                     --组织7级(库存组织)   
                    ,level1_businesstype_id            --业态1级          
                    ,level2_businesstype_id            --业态2级         
                    ,level3_businesstype_id            --业态3级          
                    ,level4_businesstype_id            --业态4级          
                    ,production_line_id                --产线
                    ,production_qty                    --自产产量(屠宰产品)
               FROM $TMP_DMF_BIRD_MATERIAL_COST_MM_1
              WHERE op_month='$OP_MONTH') t2
    ON (t1.month_id=t2.month_id
    AND t1.level1_org_id=t2.level1_org_id
    AND t1.level2_org_id=t2.level2_org_id
    AND t1.level3_org_id=t2.level3_org_id
    AND t1.level4_org_id=t2.level4_org_id
    AND t1.level5_org_id=t2.level5_org_id
    AND t1.level6_org_id=t2.level6_org_id
    --AND t1.level7_org_id=t2.level7_org_id
    --AND t1.level4_businesstype_id=t2.level4_businesstype_id
    AND t1.production_line_id=t2.production_line_id)


"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_MATERIAL_COST_MM='DMF_BIRD_MATERIAL_COST_MM'

CREATE_DMF_BIRD_MATERIAL_COST_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_MATERIAL_COST_MM(
  month_id                       string    --期间(月份)        
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
  ,production_line_id            string    --产线            
  ,production_line_descr         string    --产线            
  ,production_qty                string    --自产产量(kg)      
  ,stock_cost                    string    --原料成本(元)       
  ,carriage_cost                 string    --运费成本(元)       
  ,drugs_cost                    string    --兽药成本(元)       
  ,input_vat_cost                string    --进项税成本(元)      
  ,value_recyle_qty              string    --保值回收只数(只)     
  ,value_recyle_weight           string    --保值回收重量(kg)    
  ,value_recyle_cost             string    --保值回收金额(元)     
  ,value_recyle_carriage_cost    string    --保值回收运费        
  ,value_drugs_cost              string    --保值合同兽药金额      
  ,value_other_cost              string    --保值其他增减项       
  ,mini_recyle_qty               string    --保底回收只数(只)     
  ,mini_recyle_weight            string    --保底回收重量(kg)    
  ,mini_recyle_cost              string    --保底回收金额(元)     
  ,mini_recyle_carriage_cost     string    --保底回收运费        
  ,mini_drugs_cost               string    --保底合同兽药金额      
  ,mini_other_cost               string    --保底其他增减项       
  ,mkt_recyle_qty                string    --市场回收只数(只)     
  ,mkt_recyle_weight             string    --市场回收重量(kg)    
  ,mkt_recyle_cost               string    --市场回收金额(元)     
  ,mkt_recyle_carriage_cost      string    --市场回收运费(元)     
  ,near_recycle_qty              string    --近距离回收只数(只)    
  ,near_value_recycle_qty        string    --保值合同近距离回收只数(只)
  ,near_mini_recycle_qty         string    --保底合同近距离回收只数(只)
  ,value_put_qty                 string    --保值投放只数(只)     
  ,mini_put_qty                  string    --保底投放只数(只)     
  ,value_put_cost                string    --保值投放成本(元)
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_MATERIAL_COST_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_MATERIAL_COST_MM PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                            --期间(月份)        
       ,t1.day_id                             --期间(日)         
       ,t1.level1_org_id                      --组织1级(股份)      
       ,t1.level1_org_descr                   --组织1级(股份)      
       ,t1.level2_org_id                      --组织2级(片联)      
       ,t1.level2_org_descr                   --组织2级(片联)      
       ,t1.level3_org_id                      --组织3级(片区)      
       ,t1.level3_org_descr                   --组织3级(片区)      
       ,t1.level4_org_id                      --组织4级(小片)      
       ,t1.level4_org_descr                   --组织4级(小片)      
       ,t1.level5_org_id                      --组织5级(公司)      
       ,t1.level5_org_descr                   --组织5级(公司)      
       ,t1.level6_org_id                      --组织6级(OU)      
       ,t1.level6_org_descr                   --组织6级(OU)      
       ,t1.level7_org_id                      --组织7级(库存组织)    
       ,t1.level7_org_descr                   --组织7级(库存组织)    
       ,t1.level1_businesstype_id             --业态1级          
       ,t1.level1_businesstype_name           --业态1级          
       ,t1.level2_businesstype_id             --业态2级          
       ,t1.level2_businesstype_name           --业态2级          
       ,t1.level3_businesstype_id             --业态3级          
       ,t1.level3_businesstype_name           --业态3级          
       ,t1.level4_businesstype_id             --业态4级          
       ,t1.level4_businesstype_name           --业态4级          
       ,t1.production_line_id                 --产线            
       ,t1.production_line_descr              --产线            
       ,coalesce(t1.production_qty,'0')       --自产产量(kg)      
       ,coalesce(t1.stock_cost,'0')           --原料成本(元)       
       ,coalesce(t1.carriage_cost,'0')        --运费成本(元)       
       ,case when t1.kpi_type='BUY_BACK' then coalesce(t2.drugs_cost,'0')
        else '0' end drugs_cost                      --兽药成本(元)       
       ,case when t1.kpi_type='BUY_BACK' then coalesce(t3.input_vat_cost,'0')
        else '0' end input_vat_cost                  --进项税成本(元)      
       ,coalesce(t1.value_recyle_qty,'0')            --保值回收只数(只)     
       ,coalesce(t1.value_recyle_weight,'0')         --保值回收重量(kg)    
       ,coalesce(t1.value_recyle_cost,'0')           --保值回收金额(元)     
       ,coalesce(t1.value_recyle_carriage_cost,'0')  --保值回收运费        
       ,case when t1.kpi_type='BUY_BACK' then coalesce(t2.value_drugs_cost,'0')
        else '0' end value_drugs_cost                --保值合同兽药金额      
       ,coalesce(t1.value_other_cost,'0')            --保值其他增减项       
       ,coalesce(t1.mini_recyle_qty,'0')             --保底回收只数(只)     
       ,coalesce(t1.mini_recyle_weight,'0')          --保底回收重量(kg)    
       ,coalesce(t1.mini_recyle_cost,'0')            --保底回收金额(元)     
       ,coalesce(t1.mini_recyle_carriage_cost,'0')   --保底回收运费        
       ,case when t1.kpi_type='BUY_BACK' then coalesce(t2.mini_drugs_cost,'0')
        else '0' end mini_drugs_cost                 --保底合同兽药金额      
       ,coalesce(t1.mini_other_cost,'0')             --保底其他增减项      
       ,coalesce(t1.mkt_recyle_qty,'0')              --市场回收只数(只)     
       ,coalesce(t1.mkt_recyle_weight,'0')           --市场回收重量(kg)    
       ,coalesce(t1.mkt_recyle_cost,'0')             --市场回收金额(元)     
       ,coalesce(t1.mkt_recyle_carriage_cost,'0')    --市场回收运费(元)     
       ,coalesce(t1.near_recycle_qty,'0')            --近距离回收只数(只)    
       ,coalesce(t1.near_value_recycle_qty,'0')      --保值合同近距离回收只数(只)
       ,coalesce(t1.near_mini_recycle_qty,'0')       --保底合同近距离回收只数(只)
       ,coalesce(t1.value_put_qty,'0')               --保值投放只数(只)     
       ,coalesce(t1.mini_put_qty,'0')                --保底投放只数(只)     
       ,coalesce(t1.value_put_cost,'0')              --保值投放价格(元)
       ,'$CREATE_TIME' create_time                   --创建时间
  FROM (SELECT *
          FROM $TMP_DMF_BIRD_MATERIAL_COST_MM_2
         WHERE op_month='$OP_MONTH') t1
  LEFT JOIN (SELECT *
               FROM $TMP_DMF_BIRD_MATERIAL_COST_MM_0
              WHERE op_month='$OP_MONTH') t2
    ON (t1.month_id=t2.month_id
    AND t1.level5_org_id=t2.level5_org_id
    AND t1.level6_org_id=t2.level6_org_id
    AND t1.production_line_id=t2.production_line_id)
  LEFT JOIN (SELECT a2.level6_org_id
                    ,a2.level7_org_id
                    ,a1.month_id
                    ,a1.production_line_id
                    ,sum(coalesce(a1.input_vat_cost,0)) input_vat_cost
               FROM (SELECT substr(period_id,1,6) month_id
                            ,org_id
                            ,inv_org_id
                            ,case when product_line='10' then '1'
                                  when product_line='20' then '2'
                             else null end production_line_id
                            ,cost_amount17 input_vat_cost
                       FROM dmd_fin_exps_profits
                      WHERE coalesce(cost_amount17,0)!=0
                        AND currency_type='3') a1
                      INNER JOIN (SELECT inv_org_id,
                                         ou_org_id,
                                         level6_org_id,
                                         level7_org_id
                                    FROM mreport_global.dim_org_inv_management
                                   GROUP BY inv_org_id,
                                         ou_org_id,
                                         level6_org_id,
                                         level7_org_id) a2
                         ON (a1.org_id=a2.ou_org_id
                         AND a1.inv_org_id=a2.inv_org_id)
              GROUP BY a2.level6_org_id
                    ,a2.level7_org_id
                    ,a1.month_id
                    ,a1.production_line_id) t3
    ON (t1.level6_org_id=t3.level6_org_id
    AND t1.level7_org_id=t3.level7_org_id
    AND t1.month_id=t3.month_id
    AND t1.production_line_id=t3.production_line_id)
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_0;
    $INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_0;
    $CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_1;
    $INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_1;
    $CREATE_TMP_DMF_BIRD_MATERIAL_COST_MM_2;
    $INSERT_TMP_DMF_BIRD_MATERIAL_COST_MM_2;
    $CREATE_DMF_BIRD_MATERIAL_COST_MM;
    $INSERT_DMF_BIRD_MATERIAL_COST_MM;
"  -v