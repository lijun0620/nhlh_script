#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_killed_profits_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰生产利润分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_killed_profits_mm.sh 201801"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 获取费用项
## 变量声明
TMP_DMF_BIRD_KILLED_PROFITS_MM_0='TMP_DMF_BIRD_KILLED_PROFITS_MM_0'

CREATE_TMP_DMF_BIRD_KILLED_PROFITS_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_KILLED_PROFITS_MM_0(
  month_id                    string   --期间(月份)    
  ,day_id                     string   --期间(日)     
  ,level1_org_id              string   --组织1级(股份)  
  ,level1_org_descr           string   --组织1级(股份)  
  ,level2_org_id              string   --组织2级(片联)  
  ,level2_org_descr           string   --组织2级(片联)  
  ,level3_org_id              string   --组织3级(片区)  
  ,level3_org_descr           string   --组织3级(片区)  
  ,level4_org_id              string   --组织4级(小片)  
  ,level4_org_descr           string   --组织4级(小片)  
  ,level5_org_id              string   --组织5级(公司)  
  ,level5_org_descr           string   --组织5级(公司)  
  ,level6_org_id              string   --组织6级(OU)  
  ,level6_org_descr           string   --组织6级(OU)  
  ,level7_org_id              string   --组织7级(库存组织)
  ,level7_org_descr           string   --组织7级(库存组织)
  ,level1_businesstype_id     string   --业态1级      
  ,level1_businesstype_name   string   --业态1级      
  ,level2_businesstype_id     string   --业态2级      
  ,level2_businesstype_name   string   --业态2级      
  ,level3_businesstype_id     string   --业态3级      
  ,level3_businesstype_name   string   --业态3级      
  ,level4_businesstype_id     string   --业态4级      
  ,level4_businesstype_name   string   --业态4级      
  ,production_line_id         string   --产线        
  ,production_line_descr      string   --产线

  ,production_qty             string   --产量(kg)
  ,self_buy_production_qty    string   --自购产量(kg)
  ,sales_qty                  string   --销量
  ,sales_price_a              string   --月末累计综合售价A金额（元）
  ,sales_price_b              string   --月末累计综合售价B金额（元
  ,stock_cost                 string   --原料成本(元)
  ,input_tax                  string   --进项税(元)
  ,production_fee             string   --产量费用(费用分拆)
  ,sales_fee                  string   --销量费用(费用分拆)
  ,byproduct_income           string   --副产品收入(元)

  ,sales_hair_weights         string   --毛重量         
  ,sales_hair_amt             string   --毛售价(元)      
  ,for_blood_killed_qty       string   --鸡(鸭)血结算数量(只)
  ,sales_blood_amt            string   --血售价(元)      
  ,for_sausage_killed_qty     string   --鸡(鸭)肠结算数量(只)
  ,sales_sausage_amt          string   --肠售价(元)
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_KILLED_PROFITS_MM_0="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_KILLED_PROFITS_MM_0 PARTITION(op_month='$OP_MONTH')
SELECT substr(regexp_replace(t1.period_id,'-',''),1,6) month_id   --期间(月份)    
       ,null day_id                                     --期间(日)     
       ,t2.level1_org_id                                --组织1级(股份)  
       ,t2.level1_org_descr                             --组织1级(股份)  
       ,t2.level2_org_id                                --组织2级(片联)  
       ,t2.level2_org_descr                             --组织2级(片联)  
       ,t2.level3_org_id                                --组织3级(片区)  
       ,t2.level3_org_descr                             --组织3级(片区)  
       ,t2.level4_org_id                                --组织4级(小片)  
       ,t2.level4_org_descr                             --组织4级(小片)  
       ,t2.level5_org_id                                --组织5级(公司)  
       ,t2.level5_org_descr                             --组织5级(公司)  
       ,t2.level6_org_id                                --组织6级(OU)  
       ,t2.level6_org_descr                             --组织6级(OU)  
       ,null level7_org_id                              --组织7级(库存组织)
       ,null level7_org_descr                           --组织7级(库存组织)
       ,t3.level1_businesstype_id                       --业态1级      
       ,t3.level1_businesstype_name                     --业态1级      
       ,t3.level2_businesstype_id                       --业态2级      
       ,t3.level2_businesstype_name                     --业态2级      
       ,t3.level3_businesstype_id                       --业态3级      
       ,t3.level3_businesstype_name                     --业态3级      
       ,t3.level4_businesstype_id                       --业态4级      
       ,t3.level4_businesstype_name                     --业态4级      
       ,case when product_line='10' then '1'
             when product_line='20' then '2'
        else null end production_line_id                --产线        
       ,case when product_line='10' then '鸡线'
             when product_line='20' then '鸭线'
        else null end production_line_descr             --产线   
       
       ,t1.production_qty                               --产量(kg)
       ,t1.self_buy_production_qty                      --自购产量(kg)
       ,t1.sales_qty                                    --销量
       ,t1.sales_price_a                                --月末累计综合售价A金额（元）
       ,t1.sales_price_b                                --月末累计综合售价B金额（元
       ,t1.stock_cost                                   --原料成本(元)
       ,t1.input_tax                                    --进项税(元)
       ,t1.production_fee                               --产量费用(费用分拆)
       ,t1.sales_fee                                    --销量费用(费用分拆)
       ,t1.byproduct_income                             --副产品收入(元)

       ,0 sales_hair_weights                            --毛重量         
       ,0 sales_hair_amt                                --毛售价(元)      
       ,0 for_blood_killed_qty                          --鸡(鸭)血结算数量(只)
       ,0 sales_blood_amt                               --血售价(元)      
       ,0 for_sausage_killed_qty                        --鸡(鸭)肠结算数量(只)
       ,0 sales_sausage_amt                             --肠售价(元) 
  FROM (SELECT org_id
               ,creation_date period_id
               ,null bus_type
               ,product_line
               ,prod_qty production_qty                   --产量(kg)
               ,inner_qty  self_buy_production_qty        --自购产量(kg)
               ,out_qty sales_qty                         --销量
               ,0 sales_price_a                           --月末累计综合售价A金额（元）
               ,0 sales_price_b                           --月末累计综合售价B金额（元
               ,0 stock_cost                              --原料成本(元)
               ,0 input_tax                               --进项税(元)
               ,0 production_fee                          --产量费用(费用分拆)
               ,0 sales_fee                               --销量费用(费用分拆)
               ,0 byproduct_income                        --副产品收入(元)
          FROM dwu_cw_cw28_dd
         WHERE op_day='$OP_DAY'
           AND coalesce(prod_qty,0)+coalesce(inner_qty,0)!=0
        UNION ALL
        SELECT org_id
               ,creation_date period_id
               ,null bus_type
               ,product_line
               ,0 production_qty                    --产量(kg)
               ,0 self_buy_production_qty           --自购产量(kg)
               ,0 sales_qty                         --销量
               ,amount_a sales_price_a              --月末累计综合售价A金额（元）
               ,case when coalesce(amount_b,0)=0 then amount_a
                else amount_b end sales_price_b     --月末累计综合售价B金额（元
               ,0 stock_cost                        --原料成本(元)
               ,0 input_tax                         --进项税(元)
               ,0 production_fee                    --产量费用(费用分拆)
               ,0 sales_fee                         --销量费用(费用分拆)
               ,0 byproduct_income                  --副产品收入(元)
          FROM dwu_cw_cw31_dd
         WHERE op_day='$OP_DAY'
           AND coalesce(amount_a,0)+coalesce(amount_b,0)!=0
        UNION ALL
        SELECT org_id
               ,period_id
               ,null bus_type
               ,product_line
               ,0 production_qty                    --产量(kg)
               ,0 self_buy_production_qty           --自购产量(kg)
               ,0 sales_qty                         --销量
               ,0 sales_price_a                     --月末累计综合售价A金额（元）
               ,0 sales_price_b                     --月末累计综合售价B金额（元）
               ,0 stock_cost                        --原料成本
               ,0 input_tax                         --进项税(元)
               ,0 production_fee                    --产量费用(费用分拆)
               ,0 sales_fee                         --销量费用(费用分拆)
               ,coalesce(loc_income,0) byproduct_income  --副产品收入(元)
          FROM (SELECT *
                  FROM dwu_order_income) a1
         INNER JOIN (SELECT *
                       FROM mreport_global.dim_crm_item
                      WHERE prd_line_cate_id='1-16BW2M') a2
            ON (a1.material_item_id=a2.item_code)
        UNION ALL
        SELECT ou_id org_id
               ,regexp_replace(cost_date,'-','') period_id
               ,null bus_type
               ,line_type product_line
               ,0 production_qty                    --产量(kg)
               ,0 self_buy_production_qty           --自购产量(kg)
               ,0 sales_qty                         --销量
               ,0 sales_price_a                     --月末累计综合售价A金额（元）
               ,0 sales_price_b                     --月末累计综合售价B金额（元）
               ,coalesce(duck_chicken_amount,0)+coalesce(drugs_amount,0)+coalesce(freight_fee_amount,0) stock_cost  --原料成本
               ,0 input_tax                         --进项税(元)
               ,coalesce(wip_chg_amount,0)+coalesce(wip_fix_amount,0)+coalesce(power_amount,0)+coalesce(packing_amount,0)+coalesce(manual_amount,0) production_fee --产量费用(费用分拆)
               ,0 sales_fee                         --销量费用(费用分拆)
               ,coalesce(byproduct_amount,0) byproduct_income  --副产品收入(元)
          FROM dwu_qw_account_cost_dd               --cw30
         WHERE op_day='$OP_DAY'
           AND coalesce(duck_chicken_amount,0)+coalesce(drugs_amount,0)+coalesce(freight_fee_amount,0)+coalesce(wip_chg_amount,0)+coalesce(wip_fix_amount,0)+coalesce(power_amount,0)+coalesce(byproduct_amount,0)!=0
        UNION ALL
        SELECT org_id
               ,period_id
               ,null bus_type
               ,product_line
               ,0 production_qty                    --产量(kg)
               ,0 self_buy_production_qty           --自购产量(kg)
               ,0 sales_qty                         --销量
               ,0 sales_price_a                     --月末累计综合售价A金额（元）
               ,0 sales_price_b                     --月末累计综合售价B金额（元
               ,0 stock_cost                        --原料成本(元)
               ,coalesce(cost_amount17,0) input_tax --进项税(元)
               ,0 production_fee                    --产量费用(费用分拆)
               ,coalesce(selling_expense_fixed,0)+coalesce(selling_expense_change,0)+coalesce(fin_expense,0)+coalesce(admini_expense,0) sales_fee   --销量费用(费用分拆)
               ,0 byproduct_income                  --副产品收入(元)
          FROM dmd_fin_exps_profits                 --cw19
         WHERE currency_type='3') t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t2
    ON (t1.org_id=t2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t3
    ON (t1.bus_type=t3.level4_businesstype_id)
"

###########################################################################################
## 获取费用项
## 变量声明
TMP_DMF_BIRD_KILLED_PROFITS_MM_1='TMP_DMF_BIRD_KILLED_PROFITS_MM_1'

CREATE_TMP_DMF_BIRD_KILLED_PROFITS_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_KILLED_PROFITS_MM_1(
  month_id                    string   --期间(月份)    
  ,day_id                     string   --期间(日)     
  ,level1_org_id              string   --组织1级(股份)  
  ,level1_org_descr           string   --组织1级(股份)  
  ,level2_org_id              string   --组织2级(片联)  
  ,level2_org_descr           string   --组织2级(片联)  
  ,level3_org_id              string   --组织3级(片区)  
  ,level3_org_descr           string   --组织3级(片区)  
  ,level4_org_id              string   --组织4级(小片)  
  ,level4_org_descr           string   --组织4级(小片)  
  ,level5_org_id              string   --组织5级(公司)  
  ,level5_org_descr           string   --组织5级(公司)  
  ,level6_org_id              string   --组织6级(OU)  
  ,level6_org_descr           string   --组织6级(OU)  
  ,level7_org_id              string   --组织7级(库存组织)
  ,level7_org_descr           string   --组织7级(库存组织)
  ,level1_businesstype_id     string   --业态1级      
  ,level1_businesstype_name   string   --业态1级      
  ,level2_businesstype_id     string   --业态2级      
  ,level2_businesstype_name   string   --业态2级      
  ,level3_businesstype_id     string   --业态3级      
  ,level3_businesstype_name   string   --业态3级      
  ,level4_businesstype_id     string   --业态4级      
  ,level4_businesstype_name   string   --业态4级      
  ,production_line_id         string   --产线        
  ,production_line_descr      string   --产线

  ,production_qty             string   --产量(kg)
  ,self_buy_production_qty    string   --自购产量(kg)
  ,sales_qty                  string   --销量
  ,sales_price_a              string   --月末累计综合售价A金额（元）
  ,sales_price_b              string   --月末累计综合售价B金额（元
  ,stock_cost                 string   --原料成本(元)
  ,input_tax                  string   --进项税(元)
  ,production_fee             string   --产量费用(费用分拆)
  ,sales_fee                  string   --销量费用(费用分拆)
  ,byproduct_income           string   --副产品收入(元)

  ,sales_hair_weights         string   --毛重量         
  ,sales_hair_amt             string   --毛售价(元)      
  ,for_blood_killed_qty       string   --鸡(鸭)血结算数量(只)
  ,sales_blood_amt            string   --血售价(元)      
  ,for_sausage_killed_qty     string   --鸡(鸭)肠结算数量(只)
  ,sales_sausage_amt          string   --肠售价(元)
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_KILLED_PROFITS_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_KILLED_PROFITS_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT substr(t1.period_id,1,6) month_id                --期间(月份)    
       ,null day_id                                     --期间(日)     
       ,t2.level1_org_id                                --组织1级(股份)  
       ,t2.level1_org_descr                             --组织1级(股份)  
       ,t2.level2_org_id                                --组织2级(片联)  
       ,t2.level2_org_descr                             --组织2级(片联)  
       ,t2.level3_org_id                                --组织3级(片区)  
       ,t2.level3_org_descr                             --组织3级(片区)  
       ,t2.level4_org_id                                --组织4级(小片)  
       ,t2.level4_org_descr                             --组织4级(小片)  
       ,t2.level5_org_id                                --组织5级(公司)  
       ,t2.level5_org_descr                             --组织5级(公司)  
       ,t2.level6_org_id                                --组织6级(OU)  
       ,t2.level6_org_descr                             --组织6级(OU)  
       ,null level7_org_id                              --组织7级(库存组织)
       ,null level7_org_descr                           --组织7级(库存组织)
       ,t3.level1_businesstype_id                       --业态1级      
       ,t3.level1_businesstype_name                     --业态1级      
       ,t3.level2_businesstype_id                       --业态2级      
       ,t3.level2_businesstype_name                     --业态2级      
       ,t3.level3_businesstype_id                       --业态3级      
       ,t3.level3_businesstype_name                     --业态3级      
       ,t3.level4_businesstype_id                       --业态4级      
       ,t3.level4_businesstype_name                     --业态4级      
       ,t1.production_line_id                           --产线        
       ,t1.production_line_descr                        --产线   
       
       ,0 production_qty                                --产量(kg)
       ,0 self_buy_production_qty                       --自购产量(kg)
       ,0 sales_qty                                     --销量
       ,0 sales_price_a                                 --月末累计综合售价A金额（元）
       ,0 sales_price_b                                 --月末累计综合售价B金额（元
       ,0 stock_cost                                    --原料成本(元)
       ,0 input_tax                                     --进项税(元)
       ,0 production_fee                                --产量费用(费用分拆)
       ,0 sales_fee                                     --销量费用(费用分拆)
       ,0 byproduct_income                              --副产品收入(元)

       ,case when bp_item_code in('4099000004','4502001005') then t1.settlement_quantity
        else 0 end sales_hair_weights                   --毛重量         
       ,case when bp_item_code in('4099000004','4502001005') then t1.settlement_amount
        else 0 end sales_hair_amt                       --毛售价(元)      
       ,case when bp_item_code in('4099000005','4502001006') then t1.settlement_quantity
        else 0 end for_blood_killed_qty                 --鸡(鸭)血结算数量(只)
       ,case when bp_item_code in('4099000005','4502001006') then t1.settlement_amount
        else 0 end sales_blood_amt                      --血售价(元)      
       ,case when bp_item_code in('4099000003','4502001007') then t1.settlement_quantity
        else 0 end for_sausage_killed_qty               --鸡(鸭)肠结算数量(只)
       ,case when bp_item_code in('4099000003','4502001007') then t1.settlement_amount
        else 0 end sales_sausage_amt                    --肠售价(元) 
  FROM (SELECT organization_id org_id
               ,regexp_replace(period_name,'-','') period_id
               ,bp_item_code
               ,bp_item_desc
               ,settlement_quantity                     --结算数量
               ,settlement_amount                       --结算金额
               ,null busi_type                          --业态
               ,case when product_line='10' then '1'
                     when product_line='20' then '2'
                else null end production_line_id        --产线
               ,case when product_line='10' then '鸡线'
                     when product_line='20' then '鸭线'
                else null end production_line_descr     --产线
          FROM dwu_cw_bproduct_price_dd                 --CW21
         WHERE op_day='$OP_DAY'
           AND coalesce(settlement_quantity,0)+coalesce(settlement_amount,0)!=0
           AND bp_item_code in('4099000003','4099000004','4099000005','4502001005','4502001007','4502001006')) t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t2
    ON (t1.org_id=t2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t3
    ON (t1.busi_type=t3.level4_businesstype_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_KILLED_PROFITS_MM='DMF_BIRD_KILLED_PROFITS_MM'

CREATE_DMF_BIRD_KILLED_PROFITS_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_KILLED_PROFITS_MM(
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

  ,production_qty                string    --产量(kg)
  ,self_buy_production_qty       string    --自购产量(kg)
  ,sales_qty                     string    --销量
  ,sales_price_a                 string    --月末累计综合售价A金额（元）
  ,sales_price_b                 string    --月末累计综合售价B金额（元）
  ,stock_cost                    string    --原料成本(元)
  ,input_tax                     string    --进项税(元)
  ,production_fee                string    --产量费用(费用分拆)
  ,sales_fee                     string    --销量费用(费用分拆)
  ,byproduct_income              string    --副产品收入(元)
    
  ,sales_hair_weights            string    --毛重量         
  ,sales_hair_amt                string    --毛售价(元)      
  ,for_blood_killed_qty          string    --鸡(鸭)血结算数量(只)
  ,sales_blood_amt               string    --血售价(元)      
  ,for_sausage_killed_qty        string    --鸡(鸭)肠结算数量(只)
  ,sales_sausage_amt             string    --肠售价(元)
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_KILLED_PROFITS_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_KILLED_PROFITS_MM PARTITION(op_month='$OP_MONTH')
SELECT month_id                                   --期间(月份)    
       ,day_id                                    --期间(日)     
       ,level1_org_id                             --组织1级(股份)  
       ,level1_org_descr                          --组织1级(股份)  
       ,level2_org_id                             --组织2级(片联)  
       ,level2_org_descr                          --组织2级(片联)  
       ,level3_org_id                             --组织3级(片区)  
       ,level3_org_descr                          --组织3级(片区)  
       ,level4_org_id                             --组织4级(小片)  
       ,level4_org_descr                          --组织4级(小片)  
       ,level5_org_id                             --组织5级(公司)  
       ,level5_org_descr                          --组织5级(公司)  
       ,level6_org_id                             --组织6级(OU)  
       ,level6_org_descr                          --组织6级(OU)  
       ,level7_org_id                             --组织7级(库存组织)
       ,level7_org_descr                          --组织7级(库存组织)
       ,level1_businesstype_id                    --业态1级      
       ,level1_businesstype_name                  --业态1级      
       ,level2_businesstype_id                    --业态2级      
       ,level2_businesstype_name                  --业态2级      
       ,level3_businesstype_id                    --业态3级      
       ,level3_businesstype_name                  --业态3级      
       ,level4_businesstype_id                    --业态4级      
       ,level4_businesstype_name                  --业态4级      
       ,production_line_id                        --产线        
       ,production_line_descr                     --产线
       
       ,sum(coalesce(production_qty,0))                            --产量(kg)
       ,sum(coalesce(self_buy_production_qty,0))                   --自购产量(kg)
       ,sum(coalesce(sales_qty,0))                                 --销量
       ,sum(coalesce(sales_price_a,0))                             --月末累计综合售价A金额（元）
       ,sum(coalesce(sales_price_b,0))                             --月末累计综合售价B金额（元
       ,sum(coalesce(stock_cost,0))                                --原料成本(元)
       ,sum(coalesce(input_tax,0))                                 --进项税(元)
       ,sum(coalesce(production_fee,0))                            --产量费用(费用分拆)
       ,sum(coalesce(sales_fee,0))                                 --销量费用(费用分拆)
       ,sum(coalesce(byproduct_income,0))                          --副产品收入(元)
 
       ,sum(coalesce(sales_hair_weights,0))                        --毛重量         
       ,sum(coalesce(sales_hair_amt,0))                            --毛售价(元)      
       ,sum(coalesce(for_blood_killed_qty,0))                      --鸡(鸭)血结算数量(只)
       ,sum(coalesce(sales_blood_amt,0))                           --血售价(元)      
       ,sum(coalesce(for_sausage_killed_qty,0))                    --鸡(鸭)肠结算数量(只)
       ,sum(coalesce(sales_sausage_amt,0))                         --肠售价(元)
       ,'$CREATE_TIME' create_time                --创建时间
  FROM (SELECT *
          FROM $TMP_DMF_BIRD_KILLED_PROFITS_MM_0
         WHERE op_month='$OP_MONTH'
        UNION ALL
        SELECT *
          FROM $TMP_DMF_BIRD_KILLED_PROFITS_MM_1
         WHERE op_month='$OP_MONTH') t1
 WHERE level2_org_id NOT IN('1015')
   AND production_line_id is not null
 GROUP BY month_id                                   --期间(月份)    
       ,day_id                                    --期间(日)     
       ,level1_org_id                             --组织1级(股份)  
       ,level1_org_descr                          --组织1级(股份)  
       ,level2_org_id                             --组织2级(片联)  
       ,level2_org_descr                          --组织2级(片联)  
       ,level3_org_id                             --组织3级(片区)  
       ,level3_org_descr                          --组织3级(片区)  
       ,level4_org_id                             --组织4级(小片)  
       ,level4_org_descr                          --组织4级(小片)  
       ,level5_org_id                             --组织5级(公司)  
       ,level5_org_descr                          --组织5级(公司)  
       ,level6_org_id                             --组织6级(OU)  
       ,level6_org_descr                          --组织6级(OU)  
       ,level7_org_id                             --组织7级(库存组织)
       ,level7_org_descr                          --组织7级(库存组织)
       ,level1_businesstype_id                    --业态1级      
       ,level1_businesstype_name                  --业态1级      
       ,level2_businesstype_id                    --业态2级      
       ,level2_businesstype_name                  --业态2级      
       ,level3_businesstype_id                    --业态3级      
       ,level3_businesstype_name                  --业态3级      
       ,level4_businesstype_id                    --业态4级      
       ,level4_businesstype_name                  --业态4级      
       ,production_line_id                        --产线        
       ,production_line_descr                     --产线
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;

    $CREATE_TMP_DMF_BIRD_KILLED_PROFITS_MM_0;
    $INSERT_TMP_DMF_BIRD_KILLED_PROFITS_MM_0;
    $CREATE_TMP_DMF_BIRD_KILLED_PROFITS_MM_1;
    $INSERT_TMP_DMF_BIRD_KILLED_PROFITS_MM_1;
    $CREATE_DMF_BIRD_KILLED_PROFITS_MM;
    $INSERT_DMF_BIRD_KILLED_PROFITS_MM;
"  -v