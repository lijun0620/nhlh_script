#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_contribution_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度利润贡献表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_contribution_mm.sh 201801"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_CONTRIBUTION_MM_0='TMP_DMF_BIRD_CONTRIBUTION_MM_0'

CREATE_TMP_DMF_BIRD_CONTRIBUTION_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_CONTRIBUTION_MM_0(
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
  ,currency_id                string   --币种ID
  ,currency_descr             string   --币种名称        
  ,breed_type_id              string   --养殖模式      
  ,breed_type_descr           string   --养殖模式      
  ,sales_province             string   --销售区域(省)   
  ,customer_id                string   --客户ID      
  ,customer_name              string   --客户名称      
  ,contract_no                string   --合同号       
  ,order_no                   string   --订单号       
  ,customer_channel_id        string   --客户渠道编码    
  ,customer_channel_descr     string   --客户渠道描述    
  ,salesman_id                string   --业务员ID     
  ,salesman_name              string   --业务员名称     
  ,level1_material_id         string   --物料1级      
  ,level1_material_descr      string   --物料1级      
  ,level2_material_id         string   --物料2级      
  ,level2_material_descr      string   --物料2级      
  ,level3_material_id         string   --物料3级      
  ,level3_material_descr      string   --物料3级      
  ,level4_material_id         string   --物料4级      
  ,level4_material_descr      string   --物料4级            
  ,item_code                  string   --物料编码      
  ,item_name                  string   --物料名称      
  ,sales_type_id              string   --销售类型编码    
  ,sales_type_descr           string   --销售类型描述    
  ,total_profits_amt          string   --总利润(元)    
  ,sales_nozq_weight          string   --销量(kg)    
  ,sales_lczq_qty             string   --销量（只）     
  ,main_prod_income           string   --主产品销售收入(元)
  ,byproduct_income           string   --副产品收入(元)  
  ,main_material_cost         string   --主要材料(元)   
  ,carriage_cost              string   --运费(元)     
  ,feeding_cost               string   --饲料成本(元)   
  ,packing_material_cost      string   --包装材料(元)   
  ,excipient_material_cost    string   --辅料材料(元)   
  ,drugs_cost                 string   --兽药成本(元)   
  ,seed_cost                  string   --苗种成本(元)   
  ,direct_labor_cost          string   --直接人工(元)   
  ,fuel_cost                  string   --燃料(元)     
  ,water_power_cost           string   --水电(元)     
  ,manufacture_change_cost    string   --制造费用-变动(元)
  ,manufacture_fixed_cost     string   --制造费用-固定(元)
  ,other_material_cost        string   --其他材料(元)   
  ,other_cost                 string   --其他费用(元)   
  ,foster_cost                string   --寄养费(元)    
  ,input_tax                  string   --进项税(元)    
  ,management_cost            string   --管理费用(元)   
  ,sales_change_cost          string   --销售费用-变动(元)
  ,sales_fixed_cost           string   --销售费用-固定(元)
  ,financing_cost             string   --财务费用(元)   
  ,marginal_contribution_cost string   --边际贡献(元)   
  ,store_down_loss            string   --存货跌价损失    
  ,ar_bad_loss                string   --应收坏账损失    
  ,OTHER_LOSSES_ASSET         string   --其他减值损失  
  ,other_business_income      string   --其他业务收入    
  ,other_business_cost        string   --其他业务成本    
  ,outter_business_income     string   --营业外收入     
  ,outter_business_cost       string   --营业外支出     
  ,EXT_CON_AMT                string   --提取保险合同准备金净额
  ,CHANGE_IN_FAIR_VALUE       string   --公允价值变动收益
  ,INVESTMENT_INCOME          string   --投资收益
  ,ASSET_DISPOSIT_INCOME      string   --资产处置收益
  ,OTHER_INCOME               string   --其他收益
  ,MAIN_INCOME                string   --主营业务收入
  ,BY_PROD_COST               string   --副产品成本
  ,BIO_ASSETS_DEC             string   --生物资产折旧
  ,INV_RESALE                 string   --存货减值转销
  ,OPERATING_TAX              string   --税金及附加
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_CONTRIBUTION_MM_0="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_CONTRIBUTION_MM_0 PARTITION(op_month='$OP_MONTH')
SELECT substr(t1.period_id,1,6) month_id   --期间(月份)    
       ,'' day_id                        --期间(日)     
       ,CASE WHEN t2.level1_org_id    is null THEN coalesce(t3.level1_org_id,'-1') ELSE coalesce(t2.level1_org_id,'-1')  END as level1_org_id                --一级组织编码
       ,CASE WHEN t2.level1_org_descr is null THEN coalesce(t3.level1_org_descr,'缺失') ELSE coalesce(t2.level1_org_descr,'缺失')  END as level1_org_descr   --一级组织描述
       ,CASE WHEN t2.level2_org_id    is null THEN coalesce(t3.level2_org_id,'-1') ELSE coalesce(t2.level2_org_id,'-1')  END as level2_org_id                --二级组织编码
       ,CASE WHEN t2.level2_org_descr is null THEN coalesce(t3.level2_org_descr,'缺失') ELSE coalesce(t2.level2_org_descr,'缺失')  END as level2_org_descr   --二级组织描述
       ,CASE WHEN t2.level3_org_id    is null THEN coalesce(t3.level3_org_id,'-1') ELSE coalesce(t2.level3_org_id,'-1')  END as level3_org_id               --三级组织编码
       ,CASE WHEN t2.level3_org_descr is null THEN coalesce(t3.level3_org_descr,'缺失') ELSE coalesce(t2.level3_org_descr,'缺失')  END as level3_org_descr   --三级组织描述
       ,CASE WHEN t2.level4_org_id    is null THEN coalesce(t3.level4_org_id,'-1') ELSE coalesce(t2.level4_org_id,'-1')  END as level4_org_id                --四级组织编码
       ,CASE WHEN t2.level4_org_descr is null THEN coalesce(t3.level4_org_descr,'缺失') ELSE coalesce(t2.level4_org_descr,'缺失')  END as level4_org_descr   --四级组织描述
       ,CASE WHEN t2.level5_org_id    is null THEN coalesce(t3.level5_org_id,'-1') ELSE coalesce(t2.level5_org_id,'-1')  END as level5_org_id                --五级组织编码
       ,CASE WHEN t2.level5_org_descr is null THEN coalesce(t3.level5_org_descr,'缺失') ELSE coalesce(t2.level5_org_descr,'缺失')  END as level5_org_descr   --五级组织描述
       ,CASE WHEN t2.level6_org_id    is null THEN coalesce(t3.level6_org_id,'-1') ELSE coalesce(t2.level6_org_id,'-1')  END as level6_org_id                --六级组织编码
       ,CASE WHEN t2.level6_org_descr is null THEN coalesce(t3.level6_org_descr,'缺失') ELSE coalesce(t2.level6_org_descr,'缺失')  END as level6_org_descr   --六级组织描述        
       ,t6.level7_org_id                   --组织7级(库存组织)
       ,t6.level7_org_descr                --组织7级(库存组织)
       ,t4.level1_businesstype_id          --业态1级      
       ,t4.level1_businesstype_name        --业态1级      
       ,t4.level2_businesstype_id          --业态2级      
       ,t4.level2_businesstype_name        --业态2级      
       ,t4.level3_businesstype_id          --业态3级      
       ,t4.level3_businesstype_name        --业态3级      
       ,t4.level4_businesstype_id          --业态4级      
       ,t4.level4_businesstype_name        --业态4级      
       ,t1.production_line_id              --产线        
       ,t1.production_line_descr           --产线  
       ,t1.currency_id                     --币种ID
       ,t1.currency_descr                  --币种名称      
       ,t1.breed_type_id                   --养殖模式      
       ,t1.breed_type_descr                --养殖模式      
       ,t1.sales_province                  --销售区域(省)   
       ,t1.customer_id                     --客户ID      
       ,t1.customer_name                   --客户名称      
       ,t1.contract_no                     --合同号       
       ,t1.order_no                        --订单号       
       ,t1.customer_channel_id             --客户渠道编码    
       ,t1.customer_channel_descr          --客户渠道描述    
       ,t1.salesman_id                     --业务员ID     
       ,t1.salesman_name                   --业务员名称     
       ,t5.MATERIAL_SEGMENT1_ID            --物料1级      
       ,t5.MATERIAL_SEGMENT1_DESC          --物料1级      
       ,concat(t5.MATERIAL_SEGMENT1_ID,t5.MATERIAL_SEGMENT2_ID) MATERIAL_SEGMENT2_ID            --物料2级      
       ,t5.MATERIAL_SEGMENT2_DESC          --物料2级      
       ,concat(t5.MATERIAL_SEGMENT1_ID,t5.MATERIAL_SEGMENT2_ID,t5.MATERIAL_SEGMENT3_ID) MATERIAL_SEGMENT3_ID            --物料3级      
       ,t5.MATERIAL_SEGMENT3_DESC          --物料3级      
       ,concat(t5.MATERIAL_SEGMENT1_ID,t5.MATERIAL_SEGMENT2_ID,t5.MATERIAL_SEGMENT3_ID,t5.MATERIAL_SEGMENT4_ID)  MATERIAL_SEGMENT4_ID           --物料4级      
       ,t5.MATERIAL_SEGMENT4_DESC          --物料4级      
       ,t5.inventory_item_code             --物料编码      
       ,t5.INVENTORY_ITEM_DESC             --物料名称      
       ,t1.sales_type_id                   --销售类型编码    
       ,t1.sales_type_descr                --销售类型描述    
       ,t1.total_profits_amt               --总利润(元)    
       ,t1.sales_nozq_weight               --销量(kg)    
       ,t1.sales_lczq_qty                  --销量（只）     
       ,t1.main_prod_income                --主产品销售收入(元)
       ,t1.byproduct_income                --副产品收入(元)  
       ,t1.main_material_cost              --主要材料(元)   
       ,t1.carriage_cost                   --运费(元)     
       ,t1.feeding_cost                    --饲料成本(元)   
       ,t1.packing_material_cost           --包装材料(元)   
       ,t1.excipient_material_cost         --辅料材料(元)   
       ,t1.drugs_cost                      --兽药成本(元)   
       ,t1.seed_cost                       --苗种成本(元)   
       ,t1.direct_labor_cost               --直接人工(元)   
       ,t1.fuel_cost                       --燃料(元)     
       ,t1.water_power_cost                --水电(元)     
       ,t1.manufacture_change_cost         --制造费用-变动(元)
       ,t1.manufacture_fixed_cost          --制造费用-固定(元)
       ,t1.other_material_cost             --其他材料(元)   
       ,t1.other_cost                      --其他费用(元)   
       ,t1.foster_cost                     --寄养费(元)    
       ,t1.input_tax                       --进项税(元)    
       ,t1.management_cost                 --管理费用(元)   
       ,t1.sales_change_cost               --销售费用-变动(元)
       ,t1.sales_fixed_cost                --销售费用-固定(元)
       ,t1.financing_cost                  --财务费用(元)   
       ,t1.marginal_contribution_cost      --边际贡献(元)   
       ,t1.store_down_loss                 --存货跌价损失    
       ,t1.ar_bad_loss                     --应收坏账损失    
       ,t1.OTHER_LOSSES_ASSET              --其他减值损失  
       ,t1.other_business_income           --其他业务收入    
       ,t1.other_business_cost             --其他业务成本    
       ,t1.outter_business_income          --营业外收入     
       ,t1.outter_business_cost            --营业外支出 
       ,EXT_CON_AMT                        --提取保险合同准备金净额
       ,CHANGE_IN_FAIR_VALUE               --公允价值变动收益
       ,INVESTMENT_INCOME                  --投资收益
       ,ASSET_DISPOSIT_INCOME              --资产处置收益
       ,OTHER_INCOME                       --其他收益
       ,MAIN_INCOME                   --主营业务收入
       ,BY_PROD_COST                  --副产品成本
       ,BIO_ASSETS_DEC                --生物资产折旧
       ,INV_RESALE                    --存货减值转销
       ,OPERATING_TAX                 --税金及附加
  FROM (SELECT *
          FROM mreport_poultry.dwf_bird_contribution_dd
         WHERE currency_id = '3'
           AND OP_DAY = '$OP_DAY') t1
  LEFT JOIN mreport_global.dim_org_management t2 
    ON T1.ORG_ID=T2.ORG_ID and T2.ATTRIBUTE5='1'
  LEFT JOIN mreport_global.dim_org_management t3 
    ON T1.ORG_ID=T3.ORG_ID and T1.BUSI_TYPE=T3.BUS_TYPE_ID and T3.ATTRIBUTE5='2'

  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t6
    ON (t1.inv_org_id=t6.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t4
    ON (t1.busi_type=t4.level4_businesstype_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.DWU_DIM_MATERIAL_NEW) t5
    ON (t1.item_code=t5.inventory_item_id
    AND T1.INV_ORG_ID = T5.INV_ORG_ID)
 
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_CONTRIBUTION_MM='DMF_BIRD_CONTRIBUTION_MM'

CREATE_DMF_BIRD_CONTRIBUTION_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_CONTRIBUTION_MM(
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
  ,currency_id                   string   --币种ID
  ,currency_descr                string   --币种名称     
  ,breed_type_id                 string    --养殖模式      
  ,breed_type_descr              string    --养殖模式      
  ,sales_province                string    --销售区域(省)   
  ,customer_id                   string    --客户ID      
  ,customer_name                 string    --客户名称      
  ,contract_no                   string    --合同号       
  ,order_no                      string    --订单号       
  ,customer_channel_id           string    --客户渠道编码    
  ,customer_channel_descr        string    --客户渠道描述    
  ,salesman_id                   string    --业务员ID     
  ,salesman_name                 string    --业务员名称     
  ,level1_material_id            string    --物料1级      
  ,level1_material_descr         string    --物料1级      
  ,level2_material_id            string    --物料2级      
  ,level2_material_descr         string    --物料2级      
  ,level3_material_id            string    --物料3级      
  ,level3_material_descr         string    --物料3级      
  ,level4_material_id            string    --物料4级      
  ,level4_material_descr         string    --物料4级           
  ,item_code                     string    --物料编码      
  ,item_name                     string    --物料名称      
  ,sales_type_id                 string    --销售类型编码    
  ,sales_type_descr              string    --销售类型描述   
  ,total_profits_amt             string    --总利润(元)    
  ,sales_nozq_weight             string    --销量(kg)    
  ,sales_lczq_qty                string    --销量（只）     
  ,main_prod_income              string    --主产品销售收入(元)
  ,byproduct_income              string    --副产品收入(元)  
  ,main_material_cost            string    --主要材料(元)   
  ,carriage_cost                 string    --运费(元)     
  ,feeding_cost                  string    --饲料成本(元)   
  ,packing_material_cost         string    --包装材料(元)   
  ,excipient_material_cost       string    --辅料材料(元)   
  ,drugs_cost                    string    --兽药成本(元)   
  ,seed_cost                     string    --苗种成本(元)   
  ,direct_labor_cost             string    --直接人工(元)   
  ,fuel_cost                     string    --燃料(元)     
  ,water_power_cost              string    --水电(元)     
  ,manufacture_change_cost       string    --制造费用-变动(元)
  ,manufacture_fixed_cost        string    --制造费用-固定(元)
  ,other_material_cost           string    --其他材料(元)   
  ,other_cost                    string    --其他费用(元)   
  ,foster_cost                   string    --寄养费(元)    
  ,input_tax                     string    --进项税(元)    
  ,management_cost               string    --管理费用(元)   
  ,sales_change_cost             string    --销售费用-变动(元)
  ,sales_fixed_cost              string    --销售费用-固定(元)
  ,financing_cost                string    --财务费用(元)   
  ,marginal_contribution_cost    string    --边际贡献(元)   
  ,store_down_loss               string    --存货跌价损失    
  ,ar_bad_loss                   string    --应收坏账损失    
  ,OTHER_LOSSES_ASSET            string   --其他减值损失  
  ,other_business_income         string    --其他业务收入    
  ,other_business_cost           string    --其他业务成本    
  ,outter_business_income        string    --营业外收入     
  ,outter_business_cost          string    --营业外支出 
  ,EXT_CON_AMT                string   --提取保险合同准备金净额
  ,CHANGE_IN_FAIR_VALUE       string   --公允价值变动收益
  ,INVESTMENT_INCOME          string   --投资收益
  ,ASSET_DISPOSIT_INCOME      string   --资产处置收益
  ,OTHER_INCOME               string   --其他收益
  ,MAIN_INCOME                string   --主营业务收入
  ,BY_PROD_COST               string   --副产品成本
  ,BIO_ASSETS_DEC             string   --生物资产折旧
  ,INV_RESALE                 string   --存货减值转销
  ,OPERATING_TAX              string   --税金及附加
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_CONTRIBUTION_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_CONTRIBUTION_MM PARTITION(op_month='$OP_MONTH')
SELECT month_id                                      --期间(月份)    
       ,day_id                                       --期间(日)     
       ,level1_org_id                                --组织1级(股份)  
       ,level1_org_descr                             --组织1级(股份)  
       ,level2_org_id                                --组织2级(片联)  
       ,level2_org_descr                             --组织2级(片联)  
       ,level3_org_id                                --组织3级(片区)  
       ,level3_org_descr                             --组织3级(片区)  
       ,level4_org_id                                --组织4级(小片)  
       ,level4_org_descr                             --组织4级(小片)  
       ,level5_org_id                                --组织5级(公司)  
       ,level5_org_descr                             --组织5级(公司)  
       ,level6_org_id                                --组织6级(OU)  
       ,level6_org_descr                             --组织6级(OU)  
       ,level7_org_id                                --组织7级(库存组织)
       ,level7_org_descr                             --组织7级(库存组织)
       ,level1_businesstype_id                       --业态1级      
       ,level1_businesstype_name                     --业态1级      
       ,level2_businesstype_id                       --业态2级      
       ,level2_businesstype_name                     --业态2级      
       ,level3_businesstype_id                       --业态3级      
       ,level3_businesstype_name                     --业态3级      
       ,level4_businesstype_id                       --业态4级      
       ,level4_businesstype_name                     --业态4级      
       ,production_line_id                           --产线        
       ,production_line_descr                        --产线
       ,currency_id                                  --币种ID
       ,currency_descr                               --币种名称        
       ,breed_type_id                                --养殖模式      
       ,breed_type_descr                             --养殖模式      
       ,sales_province                               --销售区域(省)   
       ,customer_id                                  --客户ID      
       ,customer_name                                --客户名称      
       ,REGEXP_REPLACE(contract_no,'\011','') contract_no   --合同号       
       ,order_no                                     --订单号       
       ,customer_channel_id                          --客户渠道编码    
       ,customer_channel_descr                       --客户渠道描述    
       ,salesman_id                                  --业务员ID     
       ,salesman_name                                --业务员名称     
       ,level1_material_id                           --物料1级      
       ,level1_material_descr                        --物料1级      
       ,level2_material_id                           --物料2级      
       ,level2_material_descr                        --物料2级      
       ,level3_material_id                           --物料3级      
       ,level3_material_descr                        --物料3级      
       ,level4_material_id                           --物料4级      
       ,level4_material_descr                        --物料4级       
       ,item_code                                    --物料编码      
       ,item_name                                    --物料名称      
       ,sales_type_id                                --销售类型编码    
       ,sales_type_descr                             --销售类型描述    
       ,sum(t1.total_profits_amt)               --总利润(元)    
       ,sum(t1.sales_nozq_weight)               --销量(kg)    
       ,sum(t1.sales_lczq_qty)                  --销量（只）     
       ,sum(t1.main_prod_income)                --主产品销售收入(元)
       ,sum(t1.byproduct_income)                --副产品收入(元)  
       ,sum(t1.main_material_cost)              --主要材料(元)   
       ,sum(t1.carriage_cost)                   --运费(元)     
       ,sum(t1.feeding_cost)                    --饲料成本(元)   
       ,sum(t1.packing_material_cost)           --包装材料(元)   
       ,sum(t1.excipient_material_cost)         --辅料材料(元)   
       ,sum(t1.drugs_cost)                      --兽药成本(元)   
       ,sum(t1.seed_cost)                       --苗种成本(元)   
       ,sum(t1.direct_labor_cost)               --直接人工(元)   
       ,sum(t1.fuel_cost)                       --燃料(元)     
       ,sum(t1.water_power_cost)                --水电(元)     
       ,sum(t1.manufacture_change_cost)         --制造费用-变动(元)
       ,sum(t1.manufacture_fixed_cost)          --制造费用-固定(元)
       ,sum(t1.other_material_cost)             --其他材料(元)   
       ,sum(t1.other_cost)                      --其他费用(元)   
       ,sum(t1.foster_cost)                     --寄养费(元)    
       ,sum(t1.input_tax)                       --进项税(元)    
       ,sum(t1.management_cost)                 --管理费用(元)   
       ,sum(t1.sales_change_cost)               --销售费用-变动(元)
       ,sum(t1.sales_fixed_cost)                --销售费用-固定(元)
       ,sum(t1.financing_cost)                  --财务费用(元)   
       ,sum(t1.marginal_contribution_cost)      --边际贡献(元)   
       ,sum(t1.store_down_loss)                 --存货跌价损失    
       ,sum(t1.ar_bad_loss)                     --应收坏账损失    
       ,sum(t1.OTHER_LOSSES_ASSET)              --其他减值损失  
       ,sum(t1.other_business_income)           --其他业务收入    
       ,sum(t1.other_business_cost)             --其他业务成本    
       ,sum(t1.outter_business_income)          --营业外收入     
       ,sum(t1.outter_business_cost)            --营业外支出 
       ,sum(EXT_CON_AMT)                        --提取保险合同准备金净额
       ,sum(CHANGE_IN_FAIR_VALUE)               --公允价值变动收益
       ,sum(INVESTMENT_INCOME)                  --投资收益
       ,sum(ASSET_DISPOSIT_INCOME )             --资产处置收益
       ,sum(OTHER_INCOME)                       --其他收益
       ,sum(MAIN_INCOME)                   --主营业务收入
       ,sum(BY_PROD_COST)                  --副产品成本
       ,sum(BIO_ASSETS_DEC)                --生物资产折旧
       ,sum(INV_RESALE)                    --存货减值转销
       ,sum(OPERATING_TAX)                 --税金及附加
       ,'$CREATE_TIME' create_time                   --创建时间
  FROM (SELECT *
          FROM $TMP_DMF_BIRD_CONTRIBUTION_MM_0
         WHERE op_month='$OP_MONTH') t1
GROUP by month_id                                      --期间(月份)    
       ,day_id                                       --期间(日)     
       ,level1_org_id                                --组织1级(股份)  
       ,level1_org_descr                             --组织1级(股份)  
       ,level2_org_id                                --组织2级(片联)  
       ,level2_org_descr                             --组织2级(片联)  
       ,level3_org_id                                --组织3级(片区)  
       ,level3_org_descr                             --组织3级(片区)  
       ,level4_org_id                                --组织4级(小片)  
       ,level4_org_descr                             --组织4级(小片)  
       ,level5_org_id                                --组织5级(公司)  
       ,level5_org_descr                             --组织5级(公司)  
       ,level6_org_id                                --组织6级(OU)  
       ,level6_org_descr                             --组织6级(OU)  
       ,level7_org_id                                --组织7级(库存组织)
       ,level7_org_descr                             --组织7级(库存组织)
       ,level1_businesstype_id                       --业态1级      
       ,level1_businesstype_name                     --业态1级      
       ,level2_businesstype_id                       --业态2级      
       ,level2_businesstype_name                     --业态2级      
       ,level3_businesstype_id                       --业态3级      
       ,level3_businesstype_name                     --业态3级      
       ,level4_businesstype_id                       --业态4级      
       ,level4_businesstype_name                     --业态4级      
       ,production_line_id                           --产线        
       ,production_line_descr                        --产线
       ,currency_id                                  --币种ID
       ,currency_descr                               --币种名称        
       ,breed_type_id                                --养殖模式      
       ,breed_type_descr                             --养殖模式      
       ,sales_province                               --销售区域(省)   
       ,customer_id                                  --客户ID      
       ,customer_name                                --客户名称      
       ,REGEXP_REPLACE(contract_no,'\011','')        --合同号       
       ,order_no                                     --订单号       
       ,customer_channel_id                          --客户渠道编码    
       ,customer_channel_descr                       --客户渠道描述    
       ,salesman_id                                  --业务员ID     
       ,salesman_name                                --业务员名称     
       ,level1_material_id                           --物料1级      
       ,level1_material_descr                        --物料1级      
       ,level2_material_id                           --物料2级      
       ,level2_material_descr                        --物料2级      
       ,level3_material_id                           --物料3级      
       ,level3_material_descr                        --物料3级      
       ,level4_material_id                           --物料4级      
       ,level4_material_descr                        --物料4级       
       ,item_code                                    --物料编码      
       ,item_name                                    --物料名称      
       ,sales_type_id                                --销售类型编码    
       ,sales_type_descr                             --销售类型描述 
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMF_BIRD_CONTRIBUTION_MM_0;
    $INSERT_TMP_DMF_BIRD_CONTRIBUTION_MM_0;
    $CREATE_DMF_BIRD_CONTRIBUTION_MM;
    $INSERT_DMF_BIRD_CONTRIBUTION_MM;
"  -v