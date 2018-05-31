#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_contract_cost_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰合同原料成本分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_contract_cost_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMF_BIRD_CONTRACT_COST_MM_1='TMP_DMF_BIRD_CONTRACT_COST_MM_1'

CREATE_TMP_DMF_BIRD_CONTRACT_COST_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_CONTRACT_COST_MM_1(
  month_id                        string    --期间(月份)        
  ,day_id                         string    --期间(日)         
  ,level1_org_id                  string    --组织1级(股份)      
  ,level1_org_descr               string    --组织1级(股份)      
  ,level2_org_id                  string    --组织2级(片联)      
  ,level2_org_descr               string    --组织2级(片联)      
  ,level3_org_id                  string    --组织3级(片区)      
  ,level3_org_descr               string    --组织3级(片区)      
  ,level4_org_id                  string    --组织4级(小片)      
  ,level4_org_descr               string    --组织4级(小片)      
  ,level5_org_id                  string    --组织5级(公司)      
  ,level5_org_descr               string    --组织5级(公司)      
  ,level6_org_id                  string    --组织6级(OU)      
  ,level6_org_descr               string    --组织6级(OU)      
  ,level7_org_id                  string    --组织7级(库存组织)    
  ,level7_org_descr               string    --组织7级(库存组织)    
  ,level1_businesstype_id         string    --业态1级          
  ,level1_businesstype_name       string    --业态1级          
  ,level2_businesstype_id         string    --业态2级          
  ,level2_businesstype_name       string    --业态2级          
  ,level3_businesstype_id         string    --业态3级          
  ,level3_businesstype_name       string    --业态3级          
  ,level4_businesstype_id         string    --业态4级          
  ,level4_businesstype_name       string    --业态4级          
  ,production_line_id             string    --产线            
  ,production_line_descr          string    --产线
  ,recyle_qty                     string    --回收只数(只)
  ,recyle_weight                  string    --回收重量(毛鸡毛鸭重量)
  ,base_recycle_cost              string    --基础回收金额
  ,recyle_cost                    string    --回收金额(元)
  ,no_drugs_subsidy_cost          string    --无药残补贴金额(元)
  ,other_subsidy_cost             string    --其他项目补贴(元)
  ,contract_qty                   string    --投放数量
  ,put_cost                       string    --苗投放成本
  ,used_stock_qty                 string    --物料重量
  ,used_stock_cost                string    --物料成本
  ,settlement_weight              string    --结算重量(kg)
  ,recyle_carriage_cost           string    --合同运费(元)
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_CONTRACT_COST_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_CONTRACT_COST_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                            --期间(月份)        
       ,null day_id                           --期间(日)         
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
       ,t1.recycle_qty                        --回收只数(只)
       ,t1.recycle_weight                     --回收重量(毛鸡毛鸭重量)
       ,t1.base_recycle_cost                  --基础回收金额
       ,t1.recycle_cost                       --回收金额(元)
       ,t1.no_drugs_subsidy_cost              --无药残补贴金额(元)
       ,t1.other_subsidy_cost                 --其他项目补贴(元)
       ,t1.contract_qty                       --投放数量
       ,t1.put_cost                           --苗成本
       ,t1.used_stock_qty                     --物料重量
       ,t1.used_stock_cost                    --物料成本
       ,t1.settlement_weight                  --结算重量(kg)
       ,t1.recyle_carriage_cost               --合同运费(元)
  FROM (SELECT substr(a1.recycle_date,1,6) month_id       --期间(月)
               ,a1.level1_org_id                          --组织1级
               ,a1.level1_org_descr                       --组织1级
               ,a1.level2_org_id                          --组织2级
               ,a1.level2_org_descr                       --组织2级
               ,a1.level3_org_id                          --组织3级
               ,a1.level3_org_descr                       --组织3级
               ,a1.level4_org_id                          --组织4级
               ,a1.level4_org_descr                       --组织4级
               ,a1.level5_org_id                          --组织5级
               ,a1.level5_org_descr                       --组织5级
               ,a1.level6_org_id                          --组织6级
               ,a1.level6_org_descr                       --组织6级
               ,a1.level7_org_id                          --组织7级
               ,a1.level7_org_descr                       --组织7级
               ,a1.level1_businesstype_id                 --业态1级
               ,a1.level1_businesstype_name               --业态1级
               ,a1.level2_businesstype_id                 --业态2级
               ,a1.level2_businesstype_name               --业态2级
               ,a1.level3_businesstype_id                 --业态3级
               ,a1.level3_businesstype_name               --业态3级
               ,a1.level4_businesstype_id                 --业态4级
               ,a1.level4_businesstype_name               --业态4级
               ,a1.production_line_id                     --产线代码
               ,a1.production_line_descr                  --产线描述               
               ,sum(a1.recycle_qty) recycle_qty           --回收只数(只)
               ,sum(a1.recycle_weight) recycle_weight     --回收重量(毛鸡毛鸭重量)
               ,sum(a1.base_recycle_cost) base_recycle_cost    --基础回收金额(元)
               ,sum(a1.recycle_cost) recycle_cost         --回收金额
               ,'0' no_drugs_subsidy_cost                 --无药残补贴金额(元)  
               ,sum(a1.other_subsidy_cost) other_subsidy_cost   --其他项目补贴(元)
               ,sum(a1.contract_qty) contract_qty         --投放数量
               ,sum(a1.put_amount) put_cost               --苗金额
               ,sum(case when a1.kpi_type='BUY_BACK' then coalesce(a1.material_weight,'0')
                    else '0' end) used_stock_qty          --物料重量
               ,sum(a1.material_amount) used_stock_cost   --物料成本
               ,sum(a1.recycle_weight) settlement_weight  --结算重量(kg)
               ,sum(a1.recyle_carriage_cost) recyle_carriage_cost --合同运费(元)
          FROM dwf_bird_material_cost_dd a1
          LEFT JOIN (SELECT b1.contractnumber contract_no
                            ,b1.meaning
                            ,b1.contracttype
                            ,b1.guarantees_market
                       FROM dwu_qw_contract_dd b1
                      INNER JOIN (SELECT attribute1 production_line_id,
                                         meaning,
                                         description
                                    FROM mreport_global.ods_ebs_fnd_lookup_values
                                   WHERE lookup_type='CUX_TYPE_OF_CONTRACT'
                                     AND language='ZHS'
                                     AND tag='外围') b2
                         ON (b1.meaning=b2.production_line_id
                         AND b1.contracttype=b2.meaning
                         AND b1.guarantees_market=b2.description)
                      WHERE b1.op_day='$OP_DAY'
                      GROUP BY b1.contractnumber
                            ,b1.meaning
                            ,b1.contracttype
                            ,b1.guarantees_market) a2
            ON (a1.contract_no=a2.contract_no)
         WHERE a1.op_day='$OP_DAY'
           AND a1.recycle_type_id in('1')                 --取保值合同的数据
           AND a2.contract_no is null
           AND a1.kpi_type='BUY_BACK'
         GROUP BY substr(a1.recycle_date,1,6)
               ,a1.level1_org_id             
               ,a1.level1_org_descr          
               ,a1.level2_org_id             
               ,a1.level2_org_descr          
               ,a1.level3_org_id             
               ,a1.level3_org_descr          
               ,a1.level4_org_id             
               ,a1.level4_org_descr          
               ,a1.level5_org_id             
               ,a1.level5_org_descr          
               ,a1.level6_org_id             
               ,a1.level6_org_descr          
               ,a1.level7_org_id             
               ,a1.level7_org_descr          
               ,a1.level1_businesstype_id    
               ,a1.level1_businesstype_name  
               ,a1.level2_businesstype_id    
               ,a1.level2_businesstype_name  
               ,a1.level3_businesstype_id    
               ,a1.level3_businesstype_name  
               ,a1.level4_businesstype_id    
               ,a1.level4_businesstype_name  
               ,a1.production_line_id        
               ,a1.production_line_descr) t1

"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_CONTRACT_COST_MM='DMF_BIRD_CONTRACT_COST_MM'

CREATE_DMF_BIRD_CONTRACT_COST_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_CONTRACT_COST_MM(
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
  ,recyle_qty                    string    --回收只数(只)   
  ,recyle_weight                 string    --回收重量(斤)  
  ,base_recycle_cost             string    --基础回收金额 
  ,recyle_cost                   string    --回收金额(元)   
  ,no_drugs_subsidy_cost         string    --无药残补贴金额(元)
  ,other_subsidy_cost            string    --其他项目补贴(元)
  ,contract_qty                  string    --投放数量(支) 
  ,put_cost                      string    --投放成本(元)
  ,used_stock_qty                string    --实际用料量(斤)  
  ,used_stock_cost               string    --实际料价(元)   
  ,settlement_weight             string    --结算重量(斤)   
  ,recyle_carriage_cost          string    --合同运费(元)   
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_CONTRACT_COST_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_CONTRACT_COST_MM PARTITION(op_month='$OP_MONTH')
SELECT month_id                             --期间(月份)    
       ,day_id                              --期间(日)     
       ,level1_org_id                       --组织1级(股份)  
       ,level1_org_descr                    --组织1级(股份)  
       ,level2_org_id                       --组织2级(片联)  
       ,level2_org_descr                    --组织2级(片联)  
       ,level3_org_id                       --组织3级(片区)  
       ,level3_org_descr                    --组织3级(片区)  
       ,level4_org_id                       --组织4级(小片)  
       ,level4_org_descr                    --组织4级(小片)  
       ,level5_org_id                       --组织5级(公司)  
       ,level5_org_descr                    --组织5级(公司)  
       ,level6_org_id                       --组织6级(OU)  
       ,level6_org_descr                    --组织6级(OU)  
       ,level7_org_id                       --组织7级(库存组织)
       ,level7_org_descr                    --组织7级(库存组织)
       ,level1_businesstype_id              --业态1级      
       ,level1_businesstype_name            --业态1级      
       ,level2_businesstype_id              --业态2级      
       ,level2_businesstype_name            --业态2级      
       ,level3_businesstype_id              --业态3级      
       ,level3_businesstype_name            --业态3级      
       ,level4_businesstype_id              --业态4级      
       ,level4_businesstype_name            --业态4级      
       ,production_line_id                  --产线        
       ,production_line_descr               --产线        
       ,coalesce(recyle_qty,'0')                          --回收只数(只)   
       ,coalesce(recyle_weight,'0')                       --回收重量(斤) 
       ,coalesce(base_recycle_cost,'0')                   --基础回收金额(元)   
       ,coalesce(recyle_cost,'0')                         --回收金额(元)   
       ,coalesce(no_drugs_subsidy_cost,'0')               --无药残补贴金额(元)
       ,coalesce(other_subsidy_cost,'0')                  --其他项目补贴(元)
       ,coalesce(contract_qty,'0')                        --投放数量(支)
       ,coalesce(put_cost,'0')                            --投放成本(元)
       ,coalesce(used_stock_qty,'0')                      --实际用料量(斤)  
       ,coalesce(used_stock_cost,'0')                     --实际料价(元)   
       ,coalesce(settlement_weight,'0')                   --结算重量(斤)   
       ,coalesce(recyle_carriage_cost,'0')                --合同运费(元)   
       ,'$CREATE_TIME' create_time                        --创建时间
  FROM (SELECT *
          FROM $TMP_DMF_BIRD_CONTRACT_COST_MM_1
         WHERE op_month='$OP_MONTH') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMF_BIRD_CONTRACT_COST_MM_1;
    $INSERT_TMP_DMF_BIRD_CONTRACT_COST_MM_1;
    $CREATE_DMF_BIRD_CONTRACT_COST_MM;
    $INSERT_DMF_BIRD_CONTRACT_COST_MM;
"  -v