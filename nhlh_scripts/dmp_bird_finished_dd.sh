#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_finished_dd.sh                               
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
    echo "输入参数错误，调用示例: dmp_bird_finished_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_FINISHED_DD_1='TMP_DMP_BIRD_FINISHED_DD_1'

CREATE_TMP_DMP_BIRD_FINISHED_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_FINISHED_DD_1(
  month_id                     string      --期间(月份)    
  ,day_id                      string      --期间(日)     
  ,level1_org_id               string      --组织1级(股份)  
  ,level1_org_descr            string      --组织1级(股份)  
  ,level2_org_id               string      --组织2级(片联)  
  ,level2_org_descr            string      --组织2级(片联)  
  ,level3_org_id               string      --组织3级(片区)  
  ,level3_org_descr            string      --组织3级(片区)  
  ,level4_org_id               string      --组织4级(小片)  
  ,level4_org_descr            string      --组织4级(小片)  
  ,level5_org_id               string      --组织5级(公司)  
  ,level5_org_descr            string      --组织5级(公司)  
  ,level6_org_id               string      --组织6级(OU)  
  ,level6_org_descr            string      --组织6级(OU)  
  ,level7_org_id               string      --组织7级(库存组织)
  ,level7_org_descr            string      --组织7级(库存组织)
  ,level1_businesstype_id      string      --业态1级      
  ,level1_businesstype_name    string      --业态1级      
  ,level2_businesstype_id      string      --业态2级      
  ,level2_businesstype_name    string      --业态2级      
  ,level3_businesstype_id      string      --业态3级      
  ,level3_businesstype_name    string      --业态3级      
  ,level4_businesstype_id      string      --业态4级      
  ,level4_businesstype_name    string      --业态4级      
  ,production_line_id          string      --产线        
  ,production_line_descr       string      --产线        
  ,factory_id                  string      --苗厂家ID     
  ,factory_name                string      --苗厂家名称     
  ,finished_age_days           string      --出栏日龄    
  ,batch_num                   string      --批次数      
  ,settlement_weight           string      --结算重量      
  ,recycle_qty                 string      --回收数量      
  ,material_weight             string      --饲料耗用量     
  ,contract_qty                string      --合同只数      
  ,create_time                 string      --创建时间
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_FINISHED_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_FINISHED_DD_1 PARTITION(op_day='$OP_DAY')
SELECT t1.month_id                                 --期间(月份)    
       ,t1.day_id                                  --期间(日)     
       ,t1.level1_org_id                           --组织1级(股份)  
       ,t1.level1_org_descr                        --组织1级(股份)  
       ,t1.level2_org_id                           --组织2级(片联)  
       ,t1.level2_org_descr                        --组织2级(片联)  
       ,t1.level3_org_id                           --组织3级(片区)  
       ,t1.level3_org_descr                        --组织3级(片区)  
       ,t1.level4_org_id                           --组织4级(小片)  
       ,t1.level4_org_descr                        --组织4级(小片)  
       ,t1.level5_org_id                           --组织5级(公司)  
       ,t1.level5_org_descr                        --组织5级(公司)  
       ,t1.level6_org_id                           --组织6级(OU)  
       ,t1.level6_org_descr                        --组织6级(OU)  
       ,t1.level7_org_id                           --组织7级(库存组织)
       ,t1.level7_org_descr                        --组织7级(库存组织)
       ,t1.level1_businesstype_id                  --业态1级      
       ,t1.level1_businesstype_name                --业态1级      
       ,t1.level2_businesstype_id                  --业态2级      
       ,t1.level2_businesstype_name                --业态2级      
       ,t1.level3_businesstype_id                  --业态3级      
       ,t1.level3_businesstype_name                --业态3级      
       ,t1.level4_businesstype_id                  --业态4级      
       ,t1.level4_businesstype_name                --业态4级      
       ,t1.production_line_id                      --产线        
       ,t1.production_line_descr                   --产线        
       ,null factory_id                            --苗厂家ID     
       ,t1.factory_name                            --苗厂家名称     
       ,t2.finished_age_days                       --出栏日龄    
       ,t2.batch_num                               --批次数
       ,t1.recycle_weight settlement_weight        --结算重量      
       ,t1.recycle_qty                             --回收数量      
       ,t1.material_weight                         --饲料耗用量     
       ,t1.contract_qty                            --合同只数      
       ,'$CREATE_TIME' create_time              --创建时间
  FROM (SELECT substr(recycle_date,1,6) month_id         --期间(月份)    
               ,substr(recycle_date,1,8) day_id          --期间(日)
               ,production_line_id                     --产线代码
               ,production_line_descr                  --产线描述
               ,m_factory_descr factory_name           --苗场
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
               ,level1_businesstype_id                 --业态1级
               ,level1_businesstype_name               --业态1级
               ,level2_businesstype_id                 --业态2级
               ,level2_businesstype_name               --业态2级
               ,level3_businesstype_id                 --业态3级
               ,level3_businesstype_name               --业态3级
               ,level4_businesstype_id                 --业态4级
               ,level4_businesstype_name               --业态4级
               ,sum(coalesce(contract_qty,'0')) contract_qty  --合同支数
               ,sum(coalesce(material_weight_qty,'0')) material_weight  --物料耗用重量(kg)
               ,sum(coalesce(recycle_qty,'0')) recycle_qty              --回收数量(支)
               ,sum(coalesce(recycle_weight,'0')) recycle_weight        --回收重量(kg)
               ,sum(coalesce(recycle_amt,'0')) recycle_amt              --回收金额
          FROM dwp_bird_finished_dd
         WHERE op_day='$OP_DAY'
           AND recycle_type_id in('1')
         GROUP BY production_line_id                   --产线代码
               ,production_line_descr                  --产线描述
               ,recycle_date                           --回收日期
               ,m_factory_descr                        --苗场
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
               ,level1_businesstype_id                 --业态1级
               ,level1_businesstype_name               --业态1级
               ,level2_businesstype_id                 --业态2级
               ,level2_businesstype_name               --业态2级
               ,level3_businesstype_id                 --业态3级
               ,level3_businesstype_name               --业态3级
               ,level4_businesstype_id                 --业态4级
               ,level4_businesstype_name) t1
  LEFT JOIN (SELECT level6_org_id
                    ,production_line_id
                    ,m_factory_descr
                    ,recycle_date
                    ,sum(datediff(concat(substr(recycle_date,1,4),'-',substr(recycle_date,5,2),'-',substr(recycle_date,7,2)),contract_date)) finished_age_days --出栏日龄
                    ,count(1) batch_num                --批次数
               FROM (SELECT recycle_date
                            ,production_line_id
                            ,m_factory_descr
                            ,level6_org_id
                            ,contract_no
                            ,contract_date
                       FROM dwp_bird_finished_dd
                      WHERE op_day='$OP_DAY'
                        AND recycle_type_id in('1','2')
                      GROUP BY recycle_date
                            ,production_line_id
                            ,m_factory_descr
                            ,level6_org_id
                            ,contract_no
                            ,contract_date) a
              GROUP BY level6_org_id
                    ,production_line_id
                    ,m_factory_descr
                    ,recycle_date) t2
    ON (t1.level6_org_id=t2.level6_org_id
    AND t1.production_line_id=t2.production_line_id
    AND t1.factory_name=t2.m_factory_descr
    AND t1.day_id=t2.recycle_date)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_FINISHED_DD='DMP_BIRD_FINISHED_DD'

CREATE_DMP_BIRD_FINISHED_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_FINISHED_DD(
  month_id                     string      --期间(月份)    
  ,day_id                      string      --期间(日)     
  ,level1_org_id               string      --组织1级(股份)  
  ,level1_org_descr            string      --组织1级(股份)  
  ,level2_org_id               string      --组织2级(片联)  
  ,level2_org_descr            string      --组织2级(片联)  
  ,level3_org_id               string      --组织3级(片区)  
  ,level3_org_descr            string      --组织3级(片区)  
  ,level4_org_id               string      --组织4级(小片)  
  ,level4_org_descr            string      --组织4级(小片)  
  ,level5_org_id               string      --组织5级(公司)  
  ,level5_org_descr            string      --组织5级(公司)  
  ,level6_org_id               string      --组织6级(OU)  
  ,level6_org_descr            string      --组织6级(OU)  
  ,level7_org_id               string      --组织7级(库存组织)
  ,level7_org_descr            string      --组织7级(库存组织)
  ,level1_businesstype_id      string      --业态1级      
  ,level1_businesstype_name    string      --业态1级      
  ,level2_businesstype_id      string      --业态2级      
  ,level2_businesstype_name    string      --业态2级      
  ,level3_businesstype_id      string      --业态3级      
  ,level3_businesstype_name    string      --业态3级      
  ,level4_businesstype_id      string      --业态4级      
  ,level4_businesstype_name    string      --业态4级      
  ,production_line_id          string      --产线        
  ,production_line_descr       string      --产线        
  ,factory_id                  string      --苗厂家ID     
  ,factory_name                string      --苗厂家名称     
  ,finished_age_days           string      --出栏日龄    
  ,batch_num                   string      --批次数      
  ,settlement_weight           string      --结算重量      
  ,recycle_qty                 string      --回收数量      
  ,material_weight             string      --饲料耗用量     
  ,contract_qty                string      --合同只数      
  ,create_time                 string      --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_FINISHED_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_FINISHED_DD PARTITION(op_day='$OP_DAY')
SELECT month_id                            --期间(月份)    
       ,day_id                             --期间(日)     
       ,level1_org_id                      --组织1级(股份)  
       ,level1_org_descr                   --组织1级(股份)  
       ,level2_org_id                      --组织2级(片联)  
       ,level2_org_descr                   --组织2级(片联)  
       ,level3_org_id                      --组织3级(片区)  
       ,level3_org_descr                   --组织3级(片区)  
       ,level4_org_id                      --组织4级(小片)  
       ,level4_org_descr                   --组织4级(小片)  
       ,level5_org_id                      --组织5级(公司)  
       ,level5_org_descr                   --组织5级(公司)  
       ,level6_org_id                      --组织6级(OU)  
       ,level6_org_descr                   --组织6级(OU)  
       ,level7_org_id                      --组织7级(库存组织)
       ,level7_org_descr                   --组织7级(库存组织)
       ,level1_businesstype_id             --业态1级      
       ,level1_businesstype_name           --业态1级      
       ,level2_businesstype_id             --业态2级      
       ,level2_businesstype_name           --业态2级      
       ,level3_businesstype_id             --业态3级      
       ,level3_businesstype_name           --业态3级      
       ,level4_businesstype_id             --业态4级      
       ,level4_businesstype_name           --业态4级      
       ,production_line_id                 --产线        
       ,production_line_descr              --产线        
       ,factory_id                         --苗厂家ID     
       ,factory_name                       --苗厂家名称     
       ,finished_age_days                  --出栏日龄    
       ,batch_num                          --批次数      
       ,settlement_weight                  --结算重量      
       ,recycle_qty                        --回收数量      
       ,material_weight                    --饲料耗用量     
       ,contract_qty                       --合同只数      
       ,create_time                        --创建时间
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_FINISHED_DD_1
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_FINISHED_DD_1;
    $INSERT_TMP_DMP_BIRD_FINISHED_DD_1;
    $CREATE_DMP_BIRD_FINISHED_DD;
    $INSERT_DMP_BIRD_FINISHED_DD;
"  -v