#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_ar_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 应收账款余额
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_ar_dd.sh 20180101"
    exit 1
fi

# 当前时间减去30天时间
FORMAT_DAY=$(date -d $OP_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $OP_DAY +%Y-%m-01)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_AR_DD_1='TMP_DMP_BIRD_AR_DD_1'

CREATE_TMP_DMP_BIRD_AR_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_DD_1(
  month_id                      string     --期间(月份)           
  ,day_id                       string     --期间(日)            
  ,level1_org_id                string     --组织1级(股份)         
  ,level1_org_descr             string     --组织1级(股份)         
  ,level2_org_id                string     --组织2级(片联)         
  ,level2_org_descr             string     --组织2级(片联)         
  ,level3_org_id                string     --组织3级(片区)         
  ,level3_org_descr             string     --组织3级(片区)         
  ,level4_org_id                string     --组织4级(小片)         
  ,level4_org_descr             string     --组织4级(小片)         
  ,level5_org_id                string     --组织5级(公司)         
  ,level5_org_descr             string     --组织5级(公司)         
  ,level6_org_id                string     --组织6级(OU)         
  ,level6_org_descr             string     --组织6级(OU)         
  ,level7_org_id                string     --组织7级(库存组织)       
  ,level7_org_descr             string     --组织7级(库存组织)       
  ,level1_businesstype_id       string     --业态1级             
  ,level1_businesstype_name     string     --业态1级             
  ,level2_businesstype_id       string     --业态2级             
  ,level2_businesstype_name     string     --业态2级             
  ,level3_businesstype_id       string     --业态3级             
  ,level3_businesstype_name     string     --业态3级             
  ,level4_businesstype_id       string     --业态4级             
  ,level4_businesstype_name     string     --业态4级             
  ,production_line_id           string     --产线               
  ,production_line_descr        string     --产线               
  ,breed_type_id                string     --养殖模式             
  ,breed_type_descr             string     --养殖模式             
  ,recyle_type_id               string     --回收类型             
  ,recyle_type_descr            string     --回收类型             
  ,cust_id                      string     --客户ID             
  ,cust_name                    string     --客户名称             
  ,batch_id                     string     --批次号              
  ,contact_date                 string     --合同日期 
  ,salesrep_id                  string     --销售员
  ,salesrep_name                string     --销售员名称         
  ,ar_begin_amt                 string     --月初应收账款余额         
  ,ar_end_amt                   string     --期末应收账款余额         
  ,ar_due_amt                   string     --其中截止到期日金额        
  ,o_due_days                   string     --距离到期日天数          
  ,ar_due_end_amt               string     --其中：期末应收账款余额中已逾期金额
  ,ar_type_id                   string     --应收账款类型           
  ,ar_type_descr                string     --应收账款类型           
  ,due_days                     string     --逾期天数             
  ,ar_0_30                      string     --账龄分析0-30         
  ,ar_30_60                     string     --账龄分析30-60        
  ,ar_60_90                     string     --账龄分析60-90        
  ,ar_90                        string     --账龄分析90以上         
  ,usable_deposit               string     --可用保证金            
  ,create_time                  string     --创建时间
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>关联合同及相关信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_AR_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_DD_1 PARTITION(op_day='$OP_DAY')
SELECT month_id                           --期间(月份)           
       ,day_id                            --期间(日)            
       ,level1_org_id                     --组织1级(股份)         
       ,level1_org_descr                  --组织1级(股份)         
       ,level2_org_id                     --组织2级(片联)         
       ,level2_org_descr                  --组织2级(片联)         
       ,level3_org_id                     --组织3级(片区)         
       ,level3_org_descr                  --组织3级(片区)         
       ,level4_org_id                     --组织4级(小片)         
       ,level4_org_descr                  --组织4级(小片)         
       ,level5_org_id                     --组织5级(公司)         
       ,level5_org_descr                  --组织5级(公司)         
       ,level6_org_id                     --组织6级(OU)         
       ,level6_org_descr                  --组织6级(OU)         
       ,level7_org_id                     --组织7级(库存组织)       
       ,level7_org_descr                  --组织7级(库存组织)       
       ,level1_businesstype_id            --业态1级             
       ,level1_businesstype_name          --业态1级             
       ,level2_businesstype_id            --业态2级             
       ,level2_businesstype_name          --业态2级             
       ,level3_businesstype_id            --业态3级             
       ,level3_businesstype_name          --业态3级             
       ,level4_businesstype_id            --业态4级             
       ,level4_businesstype_name          --业态4级             
       ,production_line_id                --产线               
       ,production_line_descr             --产线               
       ,breed_type_id                     --养殖模式             
       ,breed_type_descr                  --养殖模式             
       ,recyle_type_id                    --回收类型             
       ,recyle_type_descr                 --回收类型             
       ,cust_id                           --客户ID             
       ,cust_name                         --客户名称             
       ,batch_id                          --批次号              
       ,contract_date                     --合同日期             
       ,salesrep_id                       --销售员
       ,salesrep_name                     --销售员             
       ,ar_begin_amt                      --月初应收账款余额         
       ,ar_end_amt                        --期末应收账款余额         
       ,ar_due_amt                        --其中截止到期日金额        
       ,o_due_days                        --距离到期日天数          
       ,ar_due_end_amt                    --其中：期末应收账款余额中已逾期金额
       ,ar_type_id                        --应收账款类型           
       ,ar_type_descr                     --应收账款类型           
       ,due_days                          --逾期天数             
       ,ar_0_30                           --账龄分析0-30         
       ,ar_30_60                          --账龄分析30-60        
       ,ar_60_90                          --账龄分析60-90        
       ,ar_90                             --账龄分析90以上         
       ,usable_deposit                    --可用保证金            
       ,'$CREATE_TIME' create_time        --创建时间
  FROM (SELECT *
          FROM dwp_bird_ar_dd
         WHERE op_day='$OP_DAY'
           AND currency_id='3') t1
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_AR_DD='DMP_BIRD_AR_DD'

CREATE_DMP_BIRD_AR_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_AR_DD(
  month_id                      string     --期间(月份)           
  ,day_id                       string     --期间(日)            
  ,level1_org_id                string     --组织1级(股份)         
  ,level1_org_descr             string     --组织1级(股份)         
  ,level2_org_id                string     --组织2级(片联)         
  ,level2_org_descr             string     --组织2级(片联)         
  ,level3_org_id                string     --组织3级(片区)         
  ,level3_org_descr             string     --组织3级(片区)         
  ,level4_org_id                string     --组织4级(小片)         
  ,level4_org_descr             string     --组织4级(小片)         
  ,level5_org_id                string     --组织5级(公司)         
  ,level5_org_descr             string     --组织5级(公司)         
  ,level6_org_id                string     --组织6级(OU)         
  ,level6_org_descr             string     --组织6级(OU)         
  ,level7_org_id                string     --组织7级(库存组织)       
  ,level7_org_descr             string     --组织7级(库存组织)       
  ,level1_businesstype_id       string     --业态1级             
  ,level1_businesstype_name     string     --业态1级             
  ,level2_businesstype_id       string     --业态2级             
  ,level2_businesstype_name     string     --业态2级             
  ,level3_businesstype_id       string     --业态3级             
  ,level3_businesstype_name     string     --业态3级             
  ,level4_businesstype_id       string     --业态4级             
  ,level4_businesstype_name     string     --业态4级             
  ,production_line_id           string     --产线               
  ,production_line_descr        string     --产线               
  ,breed_type_id                string     --养殖模式             
  ,breed_type_descr             string     --养殖模式             
  ,recyle_type_id               string     --回收类型             
  ,recyle_type_descr            string     --回收类型             
  ,cust_id                      string     --客户ID             
  ,cust_name                    string     --客户名称             
  ,batch_id                     string     --批次号              
  ,contact_date                 string     --合同日期             
  ,salesrep_id                  string     --销售员
  ,salesrep_name                string     --销售员名称
  ,ar_begin_amt                 string     --月初应收账款余额         
  ,ar_end_amt                   string     --期末应收账款余额         
  ,ar_due_amt                   string     --其中截止到期日金额        
  ,o_due_days                   string     --距离到期日天数          
  ,ar_due_end_amt               string     --其中：期末应收账款余额中已逾期金额
  ,ar_type_id                   string     --应收账款类型           
  ,ar_type_descr                string     --应收账款类型           
  ,due_days                     string     --逾期天数             
  ,ar_0_30                      string     --账龄分析0-30         
  ,ar_30_60                     string     --账龄分析30-60        
  ,ar_60_90                     string     --账龄分析60-90        
  ,ar_90                        string     --账龄分析90以上         
  ,usable_deposit               string     --可用保证金            
  ,create_time                  string     --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_AR_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_AR_DD PARTITION(op_day='$OP_DAY')
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
       ,breed_type_id                       --养殖模式             
       ,breed_type_descr                    --养殖模式             
       ,recyle_type_id                      --回收类型             
       ,recyle_type_descr                   --回收类型             
       ,cust_id                             --客户ID             
       ,cust_name                           --客户名称             
       ,batch_id                            --批次号              
       ,contact_date                        --合同日期             
       ,salesrep_id                         --销售员 
       ,salesrep_name                       --销售员名称             
       ,ar_begin_amt                        --月初应收账款余额         
       ,ar_end_amt                          --期末应收账款余额         
       ,ar_due_amt                          --其中截止到期日金额        
       ,o_due_days                          --距离到期日天数          
       ,ar_due_end_amt                      --其中：期末应收账款余额中已逾期金额
       ,ar_type_id                          --应收账款类型           
       ,ar_type_descr                       --应收账款类型           
       ,due_days                            --逾期天数             
       ,ar_0_30                             --账龄分析0-30         
       ,ar_30_60                            --账龄分析30-60        
       ,ar_60_90                            --账龄分析60-90        
       ,ar_90                               --账龄分析90以上         
       ,usable_deposit                      --可用保证金            
       ,create_time                         --创建时间
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_AR_DD_1
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_AR_DD_1;
    $INSERT_TMP_DMP_BIRD_AR_DD_1;
    $CREATE_DMP_BIRD_AR_DD;
    $INSERT_DMP_BIRD_AR_DD;
"  -v