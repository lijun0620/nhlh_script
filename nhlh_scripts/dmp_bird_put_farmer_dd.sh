#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_put_farmer_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 投放合同信息表
# 修改说明:                                                          
######################################################################

OP_DAY=$1

# 判断时间输入参数
PARAM1=${#OP_DAY}

if [ $PARAM1 -ne 8 ]
then
    echo "请正确输入参数，格式为yyyymmdd，如20180101"
    exit 1
fi

OP_MONTH=${OP_DAY:0:6}
LAST_MONTH=$(date -d $OP_MONTH"01 -2 month" +%Y%m)

# 当前时间减去30天时间
PERIOD_DAY=$(date -d $OP_DAY" -30 day" +%Y%m%d)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_put_farmer_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_FARMER_DD_1='TMP_DMP_BIRD_PUT_FARMER_DD_1'

CREATE_TMP_DMP_BIRD_PUT_FARMER_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_FARMER_DD_1(
  month_id                          string       --期间(月份)    
  ,day_id                           string       --期间(日)     
  ,level1_org_id                    string       --组织1级(股份)  
  ,level1_org_descr                 string       --组织1级(股份)  
  ,level2_org_id                    string       --组织2级(片联)  
  ,level2_org_descr                 string       --组织2级(片联)  
  ,level3_org_id                    string       --组织3级(片区)  
  ,level3_org_descr                 string       --组织3级(片区)  
  ,level4_org_id                    string       --组织4级(小片)  
  ,level4_org_descr                 string       --组织4级(小片)  
  ,level5_org_id                    string       --组织5级(公司)  
  ,level5_org_descr                 string       --组织5级(公司)  
  ,level6_org_id                    string       --组织6级(OU)  
  ,level6_org_descr                 string       --组织6级(OU)  
  ,level7_org_id                    string       --组织7级(库存组织)
  ,level7_org_descr                 string       --组织7级(库存组织)
  ,level1_businesstype_id           string       --业态1级      
  ,level1_businesstype_name         string       --业态1级      
  ,level2_businesstype_id           string       --业态2级      
  ,level2_businesstype_name         string       --业态2级      
  ,level3_businesstype_id           string       --业态3级      
  ,level3_businesstype_name         string       --业态3级      
  ,level4_businesstype_id           string       --业态4级      
  ,level4_businesstype_name         string       --业态4级      
  ,farmer_id                        string       --养殖户ID     
  ,farmer_name                      string       --养殖户名称     
  ,salesman_id                      string       --业务员ID     
  ,salesman_name                    string       --业务员名称     
  ,contract_date                    string       --合同日期      
  ,farm_addr                        string       --养殖地址      
  ,phone_no                         string       --联系电话      
  ,batch_id                         string       --批次号       
  ,material_id                      string       --物料名称      
  ,material_descr                   string       --物料名称      
  ,contract_type_id                 string       --合同类型      
  ,contract_type_descr              string       --合同类型名称
  ,production_line_id               string       --产线代码
  ,production_line_descr            string       --产线描述
  ,put_type_id                      string       --投放类型
  ,put_type_descr                   string       --投放类型
  ,breed_type_id                    string       --养殖类型      
  ,breed_type_descr                 string       --养殖类型      
  ,if_dsp                           string       --是否直供      
  ,recyle_date                      string       --预计回收日期    
  ,contract_qty                     string       --合同只数      
  ,m_factory_descr                  string       --苗厂        
  ,recyle_price                     string       --基础回收价(单价) 
  ,contract_price                   string       --合同苗价(单价)  
  ,factory_price                    string       --饲料综合出厂价   
  ,drugs_cost                       string       --兽药成本(元/只) 
  ,carriage_cost                    string       --运输运费(元/只) 
  ,put_cost_nocarriage              string       --投放成本（不含运费）
  ,put_cost_withcarriage            string       --投放成本（含运费）
  ,farmer_put_profits               string       --养户投放利润
  ,company_put_profits              string       --公司投放利润
  ,deposit_balance_amt              string       --保证金余额     
  ,distance                         string       --距离
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取指标数据(投放数量，投放成本，苗价)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_FARMER_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_FARMER_DD_1 PARTITION(op_day='$OP_DAY')
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
       ,t1.farmer_id                               --养殖户ID     
       ,t1.farmer_name                             --养殖户名称     
       ,'' salesman_id                             --业务员ID     
       ,t1.salesman_name                           --业务员名称     
       ,t1.contract_date                           --合同日期      
       ,t1.farm_addr                               --养殖地址      
       ,t1.phone_no                                --联系电话      
       ,t1.contract_no batch_id                    --批次号       
       ,t1.material_id                             --物料名称      
       ,t1.material_descr                          --物料名称      
       ,'' contract_type_id                        --合同类型      
       ,t1.contract_type_descr                     --合同类型名称
       ,t1.production_line_id                      --产线
       ,t1.production_line_descr                   --产线
       ,t1.put_type_id                             --投放类型
       ,t1.put_type_descr                          --投放类型
       ,case when t1.breed_type_descr='代养' then '1'
             when t1.breed_type_descr='放养' then '2'
        else null end breed_type_id                --养殖类型
       ,t1.breed_type_descr                        --养殖类型      
       ,t1.if_dsp                                  --是否直供      
       ,t1.contract_kill_date recyle_date          --预计回收日期    
       ,t1.contract_qty                            --合同只数      
       ,t1.m_factory_descr                         --苗厂        
       ,t1.recyle_price                            --基础回收价(单价) 
       ,t1.contract_price                          --合同苗价(单价)  
       ,t1.factory_price                           --饲料综合出厂价   
       ,t2.drugs_cost                              --兽药成本(元/只) 
       ,t2.carriage_cost                           --运输运费(元/只) 
       ,coalesce(t1.put_cost,0)-coalesce(t2.carriage_cost,0) put_cost_nocarriage  --投放成本（不含运费）
       ,t1.put_cost put_cost_withcarriage          --投放成本（含运费） 
       ,coalesce(t1.put_cost,0)-case when coalesce(t1.avg_weight,0)=0 then 0 else coalesce(t1.contract_price,0)/coalesce(t1.avg_weight,0) end-coalesce(t1.factory_price,0)-case when coalesce(t1.avg_weight,0)=0 then 0 else coalesce(t1.drugs_std,0)/coalesce(t1.avg_weight,0) end farmer_put_profits  --养户投放利润
       ,(coalesce(t1.feed_profit,0)*coalesce(t1.single_feed_cnt,0)+coalesce(cub_profit,0)+coalesce(drugs_profit,0))/coalesce(t1.avg_weight,0)+coalesce(trans_profit,0) company_put_profits      --公司投放利润
       ,t1.deposit_balance_amt                     --保证金余额   
       ,t1.distance                                --距离
  FROM (SELECT *
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY'
           AND put_type_id in('1','2')) t1
  LEFT JOIN (SELECT contract_pitch_num contract_no
                    ,sum(case when contract_para_id='18' then para_value else 0 end) drugs_cost
                    ,sum(case when contract_para_id='19' then para_value else 0 end) carriage_cost
               FROM dwu_qw_qw14_dd
              WHERE op_day='$OP_DAY'
                AND contract_para_id in ('18','19')
              GROUP BY contract_pitch_num) t2
    ON (t1.contract_no=t2.contract_no)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PUT_FARMER_DD='DMP_BIRD_PUT_FARMER_DD'

CREATE_DMP_BIRD_PUT_FARMER_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PUT_FARMER_DD(
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
  ,farmer_id                  string   --养殖户ID     
  ,farmer_name                string   --养殖户名称     
  ,salesman_id                string   --业务员ID     
  ,salesman_name              string   --业务员名称     
  ,contract_date              string   --合同日期      
  ,farm_addr                  string   --养殖地址      
  ,phone_no                   string   --联系电话      
  ,batch_id                   string   --批次号       
  ,material_id                string   --物料名称      
  ,material_descr             string   --物料名称      
  ,contract_type_id           string   --合同类型      
  ,contract_type_descr        string   --合同类型名称
  ,production_line_id         string   --产线代码
  ,production_line_descr      string   --产线描述
  ,put_type_id                string   --投放类型
  ,put_type_descr             string   --投放类型    
  ,breed_type_id              string   --养殖类型      
  ,breed_type_descr           string   --养殖类型      
  ,if_dsp                     string   --是否直供      
  ,recyle_date                string   --预计回收日期    
  ,contract_qty               string   --合同只数      
  ,m_factory_descr            string   --苗厂        
  ,recyle_price               string   --基础回收价(单价) 
  ,contract_price             string   --合同苗价(单价)  
  ,factory_price              string   --饲料综合出厂价   
  ,drugs_cost                 string   --兽药成本(元/只) 
  ,carriage_cost              string   --运输运费(元/只) 
  ,put_cost_nocarriage        string   --投放成本（不含运费）
  ,put_cost_withcarriage      string   --投放成本（含运费） 
  ,farmer_put_profits         string   --养户投放利润
  ,company_put_profits        string   --公司投放利润
  ,deposit_balance_amt        string   --保证金余额     
  ,distance                   string   --距离
  ,create_time                string   --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PUT_FARMER_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_PUT_FARMER_DD PARTITION(op_day='$OP_DAY')
SELECT month_id                       --期间(月份)    
       ,day_id                        --期间(日)     
       ,level1_org_id                 --组织1级(股份)  
       ,level1_org_descr              --组织1级(股份)  
       ,level2_org_id                 --组织2级(片联)  
       ,level2_org_descr              --组织2级(片联)  
       ,level3_org_id                 --组织3级(片区)  
       ,level3_org_descr              --组织3级(片区)  
       ,level4_org_id                 --组织4级(小片)  
       ,level4_org_descr              --组织4级(小片)  
       ,level5_org_id                 --组织5级(公司)  
       ,level5_org_descr              --组织5级(公司)  
       ,level6_org_id                 --组织6级(OU)  
       ,level6_org_descr              --组织6级(OU)  
       ,level7_org_id                 --组织7级(库存组织)
       ,level7_org_descr              --组织7级(库存组织)
       ,level1_businesstype_id        --业态1级      
       ,level1_businesstype_name      --业态1级      
       ,level2_businesstype_id        --业态2级      
       ,level2_businesstype_name      --业态2级      
       ,level3_businesstype_id        --业态3级      
       ,level3_businesstype_name      --业态3级      
       ,level4_businesstype_id        --业态4级      
       ,level4_businesstype_name      --业态4级      
       ,farmer_id                     --养殖户ID     
       ,farmer_name                   --养殖户名称     
       ,salesman_id                   --业务员ID     
       ,salesman_name                 --业务员名称     
       ,contract_date                 --合同日期      
       ,farm_addr                     --养殖地址      
       ,phone_no                      --联系电话      
       ,batch_id                      --批次号       
       ,material_id                   --物料名称      
       ,material_descr                --物料名称      
       ,contract_type_id              --合同类型      
       ,contract_type_descr           --合同类型名称
       ,production_line_id            --产线代码
       ,production_line_descr         --产线描述
       ,put_type_id                   --投放类型
       ,put_type_descr                --投放类型    
       ,breed_type_id                 --养殖类型      
       ,breed_type_descr              --养殖类型      
       ,if_dsp                        --是否直供      
       ,recyle_date                   --预计回收日期    
       ,contract_qty                  --合同只数      
       ,m_factory_descr               --苗厂        
       ,recyle_price                  --基础回收价(单价) 
       ,contract_price                --合同苗价(单价)  
       ,factory_price                 --饲料综合出厂价   
       ,drugs_cost                    --兽药成本(元/只) 
       ,carriage_cost                 --运输运费(元/只) 
       ,put_cost_nocarriage           --投放成本（不含运费）
       ,put_cost_withcarriage         --投放成本（含运费）
       ,case when breed_type_id='1' then coalesce(farmer_put_profits,0) - coalesce(company_put_profits,0)
        else coalesce(farmer_put_profits,0) end farmer_put_profits  --养户投放利润
       ,company_put_profits           --公司投放利润
       ,deposit_balance_amt           --保证金余额     
       ,distance                      --距离
       ,'$CREATE_TIME' create_time    --创建时间
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_PUT_FARMER_DD_1
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PUT_FARMER_DD_1;
    $INSERT_TMP_DMP_BIRD_PUT_FARMER_DD_1;
    $CREATE_DMP_BIRD_PUT_FARMER_DD;
    $INSERT_DMP_BIRD_PUT_FARMER_DD;
"  -v