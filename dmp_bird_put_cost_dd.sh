#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_put_cost_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-日投放成本-公司
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
    echo "输入参数错误，调用示例: dmp_bird_put_cost_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_COST_DD_1='TMP_DMP_BIRD_PUT_COST_DD_1'

CREATE_TMP_DMP_BIRD_PUT_COST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_COST_DD_1(
  month_id                       string    --期间(月)
  ,day_id                        string    --期间(日)
  ,put_date                      string    --投放日期
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
  ,put_type_id                   string    --投放类型ID
  ,put_type_descr                string    --投放类型名称
  ,breed_type_id                 string    --养殖类型
  ,breed_type_descr              string    --养殖类型
  ,kpi_type_id                   string    --指标类型  
  ,kpi_type_descr                string    --指标类型
  ,put_start_date                string    --投放开始时间
  ,put_end_date                  string    --投放结束时间
  ,put_values                    string    --投放数量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取指标数据(投放数量，投放成本，苗价)>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_COST_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_COST_DD_1 PARTITION(op_day='$OP_DAY')
SELECT month_id                              --期间(月)
       ,day_id                               --期间(日)
       ,concat(substr(day_id,1,4), '-', substr(day_id,5,2), '-', substr(day_id,7,2)) put_date
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间
       ,sum(put_values) put_values           --投放数量
  FROM (SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,case when breed_type_descr='代养' then '1'
                     when breed_type_descr='放养' then '2'
                else null end breed_type_id          --养殖类型
               ,breed_type_descr                     --养殖类型
               ,'1' kpi_type_id                      --指标类型
               ,'投放数量' kpi_type_descr             --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,contract_qty put_values              --投放数量
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,case when breed_type_descr='代养' then '1'
                     when breed_type_descr='放养' then '2'
                else null end breed_type_id          --养殖类型
               ,breed_type_descr                     --养殖类型
               ,'12' kpi_type_id                     --指标类型
               ,'投放数量(成本加权)' kpi_type_descr   --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,case when coalesce(put_cost,0)=0 then 0 else contract_qty end put_values              --投放数量
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,case when breed_type_descr='代养' then '1'
                     when breed_type_descr='放养' then '2'
                else null end breed_type_id          --养殖类型
               ,breed_type_descr                     --养殖类型
               ,'2' kpi_type_id                      --指标类型
               ,'投放成本' kpi_type_descr             --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,put_cost*contract_qty put_values     --投放数量(单位成本*数量)
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,case when breed_type_descr='代养' then '1'
                     when breed_type_descr='放养' then '2'
                else null end breed_type_id          --养殖类型
               ,breed_type_descr                     --养殖类型
               ,'13' kpi_type_id                     --指标类型
               ,'投放数量(苗价加权)' kpi_type_descr    --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,case when coalesce(contract_price,0)=0 then 0 else contract_qty end put_values   --投放数量
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,case when breed_type_descr='代养' then '1'
                     when breed_type_descr='放养' then '2'
                else null end breed_type_id          --养殖类型
               ,breed_type_descr                     --养殖类型
               ,'3' kpi_type_id                      --指标类型
               ,'苗价' kpi_type_descr                --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,contract_price*contract_qty put_values     --苗价(单位苗价*数量)
          FROM dwp_bird_put_contract_dd
         WHERE op_day='$OP_DAY') t1
 GROUP BY month_id                           --期间(月)
       ,day_id                               --期间(日)
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间
"   

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_COST_DD_1A='TMP_DMP_BIRD_PUT_COST_DD_1A'

CREATE_TMP_DMP_BIRD_PUT_COST_DD_1A="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_COST_DD_1A(
  month_id                       string    --期间(月)
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
  ,put_type_id                   string    --投放类型ID
  ,put_type_descr                string    --投放类型名称
  ,breed_type_id                 string    --养殖类型
  ,breed_type_descr              string    --养殖类型
  ,kpi_type_id                   string    --指标类型  
  ,kpi_type_descr                string    --指标类型
  ,put_start_date                string    --投放开始时间
  ,put_end_date                  string    --投放结束时间
  ,put_accum                     string    --当月累计
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_COST_DD_1A="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_COST_DD_1A PARTITION(op_day='$OP_DAY')
SELECT month_id                              --期间(月)
       ,day_id                               --期间(日)
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型  
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间

       ,sum(put_values) put_accum            --本月投放量
  FROM $TMP_DMP_BIRD_PUT_COST_DD_1
 WHERE op_day='$OP_DAY'
  GROUP BY month_id                          --期间(月)
       ,day_id                               --期间(日)
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型  
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_COST_DD_2='TMP_DMP_BIRD_PUT_COST_DD_2'

CREATE_TMP_DMP_BIRD_PUT_COST_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_COST_DD_2(
  month_id                       string    --期间(月)
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
  ,put_type_id                   string    --投放类型ID
  ,put_type_descr                string    --投放类型名称
  ,breed_type_id                 string    --养殖类型
  ,breed_type_descr              string    --养殖类型
  ,kpi_type_id                   string    --指标类型  
  ,kpi_type_descr                string    --指标类型
  ,put_start_date                string    --投放开始时间
  ,put_end_date                  string    --投放结束时间
  ,put_accum                     string    --当月累计  
  ,day1                          string    --1天  
  ,day2                          string    --2天  
  ,day3                          string    --3天  
  ,day4                          string    --4天  
  ,day5                          string    --5天  
  ,day6                          string    --6天  
  ,day7                          string    --7天  
  ,day8                          string    --8天  
  ,day9                          string    --9天  
  ,day10                         string    --10天 
  ,day11                         string    --11天 
  ,day12                         string    --12天 
  ,day13                         string    --13天 
  ,day14                         string    --14天 
  ,day15                         string    --15天 
  ,day16                         string    --16天 
  ,day17                         string    --17天 
  ,day18                         string    --18天 
  ,day19                         string    --19天 
  ,day20                         string    --20天 
  ,day21                         string    --21天 
  ,day22                         string    --22天 
  ,day23                         string    --23天 
  ,day24                         string    --24天 
  ,day25                         string    --25天 
  ,day26                         string    --26天 
  ,day27                         string    --27天 
  ,day28                         string    --28天 
  ,day29                         string    --29天 
  ,day30                         string    --30天
  ,day31                         string    --31天
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>生成N-n天指标数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_COST_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_COST_DD_2 PARTITION(op_day='$OP_DAY')
SELECT month_id                              --期间(月)
       ,day_id                               --期间(日)
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间
       ,sum(put_accum) put_accum             --当月累计  
       ,sum(day1) day1                       --1日
       ,sum(day2) day2                       --2日
       ,sum(day3) day3                       --3日
       ,sum(day4) day4                       --4日
       ,sum(day5) day5                       --5日
       ,sum(day6) day6                       --6日
       ,sum(day7) day7                       --7日
       ,sum(day8) day8                       --8日
       ,sum(day9) day9                       --9日
       ,sum(day10) day10                     --10日
       ,sum(day11) day11                     --11日
       ,sum(day12) day12                     --12日
       ,sum(day13) day13                     --13日
       ,sum(day14) day14                     --14日
       ,sum(day15) day15                     --15日
       ,sum(day16) day16                     --16日
       ,sum(day17) day17                     --17日
       ,sum(day18) day18                     --18日
       ,sum(day19) day19                     --19日
       ,sum(day20) day20                     --20日
       ,sum(day21) day21                     --21日
       ,sum(day22) day22                     --22日
       ,sum(day23) day23                     --23日
       ,sum(day24) day24                     --24日
       ,sum(day25) day25                     --25日
       ,sum(day26) day26                     --26日
       ,sum(day27) day27                     --27日
       ,sum(day28) day28                     --28日
       ,sum(day29) day29                     --29日
       ,sum(day30) day30                     --30日
       ,sum(day31) day31                     --31日
  FROM (SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,breed_type_id                        --养殖类型
               ,breed_type_descr                     --养殖类型
               ,kpi_type_id                          --指标类型
               ,kpi_type_descr                       --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,'0' put_accum                        --当月累计  
               ,(case when day(put_date)='1' then put_values else '0' end) day1        --1日
               ,(case when day(put_date)='2' then put_values else '0' end) day2        --2日
               ,(case when day(put_date)='3' then put_values else '0' end) day3        --3日
               ,(case when day(put_date)='4' then put_values else '0' end) day4        --4日
               ,(case when day(put_date)='5' then put_values else '0' end) day5        --5日
               ,(case when day(put_date)='6' then put_values else '0' end) day6        --6日
               ,(case when day(put_date)='7' then put_values else '0' end) day7        --7日
               ,(case when day(put_date)='8' then put_values else '0' end) day8        --8日
               ,(case when day(put_date)='9' then put_values else '0' end) day9        --9日
               ,(case when day(put_date)='10' then put_values else '0' end) day10      --10日
               ,(case when day(put_date)='11' then put_values else '0' end) day11      --11日
               ,(case when day(put_date)='12' then put_values else '0' end) day12      --12日
               ,(case when day(put_date)='13' then put_values else '0' end) day13      --13日
               ,(case when day(put_date)='14' then put_values else '0' end) day14      --14日
               ,(case when day(put_date)='15' then put_values else '0' end) day15      --15日
               ,(case when day(put_date)='16' then put_values else '0' end) day16      --16日
               ,(case when day(put_date)='17' then put_values else '0' end) day17      --17日
               ,(case when day(put_date)='18' then put_values else '0' end) day18      --18日
               ,(case when day(put_date)='19' then put_values else '0' end) day19      --19日
               ,(case when day(put_date)='20' then put_values else '0' end) day20      --20日
               ,(case when day(put_date)='21' then put_values else '0' end) day21      --21日
               ,(case when day(put_date)='22' then put_values else '0' end) day22      --22日
               ,(case when day(put_date)='23' then put_values else '0' end) day23      --23日
               ,(case when day(put_date)='24' then put_values else '0' end) day24      --24日
               ,(case when day(put_date)='25' then put_values else '0' end) day25      --25日
               ,(case when day(put_date)='26' then put_values else '0' end) day26      --26日
               ,(case when day(put_date)='27' then put_values else '0' end) day27      --27日
               ,(case when day(put_date)='28' then put_values else '0' end) day28      --28日
               ,(case when day(put_date)='29' then put_values else '0' end) day29      --29日
               ,(case when day(put_date)='30' then put_values else '0' end) day30      --30日
               ,(case when day(put_date)='31' then put_values else '0' end) day31      --31日
          FROM $TMP_DMP_BIRD_PUT_COST_DD_1
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT month_id                              --期间(月)
               ,day_id                               --期间(日)
               ,level1_org_id                        --组织1级(股份)  
               ,level1_org_descr                     --组织1级(股份)  
               ,level2_org_id                        --组织2级(片联)  
               ,level2_org_descr                     --组织2级(片联)  
               ,level3_org_id                        --组织3级(片区)  
               ,level3_org_descr                     --组织3级(片区)  
               ,level4_org_id                        --组织4级(小片)  
               ,level4_org_descr                     --组织4级(小片)  
               ,level5_org_id                        --组织5级(公司)  
               ,level5_org_descr                     --组织5级(公司)  
               ,level6_org_id                        --组织6级(OU)  
               ,level6_org_descr                     --组织6级(OU)  
               ,level7_org_id                        --组织7级(库存组织)
               ,level7_org_descr                     --组织7级(库存组织)
               ,level1_businesstype_id               --业态1级
               ,level1_businesstype_name             --业态1级
               ,level2_businesstype_id               --业态2级
               ,level2_businesstype_name             --业态2级
               ,level3_businesstype_id               --业态3级
               ,level3_businesstype_name             --业态3级
               ,level4_businesstype_id               --业态4级
               ,level4_businesstype_name             --业态4级
               ,production_line_id                   --产线    
               ,production_line_descr                --产线    
               ,put_type_id                          --投放类型ID
               ,put_type_descr                       --投放类型名称
               ,breed_type_id                        --养殖类型
               ,breed_type_descr                     --养殖类型
               ,kpi_type_id                          --指标类型
               ,kpi_type_descr                       --指标类型
               ,put_start_date                       --投放开始时间
               ,put_end_date                         --投放结束时间
               ,put_accum                            --当月累计  
               ,'0' day1                             --1日
               ,'0' day2                             --2日
               ,'0' day3                             --3日
               ,'0' day4                             --4日
               ,'0' day5                             --5日
               ,'0' day6                             --6日
               ,'0' day7                             --7日
               ,'0' day8                             --8日
               ,'0' day9                             --9日
               ,'0' day10                            --10日
               ,'0' day11                            --11日
               ,'0' day12                            --12日
               ,'0' day13                            --13日
               ,'0' day14                            --14日
               ,'0' day15                            --15日
               ,'0' day16                            --16日
               ,'0' day17                            --17日
               ,'0' day18                            --18日
               ,'0' day19                            --19日
               ,'0' day20                            --20日
               ,'0' day21                            --21日
               ,'0' day22                            --22日
               ,'0' day23                            --23日
               ,'0' day24                            --24日
               ,'0' day25                            --25日
               ,'0' day26                            --26日
               ,'0' day27                            --27日
               ,'0' day28                            --28日
               ,'0' day29                            --29日
               ,'0' day30                            --30日
               ,'0' day31                            --31日
          FROM $TMP_DMP_BIRD_PUT_COST_DD_1A
         WHERE op_day='$OP_DAY') t1
 GROUP BY month_id                              --期间(月)
       ,day_id                               --期间(日)
       ,level1_org_id                        --组织1级(股份)  
       ,level1_org_descr                     --组织1级(股份)  
       ,level2_org_id                        --组织2级(片联)  
       ,level2_org_descr                     --组织2级(片联)  
       ,level3_org_id                        --组织3级(片区)  
       ,level3_org_descr                     --组织3级(片区)  
       ,level4_org_id                        --组织4级(小片)  
       ,level4_org_descr                     --组织4级(小片)  
       ,level5_org_id                        --组织5级(公司)  
       ,level5_org_descr                     --组织5级(公司)  
       ,level6_org_id                        --组织6级(OU)  
       ,level6_org_descr                     --组织6级(OU)  
       ,level7_org_id                        --组织7级(库存组织)
       ,level7_org_descr                     --组织7级(库存组织)
       ,level1_businesstype_id               --业态1级
       ,level1_businesstype_name             --业态1级
       ,level2_businesstype_id               --业态2级
       ,level2_businesstype_name             --业态2级
       ,level3_businesstype_id               --业态3级
       ,level3_businesstype_name             --业态3级
       ,level4_businesstype_id               --业态4级
       ,level4_businesstype_name             --业态4级
       ,production_line_id                   --产线    
       ,production_line_descr                --产线    
       ,put_type_id                          --投放类型ID
       ,put_type_descr                       --投放类型名称
       ,breed_type_id                        --养殖类型
       ,breed_type_descr                     --养殖类型
       ,kpi_type_id                          --指标类型
       ,kpi_type_descr                       --指标类型
       ,put_start_date                       --投放开始时间
       ,put_end_date                         --投放结束时间
" 


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PUT_COST_DD='DMP_BIRD_PUT_COST_DD'

CREATE_DMP_BIRD_PUT_COST_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PUT_COST_DD(
  month_id                       string    --期间(月)
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
  ,put_type_id                   string    --投放类型ID
  ,put_type_descr                string    --投放类型名称
  ,breed_type_id                 string    --养殖类型
  ,breed_type_descr              string    --养殖类型
  ,kpi_type_id                   string    --指标类型  
  ,kpi_type_descr                string    --指标类型
  ,put_start_date                string    --投放开始时间
  ,put_end_date                  string    --投放结束时间
  ,put_accum                     string    --当月累计  
  ,day1                          string    --1日  
  ,day2                          string    --2日  
  ,day3                          string    --3日  
  ,day4                          string    --4日  
  ,day5                          string    --5日  
  ,day6                          string    --6日  
  ,day7                          string    --7日  
  ,day8                          string    --8日  
  ,day9                          string    --9日  
  ,day10                         string    --10日
  ,day11                         string    --11日
  ,day12                         string    --12日
  ,day13                         string    --13日
  ,day14                         string    --14日
  ,day15                         string    --15日
  ,day16                         string    --16日
  ,day17                         string    --17日
  ,day18                         string    --18日
  ,day19                         string    --19日
  ,day20                         string    --20日
  ,day21                         string    --21日
  ,day22                         string    --22日
  ,day23                         string    --23日
  ,day24                         string    --24日
  ,day25                         string    --25日
  ,day26                         string    --26日
  ,day27                         string    --27日
  ,day28                         string    --28日
  ,day29                         string    --29日
  ,day30                         string    --30日
  ,day31                         string    --31日
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PUT_COST_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_PUT_COST_DD PARTITION(op_day='$OP_DAY')
SELECT month_id                          --期间(月)
       ,day_id                           --期间(日)
       ,level1_org_id                    --组织1级(股份)  
       ,level1_org_descr                 --组织1级(股份)  
       ,level2_org_id                    --组织2级(片联)  
       ,level2_org_descr                 --组织2级(片联)  
       ,level3_org_id                    --组织3级(片区)  
       ,level3_org_descr                 --组织3级(片区)  
       ,level4_org_id                    --组织4级(小片)  
       ,level4_org_descr                 --组织4级(小片)  
       ,level5_org_id                    --组织5级(公司)  
       ,level5_org_descr                 --组织5级(公司)  
       ,level6_org_id                    --组织6级(OU)  
       ,level6_org_descr                 --组织6级(OU)  
       ,level7_org_id                    --组织7级(库存组织)
       ,level7_org_descr                 --组织7级(库存组织)
       ,level1_businesstype_id           --业态1级
       ,level1_businesstype_name         --业态1级
       ,level2_businesstype_id           --业态2级
       ,level2_businesstype_name         --业态2级
       ,level3_businesstype_id           --业态3级
       ,level3_businesstype_name         --业态3级
       ,level4_businesstype_id           --业态4级
       ,level4_businesstype_name         --业态4级
       ,production_line_id               --产线    
       ,production_line_descr            --产线    
       ,put_type_id                      --投放类型ID
       ,put_type_descr                   --投放类型名称
       ,breed_type_id                    --养殖类型
       ,breed_type_descr                 --养殖类型
       ,kpi_type_id                      --指标类型  
       ,kpi_type_descr                   --指标类型
       ,put_start_date                   --投放开始时间
       ,put_end_date                     --投放结束时间
       ,put_accum                        --当月累计  
       ,day1                             --1日  
       ,day2                             --2日  
       ,day3                             --3日  
       ,day4                             --4日  
       ,day5                             --5日  
       ,day6                             --6日  
       ,day7                             --7日  
       ,day8                             --8日  
       ,day9                             --9日  
       ,day10                            --10日 
       ,day11                            --11日 
       ,day12                            --12日 
       ,day13                            --13日 
       ,day14                            --14日 
       ,day15                            --15日 
       ,day16                            --16日 
       ,day17                            --17日 
       ,day18                            --18日 
       ,day19                            --19日 
       ,day20                            --20日 
       ,day21                            --21日 
       ,day22                            --22日 
       ,day23                            --23日 
       ,day24                            --24日 
       ,day25                            --25日 
       ,day26                            --26日 
       ,day27                            --27日 
       ,day28                            --28日 
       ,day29                            --29日 
       ,day30                            --30日
       ,day31                            --31日
       ,'$CREATE_TIME'
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_PUT_COST_DD_2
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PUT_COST_DD_1;
    $INSERT_TMP_DMP_BIRD_PUT_COST_DD_1;
    $CREATE_TMP_DMP_BIRD_PUT_COST_DD_1A;
    $INSERT_TMP_DMP_BIRD_PUT_COST_DD_1A;
    $CREATE_TMP_DMP_BIRD_PUT_COST_DD_2;
    $INSERT_TMP_DMP_BIRD_PUT_COST_DD_2;
    $CREATE_DMP_BIRD_PUT_COST_DD;
    $INSERT_DMP_BIRD_PUT_COST_DD;
"  -v