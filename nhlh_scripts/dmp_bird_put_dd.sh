#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_put_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 投放情况
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_put_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_DD_A0='TMP_DMP_BIRD_PUT_DD_A0'

CREATE_TMP_DMP_BIRD_PUT_DD_A0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_A0
(
  plan_date                      string    --计划投放时间
  ,production_line_id            string    --产线
  ,put_start_date                string    --投放开始日期(当月)
  ,put_end_date                  string    --投放结束日期(当月)
  ,put_start_date_last           string    --投放开始日期(上月)
  ,put_end_date_last             string    --投放结束日期(上月)
  ,put_start_date_next           string    --投放开始日期(下月)
  ,put_end_date_next             string    --投放结束日期(下月)
  ,put_start_date_next_next      string    --投放开始日期(下下月)
  ,put_end_date_next_next        string    --投放结束日期(下下月)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_A0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_A0 PARTITION(op_day='$OP_DAY')
SELECT regexp_replace(plan_date,'-','') plan_date
       ,production_line_id
       ,regexp_replace(put_start_date,'-','') put_start_date
       ,regexp_replace(put_end_date,'-','') put_end_date

       ,regexp_replace(put_start_date_last,'-','') put_start_date_last
       ,regexp_replace(put_end_date_last,'-','') put_end_date_last

       ,regexp_replace(put_start_date_next,'-','') put_start_date_next
       ,regexp_replace(put_end_date_next,'-','') put_end_date_next

       ,regexp_replace(put_start_date_next_next,'-','') put_start_date_next_next
       ,regexp_replace(put_end_date_next_next,'-','') put_end_date_next_next
  FROM (SELECT a1.production_line_id
               ,a1.plan_date
               ,date_add(concat(substr(plan_date,1,8),'01'),-a2.tag) put_start_date
               ,date_add(date_add(concat(substr(date_add(concat(substr(plan_date,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date

               ,date_add(concat(substr(plan_date_last,1,8),'01'),-a2.tag) put_start_date_last
               ,date_add(date_add(concat(substr(date_add(concat(substr(plan_date_last,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date_last

               ,date_add(concat(substr(plan_date_next,1,8),'01'),-a2.tag) put_start_date_next
               ,date_add(date_add(concat(substr(date_add(concat(substr(plan_date_next,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date_next

               ,date_add(concat(substr(plan_date_next_next,1,8),'01'),-a2.tag) put_start_date_next_next
               ,date_add(date_add(concat(substr(date_add(concat(substr(plan_date_next_next,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date_next_next
          FROM (SELECT substr(plan_date,1,10) plan_date
                       ,date_add(concat(substr(plan_date,1,8),'01'),-1) plan_date_last                         --上一个月
                       ,concat(substr(date_add(concat(substr(plan_date,1,8),'28'),5),1,8),'01') plan_date_next --下一个月
                       ,concat(substr(date_add(concat(substr(date_add(concat(substr(plan_date,1,8),'28'),5),1,8),'28'),5),1,8),'01') plan_date_next_next --下下个月
                       ,case when prod_line='鸡线' then 'CHICHEN'
                             when prod_line='鸭线' then 'DUCK'
                        else null end production_line_id
                  FROM dwu_qw_throw_in_dtl_dd
                 WHERE op_day='$OP_DAY'
                 GROUP BY substr(plan_date,1,10)
                       ,date_add(concat(substr(plan_date,1,8),'01'),-1)
                       ,concat(substr(date_add(concat(substr(plan_date,1,8),'28'),5),1,8),'01')
                       ,concat(substr(date_add(concat(substr(date_add(concat(substr(plan_date,1,8),'28'),5),1,8),'28'),5),1,8),'01')
                       ,prod_line) a1
          LEFT JOIN (SELECT case when lookup_code='1' then 'CHICHEN'
                                 when lookup_code='2' then 'DUCK'
                            else '-999' end prod_line
                            ,int(tag) tag
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                        AND language='ZHS') a2
            ON (a1.production_line_id=a2.prod_line)) t
 GROUP BY plan_date
       ,production_line_id
       ,put_start_date
       ,put_end_date
       ,put_start_date_last
       ,put_end_date_last
       ,put_start_date_next
       ,put_end_date_next
       ,put_start_date_next_next
       ,put_end_date_next_next
"

###########################################################################################
## 处理投放周期
## 变量声明
TMP_DMP_BIRD_PUT_DD_A1='TMP_DMP_BIRD_PUT_DD_A1'

CREATE_TMP_DMP_BIRD_PUT_DD_A1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_A1(
  plan_date                      string    --计划投放时间
  ,production_line_id             string   --产线
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_A1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_A1 PARTITION(op_day='$OP_DAY')
SELECT plan_date
       ,production_line_id
       ,put_start_date
       ,put_end_date
  FROM (SELECT plan_date
               ,production_line_id
               ,put_start_date
               ,put_end_date
          FROM $TMP_DMP_BIRD_PUT_DD_A0
         WHERE op_day='$OP_DAY'
           AND plan_date between put_start_date and put_end_date
        UNION ALL
        SELECT plan_date
               ,production_line_id
               ,put_start_date_last put_start_date
               ,put_end_date_last put_end_date
          FROM $TMP_DMP_BIRD_PUT_DD_A0
         WHERE op_day='$OP_DAY'
           AND plan_date between put_start_date_last and put_end_date_last
        UNION ALL
        SELECT plan_date
               ,production_line_id
               ,put_start_date_next put_start_date
               ,put_end_date_next put_end_date
          FROM $TMP_DMP_BIRD_PUT_DD_A0
         WHERE op_day='$OP_DAY'
           AND plan_date between put_start_date_next and put_end_date_next
        UNION ALL
        SELECT plan_date
               ,production_line_id
               ,put_start_date_next_next put_start_date
               ,put_end_date_next_next put_end_date
          FROM $TMP_DMP_BIRD_PUT_DD_A0
         WHERE op_day='$OP_DAY'
           AND plan_date between put_start_date_next_next and put_end_date_next_next) t
 WHERE put_start_date is not null
 GROUP BY plan_date
       ,production_line_id
       ,put_start_date
       ,put_end_date
"

###########################################################################################
## 处理投放周期
## 变量声明
TMP_DMP_BIRD_PUT_DD_A2='TMP_DMP_BIRD_PUT_DD_A2'

CREATE_TMP_DMP_BIRD_PUT_DD_A2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_A2(
  level1_org_id                  string    --组织1级(股份)  
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
  ,production_line_id            string    --产线
  ,production_line_descr         string    --产线
  ,plan_date                     string    --计划投放时间
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,put_start_date1               string    --投放开始日期(返算后)
  ,put_end_date1                 string    --投放结束日期(返算后)
  ,dmt_plan_qty                  string    --计划投放数量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_A2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_A2 PARTITION(op_day='$OP_DAY')
SELECT t3.level1_org_id
       ,t3.level1_org_descr
       ,t3.level2_org_id
       ,t3.level2_org_descr
       ,t3.level3_org_id
       ,t3.level3_org_descr
       ,t3.level4_org_id
       ,t3.level4_org_descr
       ,t3.level5_org_id
       ,t3.level5_org_descr
       ,t3.level6_org_id
       ,t3.level6_org_descr
       ,case when t1.production_line_id='CHICHEN' then '1'
             when t1.production_line_id='DUCK' then '2'
        else null end production_line_id
       ,case when t1.production_line_id='CHICHEN' then '鸡线'
             when t1.production_line_id='DUCK' then '鸭线'
        else null end production_line_descr
       ,t1.plan_date       
       ,t2.put_start_date
       ,t2.put_end_date
       ,regexp_replace(date_add(concat(substr(t2.put_start_date,1,4),'-',substr(t2.put_start_date,5,2),'-',substr(t2.put_start_date,7,2)),t5.tag),'-','') put_start_date1
       ,regexp_replace(date_add(concat(substr(t2.put_end_date,1,4),'-',substr(t2.put_end_date,5,2),'-',substr(t2.put_end_date,7,2)),t5.tag),'-','') put_end_date1
       ,t1.dmt_plan_qty
  FROM (SELECT org_id,
               bustype bus_type,
               case when prod_line='鸡线' then 'CHICHEN'
                    when prod_line='鸭线' then 'DUCK'
               else null end production_line_id,
               regexp_replace(substr(plan_date,1,10),'-','') plan_date,
               confirm_qty dmt_plan_qty
          FROM dwu_qw_throw_in_dtl_dd
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT regexp_replace(plan_date,'-','') plan_date,
                    production_line_id,
                    put_start_date,
                    put_end_date
               FROM $TMP_DMP_BIRD_PUT_DD_A1
              WHERE op_day='$OP_DAY') t2
    ON (t1.plan_date=t2.plan_date
    AND t1.production_line_id=t2.production_line_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t3
         ON (t1.org_id=t3.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t4
    ON (t1.bus_type=t4.level4_businesstype_id)
  LEFT JOIN (SELECT case when lookup_code='1' then 'CHICHEN'
                         when lookup_code='2' then 'DUCK'
                    else '-999' end prod_line
                    ,int(tag) tag
               FROM mreport_global.ods_ebs_fnd_lookup_values
              WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                AND language='ZHS') t5
    ON (t1.production_line_id=t5.prod_line)
"

###########################################################################################
## 月累计和年累计计算
## 变量声明
TMP_DMP_BIRD_PUT_DD_A3='TMP_DMP_BIRD_PUT_DD_A3'

CREATE_TMP_DMP_BIRD_PUT_DD_A3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_A3(
  level6_org_id                  string    --组织6级(OU)  
  ,production_line_id            string    --产线
  ,put_end_date1                 string    --投放结束日期
  ,put_year_kpi_qty              string    --年投放计划数量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_A3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_A3 PARTITION(op_day='$OP_DAY')
SELECT t1.level6_org_id                   --组织6级(OU) 
       ,t1.production_line_id             --产线
       ,t1.put_end_date1                  --投放周期结束时间
       ,sum(case when t1.put_end_date1>=t2.put_end_date1 then t2.dmt_plan_qty
        else '0' end) put_year_kpi_qty    --年投放计划数量
  FROM (SELECT level6_org_id
               ,production_line_id
               ,put_end_date1
          FROM $TMP_DMP_BIRD_PUT_DD_A2
         WHERE op_day='$OP_DAY'
         GROUP BY level6_org_id
               ,production_line_id
               ,put_end_date1) t1
  LEFT JOIN (SELECT level6_org_id
                    ,production_line_id
                    ,put_end_date1
                    ,sum(dmt_plan_qty) dmt_plan_qty
               FROM $TMP_DMP_BIRD_PUT_DD_A2
              WHERE op_day='$OP_DAY'
              GROUP BY level6_org_id
                    ,production_line_id
                    ,put_end_date1) t2
    ON (t1.level6_org_id=t2.level6_org_id
    AND t1.production_line_id=t2.production_line_id
    AND substr(t1.put_end_date1,1,4)=substr(t2.put_end_date1,1,4))
 GROUP BY t1.level6_org_id
       ,t1.production_line_id
       ,t1.put_end_date1
"

###########################################################################################
## 月累计和年累计关联
## 变量声明
TMP_DMP_BIRD_PUT_DD_A4='TMP_DMP_BIRD_PUT_DD_A4'

CREATE_TMP_DMP_BIRD_PUT_DD_A4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_A4(
  level1_org_id                  string    --组织1级(股份)  
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
  ,production_line_id            string    --产线
  ,production_line_descr         string    --产线
  ,plan_date                     string    --计划投放时间
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,put_day_kpi_qty               string    --日投放计划数量
  ,put_month_kpi_qty             string    --月投放计划数量
  ,put_year_kpi_qty              string    --年投放计划数量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_A4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_A4 PARTITION(op_day='$OP_DAY')
SELECT t1.level1_org_id                   --组织1级(股份)  
       ,t1.level1_org_descr               --组织1级(股份)  
       ,t1.level2_org_id                  --组织2级(片联)  
       ,t1.level2_org_descr               --组织2级(片联)  
       ,t1.level3_org_id                  --组织3级(片区)  
       ,t1.level3_org_descr               --组织3级(片区)  
       ,t1.level4_org_id                  --组织4级(小片)  
       ,t1.level4_org_descr               --组织4级(小片)  
       ,t1.level5_org_id                  --组织5级(公司)  
       ,t1.level5_org_descr               --组织5级(公司)  
       ,t1.level6_org_id                  --组织6级(OU)  
       ,t1.level6_org_descr               --组织6级(OU)  
       ,t1.production_line_id             --产线
       ,t1.production_line_descr          --产线
       ,t1.plan_date                      --计划投放时间
       ,t1.put_start_date                 --投放开始日期
       ,t1.put_end_date                   --投放结束日期
       ,t1.dmt_plan_qty put_day_kpi_qty   --日投放计划数量
       ,t2.put_month_kpi_qty              --月投放计划数量
       ,t3.put_year_kpi_qty               --年投放计划数量
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_PUT_DD_A2
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT level6_org_id
                    ,production_line_id
                    ,put_start_date
                    ,put_end_date
                    ,sum(dmt_plan_qty) put_month_kpi_qty
               FROM $TMP_DMP_BIRD_PUT_DD_A2
              WHERE op_day='$OP_DAY'
              GROUP BY level6_org_id
                    ,production_line_id
                    ,put_start_date
                    ,put_end_date) t2
    ON (t1.level6_org_id=t2.level6_org_id
    AND t1.production_line_id=t2.production_line_id
    AND t1.put_start_date=t2.put_start_date
    AND t1.put_end_date=t2.put_end_date)
  LEFT JOIN (SELECT level6_org_id
                    ,production_line_id
                    ,put_end_date1
                    ,put_year_kpi_qty
               FROM $TMP_DMP_BIRD_PUT_DD_A3
              WHERE op_day='$OP_DAY') t3
    ON (t1.level6_org_id=t3.level6_org_id
    AND t1.production_line_id=t3.production_line_id
    AND t1.put_end_date1=t3.put_end_date1)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_DD_0='TMP_DMP_BIRD_PUT_DD_0'

CREATE_TMP_DMP_BIRD_PUT_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_0
(
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
  ,if_dsp                        string    --是否直供
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,put_start_date1               string    --投放开始日期(返算后)
  ,put_end_date1                 string    --投放结束日期(返算后)

  ,contract_qty                  string    --本日投放量
  ,dmt_plan_qty                  string    --本日投放目标
  ,put_cost                      string    --本日投放成本
  ,dummy_put_qty                 string    --本日投放量(加权后)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_0 PARTITION(op_day='$OP_DAY')
SELECT substr(day_id,1,6) month_id           --期间(月)
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
       ,if_dsp                               --是否直供
       ,put_start_date                       --投放开始日期
       ,put_end_date                         --投放结束日期
       ,put_start_date1                      --投放开始日期(返算后)
       ,put_end_date1                        --投放结束日期(返算后)

       ,contract_qty                         --本日投放量
       ,dmt_plan_qty                         --本日投放目标
       ,put_cost                             --本日投放成本
       ,dummy_put_qty                        --本日投放量(加权后)
  FROM (SELECT a1.day_id                                --期间(日)  
               ,a1.level1_org_id                        --组织1级(股份)  
               ,a1.level1_org_descr                     --组织1级(股份)  
               ,a1.level2_org_id                        --组织2级(片联)  
               ,a1.level2_org_descr                     --组织2级(片联)  
               ,a1.level3_org_id                        --组织3级(片区)  
               ,a1.level3_org_descr                     --组织3级(片区)  
               ,a1.level4_org_id                        --组织4级(小片)  
               ,a1.level4_org_descr                     --组织4级(小片)  
               ,a1.level5_org_id                        --组织5级(公司)  
               ,a1.level5_org_descr                     --组织5级(公司) 
               ,a1.level6_org_id                        --组织6级(OU)  
               ,a1.level6_org_descr                     --组织6级(OU)
               ,a1.level1_businesstype_id               --业态1级
               ,a1.level1_businesstype_name             --业态1级
               ,a1.level2_businesstype_id               --业态2级
               ,a1.level2_businesstype_name             --业态2级
               ,a1.level3_businesstype_id               --业态3级
               ,a1.level3_businesstype_name             --业态3级
               ,a1.level4_businesstype_id               --业态4级
               ,a1.level4_businesstype_name             --业态4级
               ,a1.production_line_id                   --产线    
               ,a1.production_line_descr                --产线
               ,a1.put_type_id                          --投放类型ID
               ,a1.put_type_descr                       --投放类型名称
               ,a1.breed_type_id                        --养殖类型
               ,a1.breed_type_descr                     --养殖类型
               ,a1.if_dsp                               --是否直供
               ,a1.put_start_date                       --投放开始日期
               ,a1.put_end_date                         --投放结束日期
               ,regexp_replace(date_add(concat(substr(a1.put_start_date,1,4),'-',substr(a1.put_start_date,5,2),'-',substr(a1.put_start_date,7,2)),a3.tag),'-','') put_start_date1
               ,regexp_replace(date_add(concat(substr(a1.put_end_date,1,4),'-',substr(a1.put_end_date,5,2),'-',substr(a1.put_end_date,7,2)),a3.tag),'-','') put_end_date1
               ,sum(a1.contract_qty) contract_qty       --本日投放量
               ,sum(a2.dmt_plan_qty) dmt_plan_qty       --计划投放数量
               ,sum(a1.put_cost) put_cost               --投放成本
               ,sum(a1.dummy_put_qty) dummy_put_qty     --本日投放量(加权后)
         FROM (SELECT day_id                                --期间(日)  
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
                      ,contract_date                        --合同日期
                      ,if_dsp                               --是否直供
                      ,put_start_date                       --投放开始日期
                      ,put_end_date                         --投放结束日期
                      
                      ,sum(contract_qty) contract_qty       --本日投放量
                      ,sum(put_cost*contract_qty) put_cost  --投放成本
                      ,sum(case when coalesce(put_cost,0)=0 or coalesce(contract_qty,0)=0
                                then 0 else contract_qty end) dummy_put_qty
                 FROM dwp_bird_put_contract_dd
                WHERE op_day='$OP_DAY'
                GROUP BY day_id                             --期间(日)  
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
                      ,breed_type_descr
                      ,if_dsp                               --是否直供
                      ,put_start_date                       --投放开始日期
                      ,put_end_date                         --投放结束日期
                      ,contract_date) a1
         LEFT JOIN (SELECT substr(b1.plan_date,1,10) plan_date
                           ,case when b1.prod_line='鸡线' then '1'
                                 when b1.prod_line='鸭线' then '2'
                            else '-99' end production_line_id
                           ,b1.confirm_qty dmt_plan_qty
                           ,b1.org_id
                           ,b2.level6_org_id
                      FROM dwu_qw_throw_in_dtl_dd b1
                     INNER JOIN (SELECT org_id,
                                        level6_org_id
                                   FROM mreport_global.dim_org_management
                                  GROUP BY org_id,level6_org_id) b2
                        ON (b1.org_id=b2.org_id)
                     WHERE b1.op_day='$OP_DAY') a2
           ON (a1.contract_date=a2.plan_date
           AND a1.level6_org_id=a2.level6_org_id
           AND a1.production_line_id=a2.production_line_id)
         LEFT JOIN (SELECT case when lookup_code='1' then '1'
                                when lookup_code='2' then '2'
                           else '-999' end prod_line
                           ,int(tag) tag
                      FROM mreport_global.ods_ebs_fnd_lookup_values
                     WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                       AND language='ZHS') a3
           ON (a1.production_line_id=a3.prod_line)
        GROUP BY a1.day_id                               --期间(日)  
               ,a1.level1_org_id                        --组织1级(股份)  
               ,a1.level1_org_descr                     --组织1级(股份)  
               ,a1.level2_org_id                        --组织2级(片联)  
               ,a1.level2_org_descr                     --组织2级(片联)  
               ,a1.level3_org_id                        --组织3级(片区)  
               ,a1.level3_org_descr                     --组织3级(片区)  
               ,a1.level4_org_id                        --组织4级(小片)  
               ,a1.level4_org_descr                     --组织4级(小片)  
               ,a1.level5_org_id                        --组织5级(公司)  
               ,a1.level5_org_descr                     --组织5级(公司) 
               ,a1.level6_org_id                        --组织6级(OU)  
               ,a1.level6_org_descr                     --组织6级(OU)
               ,a1.level1_businesstype_id               --业态1级
               ,a1.level1_businesstype_name             --业态1级
               ,a1.level2_businesstype_id               --业态2级
               ,a1.level2_businesstype_name             --业态2级
               ,a1.level3_businesstype_id               --业态3级
               ,a1.level3_businesstype_name             --业态3级
               ,a1.level4_businesstype_id               --业态4级
               ,a1.level4_businesstype_name             --业态4级
               ,a1.production_line_id                   --产线    
               ,a1.production_line_descr                --产线
               ,a1.put_type_id                          --投放类型ID
               ,a1.put_type_descr                       --投放类型名称
               ,a1.breed_type_id                        --养殖类型
               ,a1.if_dsp                               --是否直供
               ,a1.put_start_date                       --投放开始日期
               ,a1.put_end_date                         --投放结束日期
               ,a1.breed_type_descr
               ,a3.tag) t1
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PUT_DD_1='TMP_DMP_BIRD_PUT_DD_1'

CREATE_TMP_DMP_BIRD_PUT_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PUT_DD_1
(
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
  ,put_day_qty                   string    --本日投放量
  ,dummy_put_day_qty             string    --本日投放量(加权后)
  ,put_day_kpi_qty               string    --本日投放目标
  ,put_day_cost                  string    --本日投放成本
  ,put_day_dsp_qty               string    --本日直供合同投放量
  ,put_day_feed_qty              string    --本日代养合同投放量
  ,put_month_qty                 string    --本月投放量
  ,dummy_put_month_qty           string    --本月投放量(加权后)
  ,put_month_kpi_qty             string    --本月投放目标
  ,put_month_cost                string    --本月投放成本
  ,put_month_dsp_qty             string    --本月直供合同投放量
  ,put_month_feed_qty            string    --本月代养合同投放量
  ,put_year_qty                  string    --本年投放量
  ,dummy_put_year_qty            string    --本年投放量(加权后)
  ,put_year_kpi_qty              string    --本年投放目标
  ,put_year_cost                 string    --本年投放成本
  ,put_year_dsp_qty              string    --本年直供合同投放量
  ,put_year_feed_qty             string    --本年代养合同投放量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PUT_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PUT_DD_1 PARTITION(op_day='$OP_DAY')
SELECT substr(day_id,1,6) month_id           --期间(月)
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

       ,sum(put_day_qty) put_day_qty                         --本日投放量
       ,sum(dummy_put_day_qty) dummy_put_day_qty             --本日投放量(加权后)
       ,sum(put_day_kpi_qty) put_day_kpi_qty                 --本日投放目标
       ,sum(put_day_cost) put_day_cost                       --本日投放成本
       ,sum(put_day_dsp_qty) put_day_dsp_qty                 --本日直供合同投放量
       ,sum(put_day_feed_qty) put_day_feed_qty               --本日代养合同投放量

       ,sum(put_month_qty) put_month_qty                     --本月投放量
       ,sum(dummy_put_month_qty) dummy_put_month_qty         --本月投放量(加权后)
       ,sum(put_month_kpi_qty) put_month_kpi_qty             --本月投放目标
       ,sum(put_month_cost) put_month_cost                   --本月投放成本
       ,sum(put_month_dsp_qty) put_month_dsp_qty             --本月直供合同投放量
       ,sum(put_month_feed_qty) put_month_feed_qty           --本月代养合同投放量

       ,sum(put_year_qty) put_year_qty                       --本年投放量
       ,sum(dummy_put_year_qty) dummy_put_year_qty           --本月投放量(加权后)
       ,sum(put_year_kpi_qty) putyear_kpi_qty                --本年投放目标
       ,sum(put_year_cost) put_year_cost                     --本年投放成本
       ,sum(put_year_dsp_qty) put_year_dsp_qty               --本年直供合同投放量
       ,sum(put_year_feed_qty) put_year_feed_qty             --本年代养合同投放量
  FROM (SELECT a1.day_id                                --期间(日)
               ,a2.level1_org_id                        --组织1级(股份)  
               ,a2.level1_org_descr                     --组织1级(股份)  
               ,a2.level2_org_id                        --组织2级(片联)  
               ,a2.level2_org_descr                     --组织2级(片联)  
               ,a2.level3_org_id                        --组织3级(片区)  
               ,a2.level3_org_descr                     --组织3级(片区)  
               ,a2.level4_org_id                        --组织4级(小片)  
               ,a2.level4_org_descr                     --组织4级(小片)  
               ,a2.level5_org_id                        --组织5级(公司)  
               ,a2.level5_org_descr                     --组织5级(公司)  
               ,a2.level6_org_id                        --组织6级(OU)  
               ,a2.level6_org_descr                     --组织6级(OU)  
               ,a2.level7_org_id                        --组织7级(库存组织)
               ,a2.level7_org_descr                     --组织7级(库存组织)
               ,a2.level1_businesstype_id               --业态1级
               ,a2.level1_businesstype_name             --业态1级
               ,a2.level2_businesstype_id               --业态2级
               ,a2.level2_businesstype_name             --业态2级
               ,a2.level3_businesstype_id               --业态3级
               ,a2.level3_businesstype_name             --业态3级
               ,a2.level4_businesstype_id               --业态4级
               ,a2.level4_businesstype_name             --业态4级
               ,a2.production_line_id                   --产线    
               ,a2.production_line_descr                --产线    
               ,a2.put_type_id                          --投放类型ID
               ,a2.put_type_descr                       --投放类型名称
               ,a2.breed_type_id                        --养殖类型
               ,a2.breed_type_descr                     --养殖类型

               ,case when a1.day_id=a2.day_id
                     then a2.put_day_qty else '0' end put_day_qty                    --本日投放量
               ,case when a1.day_id=a2.day_id
                     then a2.dummy_put_qty else '0' end dummy_put_day_qty            --本日投放量(加权后)
               ,0  put_day_kpi_qty                                                   --本日投放目标
               ,case when a1.day_id=a2.day_id
                     then a2.put_day_cost else '0' end put_day_cost                  --本日投放成本
               ,case when a1.day_id=a2.day_id and a2.if_dsp='Y'
                     then a2.put_day_dsp_qty else '0' end put_day_dsp_qty            --本日直供合同投放量
               ,case when a1.day_id=a2.day_id and breed_type_descr='代养'
                     then a2.put_day_feed_qty else 0 end put_day_feed_qty            --本日代养合同投放量

               ,case when a1.day_id>=a2.day_id and a1.day_id between a2.put_start_date and a2.put_end_date
                     then a2.put_day_qty else '0' end put_month_qty                  --本月投放量
               ,case when a1.day_id>=a2.day_id and a1.day_id between a2.put_start_date and a2.put_end_date
                     then a2.dummy_put_qty else '0' end dummy_put_month_qty          --本月投放量
               ,0  put_month_kpi_qty                                                 --本月投放目标
               ,case when a1.day_id>=a2.day_id and a1.day_id between a2.put_start_date and a2.put_end_date
                     then a2.put_day_cost else '0' end put_month_cost                --本月投放成本
               ,case when a1.day_id>=a2.day_id and a1.day_id between a2.put_start_date and a2.put_end_date and a2.if_dsp='Y'
                     then a2.put_day_dsp_qty else '0' end put_month_dsp_qty          --本月直供合同投放量
               ,case when a1.day_id>=a2.day_id and a1.day_id between a2.put_start_date and a2.put_end_date and breed_type_descr='代养'
                     then a2.put_day_feed_qty else 0 end put_month_feed_qty          --本月代养合同投放量

               ,case when a1.day_id>=a2.day_id and a1.year_id=substr(a2.put_start_date1,1,4)
                     then a2.put_day_qty else '0' end put_year_qty                   --本年投放量
               ,case when a1.day_id>=a2.day_id and a1.year_id=substr(a2.put_start_date1,1,4)
                     then a2.dummy_put_qty else '0' end dummy_put_year_qty           --本年投放量
               ,0  put_year_kpi_qty                                                  --本年投放目标
               ,case when a1.day_id>=a2.day_id and a1.year_id=substr(a2.put_start_date1,1,4)
                     then a2.put_day_cost else '0' end put_year_cost                 --本年投放成本
               ,case when a1.day_id>=a2.day_id and a1.year_id=substr(a2.put_start_date1,1,4) and a2.if_dsp='Y'
                     then a2.put_day_dsp_qty else '0' end put_year_dsp_qty           --本年直供合同投放量
               ,case when a1.day_id>=a2.day_id and a1.year_id=substr(a2.put_start_date1,1,4) and breed_type_descr='代养'
                     then a2.put_day_feed_qty else 0 end put_year_feed_qty           --本年代养合同投放量
          FROM (SELECT substr(put_start_date1,1,4) year_id
                       ,day_id
                       ,production_line_id
                  FROM $TMP_DMP_BIRD_PUT_DD_0
                 WHERE op_day='$OP_DAY'
                 GROUP BY substr(put_start_date1,1,4),day_id,production_line_id) a1
          LEFT JOIN (SELECT day_id                               --期间(日)
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
                            ,null level7_org_id                   --组织7级(库存组织)
                            ,null level7_org_descr                --组织7级(库存组织)
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
                            ,if_dsp                               --是否直供
                            ,put_start_date                       --投放开始日期
                            ,put_end_date                         --投放结束日期
                            ,put_start_date1                      --投放开始日期
                            ,put_end_date1                        --投放结束日期
                            ,contract_qty put_day_qty             --本日投放量
                            ,dummy_put_qty                        --本日投放量(加权后)
                            ,dmt_plan_qty put_day_kpi_qty         --本日投放目标
                            ,put_cost put_day_cost                --本日投放成本
                            ,case when if_dsp='Y' then contract_qty else '0' end put_day_dsp_qty  --本日直供合同投放量
                            ,case when breed_type_descr='代养' then contract_qty else 0 end put_day_feed_qty --本日代养合同投放量
                       FROM $TMP_DMP_BIRD_PUT_DD_0
                      WHERE op_day='$OP_DAY') a2
            ON (a1.year_id=substr(a2.put_start_date1,1,4) and a1.production_line_id=a2.production_line_id)) t1
  GROUP BY substr(day_id,1,6)                --期间(月)
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
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PUT_DD='DMP_BIRD_PUT_DD'

CREATE_DMP_BIRD_PUT_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PUT_DD
(
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
  ,put_day_qty                   string    --本日投放量
  ,dummy_put_day_qty             string    --本日投放量(加权后)
  ,put_day_kpi_qty               string    --本日投放目标
  ,put_day_cost                  string    --本日投放成本
  ,put_day_dsp_qty               string    --本日直供合同投放量
  ,put_day_feed_qty              string    --本日代养合同投放量
  ,put_month_qty                 string    --本月投放量
  ,dummy_put_month_qty           string    --本月投放量(加权后)
  ,put_month_kpi_qty             string    --本月投放目标
  ,put_month_cost                string    --本月投放成本
  ,put_month_dsp_qty             string    --本月直供合同投放量
  ,put_month_feed_qty            string    --本月代养合同投放量
  ,put_year_qty                  string    --本年投放量
  ,dummy_put_year_qty            string    --本年投放量(加权后)
  ,put_year_kpi_qty              string    --本年投放目标
  ,put_year_cost                 string    --本年投放成本
  ,put_year_dsp_qty              string    --本年直供合同投放量
  ,put_year_feed_qty             string    --本年代养合同投放量
  ,create_time                   string    --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PUT_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_PUT_DD PARTITION(op_day='$OP_DAY')
SELECT month_id                           --期间(月)
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
       ,put_type_id                       --投放类型ID
       ,put_type_descr                    --投放类型名称
       ,breed_type_id                     --养殖类型
       ,breed_type_descr                  --养殖类型
       ,put_day_qty                       --本日投放量
       ,dummy_put_day_qty                 --本日投放量(加权后)
       ,put_day_kpi_qty                   --本日投放目标
       ,put_day_cost                      --本日投放成本
       ,put_day_dsp_qty                   --本日直供合同投放量
       ,put_day_feed_qty                  --本日代养合同投放量
       ,put_month_qty                     --本月投放量
       ,dummy_put_month_qty               --本月投放量(加权后)
       ,put_month_kpi_qty                 --本月投放目标
       ,put_month_cost                    --本月投放成本
       ,put_month_dsp_qty                 --本月直供合同投放量
       ,put_month_feed_qty                --本月代养合同投放量
       ,put_year_qty                      --本年投放量
       ,dummy_put_year_qty                --本年投放量(加权后)
       ,put_year_kpi_qty                  --本年投放目标
       ,put_year_cost                     --本年投放成本
       ,put_year_dsp_qty                  --本年直供合同投放量
       ,put_year_feed_qty                 --本年代养合同投放量
       ,'$CREATE_TIME' create_time        --创建时间
  FROM (SELECT month_id                           --期间(月)
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
               ,put_type_id                       --投放类型ID
               ,put_type_descr                    --投放类型名称
               ,breed_type_id                     --养殖类型
               ,breed_type_descr                  --养殖类型
               ,put_day_qty                       --本日投放量
               ,dummy_put_day_qty                 --本日投放量(加权后)
               ,put_day_kpi_qty                   --本日投放目标
               ,put_day_cost                      --本日投放成本
               ,put_day_dsp_qty                   --本日直供合同投放量
               ,put_day_feed_qty                  --本日代养合同投放量
               ,put_month_qty                     --本月投放量
               ,dummy_put_month_qty               --本月投放量(加权后)
               ,put_month_kpi_qty                 --本月投放目标
               ,put_month_cost                    --本月投放成本
               ,put_month_dsp_qty                 --本月直供合同投放量
               ,put_month_feed_qty                --本月代养合同投放量
               ,put_year_qty                      --本年投放量
               ,dummy_put_year_qty                --本年投放量(加权后)
               ,put_year_kpi_qty                  --本年投放目标
               ,put_year_cost                     --本年投放成本
               ,put_year_dsp_qty                  --本年直供合同投放量
               ,put_year_feed_qty                 --本年代养合同投放量
          FROM $TMP_DMP_BIRD_PUT_DD_1
         WHERE op_day='$OP_DAY'         
        UNION ALL
        SELECT substr(a1.plan_date,1,6) month_id  --期间(月)
               ,a1.plan_date day_id               --期间(日)
               ,a1.level1_org_id                  --组织1级(股份)  
               ,a1.level1_org_descr               --组织1级(股份)  
               ,a1.level2_org_id                  --组织2级(片联)  
               ,a1.level2_org_descr               --组织2级(片联)  
               ,a1.level3_org_id                  --组织3级(片区)  
               ,a1.level3_org_descr               --组织3级(片区)  
               ,a1.level4_org_id                  --组织4级(小片)  
               ,a1.level4_org_descr               --组织4级(小片)  
               ,a1.level5_org_id                  --组织5级(公司)  
               ,a1.level5_org_descr               --组织5级(公司)  
               ,a1.level6_org_id                  --组织6级(OU)  
               ,a1.level6_org_descr               --组织6级(OU)  
               ,null level7_org_id                --组织7级(库存组织)
               ,null level7_org_descr             --组织7级(库存组织)
               ,null level1_businesstype_id       --业态1级
               ,null level1_businesstype_name     --业态1级
               ,null level2_businesstype_id       --业态2级
               ,null level2_businesstype_name     --业态2级
               ,null level3_businesstype_id       --业态3级
               ,null level3_businesstype_name     --业态3级
               ,null level4_businesstype_id       --业态4级
               ,null level4_businesstype_name     --业态4级
               ,a1.production_line_id             --产线    
               ,a1.production_line_descr          --产线    
               ,null put_type_id                  --投放类型ID
               ,null put_type_descr               --投放类型名称
               ,null breed_type_id                --养殖类型
               ,null breed_type_descr             --养殖类型
               ,'0' put_day_qty                   --本日投放量
               ,'0' dummy_put_day_qty             --本日投放量(加权后)
               ,a1.put_day_kpi_qty                --本日投放目标
               ,'0' put_day_cost                  --本日投放成本
               ,'0' put_day_dsp_qty               --本日直供合同投放量
               ,'0' put_day_feed_qty              --本日代养合同投放量
               ,'0' put_month_qty                 --本月投放量
               ,'0' dummy_put_month_qty           --本月投放量(加权后)
               ,a1.put_month_kpi_qty              --本月投放目标
               ,'0' put_month_cost                --本月投放成本
               ,'0' put_month_dsp_qty             --本月直供合同投放量
               ,'0' put_month_feed_qty            --本月代养合同投放量
               ,'0' put_year_qty                  --本年投放量
               ,'0' dummy_put_year_qty            --本年投放量(加权后)
               ,a1.put_year_kpi_qty               --本年投放目标
               ,'0' put_year_cost                 --本年投放成本
               ,'0' put_year_dsp_qty              --本年直供合同投放量
               ,'0' put_year_feed_qty             --本年代养合同投放量
          FROM $TMP_DMP_BIRD_PUT_DD_A4 a1
         WHERE op_day='$OP_DAY') t1
 WHERE month_id is not null
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    
    $CREATE_TMP_DMP_BIRD_PUT_DD_A0;
    $INSERT_TMP_DMP_BIRD_PUT_DD_A0;
    $CREATE_TMP_DMP_BIRD_PUT_DD_A1;
    $INSERT_TMP_DMP_BIRD_PUT_DD_A1;
    $CREATE_TMP_DMP_BIRD_PUT_DD_A2;
    $INSERT_TMP_DMP_BIRD_PUT_DD_A2;
    $CREATE_TMP_DMP_BIRD_PUT_DD_A3;
    $INSERT_TMP_DMP_BIRD_PUT_DD_A3;
    $CREATE_TMP_DMP_BIRD_PUT_DD_A4;
    $INSERT_TMP_DMP_BIRD_PUT_DD_A4;
    $CREATE_TMP_DMP_BIRD_PUT_DD_0;
    $INSERT_TMP_DMP_BIRD_PUT_DD_0;
    $CREATE_TMP_DMP_BIRD_PUT_DD_1;
    $INSERT_TMP_DMP_BIRD_PUT_DD_1;
    $CREATE_DMP_BIRD_PUT_DD;
    $INSERT_DMP_BIRD_PUT_DD;
"  -v
