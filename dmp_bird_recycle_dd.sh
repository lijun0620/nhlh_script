#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_recycle_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-回收情况
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_recycle_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_RECYCLE_DD_1='TMP_DMP_BIRD_RECYCLE_DD_1'

CREATE_TMP_DMP_BIRD_RECYCLE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_RECYCLE_DD_1(
  year_id                         string        --期间(年)
  ,month_id                       string        --期间(月份)    
  ,day_id                         string        --期间(日)
  ,production_line_id             string        --产线代码
  ,production_line_descr          string        --产线描述
  ,recycle_type_id                string        --回收类型
  ,recycle_type_descr             string        --回收类型
  ,breed_type_id                  string        --养殖类型
  ,breed_type_descr               string        --养殖类型
  ,level1_org_id                  string        --组织1级
  ,level1_org_descr               string        --组织1级
  ,level2_org_id                  string        --组织2级
  ,level2_org_descr               string        --组织2级
  ,level3_org_id                  string        --组织3级
  ,level3_org_descr               string        --组织3级
  ,level4_org_id                  string        --组织4级
  ,level4_org_descr               string        --组织4级
  ,level5_org_id                  string        --组织5级
  ,level5_org_descr               string        --组织5级
  ,level6_org_id                  string        --组织6级
  ,level6_org_descr               string        --组织6级
  ,level7_org_id                  string        --组织7级
  ,level7_org_descr               string        --组织7级
  ,level1_businesstype_id         string        --业态1级
  ,level1_businesstype_name       string        --业态1级
  ,level2_businesstype_id         string        --业态2级
  ,level2_businesstype_name       string        --业态2级
  ,level3_businesstype_id         string        --业态3级
  ,level3_businesstype_name       string        --业态3级
  ,level4_businesstype_id         string        --业态4级
  ,level4_businesstype_name       string        --业态4级
  ,contract_qty                   string        --合同支数
  ,material_weight                string        --物料耗用重量(kg)
  ,recycle_qty                    string        --回收数量(支)
  ,recycle_weight                 string        --回收重量(kg)
  ,recycle_amt                    string        --回收金额
  ,batch_num                      string        --批次数
  ,recycle_days                   string        --回收天数
  ,drugs_cost                     string        --兽药成本总额
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_RECYCLE_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_RECYCLE_DD_1 PARTITION(op_day='$OP_DAY')
SELECT t1.year_id                                 --期间(年)
       ,t1.month_id                               --期间(月份)    
       ,t1.day_id                                 --期间(日)
       ,t1.production_line_id                     --产线代码
       ,t1.production_line_descr                  --产线描述
       ,t1.recycle_type_id                        --回收类型
       ,t1.recycle_type_descr                     --回收类型
       ,t1.breed_type_id                          --养殖类型
       ,t1.breed_type_descr                       --养殖类型
       ,t1.level1_org_id                          --组织1级
       ,t1.level1_org_descr                       --组织1级
       ,t1.level2_org_id                          --组织2级
       ,t1.level2_org_descr                       --组织2级
       ,t1.level3_org_id                          --组织3级
       ,t1.level3_org_descr                       --组织3级
       ,t1.level4_org_id                          --组织4级
       ,t1.level4_org_descr                       --组织4级
       ,t1.level5_org_id                          --组织5级
       ,t1.level5_org_descr                       --组织5级
       ,t1.level6_org_id                          --组织6级
       ,t1.level6_org_descr                       --组织6级
       ,t1.level7_org_id                          --组织7级
       ,t1.level7_org_descr                       --组织7级
       ,t1.level1_businesstype_id                 --业态1级
       ,t1.level1_businesstype_name               --业态1级
       ,t1.level2_businesstype_id                 --业态2级
       ,t1.level2_businesstype_name               --业态2级
       ,t1.level3_businesstype_id                 --业态3级
       ,t1.level3_businesstype_name               --业态3级
       ,t1.level4_businesstype_id                 --业态4级
       ,t1.level4_businesstype_name               --业态4级
       ,t1.contract_qty                           --合同支数
       ,t1.material_weight                        --物料耗用重量(kg)
       ,t1.recycle_qty                            --回收数量(支)
       ,t1.recycle_weight                         --回收重量(kg)
       ,t1.recycle_amt                            --回收金额
       ,t1.batch_num                              --批次数
       ,t1.recycle_days                           --回收天数
       ,t1.drugs_cost                             --兽药成本总额
  FROM (SELECT substr(recycle_date,1,4) year_id          --期间(年)
               ,substr(recycle_date,1,6) month_id        --期间(月份)    
               ,substr(recycle_date,1,8) day_id          --期间(日)
               ,production_line_id                     --产线代码
               ,production_line_descr                  --产线描述
               ,recycle_type_id                        --回收类型
               ,recycle_type_descr                     --回收类型
               ,breed_type_id                          --养殖类型
               ,breed_type_descr                       --养殖类型
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
               ,sum(coalesce(contract_qty,'0')) contract_qty                --合同支数
               ,sum(coalesce(material_weight_qty,'0')) material_weight      --物料耗用重量(kg)
               ,sum(coalesce(recycle_qty,'0')) recycle_qty                   --回收数量(支)
               ,sum(coalesce(recycle_weight,'0')) recycle_weight             --回收重量(kg)
               ,sum(coalesce(recycle_amt,'0')) recycle_amt     --回收金额
               ,count(distinct contract_no) batch_num          --批次数
               ,sum(datediff(concat(substr(recycle_date,1,4),'-',substr(recycle_date,5,2),'-',substr(recycle_date,7,2)),contract_date)) recycle_days --回收天数
               ,sum(coalesce(drugs_cost,'0')) drugs_cost                    --兽药成本总额
          FROM dwp_bird_finished_dd
         WHERE op_day='$OP_DAY'
           AND coalesce(contract_qty,0)>0
         GROUP BY production_line_id                     --产线代码
               ,production_line_descr                  --产线描述
               ,recycle_type_id                        --回收类型
               ,recycle_type_descr                     --回收类型
               ,breed_type_id                          --养殖类型
               ,breed_type_descr                       --养殖类型
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
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_RECYCLE_DD_2='TMP_DMP_BIRD_RECYCLE_DD_2'

CREATE_TMP_DMP_BIRD_RECYCLE_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_RECYCLE_DD_2(
  month_id                       string       --期间(月份)
  ,day_id                        string       --期间(日)
  ,level1_org_id                 string       --组织1级
  ,level1_org_descr              string       --组织1级
  ,level2_org_id                 string       --组织2级
  ,level2_org_descr              string       --组织2级
  ,level3_org_id                 string       --组织3级
  ,level3_org_descr              string       --组织3级
  ,level4_org_id                 string       --组织4级
  ,level4_org_descr              string       --组织4级
  ,level5_org_id                 string       --组织5级
  ,level5_org_descr              string       --组织5级
  ,level6_org_id                 string       --组织6级
  ,level6_org_descr              string       --组织6级
  ,level7_org_id                 string       --组织7级
  ,level7_org_descr              string       --组织7级
  ,level1_businesstype_id        string       --业态1级
  ,level1_businesstype_name      string       --业态1级
  ,level2_businesstype_id        string       --业态2级
  ,level2_businesstype_name      string       --业态2级
  ,level3_businesstype_id        string       --业态3级
  ,level3_businesstype_name      string       --业态3级
  ,level4_businesstype_id        string       --业态4级
  ,level4_businesstype_name      string       --业态4级
  ,production_line_id            string       --产线代码
  ,production_line_descr         string       --产线描述
  ,breed_type_id                 string       --养殖类型
  ,breed_type_descr              string       --养殖类型
  ,recycle_type_id               string       --回收类型
  ,recycle_type_descr            string       --回收类型       
  ,day_recyle_qty                string       --本日回收量(只)
  ,day_recyle_cost               string       --本日回收成本(元)
  ,day_material_weight           string       --本日提料量(kg)
  ,day_recyle_weight             string       --本日回收重量(kg)
  ,day_batch_num                 string       --本日回收批次数
  ,day_recyle_days               string       --本日回收天数(天)
  ,mon_recyle_qty                string       --本月回收量(只)
  ,mon_contract_qty              string       --本月合同量(只)
  ,mon_recyle_cost               string       --本月回收成本(元)
  ,mon_material_weight           string       --本月提料量(kg)
  ,mon_recyle_weight             string       --本月回收重量(kg)
  ,mon_batch_num                 string       --本月回收批次数
  ,mon_recyle_days               string       --本月回收天数(天)
  ,year_recyle_qty               string       --本年回收量(只)
  ,year_contract_qty             string       --本年合同量(只)
  ,year_recyle_cost              string       --本年回收成本(元)
  ,year_material_weight          string       --本年提料量(kg)
  ,year_recyle_weight            string       --本年回收重量(kg)
  ,year_batch_num                string       --本年回收批次数
  ,year_recyle_days              string       --本年回收天数(天)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_RECYCLE_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_RECYCLE_DD_2 PARTITION(op_day='$OP_DAY')
SELECT substr(t.day_id,1,6) month_id           --期间(月份)
       ,t.day_id                               --期间(日)
       ,t.level1_org_id                        --组织1级
       ,t.level1_org_descr                     --组织1级
       ,t.level2_org_id                        --组织2级
       ,t.level2_org_descr                     --组织2级
       ,t.level3_org_id                        --组织3级
       ,t.level3_org_descr                     --组织3级
       ,t.level4_org_id                        --组织4级
       ,t.level4_org_descr                     --组织4级
       ,t.level5_org_id                        --组织5级
       ,t.level5_org_descr                     --组织5级
       ,t.level6_org_id                        --组织6级
       ,t.level6_org_descr                     --组织6级
       ,t.level7_org_id                        --组织7级
       ,t.level7_org_descr                     --组织7级
       ,t.level1_businesstype_id               --业态1级
       ,t.level1_businesstype_name             --业态1级
       ,t.level2_businesstype_id               --业态2级
       ,t.level2_businesstype_name             --业态2级
       ,t.level3_businesstype_id               --业态3级
       ,t.level3_businesstype_name             --业态3级
       ,t.level4_businesstype_id               --业态4级
       ,t.level4_businesstype_name             --业态4级
       ,t.production_line_id                   --产线代码
       ,t.production_line_descr                --产线描述
       ,t.breed_type_id                        --养殖类型
       ,t.breed_type_descr                     --养殖类型
       ,t.recycle_type_id                      --回收类型
       ,t.recycle_type_descr                   --回收类型       
       ,sum(coalesce(t.day_recyle_qty,'0')) day_recyle_qty               --本日回收量(只)
       ,sum(coalesce(t.day_recyle_cost,'0')) day_recyle_cost             --本日回收成本(元)
       ,sum(coalesce(t.day_material_weight,'0')) day_material_weight     --本日提料量(kg)
       ,sum(coalesce(t.day_recyle_weight,'0')) day_recyle_weight         --本日回收重量(kg)
       ,sum(coalesce(t.day_batch_num,'0')) day_batch_num                 --本日回收批次数
       ,sum(coalesce(t.day_recyle_days,'0')) day_recyle_days             --本日回收天数(天)
       ,sum(coalesce(t.mon_recyle_qty,'0')) mon_recyle_qty               --本月回收量(只)
       ,sum(coalesce(t.mon_contract_qty,'0')) mon_recyle_qty             --本月合同量(只)
       ,sum(coalesce(t.mon_recyle_cost,'0')) mon_recyle_cost             --本月回收成本(元)
       ,sum(coalesce(t.mon_material_weight,'0')) mon_material_weight     --本月提料量(kg)
       ,sum(coalesce(t.mon_recyle_weight,'0')) mon_recyle_weight         --本月回收重量(kg)
       ,sum(coalesce(t.mon_batch_num,'0')) mon_batch_num                 --本月回收批次数
       ,sum(coalesce(t.mon_recyle_days,'0')) mon_recyle_days             --本月回收天数(天)
       ,sum(coalesce(t.year_recyle_qty,'0')) year_recyle_qty             --本年回收量(只)
       ,sum(coalesce(t.year_contract_qty,'0')) year_contract_qty         --本年合同量(只)
       ,sum(coalesce(t.year_recyle_cost,'0')) year_recyle_cost           --本年回收成本(元)
       ,sum(coalesce(t.year_material_weight,'0')) year_material_weight   --本年提料量(kg)
       ,sum(coalesce(t.year_recyle_weight,'0')) year_recyle_weight       --本年回收重量(kg)
       ,sum(coalesce(t.year_batch_num,'0')) year_batch_num               --本年回收批次数
       ,sum(coalesce(t.year_recyle_days,'0')) year_recyle_days           --本年回收天数(天)
  FROM (SELECT day_id                                     --期间(日)
               ,level1_org_id                             --组织1级
               ,level1_org_descr                          --组织1级
               ,level2_org_id                             --组织2级
               ,level2_org_descr                          --组织2级
               ,level3_org_id                             --组织3级
               ,level3_org_descr                          --组织3级
               ,level4_org_id                             --组织4级
               ,level4_org_descr                          --组织4级
               ,level5_org_id                             --组织5级
               ,level5_org_descr                          --组织5级
               ,level6_org_id                             --组织6级
               ,level6_org_descr                          --组织6级
               ,level7_org_id                             --组织7级
               ,level7_org_descr                          --组织7级
               ,level1_businesstype_id                    --业态1级
               ,level1_businesstype_name                  --业态1级
               ,level2_businesstype_id                    --业态2级
               ,level2_businesstype_name                  --业态2级
               ,level3_businesstype_id                    --业态3级
               ,level3_businesstype_name                  --业态3级
               ,level4_businesstype_id                    --业态4级
               ,level4_businesstype_name                  --业态4级
               ,production_line_id                        --产线代码
               ,production_line_descr                     --产线描述
               ,breed_type_id                             --养殖类型
               ,breed_type_descr                          --养殖类型
               ,recycle_type_id                           --回收类型
               ,recycle_type_descr                        --回收类型

               ,recycle_qty day_recyle_qty                --本日回收量(只)
               ,(recycle_amt+drugs_cost) day_recyle_cost  --本日回收成本(元)
               ,material_weight day_material_weight       --本日提料量(kg)
               ,recycle_weight day_recyle_weight          --本日回收重量(kg)
               ,batch_num day_batch_num                   --本日回收批次数
               ,recycle_days day_recyle_days              --本日回收天数(天)

               ,'0' mon_recyle_qty                        --本月回收量(只)
               ,'0' mon_contract_qty                      --本月合同量(只)
               ,'0' mon_recyle_cost                       --本月回收成本(元)
               ,'0' mon_material_weight                   --本月提料量(kg)
               ,'0' mon_recyle_weight                     --本月回收重量(kg)
               ,'0' mon_batch_num                         --本月回收批次数
               ,'0' mon_recyle_days                       --本月回收天数(天)

               ,'0' year_recyle_qty                       --本年回收量(只)
               ,'0' year_contract_qty                     --本年合同量(只)
               ,'0' year_recyle_cost                      --本年回收成本(元)
               ,'0' year_material_weight                  --本年提料量(kg)
               ,'0' year_recyle_weight                    --本年回收重量(kg)
               ,'0' year_batch_num                        --本年回收批次数
               ,'0' year_recyle_days                      --本年回收天数(天)
          FROM $TMP_DMP_BIRD_RECYCLE_DD_1
         WHERE op_day='$OP_DAY'
         UNION ALL                      --月累计取数
         SELECT a1.day_id                                     --期间(日)
                 ,a2.level1_org_id                             --组织1级
                 ,a2.level1_org_descr                          --组织1级
                 ,a2.level2_org_id                             --组织2级
                 ,a2.level2_org_descr                          --组织2级
                 ,a2.level3_org_id                             --组织3级
                 ,a2.level3_org_descr                          --组织3级
                 ,a2.level4_org_id                             --组织4级
                 ,a2.level4_org_descr                          --组织4级
                 ,a2.level5_org_id                             --组织5级
                 ,a2.level5_org_descr                          --组织5级
                 ,a2.level6_org_id                             --组织6级
                 ,a2.level6_org_descr                          --组织6级
                 ,a2.level7_org_id                             --组织7级
                 ,a2.level7_org_descr                          --组织7级
                 ,a2.level1_businesstype_id                    --业态1级
                 ,a2.level1_businesstype_name                  --业态1级
                 ,a2.level2_businesstype_id                    --业态2级
                 ,a2.level2_businesstype_name                  --业态2级
                 ,a2.level3_businesstype_id                    --业态3级
                 ,a2.level3_businesstype_name                  --业态3级
                 ,a2.level4_businesstype_id                    --业态4级
                 ,a2.level4_businesstype_name                  --业态4级
                 ,a2.production_line_id                        --产线代码
                 ,a2.production_line_descr                     --产线描述
                 ,a2.breed_type_id                             --养殖类型
                 ,a2.breed_type_descr                          --养殖类型
                 ,a2.recycle_type_id                           --回收类型
                 ,a2.recycle_type_descr                        --回收类型

                 ,'0' day_recyle_qty                      --本日回收量(只)
                 ,'0' day_recyle_cost                     --本日回收成本(元)
                 ,'0' day_material_weight                 --本日提料量(kg)
                 ,'0' day_recyle_weight                   --本日回收重量(kg)
                 ,'0' day_batch_num                       --本日回收批次数
                 ,'0' day_recyle_days                     --本日回收天数(天)

                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.recycle_qty else '0' end mon_recyle_qty                  --本月回收量(只)
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.contract_qty else '0' end mon_contract_qty                 --本月回收量(只)
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then (a2.recycle_amt+a2.drugs_cost) else '0' end mon_recyle_cost --本月回收成本(元)
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.material_weight else '0' end mon_material_weight         --本月提料量(kg)
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.recycle_weight else '0' end mon_recyle_weight            --本月回收重量(kg)
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.batch_num else '0' end mon_batch_num                     --本月回收批次数
                 ,case when a1.month_id=a2.month_id and a1.day_id>=a2.day_id
                       then a2.recycle_days else '0' end mon_recyle_days                --本月回收天数(天)

                 ,'0' year_recyle_qty                       --本年回收量(只)
                 ,'0' year_contract_qty                     --本年合同量(只)
                 ,'0' year_recyle_cost                      --本年回收成本(元)
                 ,'0' year_material_weight                  --本年提料量(kg)
                 ,'0' year_recyle_weight                    --本年回收重量(kg)
                 ,'0' year_batch_num                        --本年回收批次数
                 ,'0' year_recyle_days                      --本年回收天数(天)
            FROM (SELECT year_id
                         ,month_id
                         ,day_id
                    FROM $TMP_DMP_BIRD_RECYCLE_DD_1
                   WHERE op_day='$OP_DAY'
                   GROUP BY year_id,month_id,day_id) a1
            LEFT JOIN (SELECT month_id                             --期间(月份)
                              ,day_id                               --期间(日)
                              ,production_line_id                   --产线代码
                              ,production_line_descr                --产线描述
                              ,recycle_type_id                      --回收类型
                              ,recycle_type_descr                   --回收类型
                              ,breed_type_id                        --养殖类型
                              ,breed_type_descr                     --养殖类型
                              ,level1_org_id                        --组织1级
                              ,level1_org_descr                     --组织1级
                              ,level2_org_id                        --组织2级
                              ,level2_org_descr                     --组织2级
                              ,level3_org_id                        --组织3级
                              ,level3_org_descr                     --组织3级
                              ,level4_org_id                        --组织4级
                              ,level4_org_descr                     --组织4级
                              ,level5_org_id                        --组织5级
                              ,level5_org_descr                     --组织5级
                              ,level6_org_id                        --组织6级
                              ,level6_org_descr                     --组织6级
                              ,level7_org_id                        --组织7级
                              ,level7_org_descr                     --组织7级
                              ,level1_businesstype_id               --业态1级
                              ,level1_businesstype_name             --业态1级
                              ,level2_businesstype_id               --业态2级
                              ,level2_businesstype_name             --业态2级
                              ,level3_businesstype_id               --业态3级
                              ,level3_businesstype_name             --业态3级
                              ,level4_businesstype_id               --业态4级
                              ,level4_businesstype_name             --业态4级
                              ,contract_qty                         --合同支数
                              ,material_weight                      --物料耗用重量(kg)
                              ,recycle_qty                          --回收数量(支)
                              ,recycle_weight                       --回收重量(kg)
                              ,recycle_amt                          --回收金额
                              ,batch_num                            --批次数
                              ,recycle_days                         --回收天数
                              ,drugs_cost                           --兽药成本总额
                        FROM $TMP_DMP_BIRD_RECYCLE_DD_1
                       WHERE op_day='$OP_DAY') a2
              ON (a1.month_id=a2.month_id)
         UNION ALL                      --年累计取数
         SELECT a1.day_id                               --期间(日)
                 ,a2.level1_org_id                             --组织1级
                 ,a2.level1_org_descr                          --组织1级
                 ,a2.level2_org_id                             --组织2级
                 ,a2.level2_org_descr                          --组织2级
                 ,a2.level3_org_id                             --组织3级
                 ,a2.level3_org_descr                          --组织3级
                 ,a2.level4_org_id                             --组织4级
                 ,a2.level4_org_descr                          --组织4级
                 ,a2.level5_org_id                             --组织5级
                 ,a2.level5_org_descr                          --组织5级
                 ,a2.level6_org_id                             --组织6级
                 ,a2.level6_org_descr                          --组织6级
                 ,a2.level7_org_id                             --组织7级
                 ,a2.level7_org_descr                          --组织7级
                 ,a2.level1_businesstype_id                    --业态1级
                 ,a2.level1_businesstype_name                  --业态1级
                 ,a2.level2_businesstype_id                    --业态2级
                 ,a2.level2_businesstype_name                  --业态2级
                 ,a2.level3_businesstype_id                    --业态3级
                 ,a2.level3_businesstype_name                  --业态3级
                 ,a2.level4_businesstype_id                    --业态4级
                 ,a2.level4_businesstype_name                  --业态4级
                 ,a2.production_line_id                        --产线代码
                 ,a2.production_line_descr                     --产线描述
                 ,a2.breed_type_id                             --养殖类型
                 ,a2.breed_type_descr                          --养殖类型
                 ,a2.recycle_type_id                           --回收类型
                 ,a2.recycle_type_descr                        --回收类型

                 ,'0' day_recyle_qty                      --本日回收量(只)
                 ,'0' day_recyle_cost                     --本日回收成本(元)
                 ,'0' day_material_weight                 --本日提料量(kg)
                 ,'0' day_recyle_weight                   --本日回收重量(kg)
                 ,'0' day_batch_num                       --本日回收批次数
                 ,'0' day_recyle_days                     --本日回收天数(天)

                 ,'0' mon_recyle_qty                      --本月回收量(只)
                 ,'0' mon_contract_qty                      --本月合同量(只)
                 ,'0' mon_recyle_cost                     --本月回收成本(元)
                 ,'0' mon_material_weight                 --本月提料量(kg)
                 ,'0' mon_recyle_weight                   --本月回收重量(kg)
                 ,'0' mon_batch_num                       --本月回收批次数
                 ,'0' mon_recyle_days                     --本月回收天数(天)
 
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id 
                       then a2.recycle_qty else '0' end year_recyle_qty                  --本年回收量(只)
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id 
                       then a2.contract_qty else '0' end year_contract_qty               --本年合同量(只)
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id 
                       then (a2.recycle_amt+a2.drugs_cost) else '0' end year_recyle_cost --本年回收成本(元)
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id
                       then a2.material_weight else '0' end year_material_weight         --本年提料量(kg)
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id
                       then a2.recycle_weight else '0' end year_recyle_weight            --本年回收重量(kg)
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id
                       then a2.batch_num else '0' end year_batch_num                     --本年回收批次数
                 ,case when a1.year_id=a2.year_id and a1.day_id>=a2.day_id
                       then a2.recycle_days else '0' end year_recyle_days                --本年回收天数(天)
            FROM (SELECT year_id
                         ,month_id
                         ,day_id
                    FROM $TMP_DMP_BIRD_RECYCLE_DD_1
                   WHERE op_day='$OP_DAY'
                   GROUP BY year_id,month_id,day_id) a1
            LEFT JOIN (SELECT year_id                               --期间(年)
                              ,day_id                               --期间(日)
                              ,production_line_id                   --产线代码
                              ,production_line_descr                --产线描述
                              ,recycle_type_id                      --回收类型
                              ,recycle_type_descr                   --回收类型
                              ,breed_type_id                        --养殖类型
                              ,breed_type_descr                     --养殖类型
                              ,level1_org_id                        --组织1级
                              ,level1_org_descr                     --组织1级
                              ,level2_org_id                        --组织2级
                              ,level2_org_descr                     --组织2级
                              ,level3_org_id                        --组织3级
                              ,level3_org_descr                     --组织3级
                              ,level4_org_id                        --组织4级
                              ,level4_org_descr                     --组织4级
                              ,level5_org_id                        --组织5级
                              ,level5_org_descr                     --组织5级
                              ,level6_org_id                        --组织6级
                              ,level6_org_descr                     --组织6级
                              ,level7_org_id                        --组织7级
                              ,level7_org_descr                     --组织7级
                              ,level1_businesstype_id               --业态1级
                              ,level1_businesstype_name             --业态1级
                              ,level2_businesstype_id               --业态2级
                              ,level2_businesstype_name             --业态2级
                              ,level3_businesstype_id               --业态3级
                              ,level3_businesstype_name             --业态3级
                              ,level4_businesstype_id               --业态4级
                              ,level4_businesstype_name             --业态4级
                              ,contract_qty                         --合同支数
                              ,material_weight                      --物料耗用重量(kg)
                              ,recycle_qty                          --回收数量(支)
                              ,recycle_weight                       --回收重量(kg)
                              ,recycle_amt                          --回收金额
                              ,batch_num                            --批次数
                              ,recycle_days                         --回收天数
                              ,drugs_cost                           --兽药成本总额
                        FROM $TMP_DMP_BIRD_RECYCLE_DD_1
                       WHERE op_day='$OP_DAY') a2
              ON (a1.year_id=a2.year_id)) t
 GROUP BY t.day_id                               --期间(日)
       ,t.level1_org_id                        --组织1级
       ,t.level1_org_descr                     --组织1级
       ,t.level2_org_id                        --组织2级
       ,t.level2_org_descr                     --组织2级
       ,t.level3_org_id                        --组织3级
       ,t.level3_org_descr                     --组织3级
       ,t.level4_org_id                        --组织4级
       ,t.level4_org_descr                     --组织4级
       ,t.level5_org_id                        --组织5级
       ,t.level5_org_descr                     --组织5级
       ,t.level6_org_id                        --组织6级
       ,t.level6_org_descr                     --组织6级
       ,t.level7_org_id                        --组织7级
       ,t.level7_org_descr                     --组织7级
       ,t.level1_businesstype_id               --业态1级
       ,t.level1_businesstype_name             --业态1级
       ,t.level2_businesstype_id               --业态2级
       ,t.level2_businesstype_name             --业态2级
       ,t.level3_businesstype_id               --业态3级
       ,t.level3_businesstype_name             --业态3级
       ,t.level4_businesstype_id               --业态4级
       ,t.level4_businesstype_name             --业态4级
       ,t.production_line_id                   --产线代码
       ,t.production_line_descr                --产线描述
       ,t.breed_type_id                        --养殖类型
       ,t.breed_type_descr                     --养殖类型
       ,t.recycle_type_id                      --回收类型
       ,t.recycle_type_descr                   --回收类型
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_RECYCLE_DD='DMP_BIRD_RECYCLE_DD'

CREATE_DMP_BIRD_RECYCLE_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_RECYCLE_DD(
  month_id                       string       --期间(月份)
  ,day_id                        string       --期间(日)
  ,level1_org_id                 string       --组织1级
  ,level1_org_descr              string       --组织1级
  ,level2_org_id                 string       --组织2级
  ,level2_org_descr              string       --组织2级
  ,level3_org_id                 string       --组织3级
  ,level3_org_descr              string       --组织3级
  ,level4_org_id                 string       --组织4级
  ,level4_org_descr              string       --组织4级
  ,level5_org_id                 string       --组织5级
  ,level5_org_descr              string       --组织5级
  ,level6_org_id                 string       --组织6级
  ,level6_org_descr              string       --组织6级
  ,level7_org_id                 string       --组织7级
  ,level7_org_descr              string       --组织7级
  ,level1_businesstype_id        string       --业态1级
  ,level1_businesstype_name      string       --业态1级
  ,level2_businesstype_id        string       --业态2级
  ,level2_businesstype_name      string       --业态2级
  ,level3_businesstype_id        string       --业态3级
  ,level3_businesstype_name      string       --业态3级
  ,level4_businesstype_id        string       --业态4级
  ,level4_businesstype_name      string       --业态4级
  ,production_line_id            string       --产线代码
  ,production_line_descr         string       --产线描述
  ,breed_type_id                 string       --养殖类型
  ,breed_type_descr              string       --养殖类型
  ,recyle_type_id                string       --回收类型
  ,recyle_type_descr             string       --回收类型       
  ,day_recyle_qty                string       --本日回收量(只)
  ,day_recyle_cost               string       --本日回收成本(元)
  ,day_recyle_base_cost          string       --本日保本价-未加权
  ,day_material_weight           string       --本日提料量(kg)
  ,day_recyle_weight             string       --本日回收重量(kg)
  ,day_batch_num                 string       --本日回收批次数
  ,day_recyle_days               string       --本日回收天数(天)
  ,mon_recyle_qty                string       --本月回收量(只)
  ,mon_contract_qty              string       --本月合同量(只)
  ,mon_recyle_cost               string       --本月回收成本(元)
  ,mon_recyle_base_cost          string       --本月保本价-未加权
  ,mon_material_weight           string       --本月提料量(kg)
  ,mon_recyle_weight             string       --本月回收重量(kg)
  ,mon_batch_num                 string       --本月回收批次数
  ,mon_recyle_days               string       --本月回收天数(天)
  ,year_recyle_qty               string       --本年回收量(只)
  ,year_contract_qty             string       --本年合同量(只)
  ,year_recyle_cost              string       --本年回收成本(元)
  ,year_material_weight          string       --本年提料量(kg)
  ,year_recyle_weight            string       --本年回收重量(kg)
  ,year_batch_num                string       --本年回收批次数
  ,year_recyle_days              string       --本年回收天数(天)
  ,create_time                   string       --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_RECYCLE_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_RECYCLE_DD PARTITION(op_day='$OP_DAY')
SELECT t1.month_id                               --期间(月份)
       ,t1.day_id                                --期间(日)
       ,t1.level1_org_id                         --组织1级
       ,t1.level1_org_descr                      --组织1级
       ,t1.level2_org_id                         --组织2级
       ,t1.level2_org_descr                      --组织2级
       ,t1.level3_org_id                         --组织3级
       ,t1.level3_org_descr                      --组织3级
       ,t1.level4_org_id                         --组织4级
       ,t1.level4_org_descr                      --组织4级
       ,t1.level5_org_id                         --组织5级
       ,t1.level5_org_descr                      --组织5级
       ,t1.level6_org_id                         --组织6级
       ,t1.level6_org_descr                      --组织6级
       ,t1.level7_org_id                         --组织7级
       ,t1.level7_org_descr                      --组织7级
       ,t1.level1_businesstype_id                --业态1级
       ,t1.level1_businesstype_name              --业态1级
       ,t1.level2_businesstype_id                --业态2级
       ,t1.level2_businesstype_name              --业态2级
       ,t1.level3_businesstype_id                --业态3级
       ,t1.level3_businesstype_name              --业态3级
       ,t1.level4_businesstype_id                --业态4级
       ,t1.level4_businesstype_name              --业态4级
       ,t1.production_line_id                    --产线代码
       ,t1.production_line_descr                 --产线描述
       ,t1.breed_type_id                         --养殖类型
       ,t1.breed_type_descr                      --养殖类型
       ,t1.recycle_type_id                       --回收类型
       ,t1.recycle_type_descr                    --回收类型       
       ,t1.day_recyle_qty                        --本日回收量(只)
       ,t1.day_recyle_cost                       --本日回收成本(元)
       ,null day_recyle_base_cost                --本日保本价-未加权(表dmp_bird_keep_price_dm提供)
       ,t1.day_material_weight                   --本日提料量(kg)
       ,t1.day_recyle_weight                     --本日回收重量(kg)
       ,t1.day_batch_num                         --本日回收批次数
       ,t1.day_recyle_days                       --本日回收天数(天)
       ,t1.mon_recyle_qty                        --本月回收量(只)
       ,t1.mon_contract_qty                      --本月合同量(只)
       ,t1.mon_recyle_cost                       --本月回收成本(元)
       ,null mon_recyle_base_cost                --本月保本价
       ,t1.mon_material_weight                   --本月提料量(kg)
       ,t1.mon_recyle_weight                     --本月回收重量(kg)
       ,t1.mon_batch_num                         --本月回收批次数
       ,t1.mon_recyle_days                       --本月回收天数(天)
       ,t1.year_recyle_qty                       --本年回收量(只)
       ,t1.year_contract_qty                     --本年合同量(只)
       ,t1.year_recyle_cost                      --本年回收成本(元)
       ,t1.year_material_weight                  --本年提料量(kg)
       ,t1.year_recyle_weight                    --本年回收重量(kg)
       ,t1.year_batch_num                        --本年回收批次数
       ,t1.year_recyle_days                      --本年回收天数(天)
       ,'$CREATE_TIME' create_time            --创建时间
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_RECYCLE_DD_2
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_RECYCLE_DD_1;
    $INSERT_TMP_DMP_BIRD_RECYCLE_DD_1;
    $CREATE_TMP_DMP_BIRD_RECYCLE_DD_2;
    $INSERT_TMP_DMP_BIRD_RECYCLE_DD_2;
    $CREATE_DMP_BIRD_RECYCLE_DD;
    $INSERT_DMP_BIRD_RECYCLE_DD;
"  -v

