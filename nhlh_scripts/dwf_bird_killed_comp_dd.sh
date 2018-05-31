#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwf_bird_killed_comp_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰出成分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwf_bird_killed_comp_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_KILLED_COMP_DD_0='TMP_DWF_BIRD_KILLED_COMP_DD_0'

CREATE_TMP_DWF_BIRD_KILLED_COMP_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_KILLED_COMP_DD_0(
  contract_no                     string       --合同号
  ,level1_org_id                  string       --组织1级
  ,level1_org_descr               string       --组织1级
  ,level2_org_id                  string       --组织2级
  ,level2_org_descr               string       --组织2级
  ,level3_org_id                  string       --组织3级
  ,level3_org_descr               string       --组织3级
  ,level4_org_id                  string       --组织4级
  ,level4_org_descr               string       --组织4级
  ,level5_org_id                  string       --组织5级
  ,level5_org_descr               string       --组织5级
  ,level6_org_id                  string       --组织6级
  ,level6_org_descr               string       --组织6级
  ,level7_org_id                  string       --组织7级
  ,level7_org_descr               string       --组织7级
  ,level1_businesstype_id         string       --业态1级
  ,level1_businesstype_name       string       --业态1级
  ,level2_businesstype_id         string       --业态2级
  ,level2_businesstype_name       string       --业态2级
  ,level3_businesstype_id         string       --业态3级
  ,level3_businesstype_name       string       --业态3级
  ,level4_businesstype_id         string       --业态4级
  ,level4_businesstype_name       string       --业态4级
  ,production_line_id             string       --产线
  ,production_line_descr          string       --产线
  ,recycle_date                   string       --采购日期
  ,recycle_qty                    string       --回收数量
  ,recycle_weight                 string       --回收重量
  ,recycle_cost                   string       --回收成本
  ,carcass_weight                 string       --胴体重量(kg)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_KILLED_COMP_DD_0="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_KILLED_COMP_DD_0 PARTITION(op_day='$OP_DAY')
SELECT t1.contract_no                         --合同号
       ,t3.level1_org_id                      --组织1级
       ,t3.level1_org_descr                   --组织1级
       ,t3.level2_org_id                      --组织2级
       ,t3.level2_org_descr                   --组织2级
       ,t3.level3_org_id                      --组织3级
       ,t3.level3_org_descr                   --组织3级
       ,t3.level4_org_id                      --组织4级
       ,t3.level4_org_descr                   --组织4级
       ,t3.level5_org_id                      --组织5级
       ,t3.level5_org_descr                   --组织5级
       ,t3.level6_org_id                      --组织6级
       ,t3.level6_org_descr                   --组织6级
       ,t4.level7_org_id                      --组织7级
       ,t4.level7_org_descr                   --组织7级
       ,t5.level1_businesstype_id             --业态1级
       ,t5.level1_businesstype_name           --业态1级
       ,t5.level2_businesstype_id             --业态2级
       ,t5.level2_businesstype_name           --业态2级
       ,t5.level3_businesstype_id             --业态3级
       ,t5.level3_businesstype_name           --业态3级
       ,t5.level4_businesstype_id             --业态4级
       ,t5.level4_businesstype_name           --业态4级
       ,case when t2.material_id='3501000002' then '1'
             when t2.material_id='3502000002' then '2'
        else null end                         --production_line_id
       ,case when t2.material_id='3501000002' then '鸡线'
             when t2.material_id='3502000002' then '鸭线'
        else null end                         --production_line_descr
       ,t2.recycle_date                       --采购日期
       ,t2.recycle_qty                        --回收数量
       ,t2.recycle_weight                     --回收重量
       ,t2.recycle_cost                       --回收成本
       ,t6.carcass_weight                     --胴体重量(kg)
  FROM (SELECT contractnumber contract_no
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY'
         GROUP BY contractnumber) t1
 INNER JOIN (SELECT contract_no                                 --合同号
                    ,org_id                                     --采购组织
                    ,inv_org_id                                 --库存组织ID
                    ,bus_type                                   --业态
                    ,material_code material_id                  --物料编码
                    ,material_description material_descr        --物料描述
                    ,substr(transaction_date,1,6) recycle_date  --采购日期(月)
                    ,sum(secondary_qty) recycle_qty             --辅助数量               
                    ,sum(quantity_received) recycle_weight      --已接收数量               
                    ,sum(price_with_tax*quantity_received) recycle_cost  --回收成本
               FROM dwu_cg_buy_list_cg01_dd
              WHERE op_day='$OP_DAY'
                AND material_code in('3501000002','3502000002')
                AND release_num like 'BWP%'
                AND cancel_flag in('CLOSED','OPEN')
              GROUP BY contract_no                       --合同号
                    ,org_id                              --采购组织
                    ,inv_org_id                          --库存组织ID
                    ,bus_type                            --业态
                    ,material_code                       --物料编码
                    ,material_description                --物料描述
                    ,substr(transaction_date,1,6)) t2
    ON (t1.contract_no=t2.contract_no)
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
                    org_id) t3
    ON (t2.org_id=t3.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t4
    ON (t2.inv_org_id=t4.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t5
    ON (t2.bus_type=t5.level4_businesstype_id)
  LEFT JOIN (SELECT contract_id,
                    sum(coalesce(weight,'0')) carcass_weight   --胴体过磅重量
               FROM dwu_qw_weighfreight_dd
              WHERE op_day='$OP_DAY'
              GROUP BY contract_id) t6
    ON (t1.contract_no=t6.contract_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_KILLED_COMP_DD='DWF_BIRD_KILLED_COMP_DD'

CREATE_DWF_BIRD_KILLED_COMP_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_KILLED_COMP_DD(
  contract_no                     string       --合同号
  ,level1_org_id                  string       --组织1级
  ,level1_org_descr               string       --组织1级
  ,level2_org_id                  string       --组织2级
  ,level2_org_descr               string       --组织2级
  ,level3_org_id                  string       --组织3级
  ,level3_org_descr               string       --组织3级
  ,level4_org_id                  string       --组织4级
  ,level4_org_descr               string       --组织4级
  ,level5_org_id                  string       --组织5级
  ,level5_org_descr               string       --组织5级
  ,level6_org_id                  string       --组织6级
  ,level6_org_descr               string       --组织6级
  ,level7_org_id                  string       --组织7级
  ,level7_org_descr               string       --组织7级
  ,level1_businesstype_id         string       --业态1级
  ,level1_businesstype_name       string       --业态1级
  ,level2_businesstype_id         string       --业态2级
  ,level2_businesstype_name       string       --业态2级
  ,level3_businesstype_id         string       --业态3级
  ,level3_businesstype_name       string       --业态3级
  ,level4_businesstype_id         string       --业态4级
  ,level4_businesstype_name       string       --业态4级
  ,production_line_id             string       --产线
  ,production_line_descr          string       --产线
  ,recycle_date                   string       --采购日期
  ,recycle_qty                    string       --回收数量
  ,recycle_weight                 string       --回收重量
  ,recycle_cost                   string       --回收成本
  ,carcass_weight                 string       --胴体重量(kg)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_KILLED_COMP_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_KILLED_COMP_DD PARTITION(op_day='$OP_DAY')
SELECT contract_no                            --合同号
       ,level1_org_id                         --组织1级
       ,level1_org_descr                      --组织1级
       ,level2_org_id                         --组织2级
       ,level2_org_descr                      --组织2级
       ,level3_org_id                         --组织3级
       ,level3_org_descr                      --组织3级
       ,level4_org_id                         --组织4级
       ,level4_org_descr                      --组织4级
       ,level5_org_id                         --组织5级
       ,level5_org_descr                      --组织5级
       ,level6_org_id                         --组织6级
       ,level6_org_descr                      --组织6级
       ,level7_org_id                         --组织7级
       ,level7_org_descr                      --组织7级
       ,level1_businesstype_id                --业态1级
       ,level1_businesstype_name              --业态1级
       ,level2_businesstype_id                --业态2级
       ,level2_businesstype_name              --业态2级
       ,level3_businesstype_id                --业态3级
       ,level3_businesstype_name              --业态3级
       ,level4_businesstype_id                --业态4级
       ,level4_businesstype_name              --业态4级
       ,production_line_id                    --产线
       ,production_line_descr                 --产线
       ,recycle_date                          --采购日期
       ,recycle_qty                           --回收数量
       ,recycle_weight                        --回收重量
       ,recycle_cost                          --回收成本
       ,carcass_weight                        --胴体重量(kg)
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_KILLED_COMP_DD_0
         WHERE op_day='$OP_DAY'
           AND level3_org_id not IN ('101512','101510')
           AND level2_org_id NOT IN('1015')) t1

"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;

    $CREATE_TMP_DWF_BIRD_KILLED_COMP_DD_0;
    $INSERT_TMP_DWF_BIRD_KILLED_COMP_DD_0;
    $CREATE_DWF_BIRD_KILLED_COMP_DD;
    $INSERT_DWF_BIRD_KILLED_COMP_DD;
"  -v