#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_unit_profits_mm.sh                               
# 创建时间: 2018年04月18日                                            
# 创 建 者: ch                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽产业单只利润
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_unit_profits_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_UNIT_PROFITS_MM='DMF_BIRD_UNIT_PROFITS_MM'

CREATE_DMF_BIRD_UNIT_PROFITS_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_UNIT_PROFITS_MM
(
 MONTH_ID                      STRING    --期间(月)
,DAY_ID                        STRING    --期间(日)
,LEVEL1_ORG_ID                 STRING    --组织1级(股份)
,LEVEL1_ORG_DESCR              STRING    --组织1级(股份)
,LEVEL2_ORG_ID                 STRING    --组织2级(片联)
,LEVEL2_ORG_DESCR              STRING    --组织2级(片联)
,LEVEL3_ORG_ID                 STRING    --组织3级(片区)
,LEVEL3_ORG_DESCR              STRING    --组织3级(片区)
,LEVEL4_ORG_ID                 STRING    --组织4级(小片)
,LEVEL4_ORG_DESCR              STRING    --组织4级(小片)
,LEVEL5_ORG_ID                 STRING    --组织5级(公司)
,LEVEL5_ORG_DESCR              STRING    --组织5级(公司)
,LEVEL6_ORG_ID                 STRING    --组织6级(OU)
,LEVEL6_ORG_DESCR              STRING    --组织6级(OU)
,LEVEL7_ORG_ID                 STRING    --组织6级(OU)
,LEVEL7_ORG_DESCR              STRING    --组织6级(OU)
,LEVEL1_BUSINESSTYPE_ID        STRING    --业态1级
,LEVEL1_BUSINESSTYPE_NAME      STRING    --业态1级
,LEVEL2_BUSINESSTYPE_ID        STRING    --业态2级
,LEVEL2_BUSINESSTYPE_NAME      STRING    --业态2级
,LEVEL3_BUSINESSTYPE_ID        STRING    --业态3级
,LEVEL3_BUSINESSTYPE_NAME      STRING    --业态3级
,LEVEL4_BUSINESSTYPE_ID        STRING    --业态4级
,LEVEL4_BUSINESSTYPE_NAME      STRING    --业态4级
,PRODUCTION_LINE_ID            STRING    --产线id
,PRODUCTION_LINE_DESCR         STRING    --产线
,CONTRACT_KILLED_QTY           STRING    --冷藏合同宰杀量(只)
,MKT_KILLED_QTY                STRING    --冷藏市场宰杀量(只)
,ZQ_PROFITS_AMT                STRING    --种禽利润总额
,QW_PROFITS_AMT                STRING    --禽旺利润总额
,MATERIAL_PROFITS_AMT          STRING    --饲料利润总额
,COLD_PROFITS_AMT              STRING    --冷藏利润总额
,FARMER_PROFITS_AMT            STRING    --养殖户利润总额
,CREATE_TIME                   STRING
)
PARTITIONED BY (OP_MONTH string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_UNIT_PROFITS_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_UNIT_PROFITS_MM PARTITION(OP_MONTH='$OP_MONTH')
SELECT MONTH_ID, --期间(月)
       '' DAY_ID, --期间(日)
       LEVEL1_ORG_ID, --组织1级(股份)
       LEVEL1_ORG_DESCR, --组织1级(股份)
       LEVEL2_ORG_ID, --组织2级(片联)
       LEVEL2_ORG_DESCR, --组织2级(片联)
       LEVEL3_ORG_ID, --组织3级(片区)
       LEVEL3_ORG_DESCR, --组织3级(片区)
       LEVEL4_ORG_ID, --组织4级(小片)
       LEVEL4_ORG_DESCR, --组织4级(小片)
       LEVEL5_ORG_ID, --组织5级(公司)
       LEVEL5_ORG_DESCR, --组织5级(公司)
       LEVEL6_ORG_ID, --组织6级(OU)
       LEVEL6_ORG_DESCR, --组织6级(OU)
       '' level7_org_id, --组织7级(库存组织)
       '' level7_org_descr, --组织7级(库存组织)
       LEVEL1_BUSINESSTYPE_ID, --业态1级
       LEVEL1_BUSINESSTYPE_NAME, --业态1级
       LEVEL2_BUSINESSTYPE_ID, --业态2级
       LEVEL2_BUSINESSTYPE_NAME, --业态2级
       LEVEL3_BUSINESSTYPE_ID, --业态3级
       LEVEL3_BUSINESSTYPE_NAME, --业态3级
       LEVEL4_BUSINESSTYPE_ID, --业态4级
       LEVEL4_BUSINESSTYPE_NAME, --业态4级
       PRODUCTION_LINE_ID, --产线id
       PRODUCTION_LINE_DESCR, --产线      
       SUM(coalesce(CONTRACT_KILLED_QTY,0)), --冷藏合同宰杀量(只)
       SUM(coalesce(MKT_KILLED_QTY,0)), --冷藏市场宰杀量(只)
       SUM(coalesce(ZQ_PROFITS_AMT,0)), --种禽利润总额
       SUM(coalesce(QW_PROFITS_AMT,0)), --禽旺利润总额
       SUM(coalesce(MATERIAL_PROFITS_AMT,0)), --饲料利润总额
       SUM(coalesce(COLD_PROFITS_AMT,0)), --冷藏利润总额
       SUM(coalesce(FARMER_PROFITS_AMT,0)), --养殖户利润总额
       '$CREATE_TIME' CREATE_TIME
  FROM mreport_poultry.DWF_BIRD_UNIT_PROFITS_DD
 WHERE OP_DAY = '$OP_DAY'
   AND level2_org_id NOT IN('1015')
 GROUP BY  MONTH_ID, --期间(月)
       DAY_ID, --期间(日)
       LEVEL1_ORG_ID, --组织1级(股份)
       LEVEL1_ORG_DESCR, --组织1级(股份)
       LEVEL2_ORG_ID, --组织2级(片联)
       LEVEL2_ORG_DESCR, --组织2级(片联)
       LEVEL3_ORG_ID, --组织3级(片区)
       LEVEL3_ORG_DESCR, --组织3级(片区)
       LEVEL4_ORG_ID, --组织4级(小片)
       LEVEL4_ORG_DESCR, --组织4级(小片)
       LEVEL5_ORG_ID, --组织5级(公司)
       LEVEL5_ORG_DESCR, --组织5级(公司)
       LEVEL6_ORG_ID, --组织6级(OU)
       LEVEL6_ORG_DESCR, --组织6级(OU)
       '', --组织7级(库存组织)
       '', --组织7级(库存组织)
       LEVEL1_BUSINESSTYPE_ID, --业态1级
       LEVEL1_BUSINESSTYPE_NAME, --业态1级
       LEVEL2_BUSINESSTYPE_ID, --业态2级
       LEVEL2_BUSINESSTYPE_NAME, --业态2级
       LEVEL3_BUSINESSTYPE_ID, --业态3级
       LEVEL3_BUSINESSTYPE_NAME, --业态3级
       LEVEL4_BUSINESSTYPE_ID, --业态4级
       LEVEL4_BUSINESSTYPE_NAME, --业态4级
       PRODUCTION_LINE_ID, --产线id
       PRODUCTION_LINE_DESCR --产线         
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    
    

    $CREATE_DMF_BIRD_UNIT_PROFITS_MM;
    $INSERT_DMF_BIRD_UNIT_PROFITS_MM;
"  -v

#     $CREATE_DMF_BIRD_UNIT_PROFITS_MM;
#     $CREATE_DMF_BIRD_UNIT_PROFITS_MM;