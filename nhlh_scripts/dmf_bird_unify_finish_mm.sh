#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_unify_finish_mm.sh                               
# 创建时间: 2018年04月23日                                            
# 创 建 者: ch                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 一体化经营完成情况           
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_unify_finish_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


###########################################################################################
## 处理基础数据
## 变量声明
TMP_DMF_BIRD_UNIFY_FINISH_MM_0='TMP_DMF_BIRD_UNIFY_FINISH_MM_0'

CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_UNIFY_FINISH_MM_0
(
 YEAR_ID                       STRING    --年
,MONTH_ID                      STRING    --期间(月)
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
,LEVEL7_ORG_ID                 STRING    --组织7级(库存组织)
,LEVEL7_ORG_DESCR              STRING    --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID        STRING    --业态1级
,LEVEL1_BUSINESSTYPE_NAME      STRING    --业态1级
,LEVEL2_BUSINESSTYPE_ID        STRING    --业态2级
,LEVEL2_BUSINESSTYPE_NAME      STRING    --业态2级
,LEVEL3_BUSINESSTYPE_ID        STRING    --业态3级
,LEVEL3_BUSINESSTYPE_NAME      STRING    --业态3级
,LEVEL4_BUSINESSTYPE_ID        STRING    --业态4级
,LEVEL4_BUSINESSTYPE_NAME      STRING    --业态4级
,KPI_TYPE_ID                   STRING    --指标类型id
,KPI_TYPE_DESCR                STRING    --指标类型
,ZQ_CHICKEN_AMT                STRING    --种禽-鸡线金额(元)
,ZQ_DUCK_AMT                   STRING    --种禽-鸭线金额(元)
,YZ_CHICKEN_AMT                STRING    --养殖-鸡线金额(元)
,YZ_DUCK_AMT                   STRING    --养殖-鸭线金额(元)
,MTL_INNER_SALES_AMT           STRING    --饲料-内销金额(元)
,MTL_OUTTER_SALES_AMT          STRING    --饲料-外销金额(元)
,COLD_CHICKEN_AMT              STRING    --冷藏-鸡线金额(元)
,COLD_DUCK_AMT                 STRING    --冷藏-鸭线金额(元)
,FOOD_FACTORY_AMT              STRING    --食品深加工(元)
,CREATE_TIME                   STRING
)
PARTITIONED BY (OP_MONTH string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_0="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_UNIFY_FINISH_MM_0 PARTITION(OP_MONTH='$OP_MONTH')
SELECT SUBSTR(MONTH_ID, 1, 4) YEAR_ID,
               MONTH_ID, --期间(月)
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
               '' LEVEL7_ORG_ID, --组织7级(库存组织)
               '' LEVEL7_ORG_DESCR, --组织7级(库存组织)
               '' LEVEL1_BUSINESSTYPE_ID, --业态1级
               '' LEVEL1_BUSINESSTYPE_NAME, --业态1级
               '' LEVEL2_BUSINESSTYPE_ID, --业态2级
               '' LEVEL2_BUSINESSTYPE_NAME, --业态2级
               '' LEVEL3_BUSINESSTYPE_ID, --业态3级
               '' LEVEL3_BUSINESSTYPE_NAME, --业态3级
               '' LEVEL4_BUSINESSTYPE_ID, --业态4级
               '' LEVEL4_BUSINESSTYPE_NAME, --业态4级
               2 KPI_TYPE_ID,
               '利润' KPI_TYPE_DESCR,
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN  ZQ_PROFITS_AMT                 
                 ELSE  0 END ZQ_CHICKEN_AMT, --种禽-鸡线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 2 
                 THEN ZQ_PROFITS_AMT
                 ELSE  0 END ZQ_DUCK_AMT, --种禽-鸭线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN YZ_PROFITS_AMT
                 ELSE  0  END YZ_CHICKEN_AMT, --养殖-鸡线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 2 
                 THEN YZ_PROFITS_AMT
                 ELSE  0 END YZ_DUCK_AMT, --养殖-鸭线金额(元)
               MTL_INNER_PROFITS_AMT MTL_INNER_SALES_AMT, --饲料-内销金额(元),
               MTL_OUTTER_PROFITS_AMT MTL_OUTTER_SALES_AMT, --饲料-外销金额(元),
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN COLD_PROFITS_AMT
                 ELSE  0 END COLD_CHICKEN_AMT, --冷藏-鸡线金额(元)
               CASE  WHEN PRODUCTION_LINE_ID = 2 
                 THEN COLD_PROFITS_AMT
                 ELSE  0 END COLD_DUCK_AMT, --冷藏-鸭线金额(元)
               FOOD_PROFITS_AMT FOOD_FACTORY_AMT, --食品深加工(元)
              '$CREATE_TIME' CREATE_TIME           
          FROM mreport_poultry.DWF_BIRD_UNIFY_FINISH_DD
         WHERE OP_DAY = '$OP_DAY'
        UNION ALL
        SELECT SUBSTR(MONTH_ID, 1, 4) YEAR_ID,
               MONTH_ID, --期间(月)
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
               '' LEVEL7_ORG_ID, --组织7级(库存组织)
               '' LEVEL7_ORG_DESCR, --组织7级(库存组织)
               '' LEVEL1_BUSINESSTYPE_ID, --业态1级
               '' LEVEL1_BUSINESSTYPE_NAME, --业态1级
               '' LEVEL2_BUSINESSTYPE_ID, --业态2级
               '' LEVEL2_BUSINESSTYPE_NAME, --业态2级
               '' LEVEL3_BUSINESSTYPE_ID, --业态3级
               '' LEVEL3_BUSINESSTYPE_NAME, --业态3级
               '' LEVEL4_BUSINESSTYPE_ID, --业态4级
               '' LEVEL4_BUSINESSTYPE_NAME, --业态4级
               1 KPI_TYPE_ID,
               '收入' KPI_TYPE_DESCR,
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN ZQ_INCOME_AMT
                 ELSE   0 END ZQ_CHICKEN_AMT, --种禽-鸡线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 2 
                 THEN ZQ_INCOME_AMT
                 ELSE   0 END ZQ_DUCK_AMT, --种禽-鸭线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN YZ_INCOME_AMT
                 ELSE   0 END YZ_CHICKEN_AMT, --养殖-鸡线金额(元)
               CASE WHEN PRODUCTION_LINE_ID = 2 
                 THEN  YZ_INCOME_AMT
                 ELSE   0 END YZ_DUCK_AMT, --养殖-鸭线金额(元)
               MTL_INNER_INCOME_AMT MTL_INNER_SALES_AMT, --饲料-内销金额(元),
               MTL_OUTTER_INCOME_AMT MTL_OUTTER_SALES_AMT, --饲料-外销金额(元),
               CASE WHEN PRODUCTION_LINE_ID = 1 
                 THEN COLD_INCOME_AMT
                 ELSE   0 END COLD_CHICKEN_AMT, --冷藏-鸡线金额(元)
               CASE  WHEN PRODUCTION_LINE_ID = 2 
                 THEN COLD_INCOME_AMT
                 ELSE   0 END COLD_DUCK_AMT, --冷藏-鸭线金额(元)
               FOOD_INCOME_AMT FOOD_FACTORY_AMT, --食品深加工(元)
               '$CREATE_TIME' CREATE_TIME            
          FROM mreport_poultry.DWF_BIRD_UNIFY_FINISH_DD
         WHERE OP_DAY = '$OP_DAY'
          

"



###########################################################################################
## 处理基础数据
## 变量声明
TMP_DMF_BIRD_UNIFY_FINISH_MM_1='TMP_DMF_BIRD_UNIFY_FINISH_MM_1'

CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_UNIFY_FINISH_MM_1
(
 YEAR_ID                       STRING    --年
,MONTH_ID                      STRING    --期间(月)
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
,LEVEL7_ORG_ID                 STRING    --组织7级(库存组织)
,LEVEL7_ORG_DESCR              STRING    --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID        STRING    --业态1级
,LEVEL1_BUSINESSTYPE_NAME      STRING    --业态1级
,LEVEL2_BUSINESSTYPE_ID        STRING    --业态2级
,LEVEL2_BUSINESSTYPE_NAME      STRING    --业态2级
,LEVEL3_BUSINESSTYPE_ID        STRING    --业态3级
,LEVEL3_BUSINESSTYPE_NAME      STRING    --业态3级
,LEVEL4_BUSINESSTYPE_ID        STRING    --业态4级
,LEVEL4_BUSINESSTYPE_NAME      STRING    --业态4级
,KPI_TYPE_ID                   STRING    --指标类型id
,KPI_TYPE_DESCR                STRING    --指标类型
,ZQ_CHICKEN_AMT                STRING    --种禽-鸡线金额(元)
,ZQ_DUCK_AMT                   STRING    --种禽-鸭线金额(元)
,YZ_CHICKEN_AMT                STRING    --养殖-鸡线金额(元)
,YZ_DUCK_AMT                   STRING    --养殖-鸭线金额(元)
,MTL_INNER_SALES_AMT           STRING    --饲料-内销金额(元)
,MTL_OUTTER_SALES_AMT          STRING    --饲料-外销金额(元)
,COLD_CHICKEN_AMT              STRING    --冷藏-鸡线金额(元)
,COLD_DUCK_AMT                 STRING    --冷藏-鸭线金额(元)
,FOOD_FACTORY_AMT              STRING    --食品深加工(元)
,CREATE_TIME                   STRING
)
PARTITIONED BY (OP_MONTH string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_UNIFY_FINISH_MM_1 PARTITION(OP_MONTH='$OP_MONTH')
SELECT YEAR_ID,
       MONTH_ID, --期间(月)
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
       LEVEL7_ORG_ID, --组织7级(库存组织)
       LEVEL7_ORG_DESCR, --组织7级(库存组织)
       LEVEL1_BUSINESSTYPE_ID, --业态1级
       LEVEL1_BUSINESSTYPE_NAME, --业态1级
       LEVEL2_BUSINESSTYPE_ID, --业态2级
       LEVEL2_BUSINESSTYPE_NAME, --业态2级
       LEVEL3_BUSINESSTYPE_ID, --业态3级
       LEVEL3_BUSINESSTYPE_NAME, --业态3级
       LEVEL4_BUSINESSTYPE_ID, --业态4级
       LEVEL4_BUSINESSTYPE_NAME, --业态4级
       KPI_TYPE_ID,
       KPI_TYPE_DESCR,
       SUM(ZQ_CHICKEN_AMT) ZQ_CHICKEN_AMT, --种禽-鸡线金额(元)
       SUM(ZQ_DUCK_AMT) ZQ_DUCK_AMT, --种禽-鸭线金额(元)
       SUM(YZ_CHICKEN_AMT) YZ_CHICKEN_AMT, --养殖-鸡线金额(元)
       SUM(YZ_DUCK_AMT) YZ_DUCK_AMT, --养殖-鸭线金额(元)
       SUM(MTL_INNER_SALES_AMT) MTL_INNER_SALES_AMT, --饲料-内销金额(元),
       SUM(MTL_OUTTER_SALES_AMT) MTL_OUTTER_SALES_AMT, --饲料-外销金额(元),
       SUM(COLD_CHICKEN_AMT) COLD_CHICKEN_AMT, --冷藏-鸡线金额(元)
       SUM(COLD_DUCK_AMT) COLD_DUCK_AMT, --冷藏-鸭线金额(元)
       SUM(FOOD_FACTORY_AMT) FOOD_FACTORY_AMT, --食品深加工(元)
       '$CREATE_TIME' CREATE_TIME
  FROM mreport_poultry.TMP_DMF_BIRD_UNIFY_FINISH_MM_0     
 GROUP BY YEAR_ID,
          MONTH_ID, --期间(月)
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
          LEVEL7_ORG_ID, --组织7级(库存组织)
          LEVEL7_ORG_DESCR, --组织7级(库存组织)
          LEVEL1_BUSINESSTYPE_ID, --业态1级
          LEVEL1_BUSINESSTYPE_NAME, --业态1级
          LEVEL2_BUSINESSTYPE_ID, --业态2级
          LEVEL2_BUSINESSTYPE_NAME, --业态2级
          LEVEL3_BUSINESSTYPE_ID, --业态3级
          LEVEL3_BUSINESSTYPE_NAME, --业态3级
          LEVEL4_BUSINESSTYPE_ID, --业态4级
          LEVEL4_BUSINESSTYPE_NAME, --业态4级
          KPI_TYPE_ID,
          KPI_TYPE_DESCR
          

"


###########################################################################################
## 处理年累计数据
## 变量声明
TMP_DMF_BIRD_UNIFY_FINISH_MM_2='TMP_DMF_BIRD_UNIFY_FINISH_MM_2'

CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_UNIFY_FINISH_MM_2
(
 YEAR_ID                       STRING    --年
,MONTH_ID                      STRING    --月
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
,LEVEL7_ORG_ID                 STRING    --组织7级(库存组织)
,LEVEL7_ORG_DESCR              STRING    --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID        STRING    --业态1级
,LEVEL1_BUSINESSTYPE_NAME      STRING    --业态1级
,LEVEL2_BUSINESSTYPE_ID        STRING    --业态2级
,LEVEL2_BUSINESSTYPE_NAME      STRING    --业态2级
,LEVEL3_BUSINESSTYPE_ID        STRING    --业态3级
,LEVEL3_BUSINESSTYPE_NAME      STRING    --业态3级
,LEVEL4_BUSINESSTYPE_ID        STRING    --业态4级
,LEVEL4_BUSINESSTYPE_NAME      STRING    --业态4级
,KPI_TYPE_ID                   STRING    --指标类型id
,KPI_TYPE_DESCR                STRING    --指标类型
,ZQ_CHICKEN_AMT                STRING    --种禽-鸡线金额(元)
,ZQ_DUCK_AMT                   STRING    --种禽-鸭线金额(元)
,YZ_CHICKEN_AMT                STRING    --养殖-鸡线金额(元)
,YZ_DUCK_AMT                   STRING    --养殖-鸭线金额(元)
,MTL_INNER_SALES_AMT           STRING    --饲料-内销金额(元)
,MTL_OUTTER_SALES_AMT          STRING    --饲料-外销金额(元)
,COLD_CHICKEN_AMT              STRING    --冷藏-鸡线金额(元)
,COLD_DUCK_AMT                 STRING    --冷藏-鸭线金额(元)
,FOOD_FACTORY_AMT              STRING    --食品深加工(元)
,YEAR_ZQ_CHICKEN_AMT           STRING    --年累计_种禽-鸡线金额(元)
,YEAR_ZQ_DUCK_AMT              STRING    --年累计_种禽-鸭线金额(元)
,YEAR_YZ_CHICKEN_AMT           STRING    --年累计_养殖-鸡线金额(元)
,YEAR_YZ_DUCK_AMT              STRING    --年累计_养殖-鸭线金额(元)
,YEAR_MTL_INNER_SALES_AMT      STRING    --年累计_饲料-内销金额(元)
,YEAR_MTL_OUTTER_SALES_AMT     STRING    --年累计_饲料-外销金额(元)
,YEAR_COLD_CHICKEN_AMT         STRING    --年累计_冷藏-鸡线金额(元)
,YEAR_COLD_DUCK_AMT            STRING    --年累计_冷藏-鸭线金额(元)
,YEAR_FOOD_FACTORY_AMT         STRING    --年累计_食品深加工(元)
,CREATE_TIME                   STRING
)
PARTITIONED BY (OP_MONTH string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_2="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_UNIFY_FINISH_MM_2 PARTITION(OP_MONTH='$OP_MONTH')
SELECT Y.YEAR_ID,
       Y.MONTH_ID,
       M.DAY_ID,
       M.LEVEL1_ORG_ID, --组织1级(股份)
       M.LEVEL1_ORG_DESCR, --组织1级(股份)
       M.LEVEL2_ORG_ID, --组织2级(片联)
       M.LEVEL2_ORG_DESCR, --组织2级(片联)
       M.LEVEL3_ORG_ID, --组织3级(片区)
       M.LEVEL3_ORG_DESCR, --组织3级(片区)
       M.LEVEL4_ORG_ID, --组织4级(小片)
       M.LEVEL4_ORG_DESCR, --组织4级(小片)
       M.LEVEL5_ORG_ID, --组织5级(公司)
       M.LEVEL5_ORG_DESCR, --组织5级(公司)
       M.LEVEL6_ORG_ID, --组织6级(OU)
       M.LEVEL6_ORG_DESCR, --组织6级(OU)
       M.LEVEL7_ORG_ID, --组织7级(库存组织)
       M.LEVEL7_ORG_DESCR, --组织7级(库存组织)
       M.LEVEL1_BUSINESSTYPE_ID, --业态1级
       M.LEVEL1_BUSINESSTYPE_NAME, --业态1级
       M.LEVEL2_BUSINESSTYPE_ID, --业态2级
       M.LEVEL2_BUSINESSTYPE_NAME, --业态2级
       M.LEVEL3_BUSINESSTYPE_ID, --业态3级
       M.LEVEL3_BUSINESSTYPE_NAME, --业态3级
       M.LEVEL4_BUSINESSTYPE_ID, --业态4级
       M.LEVEL4_BUSINESSTYPE_NAME, --业态4级
       M.KPI_TYPE_ID, --指标类型id
       M.KPI_TYPE_DESCR, --指标类型
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.ZQ_CHICKEN_AMT 
           ELSE 0 END ZQ_CHICKEN_AMT, --种禽-鸡线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.ZQ_DUCK_AMT 
           ELSE 0 END ZQ_DUCK_AMT, --种禽-鸭线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.YZ_CHICKEN_AMT 
           ELSE 0 END YZ_CHICKEN_AMT, --养殖-鸡线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.YZ_DUCK_AMT 
           ELSE 0 END YZ_DUCK_AMT, --养殖-鸭线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.MTL_INNER_SALES_AMT 
           ELSE 0 END MTL_INNER_SALES_AMT, --饲料-内销金额(元),
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.MTL_OUTTER_SALES_AMT 
           ELSE 0 END MTL_OUTTER_SALES_AMT, --饲料-外销金额(元),
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.COLD_CHICKEN_AMT 
           ELSE 0 END COLD_CHICKEN_AMT, --冷藏-鸡线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.COLD_DUCK_AMT 
           ELSE 0 END COLD_DUCK_AMT, --冷藏-鸭线金额(元)
       CASE WHEN Y.MONTH_ID = M.MONTH_ID
           THEN M.FOOD_FACTORY_AMT 
           ELSE 0 END FOOD_FACTORY_AMT, --食品深加工(元)

       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.ZQ_CHICKEN_AMT 
           ELSE 0 END YEAR_ZQ_CHICKEN_AMT, --年累计种禽-鸡线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.ZQ_DUCK_AMT 
           ELSE 0 END YEAR_ZQ_DUCK_AMT, --年累计种禽-鸭线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.YZ_CHICKEN_AMT 
           ELSE 0 END YEAR_YZ_CHICKEN_AMT, --年累计养殖-鸡线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.YZ_DUCK_AMT 
           ELSE 0 END YEAR_YZ_DUCK_AMT, --年累计养殖-鸭线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.MTL_INNER_SALES_AMT 
           ELSE 0 END YEAR_MTL_INNER_SALES_AMT, --年累计饲料-内销金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.MTL_OUTTER_SALES_AMT 
           ELSE 0 END YEAR_MTL_OUTTER_SALES_AMT, --年累计饲料-外销金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.COLD_CHICKEN_AMT 
           ELSE 0 END YEAR_COLD_CHICKEN_AMT, --年累计冷藏-鸡线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.COLD_DUCK_AMT 
           ELSE 0 END YEAR_COLD_DUCK_AMT, --年累计冷藏-鸭线金额(元)
       CASE WHEN M.MONTH_ID <= Y.MONTH_ID
           THEN M.FOOD_FACTORY_AMT 
           ELSE 0 END YEAR_FOOD_FACTORY_AMT, --年累计食品深加工(元)
       '$CREATE_TIME' CREATE_TIME
  FROM  
       (SELECT YEAR_ID,MONTH_ID
         FROM mreport_poultry.TMP_DMF_BIRD_UNIFY_FINISH_MM_1
        WHERE OP_MONTH = '$OP_MONTH'
        GROUP BY YEAR_ID,MONTH_ID) Y
  LEFT JOIN      
        (SELECT *
          FROM mreport_poultry.TMP_DMF_BIRD_UNIFY_FINISH_MM_1
         WHERE OP_MONTH = '$OP_MONTH') M
    ON M.YEAR_ID = Y.YEAR_ID
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_UNIFY_FINISH_MM='DMF_BIRD_UNIFY_FINISH_MM'

CREATE_DMF_BIRD_UNIFY_FINISH_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_UNIFY_FINISH_MM
(MONTH_ID                      STRING    --期间(月)
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
,LEVEL7_ORG_ID                 STRING    --组织7级(库存组织)
,LEVEL7_ORG_DESCR              STRING    --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID        STRING    --业态1级
,LEVEL1_BUSINESSTYPE_NAME      STRING    --业态1级
,LEVEL2_BUSINESSTYPE_ID        STRING    --业态2级
,LEVEL2_BUSINESSTYPE_NAME      STRING    --业态2级
,LEVEL3_BUSINESSTYPE_ID        STRING    --业态3级
,LEVEL3_BUSINESSTYPE_NAME      STRING    --业态3级
,LEVEL4_BUSINESSTYPE_ID        STRING    --业态4级
,LEVEL4_BUSINESSTYPE_NAME      STRING    --业态4级
,KPI_TYPE_ID                   STRING    --指标类型id
,KPI_TYPE_DESCR                STRING    --指标类型
,ZQ_CHICKEN_AMT                STRING    --种禽-鸡线金额(元)
,ZQ_DUCK_AMT                   STRING    --种禽-鸭线金额(元)
,YZ_CHICKEN_AMT                STRING    --养殖-鸡线金额(元)
,YZ_DUCK_AMT                   STRING    --养殖-鸭线金额(元)
,MTL_INNER_SALES_AMT           STRING    --饲料-内销金额(元)
,MTL_OUTTER_SALES_AMT          STRING    --饲料-外销金额(元)
,COLD_CHICKEN_AMT              STRING    --冷藏-鸡线金额(元)
,COLD_DUCK_AMT                 STRING    --冷藏-鸭线金额(元)
,FOOD_FACTORY_AMT              STRING    --食品深加工(元)
,YEAR_ZQ_CHICKEN_AMT           STRING    --年累计_种禽-鸡线金额(元)
,YEAR_ZQ_DUCK_AMT              STRING    --年累计_种禽-鸭线金额(元)
,YEAR_YZ_CHICKEN_AMT           STRING    --年累计_养殖-鸡线金额(元)
,YEAR_YZ_DUCK_AMT              STRING    --年累计_养殖-鸭线金额(元)
,YEAR_MTL_INNER_SALES_AMT      STRING    --年累计_饲料-内销金额(元)
,YEAR_MTL_OUTTER_SALES_AMT     STRING    --年累计_饲料-外销金额(元)
,YEAR_COLD_CHICKEN_AMT         STRING    --年累计_冷藏-鸡线金额(元)
,YEAR_COLD_DUCK_AMT            STRING    --年累计_冷藏-鸭线金额(元)
,YEAR_FOOD_FACTORY_AMT         STRING    --年累计_食品深加工(元)
,MBGT_ZQ_CHICKEN_AMT           STRING    --月度预算-种禽-鸡线金额(元)
,MBGT_ZQ_DUCK_AMT              STRING    --月度预算-种禽-鸭线金额(元)
,MBGT_YZ_CHICKEN_AMT           STRING    --月度预算-养殖-鸡线金额(元)
,MBGT_YZ_DUCK_AMT              STRING    --月度预算-养殖-鸭线金额(元)
,MBGT_MTL_INNER_SALES_AMT      STRING    --月度预算-饲料-内销金额(元)
,MBGT_MTL_OUTTER_SALES_AMT     STRING    --月度预算-饲料-外销金额(元)
,MBGT_COLD_CHICKEN_AMT         STRING    --月度预算-冷藏-鸡线金额(元)
,MBGT_COLD_DUCK_AMT            STRING    --月度预算-冷藏-鸭线金额(元)
,MBGT_FOOD_FACTORY_AMT         STRING    --月度预算-食品深加工(元)
,YBGT_ZQ_CHICKEN_AMT           STRING    --年度预算-种禽-鸡线金额(元)
,YBGT_ZQ_DUCK_AMT              STRING    --年度预算-种禽-鸭线金额(元)
,YBGT_YZ_CHICKEN_AMT           STRING    --年度预算-养殖-鸡线金额(元)
,YBGT_YZ_DUCK_AMT              STRING    --年度预算-养殖-鸭线金额(元)
,YBGT_MTL_INNER_SALES_AMT      STRING    --年度预算-饲料-内销金额(元)
,YBGT_MTL_OUTTER_SALES_AMT     STRING    --年度预算-饲料-外销金额(元)
,YBGT_COLD_CHICKEN_AMT         STRING    --年度预算-冷藏-鸡线金额(元)
,YBGT_COLD_DUCK_AMT            STRING    --年度预算-冷藏-鸭线金额(元)
,YBGT_FOOD_FACTORY_AMT         STRING    --年度预算-食品深加工(元)
,CREATE_TIME                   STRING
)
PARTITIONED BY (OP_MONTH string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_UNIFY_FINISH_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_UNIFY_FINISH_MM PARTITION(OP_MONTH='$OP_MONTH')
SELECT 
       MONTH_ID,
       DAY_ID ,
       LEVEL1_ORG_ID,  --组织1级(股份)
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
       LEVEL7_ORG_ID, --组织7级(库存组织)
       LEVEL7_ORG_DESCR, --组织7级(库存组织)
       LEVEL1_BUSINESSTYPE_ID, --业态1级
       LEVEL1_BUSINESSTYPE_NAME, --业态1级
       LEVEL2_BUSINESSTYPE_ID, --业态2级
       LEVEL2_BUSINESSTYPE_NAME, --业态2级
       LEVEL3_BUSINESSTYPE_ID, --业态3级
       LEVEL3_BUSINESSTYPE_NAME, --业态3级
       LEVEL4_BUSINESSTYPE_ID, --业态4级
       LEVEL4_BUSINESSTYPE_NAME, --业态4级
       KPI_TYPE_ID, --指标类型id
       KPI_TYPE_DESCR, --指标类型
       SUM(ZQ_CHICKEN_AMT), --种禽-鸡线金额(元)
       SUM(ZQ_DUCK_AMT), --种禽-鸭线金额(元)
       SUM(YZ_CHICKEN_AMT), --养殖-鸡线金额(元)
       SUM(YZ_DUCK_AMT), --养殖-鸭线金额(元)
       SUM(MTL_INNER_SALES_AMT), --饲料-内销金额(元)
       SUM(MTL_OUTTER_SALES_AMT), --饲料-外销金额(元)
       SUM(COLD_CHICKEN_AMT), --冷藏-鸡线金额(元)
       SUM(COLD_DUCK_AMT), --冷藏-鸭线金额(元)
       SUM(FOOD_FACTORY_AMT), --食品深加工(元)
       SUM(YEAR_ZQ_CHICKEN_AMT), --年累计_种禽-鸡线金额(元)
       SUM(YEAR_ZQ_DUCK_AMT), --年累计_种禽-鸭线金额(元)
       SUM(YEAR_YZ_CHICKEN_AMT), --年累计_养殖-鸡线金额(元)
       SUM(YEAR_YZ_DUCK_AMT), --年累计_养殖-鸭线金额(元)
       SUM(YEAR_MTL_INNER_SALES_AMT), --年累计_饲料-内销金额(元)
       SUM(YEAR_MTL_OUTTER_SALES_AMT), --年累计_饲料-外销金额(元)
       SUM(YEAR_COLD_CHICKEN_AMT), --年累计_冷藏-鸡线金额(元)
       SUM(YEAR_COLD_DUCK_AMT), --年累计_冷藏-鸭线金额(元)
       SUM(YEAR_FOOD_FACTORY_AMT), --年累计_食品深加工(元)
       0 MBGT_ZQ_CHICKEN_AMT           ,    --月度预算-种禽-鸡线金额(元)
       0 MBGT_ZQ_DUCK_AMT              ,    --月度预算-种禽-鸭线金额(元)
       0 MBGT_YZ_CHICKEN_AMT           ,    --月度预算-养殖-鸡线金额(元)
       0 MBGT_YZ_DUCK_AMT              ,    --月度预算-养殖-鸭线金额(元)
       0 MBGT_MTL_INNER_SALES_AMT      ,    --月度预算-饲料-内销金额(元)
       0 MBGT_MTL_OUTTER_SALES_AMT     ,    --月度预算-饲料-外销金额(元)
       0 MBGT_COLD_CHICKEN_AMT         ,    --月度预算-冷藏-鸡线金额(元)
       0 MBGT_COLD_DUCK_AMT            ,    --月度预算-冷藏-鸭线金额(元)
       0 MBGT_FOOD_FACTORY_AMT         ,    --月度预算-食品深加工(元)
       0 YBGT_ZQ_CHICKEN_AMT           ,    --年度预算-种禽-鸡线金额(元)
       0 YBGT_ZQ_DUCK_AMT              ,    --年度预算-种禽-鸭线金额(元)
       0 YBGT_YZ_CHICKEN_AMT           ,    --年度预算-养殖-鸡线金额(元)
       0 YBGT_YZ_DUCK_AMT              ,    --年度预算-养殖-鸭线金额(元)
       0 YBGT_MTL_INNER_SALES_AMT      ,    --年度预算-饲料-内销金额(元)
       0 YBGT_MTL_OUTTER_SALES_AMT     ,    --年度预算-饲料-外销金额(元)
       0 YBGT_COLD_CHICKEN_AMT         ,    --年度预算-冷藏-鸡线金额(元)
       0 YBGT_COLD_DUCK_AMT            ,    --年度预算-冷藏-鸭线金额(元)
       0 YBGT_FOOD_FACTORY_AMT         ,    --年度预算-食品深加工(元)
       '$CREATE_TIME' CREATE_TIME
  FROM mreport_poultry.TMP_DMF_BIRD_UNIFY_FINISH_MM_2 T
  WHERE OP_MONTH = '$OP_MONTH'
    AND level2_org_id NOT IN('1015')
 GROUP BY  MONTH_ID,
       DAY_ID,
       LEVEL1_ORG_ID,  --组织1级(股份)
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
       LEVEL7_ORG_ID, --组织7级(库存组织)
       LEVEL7_ORG_DESCR, --组织7级(库存组织)
       LEVEL1_BUSINESSTYPE_ID, --业态1级
       LEVEL1_BUSINESSTYPE_NAME, --业态1级
       LEVEL2_BUSINESSTYPE_ID, --业态2级
       LEVEL2_BUSINESSTYPE_NAME, --业态2级
       LEVEL3_BUSINESSTYPE_ID, --业态3级
       LEVEL3_BUSINESSTYPE_NAME, --业态3级
       LEVEL4_BUSINESSTYPE_ID, --业态4级
       LEVEL4_BUSINESSTYPE_NAME, --业态4级
       KPI_TYPE_ID, --指标类型id
       KPI_TYPE_DESCR
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    
    
    $CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_0;
    $INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_0;
    $CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_1;
    $INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_1;
    $CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_2;
    $INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_2;
    $CREATE_DMF_BIRD_UNIFY_FINISH_MM;
    $INSERT_DMF_BIRD_UNIFY_FINISH_MM;
"  -v

#    $CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_0;
#    $INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_0;
#    $CREATE_TMP_DMF_BIRD_UNIFY_FINISH_MM_1;
#    $INSERT_TMP_DMF_BIRD_UNIFY_FINISH_MM_1;
#    $CREATE_DMF_BIRD_UNIFY_FINISH_MM;
#    $INSERT_DMF_BIRD_UNIFY_FINISH_MM;
#    $CREATE_DMF_BIRD_UNIFY_FINISH_MM;