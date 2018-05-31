#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_premium_dd.sh                               
# 创建时间: 2018年04月09日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 每日溢价跟踪
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_premium_dd.sh 20180101"
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
TMP_DMP_BIRD_PREMIUM_DD_1='TMP_DMP_BIRD_PREMIUM_DD_1'

CREATE_TMP_DMP_BIRD_PREMIUM_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PREMIUM_DD_1(
     MONTH_ID			            STRING       --期间(月份)
    ,DAY_ID			              STRING       --期间(日)
    ,BUS_TYPE               	STRING       --业态
    ,FIFTH_ORG_ID             STRING       --销售组织
    ,PRODUCTION_LINE_ID		    STRING       --产线
    ,BUSINESS_NAME		        STRING       --业务员名称
    ,AGENT_NAME               STRING       --代理商名称
    ,ITEM_CODE                STRING       --物料code
    ,RUN_PRICE			          STRING       --执行价格
    ,STD_PRICE			          STRING       --标准价格
    ,YF_PRICE			            STRING       --运费价格
    ,FL_PRICE			            STRING       --返利价格
    ,SALE_CNT			            STRING       --销量
    ,OBJ_PRICE			          STRING       --本月目标溢价
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PREMIUM_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PREMIUM_DD_1 PARTITION(OP_DAY='$OP_DAY')
SELECT 
     MONTH_ID                       --期间(月份)
    ,DAY_ID                         --期间(日)
    ,BUS_TYPE                       --业态
    ,FIFTH_ORG_ID                   --销售组织
    ,PRODUCTION_LINE_ID             --产线
    ,BUSINESS_NAME                  --业务员名称
    ,AGENT_NAME
    ,ITEM_CODE
    ,SUM(RUN_PRICE)                 --执行价格
    ,SUM(STD_PRICE)                 --标准价格
    ,SUM(YF_PRICE)                  --运费价格
    ,SUM(FL_PRICE)                  --返利价格
    ,SUM(SALE_CNT)                  --销量
    ,SUM(OBJ_PRICE)                 --本月目标溢价
FROM (
     SELECT      
          T1.MONTH_ID                  --期间(月份)
         ,T1.DAY_ID                    --期间(日)
         ,T2.BUS_TYPE                  --业态
         ,T2.FIFTH_ORG_ID              --销售组织
         ,T2.PRODUCTION_LINE_ID        --产线
         ,T2.BUSINESS_NAME             --业务员名称
         ,T2.AGENT_NAME
         ,T2.ITEM_CODE
         ,SUM(CASE WHEN T1.DAY_ID = T2.DAY_ID THEN T2.RUN_PRICE ELSE 0 END)    AS RUN_PRICE             --执行价格
         ,SUM(CASE WHEN T1.DAY_ID = T2.DAY_ID THEN T2.STD_PRICE ELSE 0 END)    AS STD_PRICE             --标准价格
         ,SUM(CASE WHEN T1.DAY_ID = T2.DAY_ID THEN T2.YF_PRICE ELSE 0 END)     AS YF_PRICE              --运费价格
         ,SUM(CASE WHEN T1.DAY_ID = T2.DAY_ID THEN T2.FL_PRICE ELSE 0 END)     AS FL_PRICE              --返利价格
         ,SUM(CASE WHEN T1.DAY_ID = T2.DAY_ID THEN T2.SALE_CNT ELSE 0 END)     AS SALE_CNT              --销量
         ,0 OBJ_PRICE               --本月目标溢价
     FROM (select DAY_ID,month_id from mreport_global.dim_day a where day_id BETWEEN '20151201' AND from_unixtime(unix_timestamp(),'yyyyMMdd')) T1
     LEFT JOIN (SELECT * FROM DWP_BIRD_PREMIUM_DD WHERE OP_DAY = '${OP_DAY}' ) T2
     ON T1.MONTH_ID = T2.MONTH_ID
     GROUP BY 
          T1.MONTH_ID                  --期间(月份)
         ,T1.DAY_ID                    --期间(日)
         ,T2.BUS_TYPE                  --业态
         ,T2.FIFTH_ORG_ID              --销售组织
         ,T2.PRODUCTION_LINE_ID        --产线
         ,T2.BUSINESS_NAME             --业务员名称
         ,T2.AGENT_NAME
         ,T2.ITEM_CODE
     UNION ALL
     SELECT      
          T1.MONTH_ID                                     --期间(月份)
         ,T1.DAY_ID                                       --期间(日)
         ,T2.TYPE                    AS BUS_TYPE          --业态
         ,T2.SALE_ORG_CODE_SEGMENTS  AS FIFTH_ORG_ID      --销售组织
         ,T2.PRODUCT_LINE_CODE       AS PRODUCTION_LINE_ID--产线
         ,T2.AGENT_NAME              AS BUSINESS_NAME     --业务员名称
         ,T3.AGENT_NAME              AS AGENT_NAME
         ,T3.ITEM_CODE               AS ITEM_CODE
         ,0       AS RUN_PRICE                            --执行价格
         ,0       AS STD_PRICE                            --标准价格
         ,0       AS YF_PRICE                             --运费价格
         ,0       AS FL_PRICE                             --返利价格
         ,0       AS SALE_CNT                             --销量
         ,coalesce(t2.TASK_PRICE_AMOUNT,'0') AS OBJ_PRICE --本月目标溢价
     FROM (select DAY_ID,month_id from mreport_global.dim_day a where day_id BETWEEN '20151201' AND from_unixtime(unix_timestamp(),'yyyyMMdd')) T1
     LEFT JOIN MREPORT_GLOBAL.DWU_DIM_OE_SALE_TASK_ALL T2
     ON T1.MONTH_ID = CONCAT(SUBSTR(T2.PERIOD_NAME,1,4),SUBSTR(T2.PERIOD_NAME,6,2))
     left join (SELECT distinct 
          MONTH_ID                  --期间(月份)
         ,BUS_TYPE                  --业态
         ,FIFTH_ORG_ID              --销售组织
         ,PRODUCTION_LINE_ID        --产线
         ,BUSINESS_NAME             --业务员名称
         ,AGENT_NAME
         ,ITEM_CODE FROM DWP_BIRD_PREMIUM_DD WHERE OP_DAY = '${OP_DAY}' ) t3
     on CONCAT(SUBSTR(T2.PERIOD_NAME,1,4),SUBSTR(T2.PERIOD_NAME,6,2)) = t3.month_id
     and T2.TYPE = t3.BUS_TYPE
     and t2.SALE_ORG_CODE_SEGMENTS = t3.FIFTH_ORG_ID
     and t2.PRODUCT_LINE_CODE = t3.PRODUCTION_LINE_ID
     and t2.AGENT_NAME = t3.BUSINESS_NAME
) A
GROUP BY 
     MONTH_ID                  --期间(月份)
    ,DAY_ID                    --期间(日)
    ,BUS_TYPE                  --业态
    ,FIFTH_ORG_ID              --销售组织
    ,PRODUCTION_LINE_ID        --产线
    ,BUSINESS_NAME             --业务员名称
    ,AGENT_NAME
    ,ITEM_CODE
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PREMIUM_DD='DMP_BIRD_PREMIUM_DD'

CREATE_DMP_BIRD_PREMIUM_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PREMIUM_DD(
     MONTH_ID			            STRING       --期间(月份)
    ,DAY_ID			              STRING       --期间(日)
    ,LEVEL1_ORG_ID		        STRING       --组织1级(股份)
    ,LEVEL1_ORG_DESCR		      STRING       --组织1级(股份)
    ,LEVEL2_ORG_ID		        STRING       --组织2级(片联)
    ,LEVEL2_ORG_DESCR		      STRING       --组织2级(片联)
    ,LEVEL3_ORG_ID		        STRING       --组织3级(片区)
    ,LEVEL3_ORG_DESCR		      STRING       --组织3级(片区)
    ,LEVEL4_ORG_ID		        STRING       --组织4级(小片)
    ,LEVEL4_ORG_DESCR		      STRING       --组织4级(小片)
    ,LEVEL5_ORG_ID		        STRING       --组织5级(公司)
    ,LEVEL5_ORG_DESCR		      STRING       --组织5级(公司)
    ,LEVEL6_ORG_ID		        STRING       --组织6级(OU)
    ,LEVEL6_ORG_DESCR		      STRING       --组织6级(OU)
    ,LEVEL7_ORG_ID		        STRING       --组织7级(库存组织)
    ,LEVEL7_ORG_DESCR		      STRING       --组织7级(库存组织)
    ,LEVEL1_BUSINESSTYPE_ID	  STRING       --业态1级
    ,LEVEL1_BUSINESSTYPE_NAME	STRING       --业态1级
    ,LEVEL2_BUSINESSTYPE_ID	  STRING       --业态2级
    ,LEVEL2_BUSINESSTYPE_NAME	STRING       --业态2级
    ,LEVEL3_BUSINESSTYPE_ID	  STRING       --业态3级
    ,LEVEL3_BUSINESSTYPE_NAME	STRING       --业态3级
    ,LEVEL4_BUSINESSTYPE_ID	  STRING       --业态4级
    ,LEVEL4_BUSINESSTYPE_NAME	STRING       --业态4级
    ,LEVEL1_SALE_ID		        STRING       --销售组织1级
    ,LEVEL1_SALE_DESCR		    STRING       --销售组织1级
    ,LEVEL2_SALE_ID		        STRING       --销售组织2级
    ,LEVEL2_SALE_DESCR		    STRING       --销售组织2级
    ,LEVEL3_SALE_ID		        STRING       --销售组织3级
    ,LEVEL3_SALE_DESCR		    STRING       --销售组织3级
    ,LEVEL4_SALE_ID		        STRING       --销售组织4级
    ,LEVEL4_SALE_DESCR		    STRING       --销售组织4级
    ,LEVEL5_SALE_ID		        STRING       --销售组织5级
    ,LEVEL5_SALE_DESCR		    STRING       --销售组织5级
    ,PRODUCTION_LINE_ID		    STRING       --产线
    ,PRODUCTION_LINE_DESCR	  STRING       --产线
    ,AGENT_NAME               STRING       --代理商名称
    ,ITEM_CODE                STRING       --物料code
    ,LEVEL1_PROD_ID		        STRING       --产品线1级
    ,LEVEL1_PROD_DESCR		    STRING       --产品线1级
    ,LEVEL2_PROD_ID		        STRING       --产品线2级
    ,LEVEL2_PROD_DESCR		    STRING       --产品线2级
    ,BUSINESS_ID		          STRING       --业务员ID
    ,BUSINESS_NAME		        STRING       --业务员名称
    ,RUN_PRICE			          STRING       --执行价格
    ,STD_PRICE			          STRING       --标准价格
    ,YF_PRICE			            STRING       --运费价格
    ,FL_PRICE			            STRING       --返利价格
    ,SALE_CNT			            STRING       --销量
    ,OBJ_PRICE			          STRING       --本月目标溢价
    ,CREATE_TIME		          STRING       --数据推送时间
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PREMIUM_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_PREMIUM_DD PARTITION(OP_DAY='$OP_DAY')
SELECT 
     MONTH_ID			               --期间(月份)
    ,DAY_ID			                 --期间(日)
    ,''           --组织1级(股份)
    ,''	         --组织1级(股份)
    ,''           --组织2级(片联)
    ,''	         --组织2级(片联)
    ,''           --组织3级(片区)
    ,''	         --组织3级(片区)
    ,''           --组织4级(小片)
    ,''	         --组织4级(小片)
    ,''           --组织5级(公司)
    ,''	         --组织5级(公司)
    ,''           --组织6级(OU)
    ,''	         --组织6级(OU)
    ,''           --组织7级(库存组织)
    ,''	         --组织7级(库存组织)
    ,T3.LEVEL1_BUSINESSTYPE_ID  						 	 --业态1级
    ,T3.LEVEL1_BUSINESSTYPE_NAME 							 --业态1级
    ,T3.LEVEL2_BUSINESSTYPE_ID 								 --业态2级
    ,T3.LEVEL2_BUSINESSTYPE_NAME 							 --业态2级
    ,T3.LEVEL3_BUSINESSTYPE_ID 								 --业态3级
    ,T3.LEVEL3_BUSINESSTYPE_NAME 							 --业态3级
    ,T3.LEVEL4_BUSINESSTYPE_ID 								 --业态4级
    ,T3.LEVEL4_BUSINESSTYPE_NAME 							 --业态4级
    ,T2.FIRST_SALE_ORG_CODE 								 --销售组织1级
    ,T2.FIRST_SALE_ORG_NAME 								 --销售组织1级
    ,T2.SECOND_SALE_ORG_CODE 								 --销售组织2级
    ,T2.SECOND_SALE_ORG_NAME 								 --销售组织2级
    ,T2.THREE_SALE_ORG_CODE 								 --销售组织3级
    ,T2.THREE_SALE_ORG_NAME 								 --销售组织3级
    ,T2.FOUR_SALE_ORG_CODE 								 --销售组织4级
    ,T2.FOUR_SALE_ORG_NAME 								 --销售组织4级
    ,T2.FIVE_SALE_ORG_CODE 								 --销售组织5级
    ,T2.FIVE_SALE_ORG_NAME 								 --销售组织5级
    ,COALESCE(SUBSTR(PRODUCTION_LINE_ID,1,1),'-1')		       --产线
    ,COALESCE(CASE WHEN PRODUCTION_LINE_ID = '10' THEN '鸡线' WHEN PRODUCTION_LINE_ID = '20' THEN '鸭线' END,'缺省')	     --产线
    ,T1.AGENT_NAME
    ,T4.ITEM_NAME
    ,''                          --产品线1级
    ,''                          --产品线1级
    ,''                          --产品线2级
    ,''                          --产品线2级
    ,SUBSTR(BUSINESS_NAME,-4,4)  --业务员ID
    ,BUSINESS_NAME		           --业务员名称
    ,RUN_PRICE			             --执行价格
    ,STD_PRICE			             --标准价格
    ,YF_PRICE			               --运费价格
    ,FL_PRICE			               --返利价格
    ,SALE_CNT			               --销量
    ,OBJ_PRICE			             --本月目标溢价
    ,${CREATE_TIME}              --数据推送时间
FROM (SELECT * FROM TMP_DMP_BIRD_PREMIUM_DD_1 WHERE OP_DAY = '${OP_DAY}') T1
LEFT JOIN MREPORT_GLOBAL.DWU_DIM_XS_ORG T2 ON T1.FIFTH_ORG_ID = T2.SALE_ORG_CODE   --销售组织
LEFT JOIN MREPORT_GLOBAL.DIM_ORG_BUSINESSTYPE T3 ON T1.BUS_TYPE = T3.LEVEL4_BUSINESSTYPE_ID   --业态
LEFT JOIN MREPORT_GLOBAL.DIM_CRM_ITEM T4 ON T1.ITEM_CODE = T4.ITEM_CODE


"





echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PREMIUM_DD_1;
    $INSERT_TMP_DMP_BIRD_PREMIUM_DD_1;
    $CREATE_DMP_BIRD_PREMIUM_DD;
    $INSERT_DMP_BIRD_PREMIUM_DD;
"  -v
