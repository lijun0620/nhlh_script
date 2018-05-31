#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_channel_ay_dd.sh                               
# 创建时间: 2018年04月09日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 渠道指标分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_channel_ay_dd.sh 20180101"
    exit 1
fi

# 当前时间减去30天时间
FORMAT_DAY=$(date -d $OP_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $OP_DAY +%Y-%m-01)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)



###########################################################################################
## 将数据从大表转换至中间表DIM_DEV_DAY
## 变量声明
DIM_DEV_DAY='DIM_DEV_DAY'

CREATE_DIM_DEV_DAY="
CREATE TABLE IF NOT EXISTS $DIM_DEV_DAY(
    YEAR_ID                 STRING --年
    ,SEASON_ID              STRING --季度
    ,MONTH_ID               STRING --期间(月份)
    ,DAY_ID                 STRING --期间(日)
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"



## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从大表转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DIM_DEV_DAY="
INSERT OVERWRITE TABLE $DIM_DEV_DAY PARTITION(op_day='$OP_DAY')
select year_id
,season_id
,month_id
,concat(t1.month_id,t2.day_id) day_id
from (
select distinct month_id,quarter_id season_id,year_id from mreport_global.dim_day a where day_id BETWEEN '20151201' AND from_unixtime(unix_timestamp(),'yyyyMMdd')
) t1
inner join (
select distinct substr(day_id,7,2) day_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND '20151231'
) t2
on 1=1
"







###########################################################################################
## 将数据从大表转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1
## 变量声明
TMP_DWP_BIRD_CHANNEL_AY_DD_1='TMP_DWP_BIRD_CHANNEL_AY_DD_1'

CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_CHANNEL_AY_DD_1(
    YEAR_ID                 STRING --年
    ,SEASON_ID              STRING --季度
    ,MONTH_ID               STRING --期间(月份)
    ,DAY_ID                 STRING --期间(日)
    ,FIFTH_ORG_ID           STRING --销售组织
    --,ITEM_CODE              STRING --CRM物料表-产品线
    --,ORG_ID                 STRING --6级组织
    --,ORGANIZATION_ID        STRING --7级库存组织
    ,BUS_TYPE               STRING --业态
    --,ACCOUNT_NUMBER         STRING --客户渠道
    ,PRODUCT_LINE           STRING --产线
    ,OUT_QTY                STRING --销量
    ,LEVEL1_PROD_ID         STRING --产品线1级
    ,LEVEL1_PROD_DESCR      STRING --产品线1级
    ,LEVEL2_PROD_ID         STRING --产品线2级
    ,LEVEL2_PROD_DESCR      STRING --产品线2级
    ,LEVEL1_CHANNEL_ID      STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR   STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID      STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR   STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从大表转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_CHANNEL_AY_DD_1 PARTITION(op_day='$OP_DAY')
SELECT
    SUBSTR(T1.APPROVE_DATE,1,4) --年
    ,CASE WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('01','02','03') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'1')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('04','05','06') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'2')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('07','08','09') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'3')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('10','11','12') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'4') END --季度
    ,SUBSTR(T1.APPROVE_DATE,1,6) --期间(月份)
    ,T1.APPROVE_DATE --期间(日)
    ,T1.FIFTH_ORG_ID  --销售组织
    --,T1.ITEM_CODE  --CRM物料表-产品线
    --,T1.ORG_ID  --6级组织
    --,T1.ORGANIZATION_ID  --7级库存组织
    ,T1.BUS_TYPE  --业态
    --,T1.ACCOUNT_NUMBER  --客户渠道
    ,T1.PRODUCT_LINE  --产线
    ,SUM(coalesce(T1.OUT_MAIN_QTY,0))  --销量
    ,T8.PRD_LINE_CATE_ID--产品线1级
    ,T8.PRD_LINE_CATE--产品线1级
    ,T8.SUB_PRD_LINE_TP_ID--产品线2级
    ,T8.SUB_PRD_LINE_TP--产品线2级
    ,T12.ID_CUST_CHAN                 --客户渠道1级
    ,T12.CUST_CHAN_TYPE               --客户渠道1级
    ,T12.ID_CUST_CHAN_DETAIL_TP       --客户渠道2级
    ,T12.CUST_CHAN_DETAIL_TP          --客户渠道2级
 FROM ( select * from 
 MREPORT_POULTRY.DWU_GYL_XS01_DD
 where OP_DAY='${OP_DAY}' AND APPROVE_DATE IS NOT NULL
 ) T1  --XS01销售表
  LEFT JOIN MREPORT_GLOBAL.DIM_CRM_ITEM T8 ON T1.ITEM_CODE = T8.ITEM_CODE --CRM物料表-产品线
  LEFT JOIN MREPORT_GLOBAL.DWU_DIM_CRM_CUSTOMER T12 ON T1.ACCOUNT_NUMBER = T12.CUSTOMER_ACCOUNT_ID  --客户渠道
 GROUP BY 
    SUBSTR(T1.APPROVE_DATE,1,4) --年
    ,CASE WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('01','02','03') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'1')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('04','05','06') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'2')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('07','08','09') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'3')
        WHEN SUBSTR(T1.APPROVE_DATE,5,2) IN ('10','11','12') THEN CONCAT(SUBSTR(T1.APPROVE_DATE,1,4),'4') END --季度
    ,SUBSTR(T1.APPROVE_DATE,1,6) --期间(月份)
    ,T1.APPROVE_DATE --期间(日)
    ,T1.FIFTH_ORG_ID  --销售组织
    --,T1.ITEM_CODE  --CRM物料表-产品线
    --,T1.ORG_ID  --6级组织
    --,T1.ORGANIZATION_ID  --7级库存组织
    ,T1.BUS_TYPE  --业态
    --,T1.ACCOUNT_NUMBER  --客户渠道
    ,T1.PRODUCT_LINE  --产线
    ,T8.PRD_LINE_CATE_ID--产品线1级
    ,T8.PRD_LINE_CATE--产品线1级
    ,T8.SUB_PRD_LINE_TP_ID--产品线2级
    ,T8.SUB_PRD_LINE_TP--产品线2级
    ,T12.ID_CUST_CHAN                 --客户渠道1级
    ,T12.CUST_CHAN_TYPE               --客户渠道1级
    ,T12.ID_CUST_CHAN_DETAIL_TP       --客户渠道2级
    ,T12.CUST_CHAN_DETAIL_TP          --客户渠道2级
"



###########################################################################################
## 将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_3
## 每月销量数据累计
## 变量声明
TMP_DWP_BIRD_CHANNEL_AY_DD_3='TMP_DWP_BIRD_CHANNEL_AY_DD_3'

CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_CHANNEL_AY_DD_3(
    YEAR_ID                     STRING --年
    ,SEASON_ID                  STRING --季度
    ,MONTH_ID                   STRING --期间(月份)
    ,LAST_MONTH_DAY_ID          STRING --上月
    ,NEXT_MONTH_DAY_ID          STRING --下月
    ,DAY_ID                     STRING --期间(日)
    ,FIFTH_ORG_ID               STRING --销售组织
    --,ITEM_CODE                  STRING --CRM物料表-产品线
    --,ORG_ID                     STRING --6级组织
    --,ORGANIZATION_ID            STRING --7级库存组织
    ,BUS_TYPE                   STRING --业态
    --,ACCOUNT_NUMBER             STRING --客户渠道
    ,PRODUCT_LINE               STRING --产线
    ,OUT_QTY                    STRING --销量
    ,LEVEL1_PROD_ID             STRING --产品线1级
    ,LEVEL1_PROD_DESCR          STRING --产品线1级
    ,LEVEL2_PROD_ID             STRING --产品线2级
    ,LEVEL2_PROD_DESCR          STRING --产品线2级
    ,LEVEL1_CHANNEL_ID          STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR       STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID          STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR       STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_3>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_3="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_CHANNEL_AY_DD_3 PARTITION(OP_DAY='$OP_DAY')
SELECT
    t1.YEAR_ID
    ,t1.SEASON_ID 
    ,t1.MONTH_ID
    ,CASE WHEN SUBSTR(t1.MONTH_ID,5,2) = '01' THEN CONCAT(floor(SUBSTR(t1.MONTH_ID,1,4) - 1),'12',SUBSTR(T1.DAY_ID,7,2)) ELSE CONCAT(floor(t1.MONTH_ID - 1),SUBSTR(T1.DAY_ID,7,2)) END --上月该日期
    ,CASE WHEN SUBSTR(t1.MONTH_ID,5,2) = '12' THEN CONCAT(floor(SUBSTR(t1.MONTH_ID,1,4) + 1),'01',SUBSTR(T1.DAY_ID,7,2)) ELSE CONCAT(floor(t1.MONTH_ID + 1),SUBSTR(T1.DAY_ID,7,2)) END --下月该日期
    ,T1.DAY_ID
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE
    ,SUM(CASE WHEN T1.DAY_ID >= T2.DAY_ID THEN T2.OUT_QTY ELSE 0 END) 
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
 FROM (
select day_id,month_id,season_id,year_id from DIM_DEV_DAY where op_day = '${OP_DAY}'
-- select YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID from MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 group by YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID
 ) T1
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 T2
  ON T2.OP_DAY = '$OP_DAY'
  AND T1.MONTH_ID = T2.MONTH_ID
 GROUP BY 
    T1.YEAR_ID
    ,T1.SEASON_ID 
    ,T1.MONTH_ID
    ,T1.DAY_ID
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
"




###########################################################################################
## 将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_4
## 每季度销量数据累计
## 变量声明
TMP_DWP_BIRD_CHANNEL_AY_DD_4='TMP_DWP_BIRD_CHANNEL_AY_DD_4'

CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_CHANNEL_AY_DD_4(
    YEAR_ID                 STRING --年
    ,SEASON_ID              STRING --季度
    ,MONTH_ID               STRING --期间(月份)
    ,DAY_ID                 STRING --期间(日)
    ,FIFTH_ORG_ID           STRING --销售组织
    --,ITEM_CODE              STRING --CRM物料表-产品线
    --,ORG_ID                 STRING --6级组织
    --,ORGANIZATION_ID        STRING --7级库存组织
    ,BUS_TYPE               STRING --业态
    --,ACCOUNT_NUMBER         STRING --客户渠道
    ,PRODUCT_LINE           STRING --产线
    ,OUT_QTY                STRING --销量
    ,LEVEL1_PROD_ID           STRING --产品线1级
    ,LEVEL1_PROD_DESCR        STRING --产品线1级
    ,LEVEL2_PROD_ID           STRING --产品线2级
    ,LEVEL2_PROD_DESCR        STRING --产品线2级
    ,LEVEL1_CHANNEL_ID        STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR     STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID        STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR     STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_4>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_4="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_CHANNEL_AY_DD_4 PARTITION(OP_DAY='$OP_DAY')
SELECT
    T1.YEAR_ID
    ,T1.SEASON_ID 
    ,T1.MONTH_ID
    ,T1.DAY_ID
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE
    ,SUM(CASE WHEN T1.DAY_ID >= T2.DAY_ID THEN T2.OUT_QTY ELSE 0 END) 
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
 FROM (
select day_id,month_id,season_id,year_id from DIM_DEV_DAY where op_day = '${OP_DAY}'
-- select YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID from MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 group by YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID
 ) t1
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 T2
  ON T1.SEASON_ID = T2.SEASON_ID
  and T2.OP_DAY = '$OP_DAY'
 GROUP BY 
    T1.YEAR_ID
    ,T1.SEASON_ID  
    ,T1.MONTH_ID
    ,T1.DAY_ID
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE 
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
"



###########################################################################################
## 将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_5
## 每年销量数据统计
## 变量声明
TMP_DWP_BIRD_CHANNEL_AY_DD_5='TMP_DWP_BIRD_CHANNEL_AY_DD_5'

CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_5="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_CHANNEL_AY_DD_5(
    YEAR_ID                 STRING --年
    ,SEASON_ID              STRING --季度
    ,MONTH_ID               STRING --期间(月份)
    ,DAY_ID                 STRING --期间(日)
    ,LAST_MONTH_DAY_ID      STRING --上月
    ,FIFTH_ORG_ID           STRING --销售组织
    --,ITEM_CODE              STRING --CRM物料表-产品线
    --,ORG_ID                 STRING --6级组织
    --,ORGANIZATION_ID        STRING --7级库存组织
    ,BUS_TYPE               STRING --业态
    --,ACCOUNT_NUMBER         STRING --客户渠道
    ,PRODUCT_LINE           STRING --产线
    ,OUT_QTY                STRING --销量
    ,LEVEL1_PROD_ID           STRING --产品线1级
    ,LEVEL1_PROD_DESCR        STRING --产品线1级
    ,LEVEL2_PROD_ID           STRING --产品线2级
    ,LEVEL2_PROD_DESCR        STRING --产品线2级
    ,LEVEL1_CHANNEL_ID        STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR     STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID        STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR     STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1转换至中间表TMP_DWP_BIRD_CHANNEL_AY_DD_5>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_5="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_CHANNEL_AY_DD_5 PARTITION(OP_DAY='$OP_DAY')
SELECT
    T1.YEAR_ID
    ,T1.SEASON_ID 
    ,T1.MONTH_ID
    ,T1.DAY_ID
    ,CASE WHEN SUBSTR(t1.MONTH_ID,5,2) = '01' THEN CONCAT(floor(SUBSTR(t1.MONTH_ID,1,4) - 1),'12',SUBSTR(T1.DAY_ID,7,2)) ELSE CONCAT(floor(t1.MONTH_ID - 1),SUBSTR(T1.DAY_ID,7,2)) END --上月该日期
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE
    ,SUM(CASE WHEN T1.DAY_ID >= T2.DAY_ID THEN T2.OUT_QTY ELSE 0 END)  
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
 FROM (
select day_id,month_id,season_id,year_id from DIM_DEV_DAY where op_day = '${OP_DAY}'
-- select YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID from MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 group by YEAR_ID,SEASON_ID,MONTH_ID,DAY_ID
 ) t1
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 T2
  ON T1.YEAR_ID = T2.YEAR_ID
  and T2.OP_DAY = '$OP_DAY'
 GROUP BY 
    T1.YEAR_ID
    ,T1.SEASON_ID  
    ,T1.MONTH_ID
    ,T1.DAY_ID
    ,CASE WHEN SUBSTR(t1.MONTH_ID,5,2) = '01' THEN CONCAT(floor(SUBSTR(t1.MONTH_ID,1,4) - 1),'12',SUBSTR(T1.DAY_ID,7,2)) ELSE CONCAT(floor(t1.MONTH_ID - 1),SUBSTR(T1.DAY_ID,7,2)) END --上月该日期
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    --,T2.ORG_ID
    --,T2.ORGANIZATION_ID
    ,T2.BUS_TYPE
    --,T2.ACCOUNT_NUMBER
    ,T2.PRODUCT_LINE 
    ,LEVEL1_PROD_ID       
    ,LEVEL1_PROD_DESCR    
    ,LEVEL2_PROD_ID       
    ,LEVEL2_PROD_DESCR    
    ,LEVEL1_CHANNEL_ID    
    ,LEVEL1_CHANNEL_DESCR 
    ,LEVEL2_CHANNEL_ID    
    ,LEVEL2_CHANNEL_DESCR 
"



###########################################################################################
## 将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_2、TMP_DWP_BIRD_CHANNEL_AY_DD_3、TMP_DWP_BIRD_CHANNEL_AY_DD_4、TMP_DWP_BIRD_CHANNEL_AY_DD_5转换至目标表DWP_BIRD_CHANNEL_AY_DD
## 合并中间表数据
## 变量声明
TMP_DWP_BIRD_CHANNEL_AY_DD_6='TMP_DWP_BIRD_CHANNEL_AY_DD_6'

CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_6="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_CHANNEL_AY_DD_6(
    MONTH_ID                    STRING --期间(月份)
    ,DAY_ID                     STRING --期间(日)
    ,LAST_MONTH_DAY_ID          STRING --上月
    ,FIFTH_ORG_ID               STRING --销售组织
    --,ITEM_CODE                  STRING --物料
    ,ORG_ID                     STRING --OU组织
    ,BUS_TYPE                   STRING --业态
    ,ORGANIZATION_ID            STRING --库存组织
    --,ACCOUNT_NUMBER             STRING --客户号渠道
    ,PRODUCTION_LINE_ID         STRING --产线
    ,PRODUCTION_LINE_DESCR      STRING --产线
    ,DAY_CHL_CNT                STRING --渠道部门日销量
    ,MONTH_CHL_CNT              STRING --本月渠道部门销量累计
    ,QUARTER_CHL_CNT            STRING --季度渠道部门销量累计
    ,YEAR_CHL_CNT               STRING --本年渠道部门销量累计
    ,LEVEL1_PROD_ID           STRING --产品线1级
    ,LEVEL1_PROD_DESCR        STRING --产品线1级
    ,LEVEL2_PROD_ID           STRING --产品线2级
    ,LEVEL2_PROD_DESCR        STRING --产品线2级
    ,LEVEL1_CHANNEL_ID        STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR     STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID        STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR     STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"




## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1、TMP_DWP_BIRD_CHANNEL_AY_DD_3、TMP_DWP_BIRD_CHANNEL_AY_DD_4、TMP_DWP_BIRD_CHANNEL_AY_DD_5转换至目标表DWP_BIRD_CHANNEL_AY_DD>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_6="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_CHANNEL_AY_DD_6 PARTITION(OP_DAY='$OP_DAY')
SELECT
    t2.MONTH_ID --期间(月份)
    ,t2.DAY_ID --期间(日)
    ,CASE WHEN SUBSTR(t2.MONTH_ID,5,2) = '01' THEN CONCAT(floor(SUBSTR(t2.MONTH_ID,1,4) - 1),'12',SUBSTR(T2.DAY_ID,7,2)) ELSE CONCAT(floor(t2.MONTH_ID - 1),SUBSTR(T2.DAY_ID,7,2)) END --上月该日期
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    ,'' --T2.ORG_ID
    ,T2.BUS_TYPE
    ,'' --T2.ORGANIZATION_ID
    --,T2.ACCOUNT_NUMBER
    ,t2.PRODUCT_LINE --产线
    ,CASE WHEN t2.PRODUCT_LINE = '10' THEN '鸡线' WHEN t2.PRODUCT_LINE = '20' THEN '鸭线' ELSE t2.PRODUCT_LINE END --产线
    ,coalesce(t5.OUT_QTY,0) --渠道部门日销量
    ,coalesce(t3.OUT_QTY,0) --本月渠道部门销量累计
    ,coalesce(t4.OUT_QTY,0) --季度渠道部门销量累计
    ,coalesce(t2.OUT_QTY,0) --本年渠道部门销量累计
    ,t2.LEVEL1_PROD_ID
    ,t2.LEVEL1_PROD_DESCR     
    ,t2.LEVEL2_PROD_ID        
    ,t2.LEVEL2_PROD_DESCR     
    ,t2.LEVEL1_CHANNEL_ID     
    ,t2.LEVEL1_CHANNEL_DESCR  
    ,t2.LEVEL2_CHANNEL_ID     
    ,t2.LEVEL2_CHANNEL_DESCR  
 --合并对应销量数据
 FROM (
       SELECT * FROM MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_5
       WHERE OP_DAY = '${OP_DAY}'
       ) T2 --每年销量统计
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_3 T3 --每月销量累计
    ON T2.DAY_ID = T3.DAY_ID
    AND coalesce(T2.FIFTH_ORG_ID,'NULL_FORMAT_STRING') = coalesce(T3.FIFTH_ORG_ID,'NULL_FORMAT_STRING')
    --AND T2.ITEM_CODE = T3.ITEM_CODE
    --AND T2.ORG_ID = T3.ORG_ID
    --AND coalesce(T2.ORGANIZATION_ID,'NULL_FORMAT_STRING') = coalesce(T3.ORGANIZATION_ID,'NULL_FORMAT_STRING')
    AND coalesce(T2.BUS_TYPE,'NULL_FORMAT_STRING') = coalesce(T3.BUS_TYPE,'NULL_FORMAT_STRING')
    --AND T2.ACCOUNT_NUMBER = T3.ACCOUNT_NUMBER
    AND coalesce(T2.PRODUCT_LINE,'PRODUCT_FORMAT_STRING') = coalesce(T3.PRODUCT_LINE,'PRODUCT_FORMAT_STRING')
    AND T3.OP_DAY = '${OP_DAY}'
    and coalesce(t2.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t3.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t3.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t3.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  
    and coalesce(t2.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t3.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING')  
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_4 T4 --每季度销量累计
    ON T2.DAY_ID = T4.DAY_ID
    AND coalesce(T2.FIFTH_ORG_ID,'NULL_FORMAT_STRING') = coalesce(T4.FIFTH_ORG_ID,'NULL_FORMAT_STRING')
    --AND T2.ITEM_CODE = T4.ITEM_CODE
    --AND T2.ORG_ID = T4.ORG_ID
    --AND coalesce(T2.ORGANIZATION_ID,'NULL_FORMAT_STRING') = coalesce(T4.ORGANIZATION_ID,'NULL_FORMAT_STRING')
    AND coalesce(T2.BUS_TYPE,'NULL_FORMAT_STRING') = coalesce(T4.BUS_TYPE,'NULL_FORMAT_STRING')
    --AND T2.ACCOUNT_NUMBER = T4.ACCOUNT_NUMBER
    AND coalesce(T2.PRODUCT_LINE,'PRODUCT_FORMAT_STRING') = coalesce(T4.PRODUCT_LINE,'PRODUCT_FORMAT_STRING')
    AND T4.OP_DAY = '${OP_DAY}'
    and coalesce(t2.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t4.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t4.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t4.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  
    and coalesce(t2.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t4.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING') 
 LEFT JOIN MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_1 T5 --每日销量累计
    ON  T2.DAY_ID = T5.DAY_ID
    AND coalesce(T2.FIFTH_ORG_ID,'NULL_FORMAT_STRING') = coalesce(T5.FIFTH_ORG_ID,'NULL_FORMAT_STRING')
    --AND T2.ITEM_CODE = T5.ITEM_CODE
    --AND T2.ORG_ID = T5.ORG_ID
    --AND coalesce(T2.ORGANIZATION_ID,'NULL_FORMAT_STRING') = coalesce(T5.ORGANIZATION_ID,'NULL_FORMAT_STRING')
    AND coalesce(T2.BUS_TYPE,'NULL_FORMAT_STRING') = coalesce(T5.BUS_TYPE,'NULL_FORMAT_STRING')
    --AND T2.ACCOUNT_NUMBER = T5.ACCOUNT_NUMBER
    AND coalesce(T2.PRODUCT_LINE,'PRODUCT_FORMAT_STRING') = coalesce(T5.PRODUCT_LINE,'PRODUCT_FORMAT_STRING')
    AND T5.OP_DAY = '${OP_DAY}'
    and coalesce(t2.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t5.LEVEL1_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     =  coalesce(t5.LEVEL2_PROD_ID,'NULL_FORMAT_STRING')     
    and coalesce(t2.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t5.LEVEL1_CHANNEL_ID,'NULL_FORMAT_STRING')  
    and coalesce(t2.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING')  =  coalesce(t5.LEVEL2_CHANNEL_ID,'NULL_FORMAT_STRING') 
"








###########################################################################################
## 将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_2、TMP_DWP_BIRD_CHANNEL_AY_DD_3、TMP_DWP_BIRD_CHANNEL_AY_DD_4、TMP_DWP_BIRD_CHANNEL_AY_DD_5转换至目标表DWP_BIRD_CHANNEL_AY_DD
## 合并中间表数据
## 变量声明
DWP_BIRD_CHANNEL_AY_DD='DWP_BIRD_CHANNEL_AY_DD'

CREATE_DWP_BIRD_CHANNEL_AY_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_CHANNEL_AY_DD(
    MONTH_ID                    STRING --期间(月份)
    ,DAY_ID                     STRING --期间(日)
    ,FIFTH_ORG_ID               STRING --销售组织
    --,ITEM_CODE                  STRING --物料
    ,ORG_ID                     STRING --OU组织
    ,BUS_TYPE                   STRING --业态
    ,ORGANIZATION_ID            STRING --库存组织
    --,ACCOUNT_NUMBER             STRING --客户号渠道
    ,PRODUCTION_LINE_ID         STRING --产线
    ,PRODUCTION_LINE_DESCR      STRING --产线
    ,DAY_CHL_CNT                STRING --渠道部门日销量
    ,MONTH_CHL_CNT              STRING --本月渠道部门销量累计
    ,QUARTER_CHL_CNT            STRING --季度渠道部门销量累计
    ,YEAR_CHL_CNT               STRING --本年渠道部门销量累计
    ,LAST_MONTH_CHL_CNT         STRING --上月渠道部门销量
    ,LEVEL1_PROD_ID           STRING --产品线1级
    ,LEVEL1_PROD_DESCR        STRING --产品线1级
    ,LEVEL2_PROD_ID           STRING --产品线2级
    ,LEVEL2_PROD_DESCR        STRING --产品线2级
    ,LEVEL1_CHANNEL_ID        STRING --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR     STRING --客户渠道1级
    ,LEVEL2_CHANNEL_ID        STRING --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR     STRING --客户渠道2级
)
PARTITIONED BY (OP_DAY STRING)                       
STORED AS ORC
"





## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从中间表TMP_DWP_BIRD_CHANNEL_AY_DD_1、TMP_DWP_BIRD_CHANNEL_AY_DD_3、TMP_DWP_BIRD_CHANNEL_AY_DD_4、TMP_DWP_BIRD_CHANNEL_AY_DD_5转换至目标表DWP_BIRD_CHANNEL_AY_DD>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_CHANNEL_AY_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_CHANNEL_AY_DD PARTITION(OP_DAY='$OP_DAY')
SELECT
    t2.MONTH_ID --期间(月份)
    ,t2.DAY_ID --期间(日)
    ,T2.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    ,'' ORG_ID --T2.ORG_ID
    ,T2.BUS_TYPE
    ,'' ORGANIZATION_ID --T2.ORGANIZATION_ID
    --,T2.ACCOUNT_NUMBER
    ,t2.PRODUCTION_LINE_ID --产线
    ,CASE WHEN t2.PRODUCTION_LINE_ID = '10' THEN '鸡线' WHEN t2.PRODUCTION_LINE_ID = '20' THEN '鸭线' ELSE t2.PRODUCTION_LINE_ID END PRODUCTION_LINE_descr --产线
    ,coalesce(t2.DAY_CHL_CNT,0) DAY_CHL_CNT --渠道部门日销量
    ,coalesce(t2.MONTH_CHL_CNT,0) MONTH_CHL_CNT --本月渠道部门销量累计
    ,coalesce(t2.QUARTER_CHL_CNT,0) QUARTER_CHL_CNT --季度渠道部门销量累计
    ,coalesce(t2.YEAR_CHL_CNT,0) YEAR_CHL_CNT --本年渠道部门销量累计
    ,0     LAST_MONTH_CHL_CNT                 --上月渠道部门销量 
    ,t2.LEVEL1_PROD_ID  
    ,t2.LEVEL1_PROD_DESCR    
    ,t2.LEVEL2_PROD_ID       
    ,t2.LEVEL2_PROD_DESCR    
    ,t2.LEVEL1_CHANNEL_ID    
    ,t2.LEVEL1_CHANNEL_DESCR 
    ,t2.LEVEL2_CHANNEL_ID    
    ,t2.LEVEL2_CHANNEL_DESCR 
 --合并对应销量数据
 FROM MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_6 T2
       WHERE OP_DAY = '${OP_DAY}'
    
union all


SELECT
    substr(t6.NEXT_MONTH_DAY_ID,1,6) MONTH_ID --期间(月份)
    ,t6.NEXT_MONTH_DAY_ID DAY_ID --期间(日)
    ,T6.FIFTH_ORG_ID
    --,T2.ITEM_CODE
    ,'' ORG_ID --T2.ORG_ID
    ,T6.BUS_TYPE
    ,'' ORGANIZATION_ID --T2.ORGANIZATION_ID
    --,T2.ACCOUNT_NUMBER
    ,t6.PRODUCT_LINE PRODUCTION_LINE_ID --产线
    ,CASE WHEN t6.PRODUCT_LINE = '10' THEN '鸡线' WHEN t6.PRODUCT_LINE = '20' THEN '鸭线' ELSE t6.PRODUCT_LINE END PRODUCTION_LINE_descr --产线
    ,0 DAY_CHL_CNT --渠道部门日销量
    ,0 MONTH_CHL_CNT --本月渠道部门销量累计
    ,0 QUARTER_CHL_CNT --季度渠道部门销量累计
    ,0 YEAR_CHL_CNT --本年渠道部门销量累计
    ,coalesce(t6.OUT_QTY,0) LAST_MONTH_CHL_CNT --上月渠道部门销量 
    ,t6.LEVEL1_PROD_ID  
    ,t6.LEVEL1_PROD_DESCR    
    ,t6.LEVEL2_PROD_ID       
    ,t6.LEVEL2_PROD_DESCR    
    ,t6.LEVEL1_CHANNEL_ID    
    ,t6.LEVEL1_CHANNEL_DESCR 
    ,t6.LEVEL2_CHANNEL_ID    
    ,t6.LEVEL2_CHANNEL_DESCR 
 --合并对应销量数据
 FROM MREPORT_POULTRY.TMP_DWP_BIRD_CHANNEL_AY_DD_3 t6
 where op_day = '${OP_DAY}'

"





echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DIM_DEV_DAY;
    $INSERT_DIM_DEV_DAY;
    $CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_1;
    $INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_1;
    $CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_3;
    $INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_3;
    $CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_4;
    $INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_4;
    $CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_5;
    $INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_5;
    $CREATE_TMP_DWP_BIRD_CHANNEL_AY_DD_6;
    $INSERT_TMP_DWP_BIRD_CHANNEL_AY_DD_6;
    $CREATE_DWP_BIRD_CHANNEL_AY_DD;
    $INSERT_DWP_BIRD_CHANNEL_AY_DD;
    
"  -v
