#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_premium_dd.sh                               
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
    echo "输入参数错误，调用示例: dwp_bird_premium_dd.sh 20180101"
    exit 1
fi

# 当前时间减去30天时间
FORMAT_DAY=$(date -d $OP_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $OP_DAY +%Y-%m-01)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 订单级汇总
## 变量声明
TMP_DWP_BIRD_PREMIUM_DD_1='TMP_DWP_BIRD_PREMIUM_DD_1'

CREATE_TMP_DWP_BIRD_PREMIUM_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PREMIUM_DD_1(
     MONTH_ID               STRING       --期间(月份)
    ,DAY_ID                 STRING       --期间(日)
    ,ORDER_NUMBER           STRING       --订单号
    ,ORG_ID                 STRING       --OU
    ,ORGANIZATION_ID        STRING       --库存组织
    ,BUS_TYPE               STRING       --业态
    ,FIFTH_ORG_ID           STRING       --销售组织
    ,PRODUCTION_LINE_ID     STRING       --产线
    ,PRODUCTION_LINE_DESCR  STRING       --产线
    ,BUSINESS_ID            STRING       --业务员ID
    ,BUSINESS_NAME          STRING       --业务员名称
    ,AGENT_NAME             STRING       --代理商名称
    ,ITEM_CODE              STRING       --物料code
    ,RUN_PRICE              STRING       --执行价格
    ,STD_PRICE              STRING       --标准价格
    ,YF_PRICE               STRING       --运费价格
    ,FL_PRICE               STRING       --返利价格
    ,SALE_CNT               STRING       --销量
)
 PARTITIONED BY (op_day string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PREMIUM_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PREMIUM_DD_1 PARTITION(op_day='$OP_DAY')
select 
    a.MONTH_ID
    ,a.DAY_ID
    ,a.ORDER_NUMBER
    ,a.ORG_ID
    ,a.ORGANIZATION_ID
    ,a.BUS_TYPE
    ,a.FIFTH_ORG_ID
    ,a.PRODUCTION_LINE_ID
    ,a.PRODUCTION_LINE_DESCR
    ,a.BUSINESS_ID
    ,a.BUSINESS_NAME
    ,A.AGENT_NAME
    ,A.ITEM_CODE
    ,case when cast(a.SALE_CNT as decimal(12,6))=0 then 0 else a.RUN_PRICE/a.SALE_CNT end     --执行价格
    ,case when cast(a.SALE_CNT as decimal(12,6))=0 then 0 else a.STD_PRICE/a.SALE_CNT end     --标准价格
    ,coalesce(b.fee_unit_amount,0)     --运费价格
    ,case when cast(a.SALE_CNT as decimal(12,6))=0 then 0 else a.FL_PRICE/a.SALE_CNT  end     --返利价格
    ,a.SALE_CNT
    FROM
    (
    SELECT 
        SUBSTR(T1.APPROVE_DATE,1,6) 	MONTH_ID			 --期间(月份)
        ,T1.APPROVE_DATE DAY_ID  --期间(日)
        ,T1.ORDER_NUMBER  --订单号
        ,T1.ORG_ID
        ,T1.ORGANIZATION_ID  --7级库存组织
        ,T1.BUS_TYPE         --业态
        ,T1.FIFTH_ORG_ID     --销售
        ,T1.PRODUCT_LINE PRODUCTION_LINE_ID --产线
        ,CASE WHEN T1.PRODUCT_LINE = '10' THEN '鸡线' WHEN T1.PRODUCT_LINE = '20' THEN '鸭线' ELSE T1.PRODUCT_LINE END  PRODUCTION_LINE_DESCR --产线
        ,SUBSTR(T1.RESOURCE_NAME,-4)   BUSINESS_ID       --业务员ID
        ,T1.RESOURCE_NAME 					   BUSINESS_NAME				 --业务员名称
        ,T1.AGENT_NAME
        ,T1.ITEM_CODE
        ,SUM(COALESCE(T1.EXECUTE_PRICE,0) * COALESCE(OUT_MAIN_QTY,0) )      AS RUN_PRICE  --执行价格
        ,SUM(COALESCE(T1.STANDARD_PRICE,0) * COALESCE(OUT_MAIN_QTY,0) )     AS STD_PRICE  --标准价格
        --,SUM(COALESCE(T1.TRANSPROT_PRICE,0) * COALESCE(OUT_MAIN_QTY,0) )    AS YF_PRICE   --运费价格
        ,SUM(COALESCE(T1.RT_PRICE,0) * COALESCE(OUT_MAIN_QTY,0) )           AS FL_PRICE   --返利价格
        ,SUM(COALESCE(T1.OUT_MAIN_QTY,0))                                   AS SALE_CNT   --销量
    FROM (select * from MREPORT_POULTRY.DWU_GYL_XS01_DD 
    WHERE OP_DAY = '${OP_DAY}' 
      AND APPROVE_DATE IS NOT NULL    
    ) T1  --XS01销售表
    inner join mreport_global.DIM_CRM_ITEM t3
      on t1.item_code = t3.item_code
     and t3.PRD_LINE_CATE <> '调理深加工'
      
    left join mreport_global.DWU_DIM_GYL_XS08 t2
      on t1.AGENT_CODE = t2.SALE_CODE
      and t2.INDEX_TYPE = 'CUX_BI_STANDARD OF RWD AND PSH'
     
    where t2.SALE_CODE is null
      
    GROUP BY 
        SUBSTR(T1.APPROVE_DATE,1,6)   --期间(月份)
        ,T1.APPROVE_DATE  --期间(日)
        ,T1.ORDER_NUMBER  --订单号
        ,T1.ORG_ID
        ,T1.ORGANIZATION_ID  --7级库存组织
        ,T1.BUS_TYPE         --业态
        ,T1.FIFTH_ORG_ID     --销售
        ,T1.PRODUCT_LINE --产线
        ,CASE WHEN T1.PRODUCT_LINE = '10' THEN '鸡线' WHEN T1.PRODUCT_LINE = '20' THEN '鸭线' ELSE T1.PRODUCT_LINE END  --产线
        ,SUBSTR(T1.RESOURCE_NAME,-4)         --业务员ID
        ,T1.RESOURCE_NAME 									 --业务员名称
        ,T1.AGENT_NAME
        ,T1.ITEM_CODE
    ) a
    left join (
                  select ORDER_NUMBER,fee_unit_amount from dwu_cw_cw18_dd where op_day = '${OP_DAY}'
              ) b
    on a.ORDER_NUMBER = b.ORDER_NUMBER
"






###########################################################################################
## 将数据从大表转换至目标表
## 机构级汇总
## 变量声明
DWP_BIRD_PREMIUM_DD='DWP_BIRD_PREMIUM_DD'

CREATE_DWP_BIRD_PREMIUM_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_PREMIUM_DD(
     MONTH_ID               STRING       --期间(月份)
    ,DAY_ID                 STRING       --期间(日)
    ,BUS_TYPE               STRING       --业态
    ,FIFTH_ORG_ID           STRING       --销售组织
    ,PRODUCTION_LINE_ID     STRING       --产线
    ,PRODUCTION_LINE_DESCR  STRING       --产线
    ,BUSINESS_ID            STRING       --业务员ID
    ,BUSINESS_NAME          STRING       --业务员名称
    ,AGENT_NAME             STRING       --代理商名称
    ,ITEM_CODE              STRING       --物料code
    ,RUN_PRICE              STRING       --执行价格
    ,STD_PRICE              STRING       --标准价格
    ,YF_PRICE               STRING       --运费价格
    ,FL_PRICE               STRING       --返利价格
    ,SALE_CNT               STRING       --销量
)
 PARTITIONED BY (op_day string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_PREMIUM_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_PREMIUM_DD PARTITION(op_day='$OP_DAY')
SELECT 
    MONTH_ID                    --期间(月份)
    ,DAY_ID
    ,BUS_TYPE
    ,FIFTH_ORG_ID
    ,PRODUCTION_LINE_ID
    ,PRODUCTION_LINE_DESCR
    ,BUSINESS_ID             --业务员ID
    ,BUSINESS_NAME           --业务员名称
    ,AGENT_NAME
    ,ITEM_CODE
    ,coalesce(case when cast(SALE_CNT as decimal(12,6))=0 then 0 else t1.S_RUN_PRICE/SALE_CNT end,'0')     --执行价格
    ,coalesce(case when cast(SALE_CNT as decimal(12,6))=0 then 0 else t1.S_STD_PRICE/SALE_CNT end,'0')     --标准价格
    ,coalesce(case when cast(SALE_CNT as decimal(12,6))=0 then 0 else t1.S_YF_PRICE/SALE_CNT  end,'0')     --运费价格
    ,coalesce(case when cast(SALE_CNT as decimal(12,6))=0 then 0 else t1.S_FL_PRICE/SALE_CNT  end,'0')     --返利价格
    ,coalesce(t1.SALE_CNT,'0')       --销量
FROM (
     SELECT 
         MONTH_ID                    --期间(月份)
         ,DAY_ID
         ,BUS_TYPE
         ,FIFTH_ORG_ID
         ,PRODUCTION_LINE_ID
         ,PRODUCTION_LINE_DESCR
         ,BUSINESS_ID             --业务员ID
         ,BUSINESS_NAME           --业务员名称
         ,AGENT_NAME
         ,ITEM_CODE
         ,cast(SUM(RUN_PRICE*SALE_CNT) as decimal(12,6))  S_RUN_PRICE --执行价格
         ,cast(SUM(STD_PRICE*SALE_CNT) as decimal(12,6))  S_STD_PRICE --标准价格
         ,cast(SUM(YF_PRICE*SALE_CNT) as decimal(12,6))   S_YF_PRICE  --运费价格
         ,cast(SUM(FL_PRICE*SALE_CNT) as decimal(12,6))   S_FL_PRICE  --返利价格
         ,SUM(SALE_CNT)   SALE_CNT		                     --销量
      FROM (
      SELECT * FROM MREPORT_POULTRY.TMP_DWP_BIRD_PREMIUM_DD_1
       WHERE OP_DAY='${OP_DAY}'
       ) T1
      GROUP BY 
         MONTH_ID                    --期间(月份)
         ,DAY_ID
         ,BUS_TYPE
         ,FIFTH_ORG_ID
         ,PRODUCTION_LINE_ID
         ,PRODUCTION_LINE_DESCR
         ,BUSINESS_ID             --业务员ID
         ,BUSINESS_NAME           --业务员名称
         ,AGENT_NAME
         ,ITEM_CODE
    ) T1
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWP_BIRD_PREMIUM_DD_1;
    $INSERT_TMP_DWP_BIRD_PREMIUM_DD_1;
    $CREATE_DWP_BIRD_PREMIUM_DD;
    $INSERT_DWP_BIRD_PREMIUM_DD;
"  -v
