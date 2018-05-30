#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_kill_kpi_dd.sh
# 创建时间: 2018年04月12日
# 创 建 者: ch
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽屠宰指标日报
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误,调用示例: dwp_bird_kill_kpi_dd.sh 20180101"
    exit 1
fi



###########################################################################################
## 处理 产量 TZ01,TZ02 明细到物料
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_00='TMP_DWP_BIRD_KILL_KPI_DD_00'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_00="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_00(
  PERIOD_ID       STRING    --期间
  ,ITEM_ID        STRING    --物料
  ,ORG_ID         STRING    --组织
  ,INV_ORG_ID     STRING    --库存组织
  ,BUS_TYPE       STRING    --业态
  ,PRODUCT_LINE   STRING    --产线
  ,SRC            STRING    --来源
  ,PROD_QTY       STRING    --产量
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_00="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_00 PARTITION(op_day='$OP_DAY')
SELECT PERIOD_ID, --期间
       FINAL_INVENTORY_ITEM_ID ITEM_ID, --物料ID
       ORG_ID, --OU组织
       INV_ORG_ID, --库存组织ID
       BUS_TYPE, --业态--待新增
       PRODUCT_LINE, --产线
       'TZ01' SRC,
       SUM(TRANS_QTY) PROD_QTY --半成品入库主数量
  FROM mreport_poultry.DWU_QTZ_FRESH_GOODS_DD TZ01 --半成品
 WHERE TRANS_TYPE_ID IN ('103','107') --只取'冷藏厂-成品或半成品杂项入库','冷藏厂-成品或半成品杂项出库'
   AND NOT EXISTS (SELECT 1 
                     FROM mreport_global.DIM_CRM_ITEM WL01CRM
                    WHERE WL01CRM.PRD_LINE_CATE_ID != '1-16BW2M'
                      AND TZ01.FINAL_INVENTORY_ITEM_ID = WL01CRM.ITEM_ID) --排除CRM物料为副产品
   AND TZ01.OP_DAY = '$OP_DAY'
 GROUP BY TZ01.PERIOD_ID,
          FINAL_INVENTORY_ITEM_ID,
          ORG_ID,
          INV_ORG_ID,
          BUS_TYPE,
          PRODUCT_LINE          
UNION ALL
SELECT PERIOD_ID,
       ITEM_ID,
       ORG_ID,
       ORGANIZATION_ID INV_ORG_ID, --库存组织ID       
       BUS_TYPE,
       PRODUCT_LINE,
       'TZ02' SRC,
       SUM(PRIMARY_QUANTITY) PROD_QTY --鲜品产量
  FROM mreport_poultry.DWU_TZ_STORAGE_TRANSATION02_DD
 WHERE SUBINVENTORY_CODE LIKE '%XP%' --鲜品库
   AND OP_DAY = '$OP_DAY'
 GROUP BY PERIOD_ID,
          ITEM_ID,
          ORG_ID,
          BUS_TYPE,
          PRODUCT_LINE,
          ORGANIZATION_ID
"

###########################################################################################
## 处理宰杀只数、重量、金额,QW11 QW07 明细到物料、合同号
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_01='TMP_DWP_BIRD_KILL_KPI_DD_01'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_01="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_01(
  PERIOD_ID             STRING    --期间
  ,ITEM_ID              STRING    --物料
  ,ORG_ID               STRING    --组织
  ,INV_ORG_ID           STRING    --库存组织
  ,BUS_TYPE             STRING    --业态
  ,PRODUCT_LINE         STRING    --产线
  ,PITH_NO              STRING    --合同号
  ,AVG_WEIGHT           STRING    --只均重
  ,KILLED_QTY           STRING    --宰杀只数
  ,RECYCLE_WEIGHT       STRING    --结算重量
  ,RECYCLE_AMT          STRING    --结算金额
  ,RECYCLE_AMT_BEFTAX   STRING  --结算金额(去税)
  ,MATERIAL_TAX_RATE    STRING  --物料税率
  ,TAX_RATE             STRING  --销项税
  ,PURCHASE_TAX_RATE   STRING  --进项税
  ,PROD_QTY             STRING  --产量
  ,PRODUCT_TYPE         STRING  --主副产品
  ,IS_D_PRODUCT         STRING  --是否次品
  ,MATERIAL_SEGMENT5_DESC   STRING --物料5级
  ,GUARANTEES_MARKET        STRING --保值保底市场
  ,DISTANCE                 STRING --距离 
  ,PUT_QTY                  STRING --投放数量（QW03合同日期）
  ,PUT_QTY2                 STRING --投放数量 (QW11结算日期)
  ,PUT_COST                 STRING --投放成本
  ,PUT_AMT                  STRING --投放金额
  ,CARRIAGE_COST            STRING --运费金额
  ,BODY_WEIGHT              STRING --过磅计价重--胴体结算重量
  ,UNDER_50_KILLED_QTY      STRING --养殖距离50km以内只数
  ,50_80_KILLED_QTY        STRING --养殖距离50km-80KM只数
  ,OVER_80_KILLED_QTY      STRING --养殖距离80km以上只数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_01="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_01 PARTITION(op_day='$OP_DAY')
SELECT PERIOD_ID, --日期
       ITEM_ID, --物料
       ORG_ID, --公司ID
       INV_ORG_ID, --库存组织
       BUS_TYPE, --业态
       PRODUCT_LINE, --产线
       PITH_NO, --合同号
       AVG_WEIGHT, --只均重
       KILLED_QTY, --宰杀只数
       BUY_WEIGHT, --结算重量
       AMOUNT RECYCLE_AMT, --结算金额
       AMOUNT / (1 + coalesce(MATERIAL_TAX_RATE, 0)) RECYCLE_AMT_BEFTAX, --结算金额(去税)
       MATERIAL_TAX_RATE, --物料税率
       TAX_RATE,  --销项税
       PURCHASE_TAX_RATE,--进项税       
       PROD_QTY, --产量
       PRODUCT_TYPE, --主副产品
       IS_D_PRODUCT, --是否次品
       MATERIAL_SEGMENT5_DESC, --物料5级
       GUARANTEES_MARKET, --保值保底市场
       DISTANCE, --距离
       PUT_QTY, -- 投放数量（QW03合同日期）
       PUT_QTY2, -- 投放数量(QW11结算日期)
       PUT_COST, --投放成本
       PUT_AMT, --投放金额
       CARRIAGE_COST,             --运费金额
       BODY_WEIGHT,               --过磅计价重--胴体结算重量
       UNDER_50_KILLED_QTY,       --养殖距离50km以内只数
       50_80_KILLED_QTY,          --养殖距离50km-80KM只数
       OVER_80_KILLED_QTY         --养殖距离80km以上只数
  FROM (SELECT QW11.PERIOD_ID, --结算日期
               QW03.MATERIAL_ID ITEM_ID, --物料
               QW03.ORG_ID, --公司ID
               QW11.INV_ORG_ID, --库存组织
               '132020' BUS_TYPE, --业态
               QW03.PRODUCT_LINE, --产线
               QW11.PITH_NO, --合同号
               QW11.AVG_WEIGHT, --只均重
               QW11.KILLED_QTY, --宰杀只数
               QW11.BUY_WEIGHT, --结算重量
               QW11.AMOUNT, --结算金额
               coalesce(CG02.TAX_RATE, 0) MATERIAL_TAX_RATE, --物料税率
               0 TAX_RATE,  --销项税
               0 PURCHASE_TAX_RATE,--进项税     
               0 PROD_QTY, --产量
               WL01EBS2.PRODUCT_TYPE, --主副产品
               WL01EBS2.IS_D_PRODUCT, --是否次品
               WL01EBS2.MATERIAL_SEGMENT5_DESC, --物料5级
               QW03.GUARANTEES_MARKET, --保值保底市场
               QW03.DISTANCE, --距离
               0         PUT_QTY,  -- 投放数量（QW03合同日期）
               QW03.QTY  PUT_QTY2, -- 投放数量(QW11结算日期)
               0         PUT_COST,  --投放成本
               0         PUT_AMT,
               QW07.FREIGHT CARRIAGE_COST,      --运费金额
               QW07.WEIGHT  BODY_WEIGHT,        --过磅计价重--胴体结算重量
               QW072.UNDER_50_KILLED_QTY,       --养殖距离50km以内只数
               QW072.50_80_KILLED_QTY,          --养殖距离50km-80KM只数
               QW072.OVER_80_KILLED_QTY         --养殖距离80km以上只数
          FROM (SELECT REGEXP_REPLACE(SUBSTRING(JS_DATE, 1, 10),
                                          '-',
                                          '') PERIOD_ID,
                       PITH_NO, --合同号
                       INV_ORG.INV_ORG_ID, --库存组织
                       SUM(AVG_WEIGHT) AVG_WEIGHT, --只均重
                       SUM(KILLED_QTY) KILLED_QTY, --宰杀只数
                       SUM(BUY_WEIGHT) BUY_WEIGHT, --结算重量
                       SUM(AMOUNT) AMOUNT --结算金额
                  FROM mreport_poultry.DWU_QW_QW11_DD
                  LEFT JOIN mreport_global.dim_org_inv_management INV_ORG
                    ON Organization_code = INV_ORG.INV_ORG_CODE
                 WHERE DOC_STATUS IN ('已审核', '已完毕')
                   AND ITEM_CODE IN ('3501000002', '3502000002')
                   AND OP_DAY = '$OP_DAY'
                 GROUP BY PITH_NO, INV_ORG.INV_ORG_ID,REGEXP_REPLACE(SUBSTRING(JS_DATE, 1, 10),
                                          '-',
                                          '')) QW11
    INNER JOIN (SELECT CASE
                             WHEN MEANING = 'DUCK' THEN
                              '20'
                             WHEN MEANING = 'CHICHEN' THEN
                              '10'
                             ELSE
                              NULL
                           END PRODUCT_LINE,
                           REGEXP_REPLACE(SUBSTRING(KILLCHICKDATE, 1, 10),
                                          '-',
                                          '') PERIOD_ID,
                           *
                      FROM mreport_poultry.DWU_QW_CONTRACT_DD
                     WHERE OP_DAY = '$OP_DAY') QW03
            ON QW11.PITH_NO = QW03.CONTRACTNUMBER
          LEFT JOIN mreport_global.DWU_DIM_MATERIAL_NEW WL01EBS2
            ON QW03.MATERIAL_ID = WL01EBS2.INVENTORY_ITEM_ID
           AND WL01EBS2.INV_ORG_ID = '115'
          LEFT JOIN (SELECT DISTINCT CONTRACT_NUM, --批次号
                                    PERCENTAGE_RATE / 100 TAX_RATE --物料税率
                      FROM mreport_poultry.DWU_CG_RECEIVE02_DD A
                      LEFT JOIN (SELECT * FROM mreport_global.ODS_EBS_ZX_RATES_B
                                  WHERE ACTIVE_FLAG = 'Y') B
                              ON A.TAX_RATE = B.TAX_RATE_CODE
                     WHERE OP_DAY = '$OP_DAY') CG02
            ON QW11.PITH_NO = CG02.CONTRACT_NUM
          LEFT JOIN (SELECT CONTRACT_ID,
                            SUM(coalesce(FREIGHT,0)) FREIGHT,  --运费金额
                            SUM(coalesce(WEIGHT,0))  WEIGHT   --过磅计价重--胴体结算重量  
                       FROM mreport_poultry.DWU_QW_WEIGHFREIGHT_DD
                      WHERE OP_DAY = '$OP_DAY'
                      GROUP BY CONTRACT_ID
                    ) QW07
            ON QW11.PITH_NO = QW07.CONTRACT_ID
          LEFT JOIN (SELECT CONTRACT_ID,
                            SUM(CASE WHEN MILEAGE<=50
                                 THEN FACTNUMBER ELSE 0 END)  UNDER_50_KILLED_QTY,--养殖距离50km以内只数
                            SUM(CASE WHEN MILEAGE > 50 AND MILEAGE <= 80
                                 THEN FACTNUMBER ELSE 0 END)  50_80_KILLED_QTY,   --养殖距离50km-80KM只数
                            SUM(CASE WHEN MILEAGE>80
                                 THEN FACTNUMBER ELSE 0 END)  OVER_80_KILLED_QTY --养殖距离80km以上只数
                       FROM mreport_poultry.DWU_QW_WEIGHFREIGHT_DD
                      WHERE OP_DAY = '$OP_DAY'
                      GROUP BY CONTRACT_ID) QW072
            ON QW11.PITH_NO = QW072.CONTRACT_ID 
  UNION ALL
        SELECT T.PERIOD_ID, --结算日期
               T.ITEM_ID, --物料
               T.ORG_ID, --公司ID
               T.INV_ORG_ID, --库存组织
               T.BUS_TYPE, --业态
               T.PRODUCT_LINE, --产线
               '' PITH_NO, --合同号
               0 AVG_WEIGHT, --只均重
               0 KILLED_QTY, --宰杀只数
               0 BUY_WEIGHT, --结算重量
               0 AMOUNT, --结算金额
               0 MATERIAL_TAX_RATE, --物料税率
               R1.PERCENTAGE_RATE / 100 TAX_RATE,  --销项税
               R2.PERCENTAGE_RATE / 100 PURCHASE_TAX_RATE,--进项税     
               coalesce(T.PROD_QTY, 0) PROD_QTY, --产量
               WL01EBS2.PRODUCT_TYPE, --主副产品
               WL01EBS2.IS_D_PRODUCT, --是否次品
               WL01EBS2.MATERIAL_SEGMENT5_DESC, --物料5级
               '' GUARANTEES_MARKET, --保值保底市场
               '' DISTANCE, --距离
               0 PUT_QTY,  --投放数量（QW03合同日期）
               0 PUT_QTY2, --投放数量(QW11结算日期)
               0 PUT_COST, --投放成本
               0 PUT_AMT,
               0 CARRIAGE_COST,             --运费金额
               0 BODY_WEIGHT,               --过磅计价重--胴体结算重量
               0 UNDER_50_KILLED_QTY,       --养殖距离50km以内只数
               0 50_80_KILLED_QTY,          --养殖距离50km-80KM只数
               0 OVER_80_KILLED_QTY         --养殖距离80km以上只数
          FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_00 T
          LEFT JOIN mreport_global.DWU_DIM_MATERIAL_NEW WL01EBS2
            ON T.ITEM_ID = WL01EBS2.INVENTORY_ITEM_ID
           AND WL01EBS2.INV_ORG_ID = '115'
          LEFT JOIN mreport_global.DWU_DIM_MATERIAL_NEW WL01EBS
            ON T.ITEM_ID = WL01EBS.INVENTORY_ITEM_ID
           AND T.INV_ORG_ID = WL01EBS.INV_ORG_ID
          LEFT JOIN (SELECT * FROM mreport_global.ODS_EBS_ZX_RATES_B
                      WHERE ACTIVE_FLAG = 'Y') R1
                 ON WL01EBS.TAX_CODE = R1.TAX_RATE_CODE
          LEFT JOIN (SELECT * FROM mreport_global.ODS_EBS_ZX_RATES_B
                      WHERE ACTIVE_FLAG = 'Y') R2
                 ON WL01EBS.PURCHASING_TAX_CODE = R2.TAX_RATE_CODE
         WHERE T.OP_DAY = '$OP_DAY'
  UNION ALL
        SELECT REGEXP_REPLACE(SUBSTRING(CONTRACT_DATE, 1, 10),
                                          '-',
                                          '') PERIOD_ID, --结算日期
               QW03.MATERIAL_ID ITEM_ID, --物料
               QW03.ORG_ID, --公司ID
               '' INV_ORG_ID, --库存组织
               QW03.BUS_TYPE, --业态
               CASE
                             WHEN MEANING = 'DUCK' THEN
                              '20'
                             WHEN MEANING = 'CHICHEN' THEN
                              '10'
                             ELSE
                              NULL
                           END PRODUCT_LINE, --产线
               QW03.CONTRACTNUMBER PITH_NO, --合同号
               0 AVG_WEIGHT, --只均重
               0 KILLED_QTY, --宰杀只数
               0 BUY_WEIGHT, --结算重量
               0 AMOUNT, --结算金额
               0 MATERIAL_TAX_RATE, --物料税率
               0 TAX_RATE, --消项税
               0 PURCHASE_TAX_RATE,--进项税   
               0 PROD_QTY, --产量
               '' PRODUCT_TYPE, --主副产品
               '' IS_D_PRODUCT, --是否次品
               '' MATERIAL_SEGMENT5_DESC, --物料5级
               QW03.GUARANTEES_MARKET, --保值保底市场
               QW03.DISTANCE, --距离
               QW03.QTY PUT_QTY, -- 投放数量（QW03合同日期）
               0        PUT_QTY2, -- 投放数量(QW11结算日期)
               QW03.CHICKSALEMONEY * 2 PUT_COST, --投放成本 转换为kg
               QW03.QTY * QW03.CHICKSALEMONEY * 2 PUT_AMT, --投放金额
               0 CARRIAGE_COST,             --运费金额
               0 BODY_WEIGHT,               --过磅计价重--胴体结算重量
               0 UNDER_50_KILLED_QTY,       --养殖距离50km以内只数
               0 50_80_KILLED_QTY,          --养殖距离50km-80KM只数
               0 OVER_80_KILLED_QTY         --养殖距离80km以上只数
          FROM mreport_poultry.DWU_QW_CONTRACT_DD  QW03
         WHERE QW03.OP_DAY = '$OP_DAY'
) A
"

###########################################################################################
## 处理人数
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_02='TMP_DWP_BIRD_KILL_KPI_DD_02'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_02="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_02(
  PERIOD_ID       STRING    --期间
  ,ORG_ID         STRING    --组织
  ,BUS_TYPE       STRING    --业态
  ,PRODUCT_LINE   STRING    --产线
  ,HC             STRING  --在职人数
  ,ACT_HC         STRING  --实际出勤
  ,FRONT_HC       STRING  --前区在职
  ,ACT_FRONT_HC   STRING  --前区实际出勤
  ,BACK_HC        STRING  --后区在职
  ,ACT_BACK_HC    STRING  --后区实际出勤
  ,KT_HC          STRING  --库台在职
  ,ACT_KT_HC      STRING  --库台实际出勤
  ,WORKING_TIME   STRING  --工作时间
  ,MONTH_BEG_HC   STRING  --月初人数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_02="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_02 PARTITION(op_day='$OP_DAY')
SELECT T1.PERIOD_ID,T1.ORG_ID,T1.BUS_TYPE,T1.PRODUCT_LINE,T1.HC,T1.ACT_HC,T1.FRONT_HC,T1.ACT_FRONT_HC,
  T1.BACK_HC,T1.ACT_BACK_HC,T1.KT_HC,T1.ACT_KT_HC,T1.WORKING_TIME,
  coalesce(T2.MONTH_BEG_HC,0) MONTH_BEG_HC
FROM
      (SELECT
        REGEXP_REPLACE(SUBSTRING(DUTY_DATE, 1, 10),'-','') PERIOD_ID,  --期间
        INV.OU_ORG_ID ORG_ID,  --会计单位
        ORG_NAME, 
        BUS_TYPE,  --业态
        PRODUCT_LINE,  --产线
        COUNT(1)                              HC,                 --在职人数
        SUM(CASE WHEN WORKERTIME > 0
          THEN 1      ELSE 0 END)             ACT_HC,             --实际出勤
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%前区%'
          THEN 1      ELSE 0 END)             FRONT_HC,           --前区在职
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%前区%' AND WORKERTIME > 0
          THEN 1      ELSE 0 END)             ACT_FRONT_HC,       --前区实际出勤
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%后区%'
          THEN 1      ELSE 0 END)             BACK_HC,            --后区在职
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%后区%' AND WORKERTIME > 0
          THEN 1      ELSE 0 END)             ACT_BACK_HC,        --后区实际出勤
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%库台%'
          THEN 1      ELSE 0 END)             KT_HC,              --库台在职
        SUM(CASE WHEN WORKERDEPT_DESC LIKE '%库台%' AND WORKERTIME > 0
          THEN 1      ELSE 0 END)             ACT_KT_HC,          --库台实际出勤
        SUM(coalesce(WORKERTIME, 0))               WORKING_TIME        --工作时间
      FROM mreport_poultry.DWU_QHC_REFRIGER_CHECK_IN_DD TZ04
      LEFT JOIN mreport_global.DIM_ORG_INV_MANAGEMENT INV
         ON TZ04.ORG_ID = INV.INV_ORG_CODE
      WHERE OP_DAY = '$OP_DAY'
      GROUP BY REGEXP_REPLACE(SUBSTRING(DUTY_DATE, 1, 10),'-',''),
        INV.OU_ORG_ID ,
        ORG_NAME,
        BUS_TYPE,
        PRODUCT_LINE

      ) T1
LEFT JOIN
      (SELECT
         REGEXP_REPLACE(SUBSTRING(DUTY_DATE, 1, 7), '-', '') MONTH_ID,    --期间
         INV.OU_ORG_ID ORG_ID,                                            --会计单位
         ORG_NAME,   
         BUS_TYPE,                                                        --业态
         PRODUCT_LINE,                                                    --产线
         COUNT(*) MONTH_BEG_HC                                            --月初人数
       FROM (SELECT *  
               FROM mreport_poultry.DWU_QHC_REFRIGER_CHECK_IN_DD 
              WHERE SUBSTRING(DUTY_DATE, 9, 2) = '01'
         AND OP_DAY = '$OP_DAY') TZ04
        LEFT JOIN mreport_global.DIM_ORG_INV_MANAGEMENT INV
          ON TZ04.ORG_ID = INV.INV_ORG_CODE
       GROUP BY REGEXP_REPLACE(SUBSTRING(DUTY_DATE, 1, 7), '-', ''),
         INV.OU_ORG_ID,
         ORG_NAME,
         BUS_TYPE,
         PRODUCT_LINE
      ) T2
ON SUBSTR(T1.PERIOD_ID,1,6) = T2.MONTH_ID
  AND T1.ORG_ID = T2.ORG_ID
  AND T1.BUS_TYPE = T2.BUS_TYPE
  AND T1.PRODUCT_LINE = T2.PRODUCT_LINE
"


###########################################################################################
## 处理TZ08 生产过程指标
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_03='TMP_DWP_BIRD_KILL_KPI_DD_03'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_03="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_03(
  PERIOD_ID                     STRING    --期间
  ,ORG_ID                       STRING    --组织
  ,BUS_TYPE                     STRING    --业态
  ,PRODUCT_LINE                 STRING    --产线
  ,PRECOOLING_TEMPERATURE       STRING  --预冷温度
  ,PRECOOLING_TIME              STRING  --预冷时间
  ,EQUIPMENT_STAGNATION_TIME    STRING  --设备停滞时间
  ,RAW_CHICKEN_DUCK_TIME        STRING  --原料空鸡鸭时间
  ,HOOKS_NUMBER                 STRING  --钩数
  ,EMPTY_HOOKS_NUMBER           STRING  --空钩个数
  ,EMPTY_HOOKS_RATE             STRING  --空钩率
  ,DEATH_NUMBER                 STRING  --途中死亡只数
  ,WORKING_DAY                  STRING  --生产天数
  ,KILLING_DAY                  STRING  --均衡宰杀天数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_03="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_03 PARTITION(op_day='$OP_DAY')
SELECT PERIOD_ID,
       ORG_ID,
       BUS_TYPE,
       PRODUCT_LINE,
       AVG(PRECOOLING_TEMPERATURE) PRECOOLING_TEMPERATURE,          --预冷温度 --业务确认每天每个公司只会有一条温度数据0416
       SUM(PRECOOLING_TIME) PRECOOLING_TIME,                        --预冷时间
       SUM(EQUIPMENT_STAGNATION_TIME) EQUIPMENT_STAGNATION_TIME,    --设备停滞时间
       SUM(RAW_CHICKEN_DUCK_TIME) RAW_CHICKEN_DUCK_TIME,            --原料空鸡鸭时间
       SUM(HOOKS_NUMBER) HOOKS_NUMBER,                              --钩数
       SUM(EMPTY_HOOKS_NUMBER) EMPTY_HOOKS_NUMBER,                  --空钩个数
       SUM(EMPTY_HOOKS_RATE) EMPTY_HOOKS_RATE,                      --空钩率
       SUM(DEATH_NUMBER) DEATH_NUMBER,                              --途中死亡只数
       COUNT(1) WORKING_DAY,                                        --生产天数
       SUM(CASE WHEN SLAUGHTER_FLAG = 'Y' THEN 1 ELSE 0 END) KILLING_DAY --均衡宰杀天数
  FROM mreport_poultry.DWU_QTZ_PRODUCT_MANAGE_DD TZ08
WHERE OP_DAY = '$OP_DAY'
GROUP BY  PERIOD_ID,
       ORG_ID,
       BUS_TYPE,
       PRODUCT_LINE
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_0='TMP_DWP_BIRD_KILL_KPI_DD_0'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_0(
  PERIOD_ID             STRING    --期间
  ,ITEM_ID              STRING    --物料
  ,ORG_ID               STRING    --组织
  ,INV_ORG_ID           STRING    --组织
  ,BUS_TYPE             STRING    --业态
  ,PRODUCT_LINE         STRING    --产线
  ,PITH_NO              STRING    --合同号
  ,KILLED_QTY           STRING    --宰杀只数
  ,AVG_WEIGHT           STRING    --只均重
  ,RECYCLE_WEIGHT       STRING    --结算重量
  ,RECYCLE_AMT          STRING    --结算金额
  ,RECYCLE_AMT_BEFTAX   STRING  --结算金额(去税)
  ,PROD_QTY             STRING  --产量
  ,CARRIAGE_COST        STRING  --运费金额
  ,CONTRACT_CARRIAGE_COST       STRING  --  合同运费,
  ,VALUE_CARRIAGE_COST          STRING  --  保值运费,
  ,MINI_CARRIAGE_COST           STRING  --  保底运费,
  ,MARKET_CARRIAGE_COST         STRING  --  市场运费,
  ,BODY_WEIGHT          STRING  --过磅计价重--胴体结算重量
  ,MAINPROD_AMT         STRING  --主产结算金额
  ,BYPROD_AMT           STRING  --副产结算金额
  ,MAINPROD_QTY         STRING  --主产入库量
  ,BYPROD_QTY           STRING  --副产入库量
  ,DEF_PROD_QTY         STRING  --次品入库量
  ,DEF_HEAD_QTY         STRING  --次头入库量
  ,DEF_NECK_QTY         STRING    --次脖入库量
  ,DEF_WING_QTY         STRING    --次二节翅量
  ,DEF_ROOT_WING_QTY    STRING    --次翅根入库量
  ,DEF_FEET_QTY         STRING    --次掌入库量
  ,CONTRACT_RECYCLE_AMT         STRING    --合同结算金额
  ,VALUE_CONTRACT_RECYCLE_AMT   STRING    --保值结算金额
  ,MINI_CONTRACT_RECYCLE_AMT    STRING    --保底结算金额
  ,MARKET_RECYCLE_AMT           STRING    --市场结算金额
  ,CONTRACT_RECYCLE_AMT_BEFTAX         STRING    --合同结算金额去税
  ,VALUE_CONTRACT_RECYCLE_AMT_BEFTAX   STRING    --保值结算金额去税
  ,MINI_CONTRACT_RECYCLE_AMT_BEFTAX    STRING    --保底结算金额去税
  ,MARKET_RECYCLE_AMT_BEFTAX           STRING    --市场结算金额去税
  ,CONTRACT_QTY         STRING    --合同宰杀只数
  ,VALUE_CONTRACT_QTY   STRING    --保值宰杀只数
  ,MINI_CONTRACT_QTY    STRING    --保底宰杀只数
  ,MARKET_QTY           STRING    --市场宰杀只数
  ,CONTRACT_WEIGHT         STRING    --合同结算重量
  ,VALUE_CONTRACT_WEIGHT   STRING    --保值结算重量
  ,MINI_CONTRACT_WEIGHT    STRING    --保底结算重量
  ,MARKET_WEIGHT           STRING    --市场结算重量
  ,PUT_QTY                  STRING --投放数量（QW03合同日期）
  ,PUT_QTY2                 STRING --投放数量 (QW11结算日期)
  ,PUT_COST             STRING    --投放成本
  ,PUT_AMT              STRING --投放金额
  ,VALUE_PUT_QTY        STRING    --保值投放数量
  ,VALUE_PUT_COST       STRING    --保值投放成本
  ,VALUE_PUT_AMT        STRING    --保值投放金额
  ,BEST_RANGE_QTY       STRING    --最佳只重只数
  ,UNDER_4_QTY          STRING    --4斤以下只数（只）
  ,401_450_QTY          STRING    --4.01_4.50斤只数（只）
  ,451_480_QTY          STRING    --4.51_4.80斤只数(只)
  ,481_550_QTY          STRING    --4.81_5.50斤只数(只)
  ,OVER_551_QTY         STRING    --5.51斤以上只数（只）
  ,UNDER_5_QTY          STRING    --5斤以下只数(只）
  ,501_550_QTY          STRING    --5.01_5.50斤只数（只）
  ,551_570_QTY          STRING    --5.51_5.70斤只数（只）
  ,571_6_QTY            STRING    --5.71_6斤只数(只)
  ,601_630_QTY          STRING    --6.01_6.3斤只数（只）
  ,OVER_631_QTY         STRING    --6.3斤以上只数(只)
  ,UNDER_50_KILLED_QTY  STRING    --养殖距离50km以内只数
  ,50_80_KILLED_QTY     STRING    --养殖距离50km-80KM只数
  ,OVER_80_KILLED_QTY   STRING    --养殖距离80km以上只数
  ,TAX_RATE             STRING    --进项税 
  ,PURCHASE_TAX_RATE    STRING    --销项税码
  ,PRICE_WITH_TAX       STRING    --含税单价 --当前售价 XS07005
  ,SALES_AMT            STRING    --销售金额 产量*当前售价/(1+进项税)
  ,T_SALES_AMT          STRING    --销售金额 产量*当前售价/(1+进项税)*(1+销项税码)
  ,MAINPROD_SALES_AMT   STRING    --主产金额 主产产量*当前售价/税率
  ,BYPROD_SALES_AMT     STRING    --副产金额 副产产量*当前售价/税率
  ,CONTRACT_SALES_AMT         STRING    --合同金额 合同产量*当前售价/税率
  ,MARKET_SALES_AMT           STRING    --市场金额 市场产量*当前售价/税率
  ,CONTRACT_PROD_QTY        STRING  --合同产量
  ,VALUE_CONTRACT_PROD_QTY  STRING  --保值合同产量
  ,MINI_CONTRACT_PROD_QTY   STRING  --保底合同产量
  ,MARKET_PROD_QTY          STRING  --市场产量
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_0="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_0 PARTITION(op_day='$OP_DAY')
SELECT
  T.PERIOD_ID,
  T.ITEM_ID,
  T.ORG_ID,
  T.INV_ORG_ID,
  T.BUS_TYPE,
  T.PRODUCT_LINE,
  T.PITH_NO,                                      --合同号
  T.KILLED_QTY,                                   --宰杀只数
  T.AVG_WEIGHT,                                   --只均重
  T.RECYCLE_WEIGHT,                               --结算重量
  T.RECYCLE_AMT,                                  --结算金额
  T.RECYCLE_AMT_BEFTAX,                           --去税金额
  T.PROD_QTY,                                     --产量
  CARRIAGE_COST,  --运费金额
  CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.CARRIAGE_COST  ELSE 0 END            CONTRACT_CARRIAGE_COST,  --  合同原料运费
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.CARRIAGE_COST  ELSE 0 END VALUE_CARRIAGE_COST,  --  保值原料运费
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.CARRIAGE_COST  ELSE 0 END  MINI_CARRIAGE_COST,  --  保底原料运费
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.CARRIAGE_COST  ELSE 0 END MARKET_CARRIAGE_COST,  --  市场原料运费
  BODY_WEIGHT,    --过磅计价重--胴体结算重量
  CASE WHEN T.PRODUCT_TYPE = '主产'
    THEN RECYCLE_AMT ELSE 0 END                   MAINPROD_AMT,   --主产结算金额
  CASE WHEN T.PRODUCT_TYPE = '副产'
    THEN RECYCLE_AMT ELSE 0 END                   BYPROD_AMT,     --副产结算金额
  CASE WHEN T.PRODUCT_TYPE = '主产'
    THEN T.PROD_QTY ELSE 0 END                    MAINPROD_QTY,   --主产产量
  CASE WHEN T.PRODUCT_TYPE = '副产'
    THEN T.PROD_QTY ELSE 0 END                    BYPROD_QTY,     --副产产量
  CASE WHEN T.IS_D_PRODUCT = 'Y'
    THEN T.PROD_QTY ELSE 0 END                    DEF_PROD_QTY,  --次品入库量
  CASE WHEN T.IS_D_PRODUCT = 'Y' AND T.MATERIAL_SEGMENT5_DESC IN ('鸭头类','鸡头D类')
    THEN T.PROD_QTY ELSE 0 END                    DEF_HEAD_QTY,  --次头入库量
  CASE WHEN T.IS_D_PRODUCT = 'Y' AND T.MATERIAL_SEGMENT5_DESC IN ('鸭脖类','鸡去皮脖类','鸡带皮脖类')
    THEN T.PROD_QTY ELSE 0 END                    DEF_NECK_QTY,  --次脖入库量
  CASE WHEN T.IS_D_PRODUCT = 'Y' AND T.MATERIAL_SEGMENT5_DESC IN ('鸭二节翅类')
    THEN T.PROD_QTY ELSE 0 END                    DEF_WING_QTY,  --次二节量
  CASE WHEN T.IS_D_PRODUCT = 'Y' AND T.MATERIAL_SEGMENT5_DESC IN ('鸭翅根类','鸡翅根类')
    THEN T.PROD_QTY ELSE 0 END                    DEF_ROOT_WING_QTY,  --次翅根入库量
  CASE WHEN T.IS_D_PRODUCT = 'Y' AND T.MATERIAL_SEGMENT5_DESC IN ('鸭掌D类','鸡爪D类')
    THEN T.PROD_QTY ELSE 0 END                    DEF_FEET_QTY,--次掌入库量
  CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.RECYCLE_AMT  ELSE 0 END           CONTRACT_RECYCLE_AMT,        --合同结算金额
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.RECYCLE_AMT  ELSE 0 END           VALUE_CONTRACT_RECYCLE_AMT,  --保值合同结算金额
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.RECYCLE_AMT  ELSE 0 END           MINI_CONTRACT_RECYCLE_AMT,   --保底合同结算金额
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.RECYCLE_AMT  ELSE 0 END           MARKET_RECYCLE_AMT,          --市场结算金额
    CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.RECYCLE_AMT_BEFTAX  ELSE 0 END    CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.RECYCLE_AMT_BEFTAX  ELSE 0 END    VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.RECYCLE_AMT_BEFTAX  ELSE 0 END    MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.RECYCLE_AMT_BEFTAX  ELSE 0 END    MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
   CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.KILLED_QTY  ELSE 0 END            CONTRACT_QTY,        --合同宰杀只数
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.KILLED_QTY  ELSE 0 END            VALUE_CONTRACT_QTY,  --保值合同宰杀只数
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.KILLED_QTY  ELSE 0 END            MINI_CONTRACT_QTY,   --保底合同宰杀只数
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.KILLED_QTY  ELSE 0 END            MARKET_QTY,          --市场宰杀只数
  CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.RECYCLE_WEIGHT  ELSE 0 END        CONTRACT_WEIGHT,     --合同结算重量
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.RECYCLE_WEIGHT  ELSE 0 END        VALUE_CONTRACT_WEIGHT,  --保值合同结算重量
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.RECYCLE_WEIGHT  ELSE 0 END        MINI_CONTRACT_WEIGHT,  --保底合同结算重量
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.RECYCLE_WEIGHT  ELSE 0 END        MARKET_WEIGHT,        --市场结算重量
  T.PUT_QTY                           PUT_QTY, -- 投放数量（QW03合同日期）
  T.PUT_QTY2                          PUT_QTY2,--投放数量（QW11期间）
  T.PUT_COST                          PUT_COST, --投放成本
  T.PUT_AMT                           PUT_AMT,--投放金额
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN coalesce(T.PUT_QTY,0)     ELSE 0 END     VALUE_PUT_QTY,        --保值投放数量
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN coalesce(T.PUT_COST,0)   ELSE 0 END      VALUE_PUT_COST,       --保值投放成本
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN coalesce(T.PUT_AMT,0)   ELSE 0 END      VALUE_PUT_COST,       --保值投放金额
  CASE WHEN T.AVG_WEIGHT >= QW12.BEST_WEIGHT_FROM 
        AND T.AVG_WEIGHT <= QW12.BEST_WEIGHT_TO
       THEN T.KILLED_QTY ELSE 0 END          BEST_RANGE_QTY,       --最佳只重只数
  CASE WHEN T.AVG_WEIGHT*2<4
    THEN T.KILLED_QTY ELSE 0 END             UNDER_4_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=4.01 AND T.AVG_WEIGHT*2 <= 4.5
    THEN T.KILLED_QTY ELSE 0 END             401_450_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=4.51 AND T.AVG_WEIGHT*2 <= 4.8
    THEN T.KILLED_QTY ELSE 0 END             451_480_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=4.81 AND T.AVG_WEIGHT*2 <= 5.5
    THEN T.KILLED_QTY ELSE 0 END             481_550_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=5.51
    THEN T.KILLED_QTY ELSE 0 END             OVER_551_QTY,
  CASE WHEN T.AVG_WEIGHT*2 < 5
    THEN T.KILLED_QTY ELSE 0 END             UNDER_5_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=5.01 AND T.AVG_WEIGHT*2 <= 5.5
    THEN T.KILLED_QTY ELSE 0 END             501_550_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=5.51 AND T.AVG_WEIGHT*2 <= 5.7
    THEN T.KILLED_QTY ELSE 0 END             551_570_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=5.71 AND T.AVG_WEIGHT*2 <= 6
    THEN T.KILLED_QTY ELSE 0 END             571_6_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=6.01 AND T.AVG_WEIGHT*2 <= 6.3
    THEN T.KILLED_QTY ELSE 0 END             601_630_QTY,
  CASE WHEN T.AVG_WEIGHT*2>=6.31
    THEN T.KILLED_QTY ELSE 0 END             OVER_631_QTY,
  UNDER_50_KILLED_QTY,                       --养殖距离50km以内只数
  50_80_KILLED_QTY,                          --养殖距离50km-80KM只数
  OVER_80_KILLED_QTY,                        --养殖距离80km以上只数
  TAX_RATE,                                  --销项税
  PURCHASE_TAX_RATE,                         --进项税
  coalesce(XS07.PRICE_WITH_TAX,0) PRICE_WITH_TAX, --含税单价 --当前售价 XS07005
  coalesce(XS07.PRICE_WITH_TAX,0) * T.PROD_QTY  / (1 + coalesce(TAX_RATE,0)) SALES_AMT, --产量*当前售价/(1+消项税)
  coalesce(XS07.PRICE_WITH_TAX,0) * T.PROD_QTY  / (1 + coalesce(TAX_RATE,0)) * (1 + coalesce(PURCHASE_TAX_RATE,0)) T_SALES_AMT, --产量*当前售价/(1+消项税)*(1+进项税）
  CASE WHEN T.PRODUCT_TYPE = '主产' 
    THEN T.PROD_QTY * coalesce(XS07.PRICE_WITH_TAX,0) / (1 + coalesce(TAX_RATE,0))    
    ELSE 0 END                                MAINPROD_SALES_AMT, --主产产量*当前售价/(1+税率)
  CASE WHEN T.PRODUCT_TYPE = '副产' 
    THEN T.PROD_QTY * coalesce(XS07.PRICE_WITH_TAX,0) /  (1 + coalesce(TAX_RATE,0))    
    ELSE 0 END                                BYPROD_SALES_AMT,   --副产产量*当前售价/(1+税率)
   CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.PROD_QTY * coalesce(XS07.PRICE_WITH_TAX,0) /  (1 + coalesce(TAX_RATE,0))   
    ELSE 0 END  CONTRACT_SALES_AMT,  --合同金额 合同产量*当前售价/(1+税率)
   CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.PROD_QTY * coalesce(XS07.PRICE_WITH_TAX,0) /  (1 + coalesce(TAX_RATE,0))  
    ELSE 0 END  MARKET_SALES_AMT,    --市场金额 市场产量*当前售价/(1+税率)
  CASE WHEN T.GUARANTEES_MARKET IN ('保值', '保底')
    THEN T.PROD_QTY  ELSE 0 END            CONTRACT_PROD_QTY,  --合同产量
  CASE WHEN T.GUARANTEES_MARKET = '保值'
    THEN T.PROD_QTY  ELSE 0 END            VALUE_CONTRACT_PROD_QTY,  --保值合同产量
  CASE WHEN T.GUARANTEES_MARKET = '保底'
    THEN T.PROD_QTY  ELSE 0 END            MINI_CONTRACT_PROD_QTY,  --保底合同产量
  CASE WHEN T.GUARANTEES_MARKET  = '市场'
    THEN T.PROD_QTY  ELSE 0 END            MARKET_PROD_QTY  --市场产量
FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_01 T
LEFT JOIN (SELECT *
               FROM mreport_poultry.DWU_GYL_XS07_DD
              WHERE OP_DAY = '$OP_DAY') XS07
        ON T.PERIOD_ID  = REGEXP_REPLACE(SUBSTRING(XS07.update_date, 1, 10),
                                          '-',
                                          '')
       AND T.ORG_ID     = XS07.ORG_ID
       AND T.ITEM_ID    = XS07.ITEM_ID
LEFT JOIN (SELECT OP_DAY,
                  ORG_ID,
                  CASE WHEN KPI_TYPE = '鸡'THEN '10'
                    WHEN  KPI_TYPE = '鸭'THEN '20'
                    ELSE KPI_TYPE END PRODUCT_LINE,
                  BEST_WEIGHT_FROM,
                  BEST_WEIGHT_TO
             FROM mreport_poultry.DWU_QW_QW12_DD
            WHERE OP_DAY = '$OP_DAY') QW12 
        ON T.PERIOD_ID = QW12.OP_DAY
       AND T.ORG_ID = QW12.ORG_ID
       AND T.PRODUCT_LINE = QW12.PRODUCT_LINE
WHERE T.OP_DAY = '$OP_DAY'
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_1='TMP_DWP_BIRD_KILL_KPI_DD_1'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_1(
  PERIOD_ID             STRING    --期间
  ,ORG_ID               STRING    --组织
  ,BUS_TYPE             STRING    --业态
  ,PRODUCT_LINE         STRING    --产线
  ,KILLED_QTY           STRING    --宰杀只数
  ,AVG_WEIGHT           STRING    --只均重
  ,RECYCLE_WEIGHT       STRING    --结算重量
  ,RECYCLE_AMT          STRING    --结算金额
  ,RECYCLE_AMT_BEFTAX   STRING    --结算金额(去税)
  ,PROD_QTY             STRING    --产量
  ,CARRIAGE_COST        STRING    --运费金额
  ,CONTRACT_CARRIAGE_COST       STRING  --  合同运费,
  ,VALUE_CARRIAGE_COST          STRING  --  保值运费,
  ,MINI_CARRIAGE_COST           STRING  --  保底运费,
  ,MARKET_CARRIAGE_COST         STRING  --  市场运费,
  ,BODY_WEIGHT          STRING    --过磅计价重--胴体结算重量
  ,MAINPROD_AMT         STRING    --主产结算金额
  ,BYPROD_AMT           STRING    --副产结算金额
  ,MAINPROD_QTY         STRING    --主产入库量
  ,BYPROD_QTY           STRING    --副产入库量
  ,DEF_PROD_QTY         STRING    --次品入库量
  ,DEF_HEAD_QTY         STRING    --次头入库量
  ,DEF_NECK_QTY         STRING    --次脖入库量
  ,DEF_WING_QTY         STRING    --次二节翅量
  ,DEF_ROOT_WING_QTY    STRING    --次翅根入库量
  ,DEF_FEET_QTY         STRING    --次掌入库量
  ,CONTRACT_RECYCLE_AMT         STRING    --合同结算金额
  ,VALUE_CONTRACT_RECYCLE_AMT   STRING    --保值结算金额
  ,MINI_CONTRACT_RECYCLE_AMT    STRING    --保底结算金额
  ,MARKET_RECYCLE_AMT           STRING    --市场结算金额
  ,CONTRACT_RECYCLE_AMT_BEFTAX         STRING    --合同结算金额去税
  ,VALUE_CONTRACT_RECYCLE_AMT_BEFTAX   STRING    --保值结算金额去税
  ,MINI_CONTRACT_RECYCLE_AMT_BEFTAX    STRING    --保底结算金额去税
  ,MARKET_RECYCLE_AMT_BEFTAX           STRING    --市场结算金额去税
  ,CONTRACT_QTY         STRING    --合同结算量
  ,VALUE_CONTRACT_QTY   STRING    --保值结算量
  ,MINI_CONTRACT_QTY    STRING    --保底结算量
  ,MARKET_QTY           STRING    --市场结算量
  ,CONTRACT_WEIGHT         STRING    --合同结算重量
  ,VALUE_CONTRACT_WEIGHT   STRING    --保值结算重量
  ,MINI_CONTRACT_WEIGHT    STRING    --保底结算重量
  ,MARKET_WEIGHT           STRING    --市场结算重量
  ,PUT_QTY                  STRING --投放数量（QW03合同日期）
  ,PUT_QTY2                 STRING --投放数量 (QW11结算日期)
  ,PUT_COST             STRING    --投放成本
  ,PUT_AMT              STRING --投放金额
  ,VALUE_PUT_QTY        STRING    --保值投放数量
  ,VALUE_PUT_COST       STRING    --保值投放成本
  ,VALUE_PUT_AMT        STRING    --保值投放金额
  ,BEST_RANGE_QTY       STRING    --最佳只重只数
  ,UNDER_4_QTY          STRING    --4斤以下只数（只）
  ,401_450_QTY          STRING    --4.01_4.50斤只数（只）
  ,451_480_QTY          STRING    --4.51_4.80斤只数(只)
  ,481_550_QTY          STRING    --4.81_5.50斤只数(只)
  ,OVER_551_QTY         STRING    --5.50斤以上只数（只）
  ,UNDER_5_QTY          STRING    --5斤以下只数(只）
  ,501_550_QTY          STRING    --5.01_5.50斤只数（只）
  ,551_570_QTY          STRING    --5.51_5.70斤只数（只）
  ,571_6_QTY            STRING    --5.71_6斤只数(只)
  ,601_630_QTY          STRING    --6.01_6.3斤只数（只）
  ,OVER_631_QTY         STRING    --6.3斤以上只数(只)
  ,UNDER_50_KILLED_QTY  STRING    --养殖距离50km以内只数
  ,50_80_KILLED_QTY     STRING    --养殖距离50km-80KM只数
  ,OVER_80_KILLED_QTY   STRING    --养殖距离80km以上只数
  ,SALES_AMT            STRING    --产量*当前售价
  ,T_SALES_AMT          STRING    --销售金额 产量*当前售价/(1+进项税)*(1+销项税码)
  ,MAINPROD_SALES_AMT   STRING    --主产产量*当前售价
  ,BYPROD_SALES_AMT     STRING    --副产产量*当前售价
  ,CONTRACT_SALES_AMT  STRING  --合同金额
  ,MARKET_SALES_AMT    STRING  --市场金额
  ,CONTRACT_PROD_QTY        STRING  --合同产量
  ,VALUE_CONTRACT_PROD_QTY  STRING  --保值合同产量
  ,MINI_CONTRACT_PROD_QTY   STRING  --保底合同产量
  ,MARKET_PROD_QTY          STRING  --市场产量

  ,WIP_QTY                  STRING  --  CW产量,
  ,SALE_QTY                 STRING  --  CW销量,
  ,FREIGHT_FEE              STRING  --  原料运费,
  ,PACKING_FEE              STRING  --  包装费,
  ,G_WIP_FIX                STRING  --  G制造费用固定,
  ,G_WIP_CHG                STRING  --  G制造费用变动,
  ,G_WATER_ELEC             STRING  --  G水电费,
  ,G_FUEL                   STRING  --  G燃料费,
  ,G_MANUAL                 STRING  --  G直接人工费,
  ,INPUT_TAX                STRING  --  预计可抵扣进项税,
  ,SECND_INPUT              STRING  --  预计副产品收入,
  ,MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,FINANCIAL_FEE            STRING  --  财务费用
  ,TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
  ,TAX_RATE                 STRING  --  销项税
  ,PURCHASE_TAX_RATE        STRING  --  进项税
  ,T_FREIGHT_FEE              STRING  --  原料运费,
  ,T_PACKING_FEE              STRING  --  包装费,
  ,T_G_WIP_FIX                STRING  --  G制造费用固定,
  ,T_G_WIP_CHG                STRING  --  G制造费用变动,
  ,T_G_WATER_ELEC             STRING  --  G水电费,
  ,T_G_FUEL                   STRING  --  G燃料费,
  ,T_G_MANUAL                 STRING  --  G直接人工费,
  ,T_SECND_INPUT              STRING  --  预计副产品收入,
  ,T_MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,T_MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,T_SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,T_SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,T_FINANCIAL_FEE            STRING  --  财务费用
  ,T_TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_1 PARTITION(op_day='$OP_DAY')
SELECT T.PERIOD_ID, --期间
       T.ORG_ID, --组织
       T.BUS_TYPE, --业态
       T.PRODUCT_LINE, --产线
       KILLED_QTY, --宰杀只数
       AVG_WEIGHT, --只均重
       RECYCLE_WEIGHT, --结算重量
       RECYCLE_AMT, --结算金额
       RECYCLE_AMT_BEFTAX, --结算金额(去税
       PROD_QTY, --产量
       CARRIAGE_COST, --运费金额
       CONTRACT_CARRIAGE_COST,        --  合同运费,
       VALUE_CARRIAGE_COST,            --  保值运费,
       MINI_CARRIAGE_COST,             --  保底运费,
       MARKET_CARRIAGE_COST,           --  市场运费,
       BODY_WEIGHT, --过磅计价重--胴体结算重量
       MAINPROD_AMT, --主产结算金额
       BYPROD_AMT, --副产结算金额
       MAINPROD_QTY, --主产入库量
       BYPROD_QTY, --副产入库量
       DEF_PROD_QTY, --次品入库量
       DEF_HEAD_QTY, --次头入库量
       DEF_NECK_QTY, --次脖入库量
       DEF_WING_QTY, --次二节翅量
       DEF_ROOT_WING_QTY, --次翅根入库量
       DEF_FEET_QTY, --次掌入库量
       CONTRACT_RECYCLE_AMT, --合同结算金额
       VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
       MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
       MARKET_RECYCLE_AMT, --市场结算金额
       CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
       VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
       MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
       MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
       CONTRACT_QTY, --合同宰杀只数
       VALUE_CONTRACT_QTY, --保值宰杀只数
       MINI_CONTRACT_QTY, --保底宰杀只数
       MARKET_QTY, --市场宰杀只数
       CONTRACT_WEIGHT, --合同结算重量
       VALUE_CONTRACT_WEIGHT, --保值结算重量
       MINI_CONTRACT_WEIGHT, --保底结算重量
       MARKET_WEIGHT, --市场结结算重量
       PUT_QTY, --投放数量
       PUT_QTY2,
       PUT_COST, --投放成本
       PUT_AMT,--投放金额
       VALUE_PUT_QTY, --保值投放数量
       VALUE_PUT_COST, --保值投放成本
       VALUE_PUT_AMT, --保值投放金额
       BEST_RANGE_QTY, --最佳只重只数
       UNDER_4_QTY, --4斤以下只数（只）
       401_450_QTY, --4.01_4.50斤只数（只）
       451_480_QTY, --4.51_4.80斤只数(只
       481_550_QTY, --4.81_5.50斤只数(只
       OVER_551_QTY, --5.50斤以上只数（只）
       UNDER_5_QTY, --5斤以下只数(只）
       501_550_QTY, --5.01_5.50斤只数（只）
       551_570_QTY, --5.51_5.70斤只数（只）
       571_6_QTY, --5.71_6斤只数(只
       601_630_QTY, --6.01_6.3斤只数（只）
       OVER_631_QTY, --6.3斤以上只数(只
       UNDER_50_KILLED_QTY, --养殖距离50km以内只数
       50_80_KILLED_QTY, --养殖距离50km-80KM只数
       OVER_80_KILLED_QTY, --养殖距离80km以上只数
       SALES_AMT, --产量*当前售价/税率
       SALES_AMT* (1 + R1.PERCENTAGE_RATE / 100) T_SALES_AMT, --销售金额 产量*当前售价/(1+进项税)*(1+销项税码)
       MAINPROD_SALES_AMT, --主产产量*当前售价/税率
       BYPROD_SALES_AMT, --副产产量*当前售价/税率
       CONTRACT_SALES_AMT, --合同金额
       MARKET_SALES_AMT, --市场金额
       CONTRACT_PROD_QTY, --合同产量
       VALUE_CONTRACT_PROD_QTY, --保值合同产量
       MINI_CONTRACT_PROD_QTY, --保底合同产量
       MARKET_PROD_QTY, --市场产量
       coalesce(WIP_QTY,0) WIP_QTY, --CW产量
       coalesce(SALE_QTY,0) SALE_QTY, --CW销量
       coalesce(FREIGHT_FEE,0) FREIGHT_FEE, --原料运费
       coalesce(PACKING_FEE,0) PACKING_FEE, --包装费
       coalesce(G_WIP_FIX,0) G_WIP_FIX, --G制造费用固定
       coalesce(G_WIP_CHG,0) G_WIP_CHG, --G制造费用变动
       coalesce(G_WATER_ELEC,0) G_WATER_ELEC, --G水电费
       coalesce(G_FUEL,0) G_FUEL, --G燃料费
       coalesce(G_MANUAL,0) G_MANUAL, --G直接人工费
       coalesce(INPUT_TAX,0) INPUT_TAX, --预计可抵扣进项税
       coalesce(SECND_INPUT,0) SECND_INPUT, --预计副产品收入
       coalesce(MANAGE_FIXED_FEE,0) MANAGE_FIXED_FEE, --管理费用可控
       coalesce(MANAGE_CHG_FEE,0) MANAGE_CHG_FEE, --管理费用非可控
       coalesce(SALES_FIXED_FEE,0) SALES_FIXED_FEE, --销售费用可控
       coalesce(SALES_CHG_FEE,0) SALES_CHG_FEE, --销售费用非可控
       coalesce(FINANCIAL_FEE,0) FINANCIAL_FEE, --财务费用
       coalesce(TON_FREIGHT_RATE,0) TON_FREIGHT_RATE, --下月吨费用变动系数
       R2.PERCENTAGE_RATE / 100 TAX_RATE ,        --销项税
       R1.PERCENTAGE_RATE / 100 PURCHASE_TAX_RATE,--进项税       
       coalesce(FREIGHT_FEE,0) * (1 + R1.PERCENTAGE_RATE / 100)  T_FREIGHT_FEE, --原料运费
       coalesce(PACKING_FEE,0) * (1 + R1.PERCENTAGE_RATE / 100)  T_PACKING_FEE, --包装费
       coalesce(G_WIP_FIX,0)*(1 + R1.PERCENTAGE_RATE / 100) T_G_WIP_FIX, --G制造费用固定
       coalesce(G_WIP_CHG,0)*(1 + R1.PERCENTAGE_RATE / 100) T_G_WIP_CHG, --G制造费用变动
       coalesce(G_WATER_ELEC,0)*(1 + R1.PERCENTAGE_RATE / 100) T_G_WATER_ELEC, --G水电费
       coalesce(G_FUEL,0)*(1 + R1.PERCENTAGE_RATE / 100) T_G_FUEL, --G燃料费
       coalesce(G_MANUAL,0)*(1 + R1.PERCENTAGE_RATE / 100) T_G_MANUAL, --G直接人工费
       coalesce(SECND_INPUT,0)*(1 + R1.PERCENTAGE_RATE / 100) T_SECND_INPUT, --预计副产品收入
       coalesce(MANAGE_FIXED_FEE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_MANAGE_FIXED_FEE, --管理费用可控
       coalesce(MANAGE_CHG_FEE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_MANAGE_CHG_FEE, --管理费用非可控
       coalesce(SALES_FIXED_FEE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_SALES_FIXED_FEE, --销售费用可控
       coalesce(SALES_CHG_FEE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_SALES_CHG_FEE, --销售费用非可控
       coalesce(FINANCIAL_FEE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_FINANCIAL_FEE, --财务费用
       coalesce(TON_FREIGHT_RATE,0)*(1 + R1.PERCENTAGE_RATE / 100) T_TON_FREIGHT_RATE --下月吨费用变动系数
  FROM (SELECT PERIOD_ID, --期间
               ORG_ID, --组织
               BUS_TYPE, --业态
               PRODUCT_LINE, --产线
               ITEM_ID, --物料
               INV_ORG_ID,
               SUM(KILLED_QTY) KILLED_QTY, --宰杀只数
               SUM(AVG_WEIGHT) AVG_WEIGHT, --只均重
               SUM(RECYCLE_WEIGHT) RECYCLE_WEIGHT, --结算重量
               SUM(RECYCLE_AMT) RECYCLE_AMT, --结算金额
               SUM(RECYCLE_AMT_BEFTAX) RECYCLE_AMT_BEFTAX, --结算金额(去税)
               SUM(PROD_QTY) PROD_QTY, --产量
               SUM(CARRIAGE_COST) CARRIAGE_COST, --运费金额
               SUM(CONTRACT_CARRIAGE_COST) CONTRACT_CARRIAGE_COST,     --  合同运费
               SUM(VALUE_CARRIAGE_COST)  VALUE_CARRIAGE_COST,          --  保值运费
               SUM(MINI_CARRIAGE_COST)   MINI_CARRIAGE_COST,           --  保底运费
               SUM(MARKET_CARRIAGE_COST) MARKET_CARRIAGE_COST,         --  市场运费
               SUM(BODY_WEIGHT) BODY_WEIGHT, --过磅计价重--胴体结算重量
               SUM(MAINPROD_AMT) MAINPROD_AMT, --主产结算金额
               SUM(BYPROD_AMT) BYPROD_AMT, --副产结算金额
               SUM(MAINPROD_QTY) MAINPROD_QTY, --主产入库量
               SUM(BYPROD_QTY) BYPROD_QTY, --副产入库量
               SUM(DEF_PROD_QTY) DEF_PROD_QTY, --次品入库量
               SUM(DEF_HEAD_QTY) DEF_HEAD_QTY, --次头入库量
               SUM(DEF_NECK_QTY) DEF_NECK_QTY, --次脖入库量
               SUM(DEF_WING_QTY) DEF_WING_QTY, --次二节翅量
               SUM(DEF_ROOT_WING_QTY) DEF_ROOT_WING_QTY, --次翅根入库量
               SUM(DEF_FEET_QTY) DEF_FEET_QTY, --次掌入库量
               SUM(CONTRACT_RECYCLE_AMT) CONTRACT_RECYCLE_AMT, --合同结算金额
               SUM(VALUE_CONTRACT_RECYCLE_AMT) VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
               SUM(MINI_CONTRACT_RECYCLE_AMT) MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
               SUM(MARKET_RECYCLE_AMT) MARKET_RECYCLE_AMT, --市场结算金额
               SUM(CONTRACT_RECYCLE_AMT_BEFTAX) CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
               SUM(VALUE_CONTRACT_RECYCLE_AMT_BEFTAX) VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
               SUM(MINI_CONTRACT_RECYCLE_AMT_BEFTAX) MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
               SUM(MARKET_RECYCLE_AMT_BEFTAX) MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
               SUM(CONTRACT_QTY) CONTRACT_QTY, --合同宰杀只数
               SUM(VALUE_CONTRACT_QTY) VALUE_CONTRACT_QTY, --保值宰杀只数
               SUM(MINI_CONTRACT_QTY) MINI_CONTRACT_QTY, --保底宰杀只数
               SUM(MARKET_QTY) MARKET_QTY, --市场宰杀只数
               SUM(CONTRACT_WEIGHT) CONTRACT_WEIGHT, --合同结算重量
               SUM(VALUE_CONTRACT_WEIGHT) VALUE_CONTRACT_WEIGHT, --保值结算重量
               SUM(MINI_CONTRACT_WEIGHT) MINI_CONTRACT_WEIGHT, --保底结算重量
               SUM(MARKET_WEIGHT) MARKET_WEIGHT, --市场结结算重量
               SUM(PUT_QTY) PUT_QTY, --投放数量
               SUM(PUT_QTY2) PUT_QTY2,
               SUM(PUT_COST) PUT_COST, --投放成本
               SUM(PUT_AMT) PUT_AMT,--投放金额
               SUM(VALUE_PUT_QTY) VALUE_PUT_QTY, --保值投放数量
               SUM(VALUE_PUT_COST) VALUE_PUT_COST, --保值投放成本
               SUM(VALUE_PUT_AMT) VALUE_PUT_AMT, --保值投放金额
               SUM(BEST_RANGE_QTY) BEST_RANGE_QTY, --最佳只重只数
               SUM(UNDER_4_QTY) UNDER_4_QTY, --4斤以下只数（只）
               SUM(401_450_QTY) 401_450_QTY, --4.01_4.50斤只数（只）
               SUM(451_480_QTY) 451_480_QTY, --4.51_4.80斤只数(只)
               SUM(481_550_QTY) 481_550_QTY, --4.81_5.50斤只数(只)
               SUM(OVER_551_QTY) OVER_551_QTY, --5.50斤以上只数（只）
               SUM(UNDER_5_QTY) UNDER_5_QTY, --5斤以下只数(只）
               SUM(501_550_QTY) 501_550_QTY, --5.01_5.50斤只数（只）
               SUM(551_570_QTY) 551_570_QTY, --5.51_5.70斤只数（只）
               SUM(571_6_QTY) 571_6_QTY, --5.71_6斤只数(只)
               SUM(601_630_QTY) 601_630_QTY, --6.01_6.3斤只数（只）
               SUM(OVER_631_QTY) OVER_631_QTY, --6.3斤以上只数(只)
               SUM(UNDER_50_KILLED_QTY) UNDER_50_KILLED_QTY, --养殖距离50km以内只数
               SUM(50_80_KILLED_QTY) 50_80_KILLED_QTY, --养殖距离50km-80KM只数
               SUM(OVER_80_KILLED_QTY) OVER_80_KILLED_QTY, --养殖距离80km以上只数
               SUM(SALES_AMT) SALES_AMT, --产量*当前售价/税率
               SUM(T_SALES_AMT) T_SALES_AMT,    --销售金额 产量*当前售价/(1+进项税)*(1+销项税码)
               SUM(MAINPROD_SALES_AMT) MAINPROD_SALES_AMT, --主产产量*当前售价/税率
               SUM(BYPROD_SALES_AMT) BYPROD_SALES_AMT, --副产产量*当前售价/税率
               SUM(CONTRACT_SALES_AMT) CONTRACT_SALES_AMT, --合同金额
               SUM(MARKET_SALES_AMT) MARKET_SALES_AMT, --市场金额
               SUM(CONTRACT_PROD_QTY) CONTRACT_PROD_QTY, --合同产量
               SUM(VALUE_CONTRACT_PROD_QTY) VALUE_CONTRACT_PROD_QTY, --保值合同产量
               SUM(MINI_CONTRACT_PROD_QTY) MINI_CONTRACT_PROD_QTY, --保底合同产量
               SUM(MARKET_PROD_QTY) MARKET_PROD_QTY --市场产量
          FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_0
         WHERE OP_DAY = '$OP_DAY'
         GROUP BY PERIOD_ID,
               ORG_ID,
               BUS_TYPE,
               PRODUCT_LINE,
               ITEM_ID, --物料
               INV_ORG_ID) T
  LEFT JOIN (SELECT REGEXP_REPLACE(PERIOD_ID, '-', '') PERIOD_ID, --  月
                    ORG_ID,
                    BUS_TYPE,
                    T.PRODUCT_LINE,
                    CASE WHEN T.product_line = '10' THEN '34754'
                         WHEN T.product_line = '20' THEN '25375'
                    ELSE 0 END ITEM_ID,--物料
                    MIN(ORG.level7_org_id) INV_ORG_ID,--库存组织
                    WIP_QTY, --CW产量
                    SALE_QTY, --CW销量
                    TON_FREIGHT_FEE * coalesce(WIP_QTY,0) / 1000  FREIGHT_FEE, --原料运费
                    TON_PACKING_FEE * coalesce(WIP_QTY,0) / 1000  PACKING_FEE, --包装费
                    G_WIP_FIX, --G制造费用固定
                    G_WIP_CHG, --G制造费用变动
                    G_WATER_ELEC, --G水电费
                    G_FUEL, --G燃料费
                    G_MANUAL, --G直接人工费
                    INPUT_TAX, --预计可抵扣进项税
                    SECND_INPUT, --预计副产品收入
                    MANAGE_FIXED_FEE, --管理费用可控
                    MANAGE_CHG_FEE, --管理费用非可控
                    SALES_FIXED_FEE, --销售费用可控
                    SALES_CHG_FEE, --销售费用非可控
                    FINANCIAL_FEE, --财务费用
                    TON_FREIGHT_RATE --下月吨费用变动系数
               FROM mreport_poultry.DWU_CW_CW27_DD T
          LEFT JOIN mreport_global.ODS_EBS_CUX_ORG_STRUCTURES_ALL ORG
                 on  T.org_id = ORG.level6_org_id
                and  T.BUS_TYPE = ORG.level4_bus_type
              WHERE OP_DAY = '$OP_DAY'
              GROUP BY PERIOD_ID,
                    ORG_ID,
                    BUS_TYPE,
                    T.PRODUCT_LINE,
                    WIP_QTY, --CW产量
                    SALE_QTY, --CW销量
                    TON_FREIGHT_FEE , --原料运费
                    TON_PACKING_FEE, --包装费
                    G_WIP_FIX, --G制造费用固定
                    G_WIP_CHG, --G制造费用变动
                    G_WATER_ELEC, --G水电费
                    G_FUEL, --G燃料费
                    G_MANUAL, --G直接人工费
                    INPUT_TAX, --预计可抵扣进项税
                    SECND_INPUT, --预计副产品收入
                    MANAGE_FIXED_FEE, --管理费用可控
                    MANAGE_CHG_FEE, --管理费用非可控
                    SALES_FIXED_FEE, --销售费用可控
                    SALES_CHG_FEE, --销售费用非可控
                    FINANCIAL_FEE, --财务费用
                    TON_FREIGHT_RATE --下月吨费用变动系数
                    ) CW27
    ON SUBSTR(T.PERIOD_ID, 1, 6) = CW27.PERIOD_ID
   AND T.ORG_ID = CW27.ORG_ID
   AND T.BUS_TYPE = CW27.BUS_TYPE
   AND T.PRODUCT_LINE = CW27.PRODUCT_LINE
  LEFT JOIN mreport_global.DWU_DIM_MATERIAL_NEW WL01EBS
    ON CW27.ITEM_ID = WL01EBS.INVENTORY_ITEM_ID
   AND CW27.INV_ORG_ID = WL01EBS.INV_ORG_ID
  LEFT JOIN (SELECT * FROM mreport_global.ODS_EBS_ZX_RATES_B
              WHERE ACTIVE_FLAG = 'Y') R1
    ON WL01EBS.PURCHASING_TAX_CODE = R1.TAX_RATE_CODE --进项税码
  LEFT JOIN (SELECT * FROM mreport_global.ODS_EBS_ZX_RATES_B
              WHERE ACTIVE_FLAG = 'Y') R2
    ON WL01EBS.TAX_CODE = R2.TAX_RATE_CODE           --销项税码
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KILL_KPI_DD_2='TMP_DWP_BIRD_KILL_KPI_DD_2'

CREATE_TMP_DWP_BIRD_KILL_KPI_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KILL_KPI_DD_2(
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
,level7_org_id                 STRING    --组织7级(库存组织)
,level7_org_descr              STRING    --组织7级(库存组织)
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
  ,KILLED_QTY           STRING    --宰杀只数
  ,AVG_WEIGHT           STRING    --只均重
  ,RECYCLE_WEIGHT       STRING    --结算重量
  ,RECYCLE_AMT          STRING    --结算金额
  ,RECYCLE_AMT_BEFTAX   STRING  --结算金额(去税)
  ,PROD_QTY             STRING  --产量
  ,CARRIAGE_COST        STRING  --运费金额
  ,CONTRACT_CARRIAGE_COST       STRING  --  合同运费,
  ,VALUE_CARRIAGE_COST          STRING  --  保值运费,
  ,MINI_CARRIAGE_COST           STRING  --  保底运费,
  ,MARKET_CARRIAGE_COST         STRING  --  市场运费,
  ,BODY_WEIGHT          STRING --过磅计价重--胴体结算重量
  ,MAINPROD_AMT         STRING    --主产结算金额
  ,BYPROD_AMT           STRING    --副产结算金额
  ,MAINPROD_QTY         STRING   --主产入库量
  ,BYPROD_QTY           STRING   --副产入库量
  ,DEF_PROD_QTY         STRING   --次品入库量
  ,DEF_HEAD_QTY         STRING   --次头入库量
  ,DEF_NECK_QTY         STRING   --次脖入库量
  ,DEF_WING_QTY         STRING    --次二节翅量
  ,DEF_ROOT_WING_QTY    STRING    --次翅根入库量
  ,DEF_FEET_QTY         STRING    --次掌入库量
  ,CONTRACT_RECYCLE_AMT         STRING    --合同结算金额
  ,VALUE_CONTRACT_RECYCLE_AMT   STRING    --保值结算金额
  ,MINI_CONTRACT_RECYCLE_AMT    STRING    --保底结算金额
  ,MARKET_RECYCLE_AMT           STRING    --市场结算金额
  ,CONTRACT_RECYCLE_AMT_BEFTAX         STRING    --合同结算金额去税
  ,VALUE_CONTRACT_RECYCLE_AMT_BEFTAX   STRING    --保值结算金额去税
  ,MINI_CONTRACT_RECYCLE_AMT_BEFTAX    STRING    --保底结算金额去税
  ,MARKET_RECYCLE_AMT_BEFTAX           STRING    --市场结算金额去税
  ,CONTRACT_QTY         STRING    --合同结算量
  ,VALUE_CONTRACT_QTY   STRING    --保值结算量
  ,MINI_CONTRACT_QTY    STRING    --保底结算量
  ,MARKET_QTY           STRING    --市场结算量
  ,CONTRACT_WEIGHT         STRING    --合同结算重量
  ,VALUE_CONTRACT_WEIGHT   STRING    --保值结算重量
  ,MINI_CONTRACT_WEIGHT    STRING    --保底结算重量
  ,MARKET_WEIGHT           STRING    --市场结算重量
  ,PUT_QTY                  STRING --投放数量（QW03合同日期）
  ,PUT_QTY2                 STRING --投放数量 (QW11结算日期)
  ,PUT_COST             STRING    --投放成本
  ,PUT_AMT              STRING    --投放金额
  ,VALUE_PUT_QTY        STRING    --保值投放数量
  ,VALUE_PUT_COST       STRING    --保值投放成本
  ,VALUE_PUT_AMT        STRING    --保值投放金额
  ,BEST_RANGE_QTY       STRING    --最佳只重只数
  ,UNDER_4_QTY          STRING    --4斤以下只数（只）
  ,401_450_QTY          STRING    --4.01_4.50斤只数（只）
  ,451_480_QTY          STRING    --4.51_4.80斤只数(只)
  ,481_550_QTY          STRING    --4.81_5.50斤只数(只)
  ,OVER_551_QTY         STRING    --5.50斤以上只数（只）
  ,UNDER_5_QTY          STRING    --5斤以下只数(只）
  ,501_550_QTY          STRING    --5.01_5.50斤只数（只）
  ,551_570_QTY          STRING    --5.51_5.70斤只数（只）
  ,571_6_QTY            STRING    --5.71_6斤只数(只)
  ,601_630_QTY          STRING    --6.01_6.3斤只数（只）
  ,OVER_631_QTY         STRING    --6.3斤以上只数(只)
  ,UNDER_50_KILLED_QTY  STRING    --养殖距离50km以内只数
  ,50_80_KILLED_QTY     STRING    --养殖距离50km-80KM只数
  ,OVER_80_KILLED_QTY   STRING    --养殖距离80km以上只数
  ,SALES_AMT            STRING    --产量*当前售价
  ,T_SALES_AMT          STRING    --产量*当前售价*税
  ,MAINPROD_SALES_AMT   STRING    --主产产量*当前售价
  ,BYPROD_SALES_AMT     STRING    --副产产量*当前售价
  ,CONTRACT_SALES_AMT   STRING  --合同金额
  ,MARKET_SALES_AMT     STRING  --市场金额
  ,CONTRACT_PROD_QTY        STRING  --合同产量
  ,VALUE_CONTRACT_PROD_QTY  STRING  --保值合同产量
  ,MINI_CONTRACT_PROD_QTY   STRING  --保底合同产量
  ,MARKET_PROD_QTY          STRING  --市场产量
  ,WIP_QTY                  STRING  --  CW产量,
  ,SALE_QTY                 STRING  --  CW销量,
  ,FREIGHT_FEE              STRING  --  原料运费,
  ,PACKING_FEE              STRING  --  包装费,
  ,G_WIP_FIX                STRING  --  G制造费用固定,
  ,G_WIP_CHG                STRING  --  G制造费用变动,
  ,G_WATER_ELEC             STRING  --  G水电费,
  ,G_FUEL                   STRING  --  G燃料费,
  ,G_MANUAL                 STRING  --  G直接人工费,
  ,INPUT_TAX                STRING  --  预计可抵扣进项税,
  ,SECND_INPUT              STRING  --  预计副产品收入,
  ,MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,FINANCIAL_FEE            STRING  --  财务费用
  ,TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
  ,T_FREIGHT_FEE              STRING  --  原料运费,
  ,T_PACKING_FEE              STRING  --  包装费,
  ,T_G_WIP_FIX                STRING  --  G制造费用固定,
  ,T_G_WIP_CHG                STRING  --  G制造费用变动,
  ,T_G_WATER_ELEC             STRING  --  G水电费,
  ,T_G_FUEL                   STRING  --  G燃料费,
  ,T_G_MANUAL                 STRING  --  G直接人工费,
  ,T_SECND_INPUT              STRING  --  预计副产品收入,
  ,T_MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,T_MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,T_SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,T_SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,T_FINANCIAL_FEE            STRING  --  财务费用
  ,T_TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
  ,HC                   STRING  --在职人数
  ,ACT_HC               STRING  --实际出勤
  ,FRONT_HC             STRING  --前区在职
  ,ACT_FRONT_HC         STRING  --前区实际出勤
  ,BACK_HC              STRING  --后区在职
  ,ACT_BACK_HC          STRING  --后区实际出勤
  ,KT_HC                STRING  --库台在职
  ,ACT_KT_HC            STRING  --库台实际出勤
  ,WORKING_TIME         STRING  --工作时间
  ,MONTH_BEG_HC         STRING  --月初人数
  ,PRECOOLING_TEMPERATURE       STRING  --预冷温度
  ,PRECOOLING_TIME              STRING  --预冷时间
  ,EQUIPMENT_STAGNATION_TIME    STRING  --设备停滞时间
  ,RAW_CHICKEN_DUCK_TIME        STRING  --原料空鸡鸭时间
  ,HOOKS_NUMBER                 STRING  --钩数
  ,EMPTY_HOOKS_NUMBER           STRING  --空钩个数
  ,EMPTY_HOOKS_RATE             STRING  --空钩率
  ,DEATH_NUMBER                 STRING  --途中死亡只数
  ,WORKING_DAY                  STRING  --生产天数
  ,KILLING_DAY                  STRING  --均衡宰杀天数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KILL_KPI_DD_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KILL_KPI_DD_2 PARTITION(op_day='$OP_DAY')
SELECT SUBSTRING(T.PERIOD_ID, 1, 6) MONTH_ID,
       T.PERIOD_ID DAY_ID,
       CASE WHEN t2.level1_org_id    is null THEN coalesce(t3.level1_org_id,'-1') ELSE coalesce(t2.level1_org_id,'-1')  END as level1_org_id,                --一级组织编码
       CASE WHEN t2.level1_org_descr is null THEN coalesce(t3.level1_org_descr,'缺失') ELSE coalesce(t2.level1_org_descr,'缺失')  END as level1_org_descr,   --一级组织描述
       CASE WHEN t2.level2_org_id    is null THEN coalesce(t3.level2_org_id,'-1') ELSE coalesce(t2.level2_org_id,'-1')  END as level2_org_id,                --二级组织编码
       CASE WHEN t2.level2_org_descr is null THEN coalesce(t3.level2_org_descr,'缺失') ELSE coalesce(t2.level2_org_descr,'缺失')  END as level2_org_descr,   --二级组织描述
       CASE WHEN t2.level3_org_id    is null THEN coalesce(t3.level3_org_id,'-1') ELSE coalesce(t2.level3_org_id,'-1')  END as level3_org_id,               --三级组织编码
       CASE WHEN t2.level3_org_descr is null THEN coalesce(t3.level3_org_descr,'缺失') ELSE coalesce(t2.level3_org_descr,'缺失')  END as level3_org_descr,   --三级组织描述
       CASE WHEN t2.level4_org_id    is null THEN coalesce(t3.level4_org_id,'-1') ELSE coalesce(t2.level4_org_id,'-1')  END as level4_org_id,                --四级组织编码
       CASE WHEN t2.level4_org_descr is null THEN coalesce(t3.level4_org_descr,'缺失') ELSE coalesce(t2.level4_org_descr,'缺失')  END as level4_org_descr,   --四级组织描述
       CASE WHEN t2.level5_org_id    is null THEN coalesce(t3.level5_org_id,'-1') ELSE coalesce(t2.level5_org_id,'-1')  END as level5_org_id,                --五级组织编码
       CASE WHEN t2.level5_org_descr is null THEN coalesce(t3.level5_org_descr,'缺失') ELSE coalesce(t2.level5_org_descr,'缺失')  END as level5_org_descr,   --五级组织描述
       CASE WHEN t2.level6_org_id    is null THEN coalesce(t3.level6_org_id,'-1') ELSE coalesce(t2.level6_org_id,'-1')  END as level6_org_id,                --六级组织编码
       CASE WHEN t2.level6_org_descr is null THEN coalesce(t3.level6_org_descr,'缺失') ELSE coalesce(t2.level6_org_descr,'缺失')  END as level6_org_descr,   --六级组织描述
       '' LEVEL7_ORG_ID,
       '' LEVEL7_ORG_DESCR,
       T5.LEVEL1_BUSINESSTYPE_ID,
       T5.LEVEL1_BUSINESSTYPE_NAME,
       T5.LEVEL2_BUSINESSTYPE_ID,
       T5.LEVEL2_BUSINESSTYPE_NAME,
       T5.LEVEL3_BUSINESSTYPE_ID,
       T5.LEVEL3_BUSINESSTYPE_NAME,
       T5.LEVEL4_BUSINESSTYPE_ID,
       T5.LEVEL4_BUSINESSTYPE_NAME,
       CASE T.PRODUCT_LINE
         WHEN 10 THEN
          '1'
         WHEN 20 THEN
          '2'
         ELSE
          NULL
       END PRODUCTION_LINE_ID, --产线
       CASE T.PRODUCT_LINE
         WHEN 10 THEN
          '鸡线'
         WHEN 20 THEN
          '鸭线'
         ELSE
          NULL
       END PRODUCTION_LINE_DESCR,
       KILLED_QTY, --宰杀只数
       AVG_WEIGHT, --只均重
       RECYCLE_WEIGHT, --结算重量
       RECYCLE_AMT, --结算金额
       RECYCLE_AMT_BEFTAX, --结算金额(去税
       PROD_QTY, --产量
       CARRIAGE_COST, --运费金额
       CONTRACT_CARRIAGE_COST,        --  合同运费
       VALUE_CARRIAGE_COST,            --  保值运费
       MINI_CARRIAGE_COST,             --  保底运费
       MARKET_CARRIAGE_COST,           --  市场运费
       BODY_WEIGHT, --过磅计价重--胴体结算重量
       MAINPROD_AMT, --主产结算金额
       BYPROD_AMT, --副产结算金额
       MAINPROD_QTY, --主产入库量
       BYPROD_QTY, --副产入库量
       DEF_PROD_QTY, --次品入库量
       DEF_HEAD_QTY, --次头入库量
       DEF_NECK_QTY, --次脖入库量
       DEF_WING_QTY, --次二节翅量
       DEF_ROOT_WING_QTY, --次翅根入库量
       DEF_FEET_QTY, --次掌入库量
       CONTRACT_RECYCLE_AMT, --合同结算金额
       VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
       MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
       MARKET_RECYCLE_AMT, --市场结算金额
       CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
       VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
       MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
       MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
       CONTRACT_QTY, --合同宰杀只数
       VALUE_CONTRACT_QTY, --保值宰杀只数
       MINI_CONTRACT_QTY, --保底宰杀只数
       MARKET_QTY, --市场宰杀只数
       CONTRACT_WEIGHT, --合同结算重量
       VALUE_CONTRACT_WEIGHT, --保值结算重量
       MINI_CONTRACT_WEIGHT, --保底结算重量
       MARKET_WEIGHT, --市场结结算重量
       PUT_QTY, --投放数量
       PUT_QTY2,
       PUT_COST, --投放成本
       PUT_AMT,--投放金额
       VALUE_PUT_QTY, --保值投放数量
       VALUE_PUT_COST, --保值投放成本
       VALUE_PUT_AMT, --保值投放金额
       BEST_RANGE_QTY, --最佳只重只数
       UNDER_4_QTY, --4斤以下只数（只）
       401_450_QTY, --4.01_4.50斤只数（只）
       451_480_QTY, --4.51_4.80斤只数(只
       481_550_QTY, --4.81_5.50斤只数(只
       OVER_551_QTY, --5.50斤以上只数（只）
       UNDER_5_QTY, --5斤以下只数(只）
       501_550_QTY, --5.01_5.50斤只数（只）
       551_570_QTY, --5.51_5.70斤只数（只）
       571_6_QTY, --5.71_6斤只数(只
       601_630_QTY, --6.01_6.3斤只数（只）
       OVER_631_QTY, --6.3斤以上只数(只
       UNDER_50_KILLED_QTY, --养殖距离50km以内只数
       50_80_KILLED_QTY, --养殖距离50km-80KM只数
       OVER_80_KILLED_QTY, --养殖距离80km以上只数
       SALES_AMT            ,    --产量*当前售价/税率
       T_SALES_AMT          ,    --产量*当前售价/税率*税率
       MAINPROD_SALES_AMT   ,    --主产产量*当前售价/税率
       BYPROD_SALES_AMT     ,    --副产产量*当前售价/税率
       CONTRACT_SALES_AMT,  --合同金额
       MARKET_SALES_AMT,  --市场金额
       CONTRACT_PROD_QTY,  --合同产量
       VALUE_CONTRACT_PROD_QTY,  --保值合同产量
       MINI_CONTRACT_PROD_QTY,  --保底合同产量
       MARKET_PROD_QTY,  --市场产量
       WIP_QTY, --CW产量
       SALE_QTY, --CW销量
       FREIGHT_FEE, --原料运费
       PACKING_FEE, --包装费
       G_WIP_FIX, --G制造费用固定
       G_WIP_CHG, --G制造费用变动
       G_WATER_ELEC, --G水电费
       G_FUEL, --G燃料费
       G_MANUAL, --G直接人工费
       INPUT_TAX, --预计可抵扣进项税
       SECND_INPUT, --预计副产品收入
       MANAGE_FIXED_FEE, --管理费用可控
       MANAGE_CHG_FEE, --管理费用非可控
       SALES_FIXED_FEE, --销售费用可控
       SALES_CHG_FEE, --销售费用非可控
       FINANCIAL_FEE, --财务费用
       TON_FREIGHT_RATE, --下月吨费用变动系数
       T_FREIGHT_FEE, --原料运费
       T_PACKING_FEE, --包装费
       T_G_WIP_FIX, --G制造费用固定
       T_G_WIP_CHG, --G制造费用变动
       T_G_WATER_ELEC, --G水电费
       T_G_FUEL, --G燃料费
       T_G_MANUAL, --G直接人工费
       T_SECND_INPUT, --预计副产品收入
       T_MANAGE_FIXED_FEE, --管理费用可控
       T_MANAGE_CHG_FEE, --管理费用非可控
       T_SALES_FIXED_FEE, --销售费用可控
       T_SALES_CHG_FEE, --销售费用非可控
       T_FINANCIAL_FEE, --财务费用
       T_TON_FREIGHT_RATE, --下月吨费用变动系数
       HC, --在职人数
       ACT_HC, --实际出勤
       FRONT_HC, --前区在职
       ACT_FRONT_HC, --前区实际出勤
       BACK_HC, --后区在职
       ACT_BACK_HC, --后区实际出勤
       KT_HC, --库台在职
       ACT_KT_HC, --库台实际出勤
       WORKING_TIME, --工作时间
       MONTH_BEG_HC, --月初人数
       PRECOOLING_TEMPERATURE, --预冷温度
       PRECOOLING_TIME, --预冷时间
       EQUIPMENT_STAGNATION_TIME, --设备停滞时间
       RAW_CHICKEN_DUCK_TIME, --原料空鸡鸭时间
       HOOKS_NUMBER, --钩数
       EMPTY_HOOKS_NUMBER, --空钩个数
       EMPTY_HOOKS_RATE, --空钩率
       DEATH_NUMBER, --途中死亡只数
       WORKING_DAY, --生产天数
       KILLING_DAY --均衡宰杀天数
  FROM (
  SELECT PERIOD_ID, --期间
       ORG_ID, --组织
       BUS_TYPE, --业态
       PRODUCT_LINE, --产线
       KILLED_QTY, --宰杀只数
       AVG_WEIGHT, --只均重
       RECYCLE_WEIGHT, --结算重量
       RECYCLE_AMT, --结算金额
       RECYCLE_AMT_BEFTAX, --结算金额(去税
       PROD_QTY, --产量
       CARRIAGE_COST, --运费金额
       CONTRACT_CARRIAGE_COST,        --  合同运费,
       VALUE_CARRIAGE_COST,            --  保值运费,
       MINI_CARRIAGE_COST,             --  保底运费,
       MARKET_CARRIAGE_COST,           --  市场运费,
       BODY_WEIGHT, --过磅计价重--胴体结算重量
       MAINPROD_AMT, --主产结算金额
       BYPROD_AMT, --副产结算金额
       MAINPROD_QTY, --主产入库量
       BYPROD_QTY, --副产入库量
       DEF_PROD_QTY, --次品入库量
       DEF_HEAD_QTY, --次头入库量
       DEF_NECK_QTY, --次脖入库量
       DEF_WING_QTY, --次二节翅量
       DEF_ROOT_WING_QTY, --次翅根入库量
       DEF_FEET_QTY, --次掌入库量
       CONTRACT_RECYCLE_AMT, --合同结算金额
       VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
       MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
       MARKET_RECYCLE_AMT, --市场结算金额
       CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
       VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
       MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
       MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
       CONTRACT_QTY, --合同宰杀只数
       VALUE_CONTRACT_QTY, --保值宰杀只数
       MINI_CONTRACT_QTY, --保底宰杀只数
       MARKET_QTY, --市场宰杀只数
       CONTRACT_WEIGHT, --合同结算重量
       VALUE_CONTRACT_WEIGHT, --保值结算重量
       MINI_CONTRACT_WEIGHT, --保底结算重量
       MARKET_WEIGHT, --市场结结算重量
       PUT_QTY, --投放数量
       PUT_QTY2,
       PUT_COST, --投放成本
       PUT_AMT,--投放金额
       VALUE_PUT_QTY, --保值投放数量
       VALUE_PUT_COST, --保值投放成本
       VALUE_PUT_AMT, --保值投放金额
       BEST_RANGE_QTY, --最佳只重只数
       UNDER_4_QTY, --4斤以下只数（只）
       401_450_QTY, --4.01_4.50斤只数（只）
       451_480_QTY, --4.51_4.80斤只数(只
       481_550_QTY, --4.81_5.50斤只数(只
       OVER_551_QTY, --5.50斤以上只数（只）
       UNDER_5_QTY, --5斤以下只数(只）
       501_550_QTY, --5.01_5.50斤只数（只）
       551_570_QTY, --5.51_5.70斤只数（只）
       571_6_QTY, --5.71_6斤只数(只
       601_630_QTY, --6.01_6.3斤只数（只）
       OVER_631_QTY, --6.3斤以上只数(只
       UNDER_50_KILLED_QTY, --养殖距离50km以内只数
       50_80_KILLED_QTY, --养殖距离50km-80KM只数
       OVER_80_KILLED_QTY, --养殖距离80km以上只数
       SALES_AMT            ,    --产量*当前售价/税率
       T_SALES_AMT          ,    --产量*当前售价/税率*税率
       MAINPROD_SALES_AMT   ,    --主产产量*当前售价/税率
       BYPROD_SALES_AMT     ,    --副产产量*当前售价/税率
       CONTRACT_SALES_AMT,  --合同金额
       MARKET_SALES_AMT,  --市场金额
       CONTRACT_PROD_QTY,  --合同产量
       VALUE_CONTRACT_PROD_QTY,  --保值合同产量
       MINI_CONTRACT_PROD_QTY,  --保底合同产量
       MARKET_PROD_QTY,  --市场产量
       WIP_QTY, --CW产量
       SALE_QTY, --CW销量
       FREIGHT_FEE, --原料运费
       PACKING_FEE, --包装费
       G_WIP_FIX, --G制造费用固定
       G_WIP_CHG, --G制造费用变动
       G_WATER_ELEC, --G水电费
       G_FUEL, --G燃料费
       G_MANUAL, --G直接人工费
       INPUT_TAX, --预计可抵扣进项税
       SECND_INPUT, --预计副产品收入
       MANAGE_FIXED_FEE, --管理费用可控
       MANAGE_CHG_FEE, --管理费用非可控
       SALES_FIXED_FEE, --销售费用可控
       SALES_CHG_FEE, --销售费用非可控
       FINANCIAL_FEE, --财务费用
       TON_FREIGHT_RATE, --下月吨费用变动系数
       T_FREIGHT_FEE, --原料运费
       T_PACKING_FEE, --包装费
       T_G_WIP_FIX, --G制造费用固定
       T_G_WIP_CHG, --G制造费用变动
       T_G_WATER_ELEC, --G水电费
       T_G_FUEL, --G燃料费
       T_G_MANUAL, --G直接人工费
       T_SECND_INPUT, --预计副产品收入
       T_MANAGE_FIXED_FEE, --管理费用可控
       T_MANAGE_CHG_FEE, --管理费用非可控
       T_SALES_FIXED_FEE, --销售费用可控
       T_SALES_CHG_FEE, --销售费用非可控
       T_FINANCIAL_FEE, --财务费用
       T_TON_FREIGHT_RATE, --下月吨费用变动系数
       0 HC, --在职人数
       0 ACT_HC, --实际出勤
       0 FRONT_HC, --前区在职
       0 ACT_FRONT_HC, --前区实际出勤
       0 BACK_HC, --后区在职
       0 ACT_BACK_HC, --后区实际出勤
       0 KT_HC, --库台在职
       0 ACT_KT_HC, --库台实际出勤
       0 WORKING_TIME, --工作时间
       0 MONTH_BEG_HC, --月初人数
       0 PRECOOLING_TEMPERATURE, --预冷温度
       0 PRECOOLING_TIME, --预冷时间
       0 EQUIPMENT_STAGNATION_TIME, --设备停滞时间
       0 RAW_CHICKEN_DUCK_TIME, --原料空鸡鸭时间
       0 HOOKS_NUMBER, --钩数
       0 EMPTY_HOOKS_NUMBER, --空钩个数
       0 EMPTY_HOOKS_RATE, --空钩率
       0 DEATH_NUMBER, --途中死亡只数
       0 WORKING_DAY, --生产天数
       0 KILLING_DAY --均衡宰杀天数
  FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_1 T1
 WHERE OP_DAY = '$OP_DAY'
UNION ALL
SELECT PERIOD_ID, --期间
       ORG_ID, --组织
       BUS_TYPE, --业态
       PRODUCT_LINE, --产线
       0 KILLED_QTY, --宰杀只数
       0 AVG_WEIGHT, --只均重
       0 RECYCLE_WEIGHT, --结算重量
       0 RECYCLE_AMT, --结算金额
       0 RECYCLE_AMT_BEFTAX, --结算金额(去税)
       0 PROD_QTY, --产量
       0 CARRIAGE_COST, --运费金额
       0 CONTRACT_CARRIAGE_COST,        --  合同运费,
       0 VALUE_CARRIAGE_COST,            --  保值运费,
       0 MINI_CARRIAGE_COST,             --  保底运费,
       0 MARKET_CARRIAGE_COST,           --  市场运费,
       0 BODY_WEIGHT, --过磅计价重--胴体结算重量
       0 MAINPROD_AMT, --主产结算金额
       0 BYPROD_AMT, --副产结算金额
       0 MAINPROD_QTY, --主产入库量
       0 BYPROD_QTY, --副产入库量
       0 DEF_PROD_QTY, --次品入库量
       0 DEF_HEAD_QTY, --次头入库量
       0 DEF_NECK_QTY, --次脖入库量
       0 DEF_WING_QTY, --次二节翅量
       0 DEF_ROOT_WING_QTY, --次翅根入库量
       0 DEF_FEET_QTY, --次掌入库量
       0 CONTRACT_RECYCLE_AMT, --合同结算金额
       0 VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
       0 MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
       0 MARKET_RECYCLE_AMT, --市场结算金额
       0 CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
       0 VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
       0 MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
       0 MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
       0 CONTRACT_QTY, --合同宰杀只数
       0 VALUE_CONTRACT_QTY, --保值宰杀只数
       0 MINI_CONTRACT_QTY, --保底宰杀只数
       0 MARKET_QTY, --市场宰杀只数
       0 CONTRACT_WEIGHT, --合同结算重量
       0 VALUE_CONTRACT_WEIGHT, --保值结算重量
       0 MINI_CONTRACT_WEIGHT, --保底结算重量
       0 MARKET_WEIGHT, --市场结结算重量
       0 PUT_QTY, --投放数量
       0 PUT_QTY2,
       0 PUT_COST, --投放成本
       0 PUT_AMT,--投放金额
       0 VALUE_PUT_QTY, --保值投放数量
       0 VALUE_PUT_COST, --保值投放成本
       0 VALUE_PUT_AMT, --保值投放金额
       0 BEST_RANGE_QTY, --最佳只重只数
       0 UNDER_4_QTY, --4斤以下只数（只）
       0 401_450_QTY, --4.01_4.50斤只数（只）
       0 451_480_QTY, --4.51_4.80斤只数(只)
       0 481_550_QTY, --4.81_5.50斤只数(只)
       0 OVER_551_QTY, --5.50斤以上只数（只）
       0 UNDER_5_QTY, --5斤以下只数(只）
       0 501_550_QTY, --5.01_5.50斤只数（只）
       0 551_570_QTY, --5.51_5.70斤只数（只）
       0 571_6_QTY, --5.71_6斤只数(只)
       0 601_630_QTY, --6.01_6.3斤只数（只）
       0 OVER_631_QTY, --6.3斤以上只数(只)
       0 UNDER_50_KILLED_QTY, --养殖距离50km以内只数
       0 50_80_KILLED_QTY, --养殖距离50km-80KM只数
       0 OVER_80_KILLED_QTY, --养殖距离80km以上只数
       0 SALES_AMT, --产量*当前售价/税率
       0 T_SALES_AMT,    --产量*当前售价/税率*税率
       0 MAINPROD_SALES_AMT, --主产产量*当前售价/税率
       0 BYPROD_SALES_AMT, --副产产量*当前售价/税率
       0 CONTRACT_SALES_AMT, --合同金额
       0 MARKET_SALES_AMT, --市场金额
       0  CONTRACT_PROD_QTY, --合同产量
       0 VALUE_CONTRACT_PROD_QTY, --保值合同产量
       0 MINI_CONTRACT_PROD_QTY, --保底合同产量
       0 MARKET_PROD_QTY,  --市场产量
       0 WIP_QTY, --CW产量
       0 SALE_QTY, --CW销量
       0 FREIGHT_FEE, --原料运费
       0 PACKING_FEE, --包装费
       0 G_WIP_FIX, --G制造费用固定
       0 G_WIP_CHG, --G制造费用变动
       0 G_WATER_ELEC, --G水电费
       0 G_FUEL, --G燃料费
       0 G_MANUAL, --G直接人工费
       0 INPUT_TAX, --预计可抵扣进项税
       0 SECND_INPUT, --预计副产品收入
       0 MANAGE_FIXED_FEE, --管理费用可控
       0 MANAGE_CHG_FEE, --管理费用非可控
       0 SALES_FIXED_FEE, --销售费用可控
       0 SALES_CHG_FEE, --销售费用非可控
       0 FINANCIAL_FEE, --财务费用
       0 TON_FREIGHT_RATE, --下月吨费用变动系数
       0 T_FREIGHT_FEE, --原料运费
       0 T_PACKING_FEE, --包装费
       0 T_G_WIP_FIX, --G制造费用固定
       0 T_G_WIP_CHG, --G制造费用变动
       0 T_G_WATER_ELEC, --G水电费
       0 T_G_FUEL, --G燃料费
       0 T_G_MANUAL, --G直接人工费
       0 T_SECND_INPUT, --预计副产品收入
       0 T_MANAGE_FIXED_FEE, --管理费用可控
       0 T_MANAGE_CHG_FEE, --管理费用非可控
       0 T_SALES_FIXED_FEE, --销售费用可控
       0 T_SALES_CHG_FEE, --销售费用非可控
       0 T_FINANCIAL_FEE, --财务费用
       0 T_TON_FREIGHT_RATE, --下月吨费用变动系数
       SUM(T2.HC) HC, --在职人数
       SUM(T2.ACT_HC) ACT_HC, --实际出勤
       SUM(T2.FRONT_HC) FRONT_HC, --前区在职
       SUM(T2.ACT_FRONT_HC) ACT_FRONT_HC, --前区实际出勤
       SUM(T2.BACK_HC) BACK_HC, --后区在职
       SUM(T2.ACT_BACK_HC) ACT_BACK_HC, --后区实际出勤
       SUM(T2.KT_HC) KT_HC, --库台在职
       SUM(T2.ACT_KT_HC) ACT_KT_HC, --库台实际出勤
       SUM(T2.WORKING_TIME) WORKING_TIME, --工作时间
       SUM(T2.MONTH_BEG_HC) MONTH_BEG_HC, --月初人数
       0 PRECOOLING_TEMPERATURE, --预冷温度
       0 PRECOOLING_TIME, --预冷时间
       0 EQUIPMENT_STAGNATION_TIME, --设备停滞时间
       0 RAW_CHICKEN_DUCK_TIME, --原料空鸡鸭时间
       0 HOOKS_NUMBER, --钩数
       0 EMPTY_HOOKS_NUMBER, --空钩个数
       0 EMPTY_HOOKS_RATE, --空钩率
       0 DEATH_NUMBER, --途中死亡只数
       0 WORKING_DAY, --生产天数
       0 KILLING_DAY--均衡宰杀天数
  FROM  mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_02 T2
 WHERE OP_DAY = '$OP_DAY'
 GROUP BY PERIOD_ID, ORG_ID, BUS_TYPE, PRODUCT_LINE
UNION ALL
SELECT PERIOD_ID, --期间
       ORG_ID, --组织
       BUS_TYPE, --业态
       PRODUCT_LINE, --产线
       0 KILLED_QTY, --宰杀只数
       0 AVG_WEIGHT, --只均重
       0 RECYCLE_WEIGHT, --结算重量
       0 RECYCLE_AMT, --结算金额
       0 RECYCLE_AMT_BEFTAX, --结算金额(去税)
       0 PROD_QTY, --产量
       0 CARRIAGE_COST, --运费金额
       0 CONTRACT_CARRIAGE_COST,        --  合同运费,
       0 VALUE_CARRIAGE_COST,            --  保值运费,
       0 MINI_CARRIAGE_COST,             --  保底运费,
       0 MARKET_CARRIAGE_COST,           --  市场运费,
       0 BODY_WEIGHT, --过磅计价重--胴体结算重量
       0 MAINPROD_AMT, --主产结算金额
       0 BYPROD_AMT, --副产结算金额
       0 MAINPROD_QTY, --主产入库量
       0 BYPROD_QTY, --副产入库量
       0 DEF_PROD_QTY, --次品入库量
       0 DEF_HEAD_QTY, --次头入库量
       0 DEF_NECK_QTY, --次脖入库量
       0 DEF_WING_QTY, --次二节翅量
       0 DEF_ROOT_WING_QTY, --次翅根入库量
       0 DEF_FEET_QTY, --次掌入库量
       0 CONTRACT_RECYCLE_AMT, --合同结算金额
       0 VALUE_CONTRACT_RECYCLE_AMT, --保值结算金额
       0 MINI_CONTRACT_RECYCLE_AMT, --保底结算金额
       0 MARKET_RECYCLE_AMT, --市场结算金额
       0 CONTRACT_RECYCLE_AMT_BEFTAX,        --合同结算金额去税
       0 VALUE_CONTRACT_RECYCLE_AMT_BEFTAX,  --保值合同结算金额去税
       0 MINI_CONTRACT_RECYCLE_AMT_BEFTAX,   --保底合同结算金额去税
       0 MARKET_RECYCLE_AMT_BEFTAX,          --市场结算金额去税
       0 CONTRACT_QTY, --合同宰杀只数
       0 VALUE_CONTRACT_QTY, --保值宰杀只数
       0 MINI_CONTRACT_QTY, --保底宰杀只数
       0 MARKET_QTY, --市场宰杀只数
       0 CONTRACT_WEIGHT, --合同结算重量
       0 VALUE_CONTRACT_WEIGHT, --保值结算重量
       0 MINI_CONTRACT_WEIGHT, --保底结算重量
       0 MARKET_WEIGHT, --市场结结算重量
       0 PUT_QTY, --投放数量
       0 PUT_QTY2,
       0 PUT_COST, --投放成本
       0 PUT_AMT,--投放金额
       0 VALUE_PUT_QTY, --保值投放数量
       0 VALUE_PUT_COST, --保值投放成本
       0 VALUE_PUT_AMT, --保值投放金额
       0 BEST_RANGE_QTY, --最佳只重只数
       0 UNDER_4_QTY, --4斤以下只数（只）
       0 401_450_QTY, --4.01_4.50斤只数（只）
       0 451_480_QTY, --4.51_4.80斤只数(只)
       0 481_550_QTY, --4.81_5.50斤只数(只)
       0 OVER_551_QTY, --5.50斤以上只数（只）
       0 UNDER_5_QTY, --5斤以下只数(只）
       0 501_550_QTY, --5.01_5.50斤只数（只）
       0 551_570_QTY, --5.51_5.70斤只数（只）
       0 571_6_QTY, --5.71_6斤只数(只)
       0 601_630_QTY, --6.01_6.3斤只数（只）
       0 OVER_631_QTY, --6.3斤以上只数(只)
       0 UNDER_50_KILLED_QTY, --养殖距离50km以内只数
       0 50_80_KILLED_QTY, --养殖距离50km-80KM只数
       0 OVER_80_KILLED_QTY, --养殖距离80km以上只数
       0 SALES_AMT, --产量*当前售价/税率
       0 T_SALES_AMT,    --产量*当前售价/税率*税率
       0 MAINPROD_SALES_AMT, --主产产量*当前售价/税率
       0 BYPROD_SALES_AMT, --副产产量*当前售价/税率
       0 CONTRACT_SALES_AMT, --合同金额
       0 MARKET_SALES_AMT, --市场金额
       0 CONTRACT_PROD_QTY, --合同产量
       0 VALUE_CONTRACT_PROD_QTY, --保值合同产量
       0 MINI_CONTRACT_PROD_QTY, --保底合同产量
       0 MARKET_PROD_QTY,  --市场产量
       0 WIP_QTY, --CW产量
       0 SALE_QTY, --CW销量
       0 FREIGHT_FEE, --原料运费
       0 PACKING_FEE, --包装费
       0 G_WIP_FIX, --G制造费用固定
       0 G_WIP_CHG, --G制造费用变动
       0 G_WATER_ELEC, --G水电费
       0 G_FUEL, --G燃料费
       0 G_MANUAL, --G直接人工费
       0 INPUT_TAX, --预计可抵扣进项税
       0 SECND_INPUT, --预计副产品收入
       0 MANAGE_FIXED_FEE, --管理费用可控
       0 MANAGE_CHG_FEE, --管理费用非可控
       0 SALES_FIXED_FEE, --销售费用可控
       0 SALES_CHG_FEE, --销售费用非可控
       0 FINANCIAL_FEE, --财务费用
       0 TON_FREIGHT_RATE, --下月吨费用变动系数
       0 T_FREIGHT_FEE, --原料运费
       0 T_PACKING_FEE, --包装费
       0 T_G_WIP_FIX, --G制造费用固定
       0 T_G_WIP_CHG, --G制造费用变动
       0 T_G_WATER_ELEC, --G水电费
       0 T_G_FUEL, --G燃料费
       0 T_G_MANUAL, --G直接人工费
       0 T_SECND_INPUT, --预计副产品收入
       0 T_MANAGE_FIXED_FEE, --管理费用可控
       0 T_MANAGE_CHG_FEE, --管理费用非可控
       0 T_SALES_FIXED_FEE, --销售费用可控
       0 T_SALES_CHG_FEE, --销售费用非可控
       0 T_FINANCIAL_FEE, --财务费用
       0 T_TON_FREIGHT_RATE, --下月吨费用变动系数
       0 HC, --在职人数
       0 ACT_HC, --实际出勤
       0 FRONT_HC, --前区在职
       0 ACT_FRONT_HC, --前区实际出勤
       0 BACK_HC, --后区在职
       0 ACT_BACK_HC, --后区实际出勤
       0 KT_HC, --库台在职
       0 ACT_KT_HC, --库台实际出勤
       0 WORKING_TIME, --工作时间
       0 MONTH_BEG_HC, --月初人数
       SUM(T3.PRECOOLING_TEMPERATURE) PRECOOLING_TEMPERATURE, --预冷温度
       SUM(T3.PRECOOLING_TIME) PRECOOLING_TIME, --预冷时间
       SUM(T3.EQUIPMENT_STAGNATION_TIME) EQUIPMENT_STAGNATION_TIME, --设备停滞时间
       SUM(T3.RAW_CHICKEN_DUCK_TIME) RAW_CHICKEN_DUCK_TIME, --原料空鸡鸭时间
       SUM(T3.HOOKS_NUMBER) HOOKS_NUMBER, --钩数
       SUM(T3.EMPTY_HOOKS_NUMBER) EMPTY_HOOKS_NUMBER, --空钩个数
       SUM(T3.EMPTY_HOOKS_RATE) EMPTY_HOOKS_RATE, --空钩率
       SUM(T3.DEATH_NUMBER) DEATH_NUMBER, --途中死亡只数
       SUM(T3.WORKING_DAY) WORKING_DAY, --生产天数
       SUM(T3.KILLING_DAY)  KILLING_DAY--均衡宰杀天数
  FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_03 T3
 WHERE OP_DAY = '$OP_DAY'
 GROUP BY PERIOD_ID, ORG_ID, BUS_TYPE, PRODUCT_LINE
) T
  LEFT JOIN mreport_global.dim_org_management t2 
    ON T.ORG_ID=T2.ORG_ID and T2.ATTRIBUTE5='1'
  LEFT JOIN mreport_global.dim_org_management t3 
    ON T.ORG_ID=T3.ORG_ID and T.BUS_TYPE=T3.BUS_TYPE_ID and T3.ATTRIBUTE5='2'

  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) T5
    ON (T.bus_type = T5.level4_businesstype_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWP_BIRD_KILL_KPI_DD='DWP_BIRD_KILL_KPI_DD'

CREATE_DWP_BIRD_KILL_KPI_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_KILL_KPI_DD(
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
,level7_org_id                 STRING    --组织7级(库存组织)
,level7_org_descr              STRING    --组织7级(库存组织)
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
  ,KILLED_QTY           STRING    --宰杀只数
  ,AVG_WEIGHT           STRING    --只均重
  ,RECYCLE_WEIGHT       STRING    --结算重量
  ,RECYCLE_AMT          STRING    --结算金额
  ,RECYCLE_AMT_BEFTAX   STRING  --结算金额(去税)
  ,PROD_QTY             STRING  --产量
  ,CARRIAGE_COST        STRING  --运费金额
  ,CONTRACT_CARRIAGE_COST       STRING  --  合同运费,
  ,VALUE_CARRIAGE_COST          STRING  --  保值运费,
  ,MINI_CARRIAGE_COST           STRING  --  保底运费,
  ,MARKET_CARRIAGE_COST         STRING  --  市场运费,
  ,BODY_WEIGHT          STRING --过磅计价重--胴体结算重量
  ,MAINPROD_AMT         STRING    --主产结算金额
  ,BYPROD_AMT           STRING    --副产结算金额
  ,MAINPROD_QTY         STRING   --主产入库量
  ,BYPROD_QTY           STRING   --副产入库量
  ,DEF_PROD_QTY         STRING   --次品入库量
  ,DEF_HEAD_QTY         STRING   --次头入库量
  ,DEF_NECK_QTY         STRING   --次脖入库量
  ,DEF_WING_QTY         STRING    --次二节翅量
  ,DEF_ROOT_WING_QTY    STRING    --次翅根入库量
  ,DEF_FEET_QTY         STRING    --次掌入库量
  ,CONTRACT_RECYCLE_AMT         STRING    --合同结算金额
  ,VALUE_CONTRACT_RECYCLE_AMT   STRING    --保值结算金额
  ,MINI_CONTRACT_RECYCLE_AMT    STRING    --保底结算金额
  ,MARKET_RECYCLE_AMT           STRING    --市场结算金额
  ,CONTRACT_RECYCLE_AMT_BEFTAX         STRING    --合同结算金额去税
  ,VALUE_CONTRACT_RECYCLE_AMT_BEFTAX   STRING    --保值结算金额去税
  ,MINI_CONTRACT_RECYCLE_AMT_BEFTAX    STRING    --保底结算金额去税
  ,MARKET_RECYCLE_AMT_BEFTAX           STRING    --市场结算金额去税
  ,CONTRACT_QTY         STRING    --合同结算量
  ,VALUE_CONTRACT_QTY   STRING    --保值结算量
  ,MINI_CONTRACT_QTY    STRING    --保底结算量
  ,MARKET_QTY           STRING    --市场结算量
  ,CONTRACT_WEIGHT         STRING    --合同结算重量
  ,VALUE_CONTRACT_WEIGHT   STRING    --保值结算重量
  ,MINI_CONTRACT_WEIGHT    STRING    --保底结算重量
  ,MARKET_WEIGHT           STRING    --市场结算重量
  ,PUT_QTY                  STRING --投放数量（QW03合同日期）
  ,PUT_QTY2                 STRING --投放数量 (QW11结算日期)
  ,PUT_COST             STRING    --投放成本
  ,PUT_AMT              STRING    --投放金额
  ,VALUE_PUT_QTY        STRING    --保值投放数量
  ,VALUE_PUT_COST       STRING    --保值投放成本
  ,VALUE_PUT_AMT        STRING    --保值投放金额
  ,BEST_RANGE_QTY       STRING    --最佳只重只数
  ,UNDER_4_QTY          STRING    --4斤以下只数（只）
  ,401_450_QTY          STRING    --4.01_4.50斤只数（只）
  ,451_480_QTY          STRING    --4.51_4.80斤只数(只)
  ,481_550_QTY          STRING    --4.81_5.50斤只数(只)
  ,OVER_551_QTY         STRING    --5.50斤以上只数（只）
  ,UNDER_5_QTY          STRING    --5斤以下只数(只）
  ,501_550_QTY          STRING    --5.01_5.50斤只数（只）
  ,551_570_QTY          STRING    --5.51_5.70斤只数（只）
  ,571_6_QTY            STRING    --5.71_6斤只数(只)
  ,601_630_QTY          STRING    --6.01_6.3斤只数（只）
  ,OVER_631_QTY         STRING    --6.3斤以上只数(只)
  ,UNDER_50_KILLED_QTY  STRING    --养殖距离50km以内只数
  ,50_80_KILLED_QTY     STRING    --养殖距离50km-80KM只数
  ,OVER_80_KILLED_QTY   STRING    --养殖距离80km以上只数
  ,SALES_AMT            STRING    --产量*当前售价
  ,T_SALES_AMT          STRING    --产量*当前售价*税
  ,MAINPROD_SALES_AMT   STRING    --主产产量*当前售价
  ,BYPROD_SALES_AMT     STRING    --副产产量*当前售价
  ,CONTRACT_SALES_AMT   STRING  --合同金额
  ,MARKET_SALES_AMT     STRING  --市场金额
  ,CONTRACT_PROD_QTY        STRING  --合同产量
  ,VALUE_CONTRACT_PROD_QTY  STRING  --保值合同产量
  ,MINI_CONTRACT_PROD_QTY   STRING  --保底合同产量
  ,MARKET_PROD_QTY          STRING  --市场产量
  ,WIP_QTY                  STRING  --  CW产量,
  ,SALE_QTY                 STRING  --  CW销量,
  ,FREIGHT_FEE              STRING  --  原料运费,
  ,PACKING_FEE              STRING  --  包装费,
  ,G_WIP_FIX                STRING  --  G制造费用固定,
  ,G_WIP_CHG                STRING  --  G制造费用变动,
  ,G_WATER_ELEC             STRING  --  G水电费,
  ,G_FUEL                   STRING  --  G燃料费,
  ,G_MANUAL                 STRING  --  G直接人工费,
  ,INPUT_TAX                STRING  --  预计可抵扣进项税,
  ,SECND_INPUT              STRING  --  预计副产品收入,
  ,MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,FINANCIAL_FEE            STRING  --  财务费用
  ,TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
  ,T_FREIGHT_FEE              STRING  --  原料运费,
  ,T_PACKING_FEE              STRING  --  包装费,
  ,T_G_WIP_FIX                STRING  --  G制造费用固定,
  ,T_G_WIP_CHG                STRING  --  G制造费用变动,
  ,T_G_WATER_ELEC             STRING  --  G水电费,
  ,T_G_FUEL                   STRING  --  G燃料费,
  ,T_G_MANUAL                 STRING  --  G直接人工费,
  ,T_SECND_INPUT              STRING  --  预计副产品收入,
  ,T_MANAGE_FIXED_FEE         STRING  --  管理费用可控,
  ,T_MANAGE_CHG_FEE           STRING  --  管理费用非可控,
  ,T_SALES_FIXED_FEE          STRING  --  销售费用可控,
  ,T_SALES_CHG_FEE            STRING  --  销售费用非可控,
  ,T_FINANCIAL_FEE            STRING  --  财务费用
  ,T_TON_FREIGHT_RATE         STRING  --  下月吨费用变动系数
  ,HC                   STRING  --在职人数
  ,ACT_HC               STRING  --实际出勤
  ,FRONT_HC             STRING  --前区在职
  ,ACT_FRONT_HC         STRING  --前区实际出勤
  ,BACK_HC              STRING  --后区在职
  ,ACT_BACK_HC          STRING  --后区实际出勤
  ,KT_HC                STRING  --库台在职
  ,ACT_KT_HC            STRING  --库台实际出勤
  ,WORKING_TIME         STRING  --工作时间
  ,MONTH_BEG_HC         STRING  --月初人数
  ,PRECOOLING_TEMPERATURE       STRING  --预冷温度
  ,PRECOOLING_TIME              STRING  --预冷时间
  ,EQUIPMENT_STAGNATION_TIME    STRING  --设备停滞时间
  ,RAW_CHICKEN_DUCK_TIME        STRING  --原料空鸡鸭时间
  ,HOOKS_NUMBER                 STRING  --钩数
  ,EMPTY_HOOKS_NUMBER           STRING  --空钩个数
  ,EMPTY_HOOKS_RATE             STRING  --空钩率
  ,DEATH_NUMBER                 STRING  --途中死亡只数
  ,WORKING_DAY                  STRING  --生产天数
  ,KILLING_DAY                  STRING  --均衡宰杀天数
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_KILL_KPI_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_KILL_KPI_DD PARTITION(op_day='$OP_DAY')
SELECT MONTH_ID                          --期间(月)
,DAY_ID                            --期间(日)
,LEVEL1_ORG_ID                     --组织1级(股份)
,LEVEL1_ORG_DESCR                  --组织1级(股份)
,LEVEL2_ORG_ID                     --组织2级(片联)
,LEVEL2_ORG_DESCR                  --组织2级(片联)
,LEVEL3_ORG_ID                     --组织3级(片区)
,LEVEL3_ORG_DESCR                  --组织3级(片区)
,LEVEL4_ORG_ID                     --组织4级(小片)
,LEVEL4_ORG_DESCR                  --组织4级(小片)
,LEVEL5_ORG_ID                     --组织5级(公司)
,LEVEL5_ORG_DESCR                  --组织5级(公司)
,LEVEL6_ORG_ID                     --组织6级(OU)
,LEVEL6_ORG_DESCR                  --组织6级(OU)
,level7_org_id                     --组织7级(库存组织)
,level7_org_descr                  --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID            --业态1级
,LEVEL1_BUSINESSTYPE_NAME          --业态1级
,LEVEL2_BUSINESSTYPE_ID            --业态2级
,LEVEL2_BUSINESSTYPE_NAME          --业态2级
,LEVEL3_BUSINESSTYPE_ID            --业态3级
,LEVEL3_BUSINESSTYPE_NAME          --业态3级
,LEVEL4_BUSINESSTYPE_ID            --业态4级
,LEVEL4_BUSINESSTYPE_NAME          --业态4级
,PRODUCTION_LINE_ID                --产线id
,PRODUCTION_LINE_DESCR             --产线
  ,SUM(KILLED_QTY)               --宰杀只数
  ,SUM(AVG_WEIGHT)               --只均重
  ,SUM(RECYCLE_WEIGHT)           --结算重量
  ,SUM(RECYCLE_AMT)              --结算金额
  ,SUM(RECYCLE_AMT_BEFTAX)     --结算金额(去税)
  ,SUM(PROD_QTY)               --产量
  ,SUM(CARRIAGE_COST)          --运费金额
  ,SUM(CONTRACT_CARRIAGE_COST)         --  合同运费,
  ,SUM(VALUE_CARRIAGE_COST)            --  保值运费,
  ,SUM(MINI_CARRIAGE_COST)             --  保底运费,
  ,SUM(MARKET_CARRIAGE_COST)           --  市场运费,
  ,SUM(BODY_WEIGHT)           --过磅计价重--胴体结算重量
  ,SUM(MAINPROD_AMT)             --主产结算金额
  ,SUM(BYPROD_AMT)               --副产结算金额
  ,SUM(MAINPROD_QTY)            --主产入库量
  ,SUM(BYPROD_QTY)              --副产入库量
  ,SUM(DEF_PROD_QTY)            --次品入库量
  ,SUM(DEF_HEAD_QTY)            --次头入库量
  ,SUM(DEF_NECK_QTY)            --次脖入库量
  ,SUM(DEF_WING_QTY)             --次二节翅量
  ,SUM(DEF_ROOT_WING_QTY)        --次翅根入库量
  ,SUM(DEF_FEET_QTY)             --次掌入库量
  ,SUM(CONTRACT_RECYCLE_AMT)             --合同结算金额
  ,SUM(VALUE_CONTRACT_RECYCLE_AMT)       --保值结算金额
  ,SUM(MINI_CONTRACT_RECYCLE_AMT)        --保底结算金额
  ,SUM(MARKET_RECYCLE_AMT)               --市场结算金额
  ,SUM(CONTRACT_RECYCLE_AMT_BEFTAX)             --合同结算金额去税
  ,SUM(VALUE_CONTRACT_RECYCLE_AMT_BEFTAX)       --保值结算金额去税
  ,SUM(MINI_CONTRACT_RECYCLE_AMT_BEFTAX)        --保底结算金额去税
  ,SUM(MARKET_RECYCLE_AMT_BEFTAX)               --市场结算金额去税
  ,SUM(CONTRACT_QTY)             --合同结算量
  ,SUM(VALUE_CONTRACT_QTY)       --保值结算量
  ,SUM(MINI_CONTRACT_QTY)        --保底结算量
  ,SUM(MARKET_QTY)               --市场结算量
  ,SUM(CONTRACT_WEIGHT)             --合同结算重量
  ,SUM(VALUE_CONTRACT_WEIGHT)       --保值结算重量
  ,SUM(MINI_CONTRACT_WEIGHT)        --保底结算重量
  ,SUM(MARKET_WEIGHT)               --市场结算重量
  ,SUM(PUT_QTY)                   --投放数量（QW03合同日期）
  ,SUM(PUT_QTY2)                 --投放数量 (QW11结算日期)
  ,SUM(PUT_COST)                 --投放成本
  ,SUM(PUT_AMT)                  --投放金额
  ,SUM(VALUE_PUT_QTY)            --保值投放数量
  ,SUM(VALUE_PUT_COST)           --保值投放成本
  ,SUM(VALUE_PUT_AMT)            --保值投放金额
  ,SUM(BEST_RANGE_QTY)           --最佳只重只数
  ,SUM(UNDER_4_QTY)              --4斤以下只数（只）
  ,SUM(401_450_QTY)              --4.01_4.50斤只数（只）
  ,SUM(451_480_QTY)              --4.51_4.80斤只数(只)
  ,SUM(481_550_QTY)              --4.81_5.50斤只数(只)
  ,SUM(OVER_551_QTY)             --5.50斤以上只数（只）
  ,SUM(UNDER_5_QTY)              --5斤以下只数(只）
  ,SUM(501_550_QTY)              --5.01_5.50斤只数（只）
  ,SUM(551_570_QTY)              --5.51_5.70斤只数（只）
  ,SUM(571_6_QTY)                --5.71_6斤只数(只)
  ,SUM(601_630_QTY)              --6.01_6.3斤只数（只）
  ,SUM(OVER_631_QTY)             --6.3斤以上只数(只)
  ,SUM(UNDER_50_KILLED_QTY)      --养殖距离50km以内只数
  ,SUM(50_80_KILLED_QTY)         --养殖距离50km-80KM只数
  ,SUM(OVER_80_KILLED_QTY)       --养殖距离80km以上只数
  ,SUM(SALES_AMT)                --产量*当前售价
  ,SUM(T_SALES_AMT)              --产量*当前售价*税
  ,SUM(MAINPROD_SALES_AMT)       --主产产量*当前售价
  ,SUM(BYPROD_SALES_AMT)         --副产产量*当前售价
  ,SUM(CONTRACT_SALES_AMT)     --合同金额
  ,SUM(MARKET_SALES_AMT)       --市场金额
  ,SUM(CONTRACT_PROD_QTY)          --合同产量
  ,SUM(VALUE_CONTRACT_PROD_QTY)    --保值合同产量
  ,SUM(MINI_CONTRACT_PROD_QTY)     --保底合同产量
  ,SUM(MARKET_PROD_QTY)            --市场产量
  ,SUM(WIP_QTY)                    --CW产量
  ,SUM(SALE_QTY)                   --CW销量
  ,SUM(FREIGHT_FEE)                --原料运费
  ,SUM(PACKING_FEE)               --包装费
  ,SUM(G_WIP_FIX)                  --G制造费用固定
  ,SUM(G_WIP_CHG)                 --G制造费用变动
  ,SUM(G_WATER_ELEC)               --G水电费
  ,SUM(G_FUEL)                     --G燃料费
  ,SUM(G_MANUAL)                   --G直接人工费
  ,SUM(INPUT_TAX)                  --预计可抵扣进项税
  ,SUM(SECND_INPUT)                --预计副产品收入
  ,SUM(MANAGE_FIXED_FEE)           --管理费用可控
  ,SUM(MANAGE_CHG_FEE)             --管理费用非可控
  ,SUM(SALES_FIXED_FEE)            --销售费用可控
  ,SUM(SALES_CHG_FEE)              --销售费用非可控
  ,SUM(FINANCIAL_FEE)              --财务费用
  ,SUM(TON_FREIGHT_RATE)           --下月吨费用变动系数
  ,SUM(T_FREIGHT_FEE)               --原料运费
  ,SUM(T_PACKING_FEE)                --包装费
  ,SUM(T_G_WIP_FIX)                  --G制造费用固定
  ,SUM(T_G_WIP_CHG)                  --G制造费用变动
  ,SUM(T_G_WATER_ELEC)               --G水电费
  ,SUM(T_G_FUEL)                     --G燃料费
  ,SUM(T_G_MANUAL)                   --G直接人工费
  ,SUM(T_SECND_INPUT)                --预计副产品收入
  ,SUM(T_MANAGE_FIXED_FEE)           --管理费用可控
  ,SUM(T_MANAGE_CHG_FEE)             --管理费用非可控
  ,SUM(T_SALES_FIXED_FEE)            --销售费用可控
  ,SUM(T_SALES_CHG_FEE)              --销售费用非可控
  ,SUM(T_FINANCIAL_FEE)              --财务费用
  ,SUM(T_TON_FREIGHT_RATE)          --下月吨费用变动系数
  ,SUM(HC)                     --在职人数
  ,SUM(ACT_HC)                 --实际出勤
  ,SUM(FRONT_HC)               --前区在职
  ,SUM(ACT_FRONT_HC)           --前区实际出勤
  ,SUM(BACK_HC)                --后区在职
  ,SUM(ACT_BACK_HC)            --后区实际出勤
  ,SUM(KT_HC)                  --库台在职
  ,SUM(ACT_KT_HC)              --库台实际出勤
  ,SUM(WORKING_TIME)           --工作时间
  ,SUM(MONTH_BEG_HC)           --月初人数
  ,SUM(PRECOOLING_TEMPERATURE)         --预冷温度
  ,SUM(PRECOOLING_TIME)                --预冷时间
  ,SUM(EQUIPMENT_STAGNATION_TIME)      --设备停滞时间
  ,SUM(RAW_CHICKEN_DUCK_TIME)          --原料空鸡鸭时间
  ,SUM(HOOKS_NUMBER)                   --钩数
  ,SUM(EMPTY_HOOKS_NUMBER)             --空钩个数
  ,SUM(EMPTY_HOOKS_RATE)               --空钩率
  ,SUM(DEATH_NUMBER)                   --途中死亡只数
  ,SUM(WORKING_DAY)                    --生产天数
  ,SUM(KILLING_DAY)                    --均衡宰杀天数
FROM(
SELECT MONTH_ID                          --期间(月)
,DAY_ID                            --期间(日)
,LEVEL1_ORG_ID                     --组织1级(股份)
,LEVEL1_ORG_DESCR                  --组织1级(股份)
,LEVEL2_ORG_ID                     --组织2级(片联)
,LEVEL2_ORG_DESCR                  --组织2级(片联)
,LEVEL3_ORG_ID                     --组织3级(片区)
,LEVEL3_ORG_DESCR                  --组织3级(片区)
,LEVEL4_ORG_ID                     --组织4级(小片)
,LEVEL4_ORG_DESCR                  --组织4级(小片)
,LEVEL5_ORG_ID                     --组织5级(公司)
,LEVEL5_ORG_DESCR                  --组织5级(公司)
,LEVEL6_ORG_ID                     --组织6级(OU)
,LEVEL6_ORG_DESCR                  --组织6级(OU)
,level7_org_id                     --组织7级(库存组织)
,level7_org_descr                  --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID            --业态1级
,LEVEL1_BUSINESSTYPE_NAME          --业态1级
,LEVEL2_BUSINESSTYPE_ID            --业态2级
,LEVEL2_BUSINESSTYPE_NAME          --业态2级
,LEVEL3_BUSINESSTYPE_ID            --业态3级
,LEVEL3_BUSINESSTYPE_NAME          --业态3级
,LEVEL4_BUSINESSTYPE_ID            --业态4级
,LEVEL4_BUSINESSTYPE_NAME          --业态4级
,PRODUCTION_LINE_ID                --产线id
,PRODUCTION_LINE_DESCR             --产线
  ,SUM(KILLED_QTY) KILLED_QTY              --宰杀只数
  ,SUM(AVG_WEIGHT) AVG_WEIGHT              --只均重
  ,SUM(RECYCLE_WEIGHT) RECYCLE_WEIGHT          --结算重量
  ,SUM(RECYCLE_AMT)  RECYCLE_AMT             --结算金额
  ,SUM(RECYCLE_AMT_BEFTAX) RECYCLE_AMT_BEFTAX    --结算金额(去税)
  ,SUM(PROD_QTY)  PROD_QTY             --产量
  ,SUM(CARRIAGE_COST) CARRIAGE_COST         --运费金额
  ,SUM(CONTRACT_CARRIAGE_COST) CONTRACT_CARRIAGE_COST        --  合同运费
  ,SUM(VALUE_CARRIAGE_COST)  VALUE_CARRIAGE_COST          --  保值运费
  ,SUM(MINI_CARRIAGE_COST)  MINI_CARRIAGE_COST           --  保底运费
  ,SUM(MARKET_CARRIAGE_COST) MARKET_CARRIAGE_COST          --  市场运费
  ,SUM(BODY_WEIGHT) BODY_WEIGHT          --过磅计价重--胴体结算重量
  ,SUM(MAINPROD_AMT)   MAINPROD_AMT          --主产结算金额
  ,SUM(BYPROD_AMT)  BYPROD_AMT             --副产结算金额
  ,SUM(MAINPROD_QTY)  MAINPROD_QTY          --主产入库量
  ,SUM(BYPROD_QTY)  BYPROD_QTY            --副产入库量
  ,SUM(DEF_PROD_QTY) DEF_PROD_QTY           --次品入库量
  ,SUM(DEF_HEAD_QTY)  DEF_HEAD_QTY          --次头入库量
  ,SUM(DEF_NECK_QTY)   DEF_NECK_QTY         --次脖入库量
  ,SUM(DEF_WING_QTY) DEF_WING_QTY            --次二节翅量
  ,SUM(DEF_ROOT_WING_QTY) DEF_ROOT_WING_QTY        --次翅根入库量
  ,SUM(DEF_FEET_QTY) DEF_FEET_QTY             --次掌入库量
  ,SUM(CONTRACT_RECYCLE_AMT)  CONTRACT_RECYCLE_AMT           --合同结算金额
  ,SUM(VALUE_CONTRACT_RECYCLE_AMT) VALUE_CONTRACT_RECYCLE_AMT       --保值结算金额
  ,SUM(MINI_CONTRACT_RECYCLE_AMT)  MINI_CONTRACT_RECYCLE_AMT      --保底结算金额
  ,SUM(MARKET_RECYCLE_AMT) MARKET_RECYCLE_AMT              --市场结算金额
  ,SUM(CONTRACT_RECYCLE_AMT_BEFTAX) CONTRACT_RECYCLE_AMT_BEFTAX             --合同结算金额去税
  ,SUM(VALUE_CONTRACT_RECYCLE_AMT_BEFTAX) VALUE_CONTRACT_RECYCLE_AMT_BEFTAX      --保值结算金额去税
  ,SUM(MINI_CONTRACT_RECYCLE_AMT_BEFTAX) MINI_CONTRACT_RECYCLE_AMT_BEFTAX       --保底结算金额去税
  ,SUM(MARKET_RECYCLE_AMT_BEFTAX)  MARKET_RECYCLE_AMT_BEFTAX             --市场结算金额去税
  ,SUM(CONTRACT_QTY)  CONTRACT_QTY           --合同结算量
  ,SUM(VALUE_CONTRACT_QTY) VALUE_CONTRACT_QTY      --保值结算量
  ,SUM(MINI_CONTRACT_QTY) MINI_CONTRACT_QTY        --保底结算量
  ,SUM(MARKET_QTY) MARKET_QTY              --市场结算量
  ,SUM(CONTRACT_WEIGHT)  CONTRACT_WEIGHT           --合同结算重量
  ,SUM(VALUE_CONTRACT_WEIGHT) VALUE_CONTRACT_WEIGHT      --保值结算重量
  ,SUM(MINI_CONTRACT_WEIGHT) MINI_CONTRACT_WEIGHT       --保底结算重量
  ,SUM(MARKET_WEIGHT) MARKET_WEIGHT              --市场结算重量
  ,SUM(PUT_QTY) PUT_QTY                  --投放数量（QW03合同日期）
  ,SUM(PUT_QTY2) PUT_QTY2                --投放数量 (QW11结算日期)
  ,SUM(PUT_COST) PUT_COST                --投放成本
  ,SUM(PUT_AMT) PUT_AMT                 --投放金额
  ,SUM(VALUE_PUT_QTY) VALUE_PUT_QTY           --保值投放数量
  ,SUM(VALUE_PUT_COST) VALUE_PUT_COST          --保值投放成本
  ,SUM(VALUE_PUT_AMT) VALUE_PUT_AMT           --保值投放金额
  ,SUM(BEST_RANGE_QTY) BEST_RANGE_QTY          --最佳只重只数
  ,SUM(UNDER_4_QTY) UNDER_4_QTY             --4斤以下只数（只）
  ,SUM(401_450_QTY)  401_450_QTY            --4.01_4.50斤只数（只）
  ,SUM(451_480_QTY)  451_480_QTY            --4.51_4.80斤只数(只)
  ,SUM(481_550_QTY)  481_550_QTY            --4.81_5.50斤只数(只)
  ,SUM(OVER_551_QTY)  OVER_551_QTY           --5.50斤以上只数（只）
  ,SUM(UNDER_5_QTY)  UNDER_5_QTY            --5斤以下只数(只）
  ,SUM(501_550_QTY)  501_550_QTY            --5.01_5.50斤只数（只）
  ,SUM(551_570_QTY)  551_570_QTY            --5.51_5.70斤只数（只）
  ,SUM(571_6_QTY)  571_6_QTY              --5.71_6斤只数(只)
  ,SUM(601_630_QTY)  601_630_QTY            --6.01_6.3斤只数（只）
  ,SUM(OVER_631_QTY)   OVER_631_QTY          --6.3斤以上只数(只)
  ,SUM(UNDER_50_KILLED_QTY) UNDER_50_KILLED_QTY     --养殖距离50km以内只数
  ,SUM(50_80_KILLED_QTY)  50_80_KILLED_QTY       --养殖距离50km-80KM只数
  ,SUM(OVER_80_KILLED_QTY) OVER_80_KILLED_QTY      --养殖距离80km以上只数
  ,SUM(SALES_AMT) SALES_AMT               --产量*当前售价
  ,SUM(T_SALES_AMT)  T_SALES_AMT            --产量*当前售价*税
  ,SUM(MAINPROD_SALES_AMT)  MAINPROD_SALES_AMT     --主产产量*当前售价
  ,SUM(BYPROD_SALES_AMT) BYPROD_SALES_AMT        --副产产量*当前售价
  ,SUM(CONTRACT_SALES_AMT) CONTRACT_SALES_AMT    --合同金额
  ,SUM(MARKET_SALES_AMT)  MARKET_SALES_AMT     --市场金额
  ,SUM(CONTRACT_PROD_QTY) CONTRACT_PROD_QTY         --合同产量
  ,SUM(VALUE_CONTRACT_PROD_QTY) VALUE_CONTRACT_PROD_QTY   --保值合同产量
  ,SUM(MINI_CONTRACT_PROD_QTY) MINI_CONTRACT_PROD_QTY    --保底合同产量
  ,SUM(MARKET_PROD_QTY)  MARKET_PROD_QTY          --市场产量
  ,WIP_QTY                    --CW产量
  ,SALE_QTY                   --CW销量
  ,FREIGHT_FEE                --原料运费
  ,PACKING_FEE               --包装费
  ,G_WIP_FIX                  --G制造费用固定
  ,G_WIP_CHG                 --G制造费用变动
  ,G_WATER_ELEC               --G水电费
  ,G_FUEL                     --G燃料费
  ,G_MANUAL                   --G直接人工费
  ,INPUT_TAX                  --预计可抵扣进项税
  ,SECND_INPUT                --预计副产品收入
  ,MANAGE_FIXED_FEE           --管理费用可控
  ,MANAGE_CHG_FEE             --管理费用非可控
  ,SALES_FIXED_FEE            --销售费用可控
  ,SALES_CHG_FEE              --销售费用非可控
  ,FINANCIAL_FEE              --财务费用
  ,TON_FREIGHT_RATE           --下月吨费用变动系数
  ,T_FREIGHT_FEE               --原料运费
  ,T_PACKING_FEE                --包装费
  ,T_G_WIP_FIX                  --G制造费用固定
  ,T_G_WIP_CHG                  --G制造费用变动
  ,T_G_WATER_ELEC               --G水电费
  ,T_G_FUEL                     --G燃料费
  ,T_G_MANUAL                   --G直接人工费
  ,T_SECND_INPUT                --预计副产品收入
  ,T_MANAGE_FIXED_FEE           --管理费用可控
  ,T_MANAGE_CHG_FEE             --管理费用非可控
  ,T_SALES_FIXED_FEE            --销售费用可控
  ,T_SALES_CHG_FEE              --销售费用非可控
  ,T_FINANCIAL_FEE              --财务费用
  ,T_TON_FREIGHT_RATE          --下月吨费用变动系数
  ,SUM(HC)  HC                   --在职人数
  ,SUM(ACT_HC)  ACT_HC               --实际出勤
  ,SUM(FRONT_HC)  FRONT_HC             --前区在职
  ,SUM(ACT_FRONT_HC)  ACT_FRONT_HC         --前区实际出勤
  ,SUM(BACK_HC)  BACK_HC              --后区在职
  ,SUM(ACT_BACK_HC)  ACT_BACK_HC          --后区实际出勤
  ,SUM(KT_HC)  KT_HC                --库台在职
  ,SUM(ACT_KT_HC)  ACT_KT_HC            --库台实际出勤
  ,SUM(WORKING_TIME) WORKING_TIME          --工作时间
  ,SUM(MONTH_BEG_HC) MONTH_BEG_HC          --月初人数
  ,SUM(PRECOOLING_TEMPERATURE) PRECOOLING_TEMPERATURE        --预冷温度
  ,SUM(PRECOOLING_TIME) PRECOOLING_TIME               --预冷时间
  ,SUM(EQUIPMENT_STAGNATION_TIME)  EQUIPMENT_STAGNATION_TIME    --设备停滞时间
  ,SUM(RAW_CHICKEN_DUCK_TIME) RAW_CHICKEN_DUCK_TIME         --原料空鸡鸭时间
  ,SUM(HOOKS_NUMBER)   HOOKS_NUMBER                --钩数
  ,SUM(EMPTY_HOOKS_NUMBER)  EMPTY_HOOKS_NUMBER           --空钩个数
  ,SUM(EMPTY_HOOKS_RATE)  EMPTY_HOOKS_RATE             --空钩率
  ,SUM(DEATH_NUMBER)  DEATH_NUMBER                 --途中死亡只数
  ,SUM(WORKING_DAY)   WORKING_DAY                 --生产天数
  ,SUM(KILLING_DAY)   KILLING_DAY                 --均衡宰杀天数
FROM mreport_poultry.TMP_DWP_BIRD_KILL_KPI_DD_2
WHERE OP_DAY = '$OP_DAY'
GROUP BY MONTH_ID                          --期间(月)
,DAY_ID                            --期间(日)
,LEVEL1_ORG_ID                     --组织1级(股份)
,LEVEL1_ORG_DESCR                  --组织1级(股份)
,LEVEL2_ORG_ID                     --组织2级(片联)
,LEVEL2_ORG_DESCR                  --组织2级(片联)
,LEVEL3_ORG_ID                     --组织3级(片区)
,LEVEL3_ORG_DESCR                  --组织3级(片区)
,LEVEL4_ORG_ID                     --组织4级(小片)
,LEVEL4_ORG_DESCR                  --组织4级(小片)
,LEVEL5_ORG_ID                     --组织5级(公司)
,LEVEL5_ORG_DESCR                  --组织5级(公司)
,LEVEL6_ORG_ID                     --组织6级(OU)
,LEVEL6_ORG_DESCR                  --组织6级(OU)
,level7_org_id                     --组织7级(库存组织)
,level7_org_descr                  --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID            --业态1级
,LEVEL1_BUSINESSTYPE_NAME          --业态1级
,LEVEL2_BUSINESSTYPE_ID            --业态2级
,LEVEL2_BUSINESSTYPE_NAME          --业态2级
,LEVEL3_BUSINESSTYPE_ID            --业态3级
,LEVEL3_BUSINESSTYPE_NAME          --业态3级
,LEVEL4_BUSINESSTYPE_ID            --业态4级
,LEVEL4_BUSINESSTYPE_NAME          --业态4级
,PRODUCTION_LINE_ID                --产线id
,PRODUCTION_LINE_DESCR             --产线
,WIP_QTY                    --CW产量
  ,SALE_QTY                   --CW销量
  ,FREIGHT_FEE                --原料运费
  ,CONTRACT_CARRIAGE_COST        --  合同运费,
  ,VALUE_CARRIAGE_COST            --  保值运费,
  ,MINI_CARRIAGE_COST            --  保底运费,
  ,MARKET_CARRIAGE_COST           --  市场运费,
  ,PACKING_FEE               --包装费
  ,G_WIP_FIX                  --G制造费用固定
  ,G_WIP_CHG                 --G制造费用变动
  ,G_WATER_ELEC               --G水电费
  ,G_FUEL                     --G燃料费
  ,G_MANUAL                   --G直接人工费
  ,INPUT_TAX                  --预计可抵扣进项税
  ,SECND_INPUT                --预计副产品收入
  ,MANAGE_FIXED_FEE           --管理费用可控
  ,MANAGE_CHG_FEE             --管理费用非可控
  ,SALES_FIXED_FEE            --销售费用可控
  ,SALES_CHG_FEE              --销售费用非可控
  ,FINANCIAL_FEE              --财务费用
  ,TON_FREIGHT_RATE           --下月吨费用变动系数
  ,T_FREIGHT_FEE               --原料运费
  ,T_PACKING_FEE                --包装费
  ,T_G_WIP_FIX                  --G制造费用固定
  ,T_G_WIP_CHG                  --G制造费用变动
  ,T_G_WATER_ELEC               --G水电费
  ,T_G_FUEL                     --G燃料费
  ,T_G_MANUAL                   --G直接人工费
  ,T_SECND_INPUT                --预计副产品收入
  ,T_MANAGE_FIXED_FEE           --管理费用可控
  ,T_MANAGE_CHG_FEE             --管理费用非可控
  ,T_SALES_FIXED_FEE            --销售费用可控
  ,T_SALES_CHG_FEE              --销售费用非可控
  ,T_FINANCIAL_FEE              --财务费用
  ,T_TON_FREIGHT_RATE          --下月吨费用变动系数
  ) T
GROUP BY MONTH_ID                          --期间(月)
,DAY_ID                            --期间(日)
,LEVEL1_ORG_ID                     --组织1级(股份)
,LEVEL1_ORG_DESCR                  --组织1级(股份)
,LEVEL2_ORG_ID                     --组织2级(片联)
,LEVEL2_ORG_DESCR                  --组织2级(片联)
,LEVEL3_ORG_ID                     --组织3级(片区)
,LEVEL3_ORG_DESCR                  --组织3级(片区)
,LEVEL4_ORG_ID                     --组织4级(小片)
,LEVEL4_ORG_DESCR                  --组织4级(小片)
,LEVEL5_ORG_ID                     --组织5级(公司)
,LEVEL5_ORG_DESCR                  --组织5级(公司)
,LEVEL6_ORG_ID                     --组织6级(OU)
,LEVEL6_ORG_DESCR                  --组织6级(OU)
,level7_org_id                     --组织7级(库存组织)
,level7_org_descr                  --组织7级(库存组织)
,LEVEL1_BUSINESSTYPE_ID            --业态1级
,LEVEL1_BUSINESSTYPE_NAME          --业态1级
,LEVEL2_BUSINESSTYPE_ID            --业态2级
,LEVEL2_BUSINESSTYPE_NAME          --业态2级
,LEVEL3_BUSINESSTYPE_ID            --业态3级
,LEVEL3_BUSINESSTYPE_NAME          --业态3级
,LEVEL4_BUSINESSTYPE_ID            --业态4级
,LEVEL4_BUSINESSTYPE_NAME          --业态4级
,PRODUCTION_LINE_ID                --产线id
,PRODUCTION_LINE_DESCR             --产线
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    set mapred.max.split.size=10000000;
    set mapred.min.split.size.per.node=10000000;
    set mapred.min.split.size.per.rack=10000000;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    set hive.hadoop.supports.splittable.combineinputformat=true;
    set hive.auto.convert.join=false;
    set mapred.reduce.tasks=20;

    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_00;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_00;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_01;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_01;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_02;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_02;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_03;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_03;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_0;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_0;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_1;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_1;
    $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_2;
    $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_2;
    $CREATE_DWP_BIRD_KILL_KPI_DD;
    $INSERT_DWP_BIRD_KILL_KPI_DD;

"  -v

#     $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_00;
#     $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_00;
#     $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_01;
#     $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_01;
#     $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_02;
#     $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_02;
#     $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_03;
#     $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_03;
#     $CREATE_TMP_DWP_BIRD_KILL_KPI_DD_0;
#     $INSERT_TMP_DWP_BIRD_KILL_KPI_DD_0;
#     $CREATE_DWP_BIRD_KILL_KPI_DD;
#     $INSERT_DWP_BIRD_KILL_KPI_DD;
