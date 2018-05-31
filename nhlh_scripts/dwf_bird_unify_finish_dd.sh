#!/bin/bash

######################################################################
#
# 程    序: dwf_bird_unify_finish_dd.sh
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

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwf_bird_unify_finish_dd.sh 20180101"
    exit 1
fi



###########################################################################################
## 处理 基础指标
## 变量声明
TMP_DWF_BIRD_UNIFY_FINISH_DD_00='TMP_DWF_BIRD_UNIFY_FINISH_DD_00'

CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_00="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIFY_FINISH_DD_00(
ORDER_NUMBER            STRING --订单号
,CURRENCY_ID            STRING --币种
,CURRENCY_DESCR         STRING --币种
,PERIOD_ID              STRING --期间
,ORG_ID                 STRING --组织
,INV_ORG_ID             STRING --库存组织
,ITEM_ID                STRING --物料
,BUS_TYPE               STRING --业态
,PRODUCT_LINE           STRING --产线
,CONTRACT_NUM           STRING --合同号
,SUBJECT_ID             STRING --科目ID      
,CUST_ID                STRING --客户ID                 
,CUST_ADDRESS_ID        STRING --客户地址ID
,CUSTOMER_TYPE_ID       STRING --客户销售类型编码 
,CUSTOMER_TYPE_DESCR    STRING --客户销售类型描述
,PROFIT                 STRING --利润
,SRC_TYPE               STRING --收入类型
,INCOME                 STRING --收入
,COST_AMOUNT_T          STRING --总成本
,SELLING_EXPENSE_FIXED  STRING --销售费用-固定
,SELLING_EXPENSE_CHANGE STRING --销售费用-变动
,FIN_EXPENSE            STRING --财务费用
,ADMINI_EXPENSE         STRING --管理费用
,OPERATING_TAX          STRING --税金及附加
,AR_LOSSES_ASSET        STRING --应收坏账损失 
,OTHER_LOSSES_ASSET     STRING --其他减值损失 
,NON_INCOME             STRING --营业外收入
,NON_EXPENSE            STRING --营业外支出
,CHANGE_IN_FAIR_VALUE   STRING --公允价值变动收益
,INVESTMENT_INCOME      STRING --投资收益
,OTHER_INCOME           STRING --其他收益
,ASSET_DISPOSIT_INCOME  STRING --资产处置收益 
)
PARTITIONED BY (OP_DAY STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_00="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIFY_FINISH_DD_00 PARTITION(OP_DAY='$OP_DAY')
SELECT ORDER_NUMBER,            -- 销售订单号码
       CURRENCY_TYPE  CURRENCY_ID,
       CASE WHEN CURRENCY_TYPE ='3' THEN '母币'
            WHEN CURRENCY_TYPE ='2' THEN '本位币'
            ELSE NULL END CURRENCY_DESCR, --币种名称
       PERIOD_ID,
       CW19.ORG_ID,                   --组织
       CW19.INV_ORG_ID,               --库存组织
       INVENTORY_ITEM_ID ITEM_ID, --物料
       M.BUS_TYPE,
       PRODUCT_LINE,
       CONTRACT_NUM,            --合同号
       SUBJECT_ID,              --科目ID
       CUST_ID,                 --客户ID
       CUST_ADDRESS_ID,         --客户地址ID
       HA.CUSTOMER_TYPE_ID,      --客户销售类型编码
       HA.CUSTOMER_TYPE_DESCR,   --客户销售类型描述
       INCOME - COST_AMOUNT_T- SELLING_EXPENSE_FIXED - SELLING_EXPENSE_CHANGE
         - FIN_EXPENSE - ADMINI_EXPENSE - OPERATING_TAX - AR_LOSSES_ASSET- OTHER_LOSSES_ASSET
         + NON_INCOME - NON_EXPENSE + CHANGE_IN_FAIR_VALUE  + INVESTMENT_INCOME + OTHER_INCOME
         + ASSET_DISPOSIT_INCOME - cost_amount20 PROFIT, --利润
       SRC_TYPE,                --收入类型编码
       INCOME,                  --收入
       COST_AMOUNT_T,           --总成本
       SELLING_EXPENSE_FIXED,   --销售费用-固定
       SELLING_EXPENSE_CHANGE,  --销售费用-变动
       FIN_EXPENSE,             --财务费用
       ADMINI_EXPENSE,          --管理费用
       OPERATING_TAX,           --税金及附加
       AR_LOSSES_ASSET,         --应收坏账损失
       OTHER_LOSSES_ASSET,      --其他减值损失
       NON_INCOME,              --营业外收入
       NON_EXPENSE,             --营业外支出
       CHANGE_IN_FAIR_VALUE,    --公允价值变动收益
       INVESTMENT_INCOME,       --投资收益
       OTHER_INCOME,            --其他收益
       ASSET_DISPOSIT_INCOME    --资产处置收益
  FROM (SELECT * FROM mreport_poultry.DMD_FIN_EXPS_PROFITS
         WHERE CURRENCY_TYPE = '3'  --仅取母币数据
       ) CW19
 INNER JOIN (SELECT * FROM MREPORT_GLOBAL.DIM_ORG_MANAGEMENT
              WHERE  level2_org_id in('1010','1012')) ORG
    ON CW19.ORG_ID = ORG.ORG_ID
  LEFT JOIN (select hca.CUST_ACCOUNT_ID,
                  hca.ACCOUNT_NUMBER,
                  hca.ACCOUNT_NAME,
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '1' ELSE '2' END CUSTOMER_TYPE_ID, --I-1 内部 R-2 外部
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '内部' ELSE '外部' END CUSTOMER_TYPE_DESCR
            from MREPORT_GLOBAL.ODS_EBS_HZ_CUST_ACCOUNTS hca) HA
       ON (CW19.cust_id = HA.cust_account_id)
  LEFT JOIN (SELECT M.ORG_ID,
                    M.ORGANIZATION_ID INV_ORG_ID,
                    ORG.LEVEL4_BUS_TYPE BUS_TYPE
               FROM MREPORT_GLOBAL.ods_ebs_cux_bi_ar_ou_inv_mapping M
               LEFT JOIN MREPORT_GLOBAL.ods_ebs_cux_org_structures_all ORG
                 ON M.ORGANIZATION_ID = ORG.LEVEL7_ORG_ID) m
    ON CW19.ORG_ID = M.ORG_ID

"

###########################################################################################
## 处理 基础指标 饲料
## 变量声明
TMP_DWF_BIRD_UNIFY_FINISH_DD_01='TMP_DWF_BIRD_UNIFY_FINISH_DD_01'

CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_01="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIFY_FINISH_DD_01(
ORDER_NUMBER            STRING --订单号
,CURRENCY_ID            STRING --币种
,CURRENCY_DESCR         STRING --币种
,PERIOD_ID              STRING --期间
,ORG_ID                 STRING --组织
,INV_ORG_ID             STRING --库存组织
,ITEM_ID                STRING --物料
,BUS_TYPE               STRING --业态
,PRODUCT_LINE           STRING --产线
,CONTRACT_NUM           STRING --合同号
,SUBJECT_ID             STRING --科目ID      
,CUST_ID                STRING --客户ID                 
,CUST_ADDRESS_ID        STRING --客户地址ID
,CUSTOMER_TYPE_ID       STRING --客户销售类型编码 
,CUSTOMER_TYPE_DESCR    STRING --客户销售类型描述
,PROFIT                 STRING --利润
,SRC_TYPE               STRING --收入类型
,INCOME                 STRING --收入
,COST_AMOUNT_T          STRING --总成本
,SELLING_EXPENSE_FIXED  STRING --销售费用-固定
,SELLING_EXPENSE_CHANGE STRING --销售费用-变动
,FIN_EXPENSE            STRING --财务费用
,ADMINI_EXPENSE         STRING --管理费用
,OPERATING_TAX          STRING --税金及附加
,AR_LOSSES_ASSET        STRING --应收坏账损失 
,OTHER_LOSSES_ASSET     STRING --其他减值损失 
,NON_INCOME             STRING --营业外收入
,NON_EXPENSE            STRING --营业外支出
,CHANGE_IN_FAIR_VALUE   STRING --公允价值变动收益
,INVESTMENT_INCOME      STRING --投资收益
,OTHER_INCOME           STRING --其他收益
,ASSET_DISPOSIT_INCOME  STRING --资产处置收益 
)
PARTITIONED BY (OP_DAY STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_01="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIFY_FINISH_DD_01 PARTITION(OP_DAY='$OP_DAY')
SELECT interface_header_attribute1 ORDER_NUMBER,            -- 销售订单号码
       '3'    CURRENCY_ID,
       '母币' CURRENCY_DESCR,    --币种名称
       period_code PERIOD_ID,
       SL.ORG_ID,                   --组织
       SL.INV_ORG_ID,               --库存组织
       segment1 ITEM_ID,         --物料
       M.BUS_TYPE,
       '' PRODUCT_LINE,
       '' CONTRACT_NUM,          --合同号
       segment3 SUBJECT_ID,      --科目ID
       bill_to_customer_id CUST_ID,    --客户ID
       '' CUST_ADDRESS_ID,             --客户地址ID
       HA.CUSTOMER_TYPE_ID,      --客户销售类型编码
       HA.CUSTOMER_TYPE_DESCR,   --客户销售类型描述
       COALESCE(SL.YB_STANDARD_AMT,0)   --厂价收入
                        +COALESCE(SL.YB_M_DISCOUNT,0)+COALESCE(SL.YB_Q_DISCOUNT,0)+COALESCE(SL.YB_Y_DISCOUNT,0)+COALESCE(SL.YB_O_DISCOUNT,0)+COALESCE(SL.YB_S_DISCOUNT,0) --期间折扣
                        +COALESCE(SL.YB_XCZK_AMT_DISCOUNT,0)    ----现折
                        -COALESCE(SL.YB_TAX,0)           ---税收
                        -COALESCE(SL.CMPNT_COST_1_G,0)-COALESCE(SL.CMPNT_COST_2_G,0)-COALESCE(SL.CMPNT_COST_3_G,0)-COALESCE(SL.CMPNT_COST_4_G,0)
                        -COALESCE(SL.CMPNT_COST_5_G,0)-COALESCE(SL.CMPNT_COST_6_G,0)-COALESCE(SL.CMPNT_COST_7_G,0)-COALESCE(SL.CMPNT_COST_8_G,0)
                        -COALESCE(SL.CMPNT_COST_9_G,0)-COALESCE(SL.MAIN_INCOME_COST_G,0)
                        -COALESCE(SL.SELLING_EXPENSE_CHANGE,0)-COALESCE(SL.SELLING_EXPENSE_CHANGE_G,0)       --销售费用-变动（元）
                        -COALESCE(SL.SELLING_EXPENSE_FIXED,0)-COALESCE(SL.SELLING_EXPENSE_FIXED_G,0)         ---销售费用-固定
                        -COALESCE(SL.ADMINI_EXPENSE,0)-COALESCE(SL.ADMINI_EXPENSE_G,0)                       --管理费用
                        -COALESCE(SL.FIN_EXPENSE,0)-COALESCE(SL.FIN_EXPENSE_G,0)                             --财务费用
                        -COALESCE(SL.OPERATING_TAX,0)-COALESCE(SL.OPERATING_TAX_G,0)                         --营业税金
                        -COALESCE(SL.LOSSES_ASSET,0)-COALESCE(SL.LOSSES_ASSET_G,0)                           --资产减值
                        +COALESCE(SL.OTHER_REVENUE,0)+COALESCE(SL.OTHER_REVENUE_G,0)-COALESCE(SL.OTHER_COSTS,0)-COALESCE(SL.OTHER_COSTS_G,0)  ---其他业务利润
                        +COALESCE(SL.NON_INCOME,0)+COALESCE(SL.NON_INCOME_G,0)-COALESCE(SL.NON_EXPENSE,0)-COALESCE(SL.NON_EXPENSE_G,0) ---营业外利润
                        +COALESCE(SL.CHANGE_IN_FAIR_VALUE,0)+COALESCE(SL.CHANGE_IN_FAIR_VALUE_G,0)   -- 公允价值变动收益
                        +COALESCE(SL.INVESTMENT_INCOME,0)+COALESCE(SL.INVESTMENT_INCOME_G,0) -- 投资收益
                        +COALESCE(SL.OTHER_INCOME,0)+COALESCE(SL.OTHER_INCOME_G,0)  -- 其他收益
                        +COALESCE(SL.ASSET_DISPOSE,0)+COALESCE(SL.ASSET_DISPOSE_G,0) PROFIT, --利润
       '' SRC_TYPE,                --收入类型编码
       mb_amount INCOME,           --收入
       0 COST_AMOUNT_T,           --总成本
       0 SELLING_EXPENSE_FIXED,   --销售费用-固定
       0 SELLING_EXPENSE_CHANGE,  --销售费用-变动
       0 FIN_EXPENSE,             --财务费用
       0 ADMINI_EXPENSE,          --管理费用
       0 OPERATING_TAX,           --税金及附加
       0 AR_LOSSES_ASSET,         --应收坏账损失
       0 OTHER_LOSSES_ASSET,      --其他减值损失
       0 NON_INCOME,              --营业外收入
       0 NON_EXPENSE,             --营业外支出
       0 CHANGE_IN_FAIR_VALUE,    --公允价值变动收益
       0 INVESTMENT_INCOME,       --投资收益
       0 OTHER_INCOME,            --其他收益
       0 ASSET_DISPOSIT_INCOME    --资产处置收益
  FROM (SELECT * FROM mreport_feed.dwu_finance_budget_restore
          ) SL
  LEFT JOIN (select hca.CUST_ACCOUNT_ID,
                  hca.ACCOUNT_NUMBER,
                  hca.ACCOUNT_NAME,
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '1' ELSE '2' END CUSTOMER_TYPE_ID, --I-1 内部 R-2 外部
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '内部' ELSE '外部' END CUSTOMER_TYPE_DESCR
            from MREPORT_GLOBAL.ODS_EBS_HZ_CUST_ACCOUNTS hca) HA
       ON (SL.bill_to_customer_id = HA.cust_account_id)
  LEFT JOIN (SELECT M.ORG_ID,
                    M.ORGANIZATION_ID INV_ORG_ID,
                    ORG.LEVEL4_BUS_TYPE BUS_TYPE
               FROM MREPORT_GLOBAL.ods_ebs_cux_bi_ar_ou_inv_mapping M
               LEFT JOIN MREPORT_GLOBAL.ods_ebs_cux_org_structures_all ORG
                 ON M.ORGANIZATION_ID = ORG.LEVEL7_ORG_ID) m
    ON SL.ORG_ID = M.ORG_ID

"
###########################################################################################
## 处理延伸指标
## 变量声明
TMP_DWF_BIRD_UNIFY_FINISH_DD_1='TMP_DWF_BIRD_UNIFY_FINISH_DD_1'

CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIFY_FINISH_DD_1(
ORDER_NUMBER            STRING --销售订单号码 
,CURRENCY_ID            STRING --币种
,CURRENCY_DESCR         STRING --币种
,PERIOD_ID              STRING --期间
,ORG_ID                 STRING --组织
,INV_ORG_ID             STRING --库存组织
,ITEM_ID                STRING --物料
,BUS_TYPE               STRING --业态
,PRODUCT_LINE           STRING --产线
,CONTRACT_NUM           STRING --合同号
,SUBJECT_ID             STRING --科目ID
,CUST_ID                STRING --客户ID
,CUST_ADDRESS_ID        STRING --客户地址ID
,CUSTOMER_TYPE_ID       STRING --客户销售类型编码 
,CUSTOMER_TYPE_DESCR    STRING --客户销售类型描述
,PROFIT                 STRING --利润
,SRC_TYPE               STRING --收入类型
,INCOME                 STRING --收入
,COST_AMOUNT_T          STRING --总成本
,SELLING_EXPENSE_FIXED  STRING --销售费用-固定
,SELLING_EXPENSE_CHANGE STRING --销售费用-变动
,FIN_EXPENSE            STRING --财务费用
,ADMINI_EXPENSE         STRING --管理费用
,OPERATING_TAX          STRING --税金及附加
,AR_LOSSES_ASSET        STRING --应收坏账损失 
,OTHER_LOSSES_ASSET     STRING --其他减值损失 
,NON_INCOME             STRING --营业外收入
,NON_EXPENSE            STRING --营业外支出
,CHANGE_IN_FAIR_VALUE   STRING --公允价值变动收益
,INVESTMENT_INCOME      STRING --投资收益
,OTHER_INCOME           STRING --其他收益
,ASSET_DISPOSIT_INCOME  STRING --资产处置收益
,ZQ_PROFITS_AMT         STRING --种禽利润总额
,YZ_PROFITS_AMT         STRING --养殖利润总额（养殖 = 禽旺)
,MTL_OUTTER_PROFITS_AMT STRING --饲料内销利润总额
,MTL_INNER_PROFITS_AMT  STRING --饲料外利润总额
,COLD_PROFITS_AMT       STRING --冷藏利润总额
,FOOD_PROFITS_AMT       STRING --食品深加工利润总额
,ZQ_INCOME_AMT          STRING --种禽收入总额
,YZ_INCOME_AMT          STRING --养殖收入总额（养殖 = 禽旺)
,MTL_OUTTER_INCOME_AMT  STRING --饲料内销收入总额
,MTL_INNER_INCOME_AMT   STRING --饲料外利收入总额
,COLD_INCOME_AMT        STRING --冷藏收入总额
,FOOD_INCOME_AMT        STRING --食品深加工收入总额
)
PARTITIONED BY (OP_DAY STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_1="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIFY_FINISH_DD_1 PARTITION(OP_DAY='$OP_DAY')
SELECT ORDER_NUMBER,            -- 销售订单号码    
       CURRENCY_ID,  
       CURRENCY_DESCR,   
       PERIOD_ID,               --期间
       ORG_ID,                  --组织
       INV_ORG_ID,              --库存组织
       ITEM_ID,                 --物料
       BUS_TYPE,                --业态
       PRODUCT_LINE,            --产线
       CONTRACT_NUM,            --合同号
       SUBJECT_ID,              --科目ID
       CUST_ID,
       CUST_ADDRESS_ID,       
       CUSTOMER_TYPE_ID,         --客户销售类型编码 
       CUSTOMER_TYPE_DESCR,      --客户销售类型描述
       PROFIT,                  --利润
       SRC_TYPE,                --收入类型
       INCOME,                  --收入
       COST_AMOUNT_T,           --总成本
       SELLING_EXPENSE_FIXED,   --销售费用-固定
       SELLING_EXPENSE_CHANGE,  --销售费用-变动
       FIN_EXPENSE,             --财务费用
       ADMINI_EXPENSE,          --管理费用
       OPERATING_TAX,           --税金及附加
       AR_LOSSES_ASSET,         --应收坏账损失 
       OTHER_LOSSES_ASSET,      --其他减值损失 
       NON_INCOME,              --营业外收入
       NON_EXPENSE,             --营业外支出
       CHANGE_IN_FAIR_VALUE,    --公允价值变动收益
       INVESTMENT_INCOME,       --投资收益
       OTHER_INCOME,            --其他收益
       ASSET_DISPOSIT_INCOME,    --资产处置收益
       CASE WHEN BUS_TYPE = '132011'
         THEN PROFIT ELSE  0 END ZQ_PROFITS_AMT,           --种禽利润总额 
       CASE WHEN BUS_TYPE in ('135020' ,'132012','135010')
         THEN PROFIT ELSE  0 END YZ_PROFITS_AMT,           --养殖利润总额
       0 MTL_OUTTER_PROFITS_AMT,   --饲料外销利润总额
       0 MTL_INNER_PROFITS_AMT,    --饲料内销利润总额  
       CASE WHEN BUS_TYPE = '132020' 
         THEN PROFIT ELSE  0 END COLD_PROFITS_AMT,         --冷藏利润总额
       CASE WHEN BUS_TYPE = '134020' 
         THEN PROFIT ELSE  0 END FOOD_PROFITS_AMT,         --食品深加工利润总额
       CASE WHEN BUS_TYPE = '132011' 
         THEN INCOME ELSE  0 END ZQ_INCOME_AMT,            --种禽收入总额
       CASE WHEN BUS_TYPE in ('135020' ,'132012','135010')
         THEN INCOME ELSE  0 END YZ_INCOME_AMT,            --养殖收入总额
       0 MTL_OUTTER_INCOME_AMT,    --饲料外销收入总额
       0 MTL_INNER_INCOME_AMT,     --饲料内销收入总额 
       CASE WHEN BUS_TYPE = '132020' 
         THEN INCOME ELSE  0 END COLD_INCOME_AMT,          --冷藏收入总额
       CASE WHEN BUS_TYPE = '134020' 
         THEN INCOME ELSE  0 END FOOD_INCOME_AMT           --食品深加工收入总额
  FROM mreport_poultry.TMP_DWF_BIRD_UNIFY_FINISH_DD_00 T1
  WHERE OP_DAY = '$OP_DAY'
  UNION ALL
  SELECT ORDER_NUMBER,            -- 销售订单号码    
       CURRENCY_ID,  
       CURRENCY_DESCR,   
       PERIOD_ID,               --期间
       ORG_ID,                  --组织
       INV_ORG_ID,              --库存组织
       ITEM_ID,                 --物料
       BUS_TYPE,                --业态
       PRODUCT_LINE,            --产线
       CONTRACT_NUM,            --合同号
       SUBJECT_ID,              --科目ID
       CUST_ID,
       CUST_ADDRESS_ID,       
       CUSTOMER_TYPE_ID,         --客户销售类型编码 
       CUSTOMER_TYPE_DESCR,      --客户销售类型描述
       PROFIT,                  --利润
       SRC_TYPE,                --收入类型
       INCOME,                  --收入
       COST_AMOUNT_T,           --总成本
       SELLING_EXPENSE_FIXED,   --销售费用-固定
       SELLING_EXPENSE_CHANGE,  --销售费用-变动
       FIN_EXPENSE,             --财务费用
       ADMINI_EXPENSE,          --管理费用
       OPERATING_TAX,           --税金及附加
       AR_LOSSES_ASSET,         --应收坏账损失 
       OTHER_LOSSES_ASSET,      --其他减值损失 
       NON_INCOME,              --营业外收入
       NON_EXPENSE,             --营业外支出
       CHANGE_IN_FAIR_VALUE,    --公允价值变动收益
       INVESTMENT_INCOME,       --投资收益
       OTHER_INCOME,            --其他收益
       ASSET_DISPOSIT_INCOME,    --资产处置收益
       0 ZQ_PROFITS_AMT,           --种禽利润总额 --暂时取不到
       0 YZ_PROFITS_AMT,           --养殖利润总额
       CASE WHEN BUS_TYPE IN (131010, 131020) AND CUSTOMER_TYPE_ID='2'  --2-外部
         THEN PROFIT ELSE  0 END MTL_OUTTER_PROFITS_AMT,   --饲料外销利润总额
       CASE WHEN BUS_TYPE IN (131010, 131020) AND CUSTOMER_TYPE_ID='1' --1-内部
         THEN PROFIT ELSE  0 END MTL_INNER_PROFITS_AMT,    --饲料内销利润总额  
       0 COLD_PROFITS_AMT,         --冷藏利润总额
       0 FOOD_PROFITS_AMT,         --食品深加工利润总额
       0 ZQ_INCOME_AMT,            --种禽收入总额
       0 YZ_INCOME_AMT,            --养殖收入总额
       CASE WHEN BUS_TYPE IN (131010, 131020) AND CUSTOMER_TYPE_ID='2'  --2-外部
         THEN INCOME ELSE  0 END MTL_OUTTER_INCOME_AMT,    --饲料外销收入总额
       CASE WHEN BUS_TYPE IN (131010, 131020) AND CUSTOMER_TYPE_ID='1' --1-内部
         THEN INCOME ELSE  0 END MTL_INNER_INCOME_AMT,     --饲料内销收入总额 
       0 COLD_INCOME_AMT,          --冷藏收入总额
       0 FOOD_INCOME_AMT           --食品深加工收入总额
  FROM mreport_poultry.TMP_DWF_BIRD_UNIFY_FINISH_DD_01 T1
  WHERE OP_DAY = '$OP_DAY'
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_UNIFY_FINISH_DD='DWF_BIRD_UNIFY_FINISH_DD'

CREATE_DWF_BIRD_UNIFY_FINISH_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_UNIFY_FINISH_DD(
ORDER_NUMBER                   STRING --销售订单号码 
,CURRENCY_ID                   STRING --币种
,CURRENCY_DESCR                STRING --币种
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
,PRODUCTION_LINE_ID            STRING    --产线id
,PRODUCTION_LINE_DESCR         STRING    --产线
,CONTRACT_NUM                  STRING --合同号
,SUBJECT_ID             STRING --科目ID
,CUST_ID                STRING --客户ID                 
,CUST_ADDRESS_ID        STRING --客户地址ID
,PROFIT                 STRING --利润
,SRC_TYPE               STRING --收入类型
,INCOME                 STRING --收入
,COST_AMOUNT_T          STRING --总成本
,SELLING_EXPENSE_FIXED  STRING --销售费用-固定
,SELLING_EXPENSE_CHANGE STRING --销售费用-变动
,FIN_EXPENSE            STRING --财务费用
,ADMINI_EXPENSE         STRING --管理费用
,OPERATING_TAX          STRING --税金及附加
,AR_LOSSES_ASSET        STRING --应收坏账损失 
,OTHER_LOSSES_ASSET     STRING --其他减值损失 
,NON_INCOME             STRING --营业外收入
,NON_EXPENSE            STRING --营业外支出
,CHANGE_IN_FAIR_VALUE   STRING --公允价值变动收益
,INVESTMENT_INCOME      STRING --投资收益
,OTHER_INCOME           STRING --其他收益
,ASSET_DISPOSIT_INCOME  STRING --资产处置收益 
,ZQ_PROFITS_AMT         STRING --种禽利润总额
,YZ_PROFITS_AMT         STRING --养殖利润总额（养殖 = 禽旺)
,MTL_OUTTER_PROFITS_AMT STRING --饲料内销利润总额
,MTL_INNER_PROFITS_AMT  STRING --饲料外利润总额
,COLD_PROFITS_AMT       STRING --冷藏利润总额
,FOOD_PROFITS_AMT       STRING --食品深加工利润总额
,ZQ_INCOME_AMT          STRING --种禽收入总额
,YZ_INCOME_AMT          STRING --养殖收入总额（养殖 = 禽旺)
,MTL_OUTTER_INCOME_AMT  STRING --饲料内销收入总额
,MTL_INNER_INCOME_AMT   STRING --饲料外利收入总额
,COLD_INCOME_AMT        STRING --冷藏收入总额
,FOOD_INCOME_AMT        STRING --食品深加工收入总额
)
PARTITIONED BY (OP_DAY STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_UNIFY_FINISH_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_UNIFY_FINISH_DD PARTITION(OP_DAY='$OP_DAY')
select T1.ORDER_NUMBER,-- 销售订单号码    
       T1.CURRENCY_ID,  
       T1.CURRENCY_DESCR,
       SUBSTR(T1.PERIOD_ID,1,6) MONTH_ID,
       T1.PERIOD_ID DAY_ID, 
       CASE WHEN t2.level1_org_id    is null THEN coalesce(t3.level1_org_id,'-1') ELSE coalesce(t2.level1_org_id,'-1')  END as level1_org_id,                --一级组织编码
       CASE WHEN t2.level1_org_descr is null THEN coalesce(t3.level1_org_descr,'缺失') ELSE coalesce(t2.level1_org_descr,'缺失')  END as level1_org_descr,   --一级组织描述
       CASE WHEN t2.level2_org_id is null    THEN coalesce(t3.level2_org_id,'-1') ELSE coalesce(t2.level2_org_id,'-1')  END as level2_org_id,                --二级组织编码
       CASE WHEN t2.level2_org_descr is null THEN coalesce(t3.level2_org_descr,'缺失') ELSE coalesce(t2.level2_org_descr,'缺失')  END as level2_org_descr,   --二级组织描述
       CASE WHEN t2.level3_org_id    is null THEN coalesce(t3.level3_org_id,'-1') ELSE coalesce(t2.level3_org_id,'-1')  END as level3_org_id,               --三级组织编码
       CASE WHEN t2.level3_org_descr is null THEN coalesce(t3.level3_org_descr,'缺失') ELSE coalesce(t2.level3_org_descr,'缺失')  END as level3_org_descr,   --三级组织描述
       CASE WHEN t2.level4_org_id    is null THEN coalesce(t3.level4_org_id,'-1') ELSE coalesce(t2.level4_org_id,'-1')  END as level4_org_id,                --四级组织编码
       CASE WHEN t2.level4_org_descr is null THEN coalesce(t3.level4_org_descr,'缺失') ELSE coalesce(t2.level4_org_descr,'缺失')  END as level4_org_descr,   --四级组织描述
       CASE WHEN t2.level5_org_id    is null THEN coalesce(t3.level5_org_id,'-1') ELSE coalesce(t2.level5_org_id,'-1')  END as level5_org_id,                --五级组织编码
       CASE WHEN t2.level5_org_descr is null THEN coalesce(t3.level5_org_descr,'缺失') ELSE coalesce(t2.level5_org_descr,'缺失')  END as level5_org_descr,   --五级组织描述
       CASE WHEN t2.level6_org_id    is null THEN coalesce(t3.level6_org_id,'-1') ELSE coalesce(t2.level6_org_id,'-1')  END as level6_org_id,                --六级组织编码
       CASE WHEN t2.level6_org_descr is null THEN coalesce(t3.level6_org_descr,'缺失') ELSE coalesce(t2.level6_org_descr,'缺失')  END as level6_org_descr,   --六级组织描述
       '' LEVEL7_ORG_ID,--T1.INV_ORG_ID
       '' LEVEL7_ORG_DESCR,
       '' LEVEL1_BUSINESSTYPE_ID,
       '' LEVEL1_BUSINESSTYPE_NAME,
       '' LEVEL2_BUSINESSTYPE_ID,
       '' LEVEL2_BUSINESSTYPE_NAME,
       '' LEVEL3_BUSINESSTYPE_ID,
       '' LEVEL3_BUSINESSTYPE_NAME,
       '' LEVEL4_BUSINESSTYPE_ID,
       '' LEVEL4_BUSINESSTYPE_NAME,
       CASE T1.PRODUCT_LINE
         WHEN 10 THEN '1'
         WHEN 20 THEN '2'
         ELSE NULL END PRODUCTION_LINE_ID, --产线
       CASE T1.PRODUCT_LINE
         WHEN 10 THEN          '鸡线'
         WHEN 20 THEN          '鸭线'
         ELSE NULL END PRODUCTION_LINE_DESCR,
       CONTRACT_NUM,            --合同号
       SUBJECT_ID,              --科目ID
       CUST_ID,
       CUST_ADDRESS_ID,
       PROFIT,                  --利润
       SRC_TYPE,                --收入类型
       INCOME,                  --收入
       COST_AMOUNT_T,           --总成本
       SELLING_EXPENSE_FIXED,   --销售费用-固定
       SELLING_EXPENSE_CHANGE,  --销售费用-变动
       FIN_EXPENSE,             --财务费用
       ADMINI_EXPENSE,          --管理费用
       OPERATING_TAX,           --税金及附加
       AR_LOSSES_ASSET,         --应收坏账损失 
       OTHER_LOSSES_ASSET,      --其他减值损失 
       NON_INCOME,              --营业外收入
       NON_EXPENSE,             --营业外支出
       CHANGE_IN_FAIR_VALUE,    --公允价值变动收益
       INVESTMENT_INCOME,       --投资收益
       OTHER_INCOME,            --其他收益
       ASSET_DISPOSIT_INCOME,   --资产处置收益
       ZQ_PROFITS_AMT,          --种禽利润总额
       YZ_PROFITS_AMT,          --养殖利润总额
       MTL_OUTTER_PROFITS_AMT,  --饲料外销利润总额
       MTL_INNER_PROFITS_AMT,   --饲料内销利润总额
       COLD_PROFITS_AMT,        --冷藏利润总额
       FOOD_PROFITS_AMT,        --食品深加工利润总额
       ZQ_INCOME_AMT,           --种禽收入总额
       YZ_INCOME_AMT,           --养殖收入总额
       MTL_OUTTER_INCOME_AMT,   --饲料外销收入总额
       MTL_INNER_INCOME_AMT,    --饲料内销收入总额
       COLD_INCOME_AMT,         --冷藏收入总额
       FOOD_INCOME_AMT          --食品深加工收入总额
  from mreport_poultry.TMP_DWF_BIRD_UNIFY_FINISH_DD_1 T1
  LEFT JOIN mreport_global.dim_org_management t2 
    ON T1.ORG_ID=T2.ORG_ID and T2.ATTRIBUTE5='1'
  LEFT JOIN mreport_global.dim_org_management t3 
    ON T1.ORG_ID=T3.ORG_ID and T1.BUS_TYPE=T3.BUS_TYPE_ID and T3.ATTRIBUTE5='2'
  WHERE T1.OP_DAY = '$OP_DAY'
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


    $CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_00;
    $INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_00;
    $CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_01;
    $INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_01;
    $CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_1;
    $INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_1;
    $CREATE_DWF_BIRD_UNIFY_FINISH_DD;
    $INSERT_DWF_BIRD_UNIFY_FINISH_DD;

"  -v

#     $CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_00;
#     $INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_00;
#     $CREATE_TMP_DWF_BIRD_UNIFY_FINISH_DD_01;
#     $INSERT_TMP_DWF_BIRD_UNIFY_FINISH_DD_01;
#     $CREATE_DWF_BIRD_UNIFY_FINISH_DD;
#     $INSERT_DWF_BIRD_UNIFY_FINISH_DD;
