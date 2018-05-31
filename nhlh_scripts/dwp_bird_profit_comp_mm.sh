#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_profit_comp_mm.sh                               
# 创建时间: 2018年04月10日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-利润构成-股份
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}
OP_LAST_YEAR_MONTH=$(date -d "$OP_DAY -1 years" "+%Y%m" )

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_profit_comp_mm.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_PROFIT_COMP_MM_1='TMP_DWP_BIRD_PROFIT_COMP_MM_1'

CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PROFIT_COMP_MM_1(
     PERIOD_ID             STRING   --期间
     ,CONTRACT_NUM         STRING   --合同号
     ,ORG_ID               STRING   --OU组织
     ,BUS_TYPE             STRING   --业态	
     ,INV_ORG_ID           STRING
     ,PRODUCT_LINE         STRING
     ,INCOME               STRING   --收入
     ,ORDERED_QTY          STRING   --销量
     ,LOC_COST_AMOUNT_T    STRING   --成本
     ,SUBJECT_ID           STRING   --科目
     ,MATERIAL_SEGMENT1_ID STRING   --物料1级
     ,MATERIAL_SEGMENT2_ID STRING   --物料2级
     ,INVENTORY_ITEM_ID    STRING   --物料
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PROFIT_COMP_MM_1  PARTITION(op_month='${OP_MONTH}')
    select substr(T1.PERIOD_ID,1,6) --期间
     ,T1.CONTRACT_NUM  --合同号
     ,T1.ORG_ID     --OU组织
     ,T1.BUS_TYPE  --业态	
     ,T1.INV_ORG_ID
     ,t1.product_line
     ,SUM(T1.INCOME)        --收入
     ,SUM(T1.ORDERED_QTY)   --销量
     ,sum(t1.LOC_COST_AMOUNT_T)
     ,T1.SUBJECT_ID    --科目
     ,coalesce(T3.MATERIAL_SEGMENT1_ID,T4.MATERIAL_SEGMENT1_ID) --物料1级
     ,coalesce(T3.MATERIAL_SEGMENT2_ID,T4.MATERIAL_SEGMENT2_ID) --物料2级
     ,T1.INVENTORY_ITEM_ID
      from (SELECT * FROM MREPORT_POULTRY.DWU_ORDER_INCOME_COST
    WHERE OP_MONTH = '${OP_MONTH}' and is_writeoff='1' ) T1
    INNER JOIN MREPORT_GLOBAL.ODS_EBS_HR_ALL_ORGANIZATION_UNITS T2
    ON T2.ATTRIBUTE2='合作社-生产性' AND T1.ORG_ID = T2.ORGANIZATION_ID
   LEFT JOIN ( 
         SELECT INV_ORG_ID,INVENTORY_ITEM_ID,MATERIAL_SEGMENT1_ID,MATERIAL_SEGMENT2_ID FROM MREPORT_GLOBAL.DWU_DIM_MATERIAL_NEW
         GROUP BY INV_ORG_ID,INVENTORY_ITEM_ID,MATERIAL_SEGMENT1_ID,MATERIAL_SEGMENT2_ID
         ) T3 --EBS物料
     ON T1.INV_ORG_ID = T3.INV_ORG_ID AND T1.INVENTORY_ITEM_ID = T3.INVENTORY_ITEM_ID AND coalesce(T1.INV_ORG_ID,'') <> ''

   LEFT JOIN ( 
         SELECT A.OPERATING_UNIT ORG_ID,B.INVENTORY_ITEM_ID,B.MATERIAL_SEGMENT1_ID,B.MATERIAL_SEGMENT2_ID 
         FROM MREPORT_GLOBAL.ODS_EBS_ORG_ORGANIZATION_DEFINITIONS A
         LEFT JOIN MREPORT_GLOBAL.DWU_DIM_MATERIAL_NEW B
         ON A.ORGANIZATION_ID = B.INV_ORG_ID
         GROUP BY A.OPERATING_UNIT,B.INVENTORY_ITEM_ID,B.MATERIAL_SEGMENT1_ID,B.MATERIAL_SEGMENT2_ID
         ) T4 --EBS物料
     ON T1.ORG_ID = T4.ORG_ID AND T1.INVENTORY_ITEM_ID = T4.INVENTORY_ITEM_ID AND coalesce(T1.INV_ORG_ID,'') = ''
   group BY substr(T1.PERIOD_ID,1,6) --期间
     ,T1.CONTRACT_NUM  --合同号
     ,T1.ORG_ID     --OU组织
     ,T1.BUS_TYPE  --业态	
     ,T1.INV_ORG_ID
     ,t1.product_line
     ,T1.SUBJECT_ID    --科目
     ,coalesce(T3.MATERIAL_SEGMENT1_ID,T4.MATERIAL_SEGMENT1_ID) --物料1级
     ,coalesce(T3.MATERIAL_SEGMENT2_ID,T4.MATERIAL_SEGMENT2_ID) --物料2级
     ,T1.INVENTORY_ITEM_ID
 "




###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_PROFIT_COMP_MM_2='TMP_DWP_BIRD_PROFIT_COMP_MM_2'

CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PROFIT_COMP_MM_2(
     PERIOD_ID             STRING   --期间
     ,CONTRACT_NUM         STRING   --合同号
     ,ORG_ID               STRING   --OU组织
     ,BUS_TYPE             STRING   --业态	
     ,INVENTORY_ITEM_ID    STRING   --物料
     ,tax_etc_amt          STRING   --税金及附加
     ,total_amt	           STRING   --费用总额  --制造费用-变动+制造费用-固定+销售费用-固定+销售费用-变动+管理费用
     ,impair_amt           STRING   --资产减值损失
     ,no_oper_income       STRING   --营业外收支
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PROFIT_COMP_MM_2  PARTITION(op_month='${OP_MONTH}')
    SELECT substr(T1.PERIOD_ID,1,6)
    ,T1.CONTRACT_NUM
    ,T1.ORG_ID
    ,T1.BUS_TYPE
    ,T1.INVENTORY_ITEM_ID
     ,SUM(T1.OPERATING_TAX)                       as tax_etc_amt    --税金及附加
     ,SUM(T1.SELLING_EXPENSE_FIXED + T1.SELLING_EXPENSE_CHANGE + T1.ADMINI_EXPENSE + T1.FIN_EXPENSE)   as total_amt	--费用总额  --销售费用-固定+销售费用-变动+管理费用 + 财务费用
     ,SUM(T1.AR_LOSSES_ASSET)                      as impair_amt     --资产减值损失
     ,SUM((T1.NON_INCOME  - T1.NON_EXPENSE))          as no_oper_income --营业外收支
     --,SUM(INVESTMENT_INCOME)                 --投资收益                 
    FROM 
    (SELECT * FROM MREPORT_POULTRY.DMD_FIN_EXPS_PROFITS  --CW19禽利润大表
    WHERE CURRENCY_TYPE = '3' --币种类型：2:本位币，3：母币
      --AND OP_DAY = '20180101'
      ) T1
     GROUP BY substr(T1.PERIOD_ID,1,6)
    ,T1.CONTRACT_NUM
    ,T1.ORG_ID
    ,T1.BUS_TYPE
    ,T1.INVENTORY_ITEM_ID
"




###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_PROFIT_COMP_MM_3='TMP_DWP_BIRD_PROFIT_COMP_MM_3'

CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PROFIT_COMP_MM_3(
     PERIOD_ID             STRING   --期间
     ,CONTRACT_NUM         STRING   --合同号
     ,ORG_ID               STRING   --OU组织
     ,BUS_TYPE             STRING   --业态	
     ,INVENTORY_ITEM_ID    STRING   --物料
     ,OTHER_INCOME_AMT     STRING   --其他业务收入
     ,OTHER_COST_AMT       STRING   --其他业务成本 
     ,tax_etc_amt          STRING   --税金及附加
     ,total_amt	           STRING   --费用总额  --制造费用-变动+制造费用-固定+销售费用-固定+销售费用-变动+管理费用
     ,impair_amt           STRING   --资产减值损失
     ,no_oper_income       STRING   --营业外收支
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_3="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PROFIT_COMP_MM_3  PARTITION(op_month='${OP_MONTH}')
SELECT 

     T8.PERIOD_ID
     ,T8.CONTRACT_NUM
     ,T8.ORG_ID
     ,T8.BUS_TYPE
     ,T8.INVENTORY_ITEM_ID
     ,T8.OTHER_INCOME_AMT --其他业务收入
     ,T8.OTHER_COST_AMT  --其他业务成本
     ,T7.TAX_ETC_AMT  --税金及附加
     ,T7.TOTAL_AMT  --费用总额
     ,T7.IMPAIR_AMT  --资产减值损失
     ,T7.NO_OPER_INCOME  --营业外收支
 FROM (
            SELECT substr(T1.PERIOD_ID,1,6) PERIOD_ID
            ,T1.CONTRACT_NUM
            ,T1.ORG_ID
            ,T1.BUS_TYPE
            ,T1.INVENTORY_ITEM_ID
            ,SUM(CASE WHEN T1.SUBJECT_ID like '60510101%' THEN T1.INCOME ELSE 0 end)        AS OTHER_INCOME_AMT  --其他业务收入
            ,SUM(CASE WHEN T1.SUBJECT_ID like '60410101%' THEN T1.COST_AMOUNT_T ELSE 0 end) AS OTHER_COST_AMT  --其他业务成本 
        FROM 
        (SELECT * FROM MREPORT_POULTRY.DMD_FIN_EXPS_PROFITS  --CW19禽利润大表
        WHERE CURRENCY_TYPE = '3' --币种类型：2:本位币，3：母币
          --AND OP_DAY = '20180101'
          ) T1
         GROUP BY substr(T1.PERIOD_ID,1,6)
            ,T1.CONTRACT_NUM
            ,T1.ORG_ID
            ,T1.BUS_TYPE
            ,T1.INVENTORY_ITEM_ID
        ) T8
        LEFT JOIN TMP_DWP_BIRD_PROFIT_COMP_MM_2 T7
         ON coalesce(T8.CONTRACT_NUM,'NULL_FORMAT_STRING') = coalesce(T7.CONTRACT_NUM,'NULL_FORMAT_STRING') 
         AND T8.ORG_ID = T7.ORG_ID
         AND coalesce(T8.BUS_TYPE,'NULL_FORMAT_STRING') = coalesce(T7.BUS_TYPE,'NULL_FORMAT_STRING')
         AND coalesce(T8.INVENTORY_ITEM_ID,'NULL_FORMAT_STRING') = coalesce(T7.INVENTORY_ITEM_ID,'NULL_FORMAT_STRING')
         AND T8.PERIOD_ID = T7.PERIOD_ID
         and t7.op_month='${OP_MONTH}'
"





###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWP_BIRD_PROFIT_COMP_MM='DWP_BIRD_PROFIT_COMP_MM'

CREATE_DWP_BIRD_PROFIT_COMP_MM="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_PROFIT_COMP_MM(
     PERIOD_ID						string	--期间
    ,contract_no          string  --合同号
	  ,org_id               string  --公司ID
	  ,organization_id      string  --库存组织ID
	  ,bus_type             string  --业态
    ,product_line         string  --产线
    ,feed_income_amt      string  --饲料收入
    ,feed_cost_amt        string  --饲料销售成本
    ,breed_income_amt     string  --放养种苗收入
    ,breed_cost_amt       string  --放养种苗销售成本
    ,breed_vet_amt        string  --放养兽药收入
    ,breed_vet_sale_amt   string  --放养兽药销售成本
    ,sale_income_amt      string  --外销收入
    ,sale_cost_amt        string  --外销成本
    ,fost_income_amt      string  --代养收入
    ,fost_cost_amt        string  --代养销售成本
    ,tech_serv_amt        string  --技术服务费
    ,other_amt            string  --其他业务利润
    ,tax_etc_amt          string  --税金及附加
    ,total_amt            string  --费用总额
    ,impair_amt           string  --资产减值损失
    ,no_oper_income       string  --营业外收支
    ,recycle_cnt          string  --回收只数
    ,create_time          string  --数据推送时间
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_PROFIT_COMP_MM="
INSERT OVERWRITE TABLE $DWP_BIRD_PROFIT_COMP_MM  PARTITION(op_month='${OP_MONTH}')
SELECT 
     T3.PERIOD_ID --期间
     ,T3.CONTRACT_NUM  --合同号
     ,T3.ORG_ID     --OU组织
     ,'' --库存组织ID
     ,T3.BUS_TYPE  --业态	
     ,T3.PRODUCT_LINE --产线	
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '15' AND T3.MATERIAL_SEGMENT2_ID = '03' THEN T3.INCOME ELSE 0 END --饲料收入
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '15' AND T3.MATERIAL_SEGMENT2_ID = '03' THEN T3.LOC_COST_AMOUNT_T ELSE 0 END --饲料成本
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '25' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02') THEN T3.INCOME ELSE 0 END --放养种苗收入
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '25' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02') THEN T3.LOC_COST_AMOUNT_T ELSE 0 END --放养种苗成本
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '65' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','03','04','05','99') THEN T3.INCOME ELSE 0 END --放养兽药收入
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '65' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','03','04','05','99') THEN T3.LOC_COST_AMOUNT_T ELSE 0 END --放养兽药成本
     ,CASE WHEN T2.category_desc = '提供养殖服务'
           AND (
                   (T3.MATERIAL_SEGMENT1_ID = '15' AND T3.MATERIAL_SEGMENT2_ID = '03' )   --饲料
                   OR
                   (T3.MATERIAL_SEGMENT1_ID = '65' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','03','04','05','99'))  --兽药
               )
           THEN T3.INCOME ELSE 0 END --外销收入
     ,(CASE WHEN T2.category_desc = '提供养殖服务'
           AND T3.MATERIAL_SEGMENT1_ID = '15' AND T3.MATERIAL_SEGMENT2_ID = '03'   --饲料
              THEN LOC_COST_AMOUNT_T ELSE 0 END)
           +
           (CASE WHEN T2.category_desc = '提供养殖服务'
           AND T3.MATERIAL_SEGMENT1_ID = '65' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','03','04','05','99')  --兽药
               THEN LOC_COST_AMOUNT_T ELSE 0 END)
                  --外销成本
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '35' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','31','32')  --合同毛鸡/毛鸭
           THEN T3.INCOME ELSE 0 END  --代养收入
     ,CASE WHEN T3.MATERIAL_SEGMENT1_ID = '35' AND T3.MATERIAL_SEGMENT2_ID IN ('01','02','31','32')  --合同毛鸡/毛鸭
           THEN LOC_COST_AMOUNT_T ELSE 0 END  --代养成本
     ,case when t3.SUBJECT_ID = '6001010191' then T3.INCOME else 0 end   TECH_SERV_AMT  --技术服务费
     ,T7.OTHER_INCOME_AMT - T7.OTHER_COST_AMT  --其他业务利润
     ,T7.TAX_ETC_AMT  --税金及附加
     ,T7.TOTAL_AMT  --费用总额
     ,T7.IMPAIR_AMT  --资产减值损失
     ,T7.NO_OPER_INCOME  --营业外收支
     ,0 RECYCLE_CNT  --回收只数
     ,${CREATE_TIME}
FROM (
    select * from MREPORT_POULTRY.TMP_DWP_BIRD_PROFIT_COMP_MM_1
    WHERE OP_MONTH = '${OP_MONTH}'
) t3 --CW01 财务
LEFT JOIN MREPORT_POULTRY.DWU_QW_CONTRACT_DD T2 --QW03 合同
 ON T2.CONTRACTNUMBER = T3.CONTRACT_NUM
 and t2.op_day = '${OP_DAY}'
 and T3.CONTRACT_NUM is not null

--LEFT JOIN MREPORT_POULTRY.DWU_FINANCE_COST_PRIC T5 --CW02 财务成本
-- ON T5.OP_MONTH = '${OP_MONTH}' AND T3.PERIOD_ID = T5.PERIOD_ID AND T3.ORG_ID = T5.ORG_ID AND T3.INV_ORG_ID = T5.ORGANIZATION_ID AND T3.INVENTORY_ITEM_ID = T5.INVENTORY_ITEM_ID

--left join (select pith_no,sum(KILLED_QTY) KILLED_QTY 
--             from MREPORT_POULTRY.DWU_QW_QW11_DD 
--             where op_day = '${OP_DAY}'
--             and pith_no is not null
--             group by pith_no
--          ) --QW11 回收信息
-- ON T3.CONTRACT_NUM = T6.PITH_NO 

left join TMP_DWP_BIRD_PROFIT_COMP_MM_3 T7
 ON coalesce(T3.CONTRACT_NUM,'NULL_FORMAT_STRING') = coalesce(T7.CONTRACT_NUM,'NULL_FORMAT_STRING')
 AND T3.ORG_ID = T7.ORG_ID
 AND coalesce(T3.BUS_TYPE,'NULL_FORMAT_STRING') = coalesce(T7.BUS_TYPE,'NULL_FORMAT_STRING')
 AND coalesce(T3.INVENTORY_ITEM_ID,'NULL_FORMAT_STRING') = coalesce(T7.INVENTORY_ITEM_ID,'NULL_FORMAT_STRING')
 and T3.PERIOD_ID = T7.PERIOD_ID
 and t7.op_month = '${OP_MONTH}'
"




echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_1;
    $INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_1;
    $CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_2;
    $INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_2;
    $CREATE_TMP_DWP_BIRD_PROFIT_COMP_MM_3;
    $INSERT_TMP_DWP_BIRD_PROFIT_COMP_MM_3;
    $CREATE_DWP_BIRD_PROFIT_COMP_MM;
    $INSERT_DWP_BIRD_PROFIT_COMP_MM;
"  -v
