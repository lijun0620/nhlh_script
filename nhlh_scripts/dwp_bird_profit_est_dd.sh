#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_profit_est_dd.sh                               
# 创建时间: 2018年04月26日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyyMMdd]                                             
# 补充说明: 
# 功    能: 禽旺利润测算表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_profit_est_dd.sh 20180101"
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
DWP_BIRD_PROFIT_EST_DD='DWP_BIRD_PROFIT_EST_DD'

CREATE_DWP_BIRD_PROFIT_EST_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_PROFIT_EST_DD(
    DAY_ID                                STRING  --期间(日)
    ,CONTRACT_NO                          STRING  --合同
    ,ORDER_NUMBER                         STRING  --订单号
    ,ORG_ID                               STRING  --OU组织  
    ,INV_ORG_ID                           STRING  --库存组织ID
    ,BUS_TYPE                             STRING  --业态
    ,PRODUCTION_LINE_ID                   STRING  --产线
    ,PRODUCTION_LINE_DESCR                STRING  --产线
    ,FEED_MODEL_ID                        STRING --养殖模式id
    ,FEED_MODEL_NAME                      STRING --养殖模式名称
    ,INVENTORY_ITEM_ID                    STRING --物料
    ,SALE_CNT                             STRING --实际销量
    ,INCOME_AMT                           STRING --收入
    ,COST_AMT                             STRING --成本
    ,OTHER_PROFIT_AMT                     STRING --其他业务利润
    ,NO_OPER_INCOME                       STRING --营业外收支
    ,COMP_MANAGER_AMT                     STRING --管理费用
    ,COMP_SALE_AMT                        STRING --销售费用
    ,COMP_FINC_AMT                        STRING --财务费用
    ,COMP_PROFIT_AMT                      STRING --利润
    ,fct_other_amt                        STRING --预测其他业务利润
    ,fct_oper_inc_amt                     STRING --预测营业外收支
    ,fct_manage_amt                       STRING --预测管理费用
    ,fct_sale_amt                         STRING --预测销售费用
    ,fct_finc_amt                         STRING --预测财务费用
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC	
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_PROFIT_EST_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_PROFIT_EST_DD PARTITION(op_day='${OP_DAY}')
SELECT
    T3.PERIOD_ID DAY_ID	--期间(日)
    ,T3.CONTRACT_NUM   --合同号
    ,T3.ORDER_NUMBER  --订单号
    ,T3.ORG_ID    --OU
    ,T3.INV_ORG_ID  --库存组织
    ,T3.BUS_TYPE  --业态
    ,T3.PRODUCT_LINE PRODUCTION_LINE_ID	--产线
    ,T3.PRODUCT_LINE PRODUCTION_LINE_DESCR	--产线
    ,CASE WHEN T1.CONTRACTTYPE_GRP = '代养' THEN '代养'
          WHEN T1.CONTRACTTYPE_GRP = '放养' THEN '放养' END	--养殖模式ID
    ,T1.CONTRACTTYPE_GRP --养殖模式名称
    ,T3.INVENTORY_ITEM_ID	--物料
    ,T3.ORDERED_QTY SALE_CNT	--实际销量
    ,t3.INCOME_AMT --收入
    ,t3.COST_AMT --成本
    ,t3.OTHER_INCOME_AMT - t3.OTHER_COST_AMT --其他业务利润
    ,t3.NO_OPER_INCOME --营业外收支
    ,T3.COMP_MANAGER_AMT --管理费用
    ,T3.COMP_SALE_AMT    --销售费用
    ,T3.COMP_FINC_AMT    --财务费用
    ,T3.COMP_PROFIT_AMT  --利润
    ,(t3.OTHER_REVENUE_FORCAST - t3.OTHER_COSTS_FORCAST)  --预测其他业务利润
    ,T3.fct_oper_inc_amt --预测营业外收支    
    ,T3.fct_manage_amt --预测管理费用       
    ,T3.fct_sale_amt --预测销售费用
    ,T3.fct_finc_amt --预测财务费用         
FROM (
        SELECT A.CONTRACT_NUM,A.PERIOD_ID,A.INVENTORY_ITEM_ID,A.INV_ORG_ID,A.ORG_ID,A.BUS_TYPE,A.PRODUCT_LINE
        ,A.ORDER_NUMBER
        ,sum(case when A.SRC_TYPE not in ('03','04','07') and a.ACC_MOD = '01' and a.bus_type in ('135010','135020') then A.INVOICE_TRANS_QTY 
                  when A.SRC_TYPE not in ('03','04','07') and a.bus_type not in ('135010','135020') then A.INVOICE_TRANS_QTY 
                  else 0 end ) ORDERED_QTY                                 --订单数量
        ,sum(A.NON_INCOME - A.NON_EXPENSE )                           as NO_OPER_INCOME --营业外收支
        ,SUM(A.ADMINI_EXPENSE)                                      AS COMP_MANAGER_AMT --管理费用
        ,SUM(A.SELLING_EXPENSE_FIXED + A.SELLING_EXPENSE_CHANGE) AS COMP_SALE_AMT --销售费用
        ,SUM(A.FIN_EXPENSE)                                         AS COMP_FINC_AMT --财务费用
        ,SUM(coalesce(A.INCOME,0)  --收入
             - coalesce(A.COST_AMOUNT_T,0) --总成本
             - coalesce(A.SELLING_EXPENSE_FIXED,0) - coalesce(A.SELLING_EXPENSE_CHANGE,0)  --销售费用(固定、变动)
             - coalesce(A.FIN_EXPENSE,0)  --财务费用
             - coalesce(A.ADMINI_EXPENSE,0) --管理费用
             - coalesce(A.OPERATING_TAX,0) --税金及附加
             - coalesce(A.AR_LOSSES_ASSET,0) --应收坏账损失
             - coalesce(A.OTHER_LOSSES_ASSET,0) --其他减值损失
             + coalesce(A.NON_INCOME,0) - coalesce(A.NON_EXPENSE,0)  --营业外收支
             + coalesce(A.CHANGE_IN_FAIR_VALUE,0)   --公允价值变动收益
             + coalesce(A.INVESTMENT_INCOME,0)      --投资收益
             + coalesce(A.OTHER_INCOME,0)           --其他收益
             + coalesce(A.ASSET_DISPOSIT_INCOME,0)  --资产处置收益
        ) AS COMP_PROFIT_AMT --利润  利润=收入-总成本-销售费用-固定-销售费用-变动-财务费用-管理费用-税金及附加-应收坏账损失-其他减值损失+营业外收入-营业外支出+公允价值变动收益+投资收益+其他收益+资产处置收益

        ,sum(A.NON_INCOME_FORCAST - A.NON_EXPENSE_FORCAST )                 as fct_oper_inc_amt --预测营业外收支
        ,SUM(A.ADMINI_EXPENSE_FORCAST)                                      AS fct_manage_amt --预测管理费用
        ,SUM(A.SELLING_EXPENSE_FIXED_FORCAST + A.SELLING_EXPENSE_CHANGE_FORCAST) AS fct_sale_amt --预测销售费用
        ,SUM(A.FIN_EXPENSE_FORCAST)                                         AS fct_finc_amt --预测财务费用

        ,SUM(case when SUBJECT_ID like '6001%' and a.ACC_MOD = '01' and a.bus_type in ('135010','135020') then INCOME 
                  when SUBJECT_ID like '6001%' and a.bus_type not in ('135010','135020') then INCOME
              else 0 end)                AS INCOME_AMT --收入(主营)
        ,SUM(CASE WHEN SUBJECT_ID like '6001%' and a.ACC_MOD = '01' and a.bus_type in ('135010','135020') THEN COST_AMOUNT_T 
                  WHEN SUBJECT_ID like '6001%' and a.bus_type not in ('135010','135020') THEN COST_AMOUNT_T 
              ELSE 0 end)         AS COST_AMT --成本（主营）
        ,SUM(CASE WHEN SUBJECT_ID like '60510101%' THEN INCOME ELSE 0 end)        AS OTHER_INCOME_AMT  --其他业务收入
        ,SUM(CASE WHEN SUBJECT_ID like '60510101%' THEN COST_AMOUNT_T ELSE 0 end) AS OTHER_COST_AMT  --其他业务成本 
        
        ,0     AS OTHER_REVENUE_FORCAST  --预测其他业务收入 
        ,0     AS OTHER_COSTS_FORCAST  --预测其他业务成本 
        
        FROM (
        select * from DMD_FIN_EXPS_PROFITS
        where CURRENCY_TYPE = '3' --币种类型：2:本位币，3：母币
        --and op_day = '${OP_DAY}'
        ) A --CW19
        inner join
        (select * from mreport_global.dwu_dim_material_new
          where (
            --兽药 
            (material_segment1_id = '65' and material_segment2_id in ('01','02','03','04','05','99'))
            or
            --饲料
            (material_segment1_id = '15' and material_segment2_id = '03')
            or
            --种苗
            (material_segment1_id = '25' and material_segment2_id in ('01','02'))
            or
            --合同毛鸡、毛鸭
            (material_segment1_id = '35' and material_segment2_id in ('31','32'))
            )
        ) b
        on a.INVENTORY_ITEM_ID = b.INVENTORY_ITEM_ID and a.INV_ORG_ID = b.INV_ORG_ID

         INNER JOIN MREPORT_GLOBAL.ODS_EBS_HR_ALL_ORGANIZATION_UNITS C
         ON C.ATTRIBUTE2='合作社-生产性' AND A.ORG_ID = C.ORGANIZATION_ID
        GROUP BY A.CONTRACT_NUM,A.PERIOD_ID,A.INVENTORY_ITEM_ID,A.INV_ORG_ID,A.ORG_ID,A.BUS_TYPE,A.PRODUCT_LINE,A.ORDER_NUMBER
) T3   --CW19费用
left JOIN (
    select 
  material_id
  ,CONTRACTNUMBER   --合同号
  ,CONTRACTTYPE_GRP   
  from DWU_QW_CONTRACT_DD 
  WHERE OP_DAY = '${OP_DAY}'
  group by  material_id
  ,CONTRACTNUMBER   --合同号
  ,CONTRACTTYPE_GRP
) T1        --QW03
ON T1.CONTRACTNUMBER = T3.CONTRACT_NUM and t1.material_id = t3.INVENTORY_ITEM_ID

"



echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DWP_BIRD_PROFIT_EST_DD;
    $INSERT_DWP_BIRD_PROFIT_EST_DD;
"  -v
