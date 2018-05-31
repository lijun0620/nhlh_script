#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_ar_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 应收账款余额
# 修改说明:                                                          
######################################################################

# 取总账期数据
GL_DAY=$1

OP_MONTH=${GL_DAY:0:6}
OP_DAY="20180520"

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_ar_dd.sh 20180101"
    exit 1
fi

# 当前时间减去30天时间
FORMAT_DAY=$(date -d $GL_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $GL_DAY +%Y%m01)
LAST_MONTH_END=$(date -d "$FIRST_DAY_MONTH -1 days" +%Y%m%d)

###########################################################################################
## 获取账单
## 变量声明
TMP_DWP_BIRD_AR_DD_0='TMP_DWP_BIRD_AR_DD_0'

CREATE_TMP_DWP_BIRD_AR_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_0(
  gl_date                       string     --总账日期
  ,off_date                     string     --核销账日期
  ,org_id                       string     --组织ID
  ,org_name                     string     --组织名称
  ,currency_id                  string     --原币币种
  ,loc_currency_id              string     --本位币种
  ,busi_type_id                 string     --业态  
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型

  ,debit_amt                    string     --应收金额(原币)
  ,loc_debit_amt                string     --应收金额(本位币)
  ,credit_amt                   string     --核销金额(原币)
  ,loc_credit_amt               string     --核销金额(本位币)
  ,ori_deposit_amt1             string     --原始收款单
  ,ori_deposit_amt2             string     --原始收款单(核销金额)
  ,loc_ori_deposit_amt1         string     --原始收款单(本位币)
  ,loc_ori_deposit_amt2         string     --原始收款单(本位币核销金额)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_0="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_0 PARTITION(op_day='$GL_DAY')
SELECT a1.gl_date
       ,a1.off_date
       ,a1.org_id
       ,a1.org_name
       ,a1.currency_id
       ,a1.loc_currency_id
       ,a1.bus_type busi_type_id
       ,a1.account_number cust_id
       ,a1.party_name cust_name
       ,a1.contract_no
       ,a1.salesrep_id
       ,null salesrep_name
       ,a1.invoice_type
       ,a1.invoice_type_name
       ,a1.debit_amt
       ,a1.loc_debit_amt
       ,a1.credit_amt
       ,a1.loc_credit_amt
       ,a1.ori_deposit_amt1
       ,a1.ori_deposit_amt2
       ,a1.loc_ori_deposit_amt1
       ,a1.loc_ori_deposit_amt2
  FROM (SELECT substr(t1.gl_date,1,10) gl_date            --应收事务处理总账日期
               ,substr(t2.gl_date,1,10) off_date          --销账日期
               ,t1.org_id                                 --OU组织      
               ,t1.org_name                               --OU组织名称
               ,t1.currency_id                            --原币种       
               ,t1.loc_currency_id                        --本位币种      
               ,t1.bus_type                               --业态        
               ,t1.account_number                         --客户编号      
               ,t1.party_name                             --客户名
               ,t1.purchase_order contract_no             --合同号
               ,t1.salesrep_id                            --销售员ID
               ,t1.invoice_type                           --应收单据类型
               ,coalesce(t1.invoice_type_desc,t1.invoice_type_name) invoice_type_name  --应收单据类型描述
               ,t1.amount_due_original debit_amt          --原币初始金额    
               ,t1.loc_amount_due_original loc_debit_amt  --本位币初始金额
               ,t2.amount credit_amt                      --核销金额(原币)
               ,t2.loc_amount loc_credit_amt              --核销金额(本位币)
               ,0 ori_deposit_amt1                        --原始收款单
               ,0 ori_deposit_amt2                        --原始收款单(核销金额)
               ,0 loc_ori_deposit_amt1                    --原始收款单(本位币)
               ,0 loc_ori_deposit_amt2                    --原始收款单(本位币核销金额)
          FROM (SELECT *
                  FROM dwu_cw_receipt_standard_invoice_dd
                 WHERE op_day='$OP_DAY'
                   AND invoice_type='INV'
                   AND regexp_replace(substr(gl_date,1,10),'-','')<='$GL_DAY') t1
          LEFT JOIN (SELECT account_number
                            ,org_id
                            ,applied_customer_trx_id
                            ,max(substr(gl_date,1,10)) gl_date
                            ,sum(amount) amount
                            ,sum(loc_amount) loc_amount
                       FROM dwu_cw_receipt_write_off_dd
                      WHERE op_day='$OP_DAY'
                        AND invoice_type='INV'
                        AND regexp_replace(substr(gl_date,1,10),'-','')<='$GL_DAY'
                      GROUP BY account_number
                            ,org_id
                            ,applied_customer_trx_id) t2
            ON (t1.account_number=t2.account_number
            AND t1.org_id=t2.org_id
            AND t1.customer_trx_id=t2.applied_customer_trx_id)
        UNION ALL
        SELECT substr(t1.gl_date,1,10) gl_date        --应收事务处理总账日期
               ,substr(t2.gl_date,1,10) off_date      --收款变动
               ,t1.org_id                             --OU组织      
               ,t1.org_name                           --OU组织名称
               ,t1.currency_id                        --原币种       
               ,t1.loc_currency_id                    --本位币种      
               ,t1.bus_type                           --业态        
               ,t1.account_number                     --客户编号      
               ,t1.party_name                         --客户名
               ,t1.purchase_order contract_no         --合同号
               ,t1.salesrep_id                        --销售员ID
               ,t1.invoice_type                       --应收单据类型
               ,coalesce(t1.invoice_type_desc,t1.invoice_type_name) invoice_type_name  --应收单据类型描述
               ,0 debit_amt                           --原币初始金额    
               ,0 loc_debit_amt                       --本位币初始金额
               ,0 credit_amt
               ,0 loc_credit_amt
               ,t1.amount_due_original ori_deposit_amt1          --原始收款单
               ,coalesce(t2.amount,0) ori_deposit_amt2           --原始收款单(核销金额)
               ,t1.loc_amount_due_original loc_ori_deposit_amt1  --原始收款单(本位币)
               ,coalesce(t2.loc_amount,0) loc_ori_deposit_amt2   --原始收款单(本位币核销金额)
          FROM (SELECT *
                  FROM dwu_cw_receipt_standard_invoice_dd
                 WHERE op_day='$OP_DAY'
                   AND invoice_type='PMT'
                   AND regexp_replace(substr(gl_date,1,10),'-','')<='$GL_DAY') t1
          LEFT JOIN (SELECT account_number
                            ,org_id
                            ,max(substr(gl_date,1,10)) gl_date
                            ,trx_number
                            ,sum(amount) amount
                            ,sum(loc_amount) loc_amount
                       FROM dwu_cw_receipt_write_off_his_dd
                      WHERE op_day='$OP_DAY'
                        AND invoice_type_desc in('收款退款','收款核销发票')
                        AND regexp_replace(substr(gl_date,1,10),'-','')<='$GL_DAY'
                      GROUP BY account_number
                            ,org_id
                            ,trx_number) t2
            ON (t1.account_number=t2.account_number
            AND t1.org_id=t2.org_id
            AND t1.trx_number=t2.trx_number)) a1
"

###########################################################################################
## 应收账款余额计算
## 变量声明
TMP_DWP_BIRD_AR_DD_1='TMP_DWP_BIRD_AR_DD_1'

CREATE_TMP_DWP_BIRD_AR_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_1(
  gl_date                       string     --总账日期
  ,org_id                       string     --组织ID
  ,org_name                     string     --组织名称
  ,currency_id                  string     --原币币种
  ,loc_currency_id              string     --本位币种
  ,busi_type_id                 string     --业态  
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型

  ,debit_amt                    string     --应收金额(原币)
  ,loc_debit_amt                string     --应收金额(本位币)
  ,usage_deposit_amt            string     --可用保证金(原币)
  ,loc_usage_deposit_amt        string     --可用保证金(本位币)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_1 PARTITION(op_day='$GL_DAY')
SELECT gl_date
       ,org_id
       ,org_name
       ,currency_id
       ,loc_currency_id
       ,busi_type_id
       ,cust_id
       ,cust_name
       ,contract_no
       ,salesrep_id
       ,salesrep_name
       ,invoice_type
       ,invoice_type_name
       ,sum(debit_amt) debit_amt
       ,sum(loc_debit_amt) loc_debit_amt
       ,sum(usage_deposit_amt) usage_deposit_amt
       ,sum(loc_usage_deposit_amt) loc_usage_deposit_amt
  FROM (SELECT '$GL_DAY' gl_date
               ,org_id
               ,org_name
               ,currency_id
               ,loc_currency_id
               ,busi_type_id
               ,cust_id
               ,cust_name
               ,contract_no
               ,salesrep_id
               ,salesrep_name
               ,invoice_type
               ,invoice_type_name
               ,debit_amt
               ,loc_debit_amt
               ,ori_deposit_amt1 usage_deposit_amt
               ,loc_ori_deposit_amt1 loc_usage_deposit_amt
          FROM $TMP_DWP_BIRD_AR_DD_0
         WHERE op_day='$GL_DAY'
           AND regexp_replace(gl_date,'-','')<='$GL_DAY'
        UNION ALL
        SELECT '$GL_DAY' gl_date
               ,org_id
               ,org_name
               ,currency_id
               ,loc_currency_id
               ,busi_type_id
               ,cust_id
               ,cust_name
               ,contract_no
               ,salesrep_id
               ,salesrep_name
               ,invoice_type
               ,invoice_type_name
               ,-credit_amt debit_amt
               ,-loc_credit_amt loc_debit_amt
               ,ori_deposit_amt2 usage_deposit_amt
               ,loc_ori_deposit_amt2 loc_usage_deposit_amt
          FROM $TMP_DWP_BIRD_AR_DD_0
         WHERE op_day='$GL_DAY'
           AND regexp_replace(off_date,'-','')<='$GL_DAY'
           AND off_date is not null) t1
 GROUP BY gl_date
       ,org_id
       ,org_name
       ,currency_id
       ,loc_currency_id
       ,busi_type_id
       ,cust_id
       ,cust_name
       ,contract_no
       ,salesrep_id
       ,salesrep_name
       ,invoice_type
       ,invoice_type_name
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_AR_DD_2='TMP_DWP_BIRD_AR_DD_2'

CREATE_TMP_DWP_BIRD_AR_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_2(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级      
  ,production_line_id           string     --产线    
  ,production_line_descr        string     --产线
  ,breed_type_id                string     --养殖模式
  ,breed_type_descr             string     --养殖模式
  ,recyle_type_id               string     --回收类型
  ,recyle_type_descr            string     --回收类型
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,contract_date                string     --合同日期
  ,mature_date                  string     --到期日
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型
  ,currency_id                  string     --原币种       
  ,loc_currency_id              string     --本位币种 
  ,original_ar_amount           string     --应该账款余额原币
  ,loc_ar_amount                string     --应该账款余额本位币
  ,original_deposit_amount      string     --可用保证金余额
  ,loc_deposit_amount           string     --可用保证金余额
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_2 PARTITION(op_day='$GL_DAY')
SELECT substr(gl_date,1,6) month_id      --期间(月份)    
       ,gl_date day_id                   --期间(日)     
       ,t3.level1_org_id                 --组织1级(股份)  
       ,t3.level1_org_descr              --组织1级(股份)  
       ,t3.level2_org_id                 --组织2级(片联)  
       ,t3.level2_org_descr              --组织2级(片联)  
       ,t3.level3_org_id                 --组织3级(片区)  
       ,t3.level3_org_descr              --组织3级(片区)  
       ,t3.level4_org_id                 --组织4级(小片)  
       ,t3.level4_org_descr              --组织4级(小片)  
       ,t3.level5_org_id                 --组织5级(公司)  
       ,t3.level5_org_descr              --组织5级(公司)  
       ,t3.level6_org_id                 --组织6级(OU)  
       ,t3.level6_org_descr              --组织6级(OU)  
       ,t4.level7_org_id                 --组织7级(库存组织)
       ,t4.level7_org_descr              --组织7级(库存组织)
       ,t5.level1_businesstype_id        --业态1级      
       ,t5.level1_businesstype_name      --业态1级      
       ,t5.level2_businesstype_id        --业态2级      
       ,t5.level2_businesstype_name      --业态2级      
       ,t5.level3_businesstype_id        --业态3级      
       ,t5.level3_businesstype_name      --业态3级      
       ,t5.level4_businesstype_id        --业态4级      
       ,t5.level4_businesstype_name      --业态4级
       ,t6.production_line_id            --产线    
       ,t6.production_line_descr         --产线
       ,t6.breed_type_id                 --养殖模式
       ,t6.breed_type_descr              --养殖模式
       ,t6.recyle_type_id                --回收类型
       ,t6.recyle_type_descr             --回收类型
       ,t1.cust_id                       --客户编号
       ,t1.cust_name                     --客户名称
       ,t1.contract_no                   --合同号(批次)
       ,t6.contract_date                 --合同日期
       ,t6.deadline_date mature_date     --到期日
       ,t1.salesrep_id                   --销售员ID
       ,t6.breedsaleman salesrep_name    --销售员名称
       ,t1.invoice_type                  --应收账款类型
       ,t1.invoice_type_name             --应收账款类型
       ,t1.currency_id                   --原币种       
       ,t1.loc_currency_id               --本位币种 
       ,t1.debit_amt original_ar_amount  --应该账款余额原币
       ,t1.loc_debit_amt loc_ar_amount   --应该账款余额本位币
       ,t1.usage_deposit_amt original_deposit_amount  --可用保证金余额原币
       ,t1.loc_usage_deposit_amt loc_deposit_amount       --可用保证金余额本位币
  FROM (SELECT gl_date                       --总账日期
               ,org_id                       --组织ID
               ,null inv_org_id              --库存组织
               ,org_name                     --组织名称
               ,currency_id                  --原币币种
               ,loc_currency_id              --本位币种
               ,busi_type_id                 --业态  
               ,cust_id                      --客户ID
               ,cust_name                    --客户名称
               ,contract_no                  --合同号(批次)
               ,salesrep_id                  --销售员ID
               ,salesrep_name                --销售员名称 
               ,invoice_type                 --应收账款类型
               ,invoice_type_name            --应收账款类型
               ,debit_amt                    --应收金额(原币)
               ,loc_debit_amt                --应收金额(本位币)
               ,usage_deposit_amt            --可用保证金(原币)
               ,loc_usage_deposit_amt        --可用保证金(本位币)
          FROM $TMP_DWP_BIRD_AR_DD_1
         WHERE op_day='$GL_DAY'
           AND (coalesce(loc_debit_amt,0)+coalesce(loc_usage_deposit_amt,0))!=0) t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t3
    ON (t1.org_id=t3.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t4
    ON (t1.inv_org_id=t4.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t5
    ON (t1.busi_type_id=t5.level4_businesstype_id)
  LEFT JOIN (SELECT case when contracttype_grp='代养' then '1'
                         when contracttype_grp='放养' then '2'
                    else null end breed_type_id
                    ,contracttype_grp breed_type_descr                    
                    ,case when guarantees_market='保值' then '1'
                          when guarantees_market='保底' then '2'
                          when guarantees_market='市场' then '3'
                     else null end recyle_type_id
                    ,guarantees_market recyle_type_descr        --投放类型
                    ,case when meaning='CHICHEN' then '1'
                          when meaning='DUCK' then '2'
                     else null end production_line_id           --产线代码
                    ,case when meaning='CHICHEN' then '鸡线'
                          when meaning='DUCK' then '鸭线'
                     else null end production_line_descr        --产线描述
                    ,contractnumber contract_no
                    ,substr(regexp_replace(contract_date,'-',''),1,8) contract_date --合同日期
                    ,substr(date_add(contract_date,50),1,10) deadline_date   --欠款到期日(50天)
                    ,breedsaleman
               FROM dwu_qw_contract_dd
              WHERE op_day='$OP_DAY') t6
    ON (t1.contract_no=t6.contract_no)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_AR_DD_3='TMP_DWP_BIRD_AR_DD_3'

CREATE_TMP_DWP_BIRD_AR_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_3(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级      
  ,production_line_id           string     --产线    
  ,production_line_descr        string     --产线
  ,breed_type_id                string     --养殖模式
  ,breed_type_descr             string     --养殖模式
  ,recyle_type_id               string     --回收类型
  ,recyle_type_descr            string     --回收类型
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,contract_date                string     --合同日期
  ,mature_date                  string     --到期日
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型
  ,currency_id                  string     --原币种       
  ,loc_currency_id              string     --本位币种 
  ,original_ar_amount           string     --应该账款余额原币
  ,loc_ar_amount                string     --应该账款余额本位币
  ,original_deposit_amount      string     --可用保证金余额
  ,loc_deposit_amount           string     --可用保证金余额
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>聚合计算>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_3="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_3 PARTITION(op_day='$GL_DAY')
SELECT month_id                             --期间(月份)    
       ,day_id                              --期间(日)     
       ,level1_org_id                       --组织1级(股份)  
       ,level1_org_descr                    --组织1级(股份)  
       ,level2_org_id                       --组织2级(片联)  
       ,level2_org_descr                    --组织2级(片联)  
       ,level3_org_id                       --组织3级(片区)  
       ,level3_org_descr                    --组织3级(片区)  
       ,level4_org_id                       --组织4级(小片)  
       ,level4_org_descr                    --组织4级(小片)  
       ,level5_org_id                       --组织5级(公司)  
       ,level5_org_descr                    --组织5级(公司)  
       ,level6_org_id                       --组织6级(OU)  
       ,level6_org_descr                    --组织6级(OU)  
       ,level7_org_id                       --组织7级(库存组织)
       ,level7_org_descr                    --组织7级(库存组织)
       ,level1_businesstype_id              --业态1级      
       ,level1_businesstype_name            --业态1级      
       ,level2_businesstype_id              --业态2级      
       ,level2_businesstype_name            --业态2级      
       ,level3_businesstype_id              --业态3级      
       ,level3_businesstype_name            --业态3级      
       ,level4_businesstype_id              --业态4级      
       ,level4_businesstype_name            --业态4级      
       ,production_line_id                  --产线    
       ,production_line_descr               --产线
       ,breed_type_id                       --养殖模式
       ,breed_type_descr                    --养殖模式
       ,recyle_type_id                      --回收类型
       ,recyle_type_descr                   --回收类型
       ,cust_id                             --客户ID
       ,cust_name                           --客户名称
       ,contract_no                         --合同号(批次)
       ,contract_date                       --合同日期
       ,mature_date                         --到期日
       ,salesrep_id                         --销售员ID
       ,salesrep_name                       --销售员名称
       ,invoice_type                        --应收账款类型
       ,invoice_type_name                   --应收账款类型
       ,currency_id                         --原币种       
       ,loc_currency_id                     --本位币种 
       ,original_ar_amount                  --应该账款余额原币
       ,loc_ar_amount                       --应该账款余额本位币
       ,original_deposit_amount             --可用保证金余额
       ,loc_deposit_amount                  --可用保证金余额
  FROM (SELECT month_id                           --期间(月份)    
               ,day_id                            --期间(日)     
               ,level1_org_id                     --组织1级(股份)  
               ,level1_org_descr                  --组织1级(股份)  
               ,level2_org_id                     --组织2级(片联)  
               ,level2_org_descr                  --组织2级(片联)  
               ,level3_org_id                     --组织3级(片区)  
               ,level3_org_descr                  --组织3级(片区)  
               ,level4_org_id                     --组织4级(小片)  
               ,level4_org_descr                  --组织4级(小片)  
               ,level5_org_id                     --组织5级(公司)  
               ,level5_org_descr                  --组织5级(公司)  
               ,level6_org_id                     --组织6级(OU)  
               ,level6_org_descr                  --组织6级(OU)  
               ,level7_org_id                     --组织7级(库存组织)
               ,level7_org_descr                  --组织7级(库存组织)
               ,level1_businesstype_id            --业态1级      
               ,level1_businesstype_name          --业态1级      
               ,level2_businesstype_id            --业态2级      
               ,level2_businesstype_name          --业态2级      
               ,level3_businesstype_id            --业态3级      
               ,level3_businesstype_name          --业态3级      
               ,level4_businesstype_id            --业态4级      
               ,level4_businesstype_name          --业态4级      
               ,production_line_id                --产线    
               ,production_line_descr             --产线
               ,breed_type_id                     --养殖模式
               ,breed_type_descr                  --养殖模式
               ,recyle_type_id                    --回收类型
               ,recyle_type_descr                 --回收类型
               ,cust_id                           --客户ID
               ,cust_name                         --客户名称
               ,contract_no                       --合同号(批次)
               ,contract_date                     --合同日期
               ,mature_date                       --到期日
               ,salesrep_id                       --销售员ID
               ,salesrep_name                     --销售员名称
               ,invoice_type                      --应收账款类型
               ,invoice_type_name                 --应收账款类型
               ,currency_id                       --原币种       
               ,loc_currency_id                   --本位币种 
               ,sum(original_ar_amount) original_ar_amount   --应该账款余额原币
               ,sum(loc_ar_amount) loc_ar_amount             --应该账款余额本位币
               ,sum(original_deposit_amount) original_deposit_amount   --可用保证金余额
               ,sum(loc_deposit_amount) loc_deposit_amount             --可用保证金余额
          FROM $TMP_DWP_BIRD_AR_DD_2
         WHERE op_day='$GL_DAY'
         GROUP BY month_id                        --期间(月份)    
               ,day_id                            --期间(日)     
               ,level1_org_id                     --组织1级(股份)  
               ,level1_org_descr                  --组织1级(股份)  
               ,level2_org_id                     --组织2级(片联)  
               ,level2_org_descr                  --组织2级(片联)  
               ,level3_org_id                     --组织3级(片区)  
               ,level3_org_descr                  --组织3级(片区)  
               ,level4_org_id                     --组织4级(小片)  
               ,level4_org_descr                  --组织4级(小片)  
               ,level5_org_id                     --组织5级(公司)  
               ,level5_org_descr                  --组织5级(公司)  
               ,level6_org_id                     --组织6级(OU)  
               ,level6_org_descr                  --组织6级(OU)  
               ,level7_org_id                     --组织7级(库存组织)
               ,level7_org_descr                  --组织7级(库存组织)
               ,level1_businesstype_id            --业态1级      
               ,level1_businesstype_name          --业态1级      
               ,level2_businesstype_id            --业态2级      
               ,level2_businesstype_name          --业态2级      
               ,level3_businesstype_id            --业态3级      
               ,level3_businesstype_name          --业态3级      
               ,level4_businesstype_id            --业态4级      
               ,level4_businesstype_name          --业态4级      
               ,production_line_id                --产线    
               ,production_line_descr             --产线
               ,breed_type_id                     --养殖模式
               ,breed_type_descr                  --养殖模式
               ,recyle_type_id                    --回收类型
               ,recyle_type_descr                 --回收类型
               ,cust_id                           --客户ID
               ,cust_name                         --客户名称
               ,contract_no                       --合同号(批次)
               ,contract_date                     --合同日期
               ,mature_date                       --到期日
               ,salesrep_id                       --销售员ID
               ,salesrep_name                     --销售员名称
               ,invoice_type                      --应收账款类型
               ,invoice_type_name                 --应收账款类型
               ,currency_id                       --原币种       
               ,loc_currency_id                   --本位币种
        ) t1
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_AR_DD_4='TMP_DWP_BIRD_AR_DD_4'

CREATE_TMP_DWP_BIRD_AR_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_4(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级      
  ,production_line_id           string     --产线    
  ,production_line_descr        string     --产线
  ,breed_type_id                string     --养殖模式
  ,breed_type_descr             string     --养殖模式
  ,recyle_type_id               string     --回收类型
  ,recyle_type_descr            string     --回收类型
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,contract_date                string     --合同日期
  ,mature_date                  string     --到期日
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型
  ,currency_id                  string     --原币种       
  ,currency_descr               string     --本位币种
  ,ar_amt                       string     --应该账款余额
  ,usage_deposit_amt            string     --可用保证金余额
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>处理币种>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_4="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_4 PARTITION(op_day='$GL_DAY')
SELECT month_id                             --期间(月份)    
       ,day_id                              --期间(日)     
       ,level1_org_id                       --组织1级(股份)  
       ,level1_org_descr                    --组织1级(股份)  
       ,level2_org_id                       --组织2级(片联)  
       ,level2_org_descr                    --组织2级(片联)  
       ,level3_org_id                       --组织3级(片区)  
       ,level3_org_descr                    --组织3级(片区)  
       ,level4_org_id                       --组织4级(小片)  
       ,level4_org_descr                    --组织4级(小片)  
       ,level5_org_id                       --组织5级(公司)  
       ,level5_org_descr                    --组织5级(公司)  
       ,level6_org_id                       --组织6级(OU)  
       ,level6_org_descr                    --组织6级(OU)  
       ,level7_org_id                       --组织7级(库存组织)
       ,level7_org_descr                    --组织7级(库存组织)
       ,level1_businesstype_id              --业态1级      
       ,level1_businesstype_name            --业态1级      
       ,level2_businesstype_id              --业态2级      
       ,level2_businesstype_name            --业态2级      
       ,level3_businesstype_id              --业态3级      
       ,level3_businesstype_name            --业态3级      
       ,level4_businesstype_id              --业态4级      
       ,level4_businesstype_name            --业态4级      
       ,production_line_id                  --产线    
       ,production_line_descr               --产线
       ,breed_type_id                       --养殖模式
       ,breed_type_descr                    --养殖模式
       ,recyle_type_id                      --回收类型
       ,recyle_type_descr                   --回收类型
       ,cust_id                             --客户ID
       ,cust_name                           --客户名称
       ,contract_no                         --合同号(批次)
       ,contract_date                       --合同日期
       ,mature_date                         --到期日
       ,salesrep_id                         --销售员ID
       ,salesrep_name                       --销售员名称
       ,invoice_type                        --应收账款类型
       ,invoice_type_name                   --应收账款类型
       ,currency_id                         --币种       
       ,currency_descr                      --币种 
       ,ar_amt                              --应该账款余额
       ,usage_deposit_amt                   --可用保证金余额
  FROM (SELECT month_id                           --期间(月份)    
               ,day_id                            --期间(日)     
               ,level1_org_id                     --组织1级(股份)  
               ,level1_org_descr                  --组织1级(股份)  
               ,level2_org_id                     --组织2级(片联)  
               ,level2_org_descr                  --组织2级(片联)  
               ,level3_org_id                     --组织3级(片区)  
               ,level3_org_descr                  --组织3级(片区)  
               ,level4_org_id                     --组织4级(小片)  
               ,level4_org_descr                  --组织4级(小片)  
               ,level5_org_id                     --组织5级(公司)  
               ,level5_org_descr                  --组织5级(公司)  
               ,level6_org_id                     --组织6级(OU)  
               ,level6_org_descr                  --组织6级(OU)  
               ,level7_org_id                     --组织7级(库存组织)
               ,level7_org_descr                  --组织7级(库存组织)
               ,level1_businesstype_id            --业态1级      
               ,level1_businesstype_name          --业态1级      
               ,level2_businesstype_id            --业态2级      
               ,level2_businesstype_name          --业态2级      
               ,level3_businesstype_id            --业态3级      
               ,level3_businesstype_name          --业态3级      
               ,level4_businesstype_id            --业态4级      
               ,level4_businesstype_name          --业态4级      
               ,production_line_id                --产线    
               ,production_line_descr             --产线
               ,breed_type_id                     --养殖模式
               ,breed_type_descr                  --养殖模式
               ,recyle_type_id                    --回收类型
               ,recyle_type_descr                 --回收类型
               ,cust_id                           --客户ID
               ,cust_name                         --客户名称
               ,contract_no                       --合同号(批次)
               ,contract_date                     --合同日期
               ,mature_date                       --到期日
               ,salesrep_id                       --销售员ID
               ,salesrep_name                     --销售员名称
               ,invoice_type                      --应收账款类型
               ,invoice_type_name                 --应收账款类型
               ,'2' currency_id                   --币种(本位币)
               ,loc_currency_id currency_descr    --币种 
               ,loc_ar_amount ar_amt              --应该账款余额本位币
               ,loc_deposit_amount usage_deposit_amt  --可用保证金余额本位币
          FROM $TMP_DWP_BIRD_AR_DD_3
         WHERE op_day='$GL_DAY'
        UNION ALL
        SELECT month_id                           --期间(月份)    
               ,day_id                            --期间(日)     
               ,level1_org_id                     --组织1级(股份)  
               ,level1_org_descr                  --组织1级(股份)  
               ,level2_org_id                     --组织2级(片联)  
               ,level2_org_descr                  --组织2级(片联)  
               ,level3_org_id                     --组织3级(片区)  
               ,level3_org_descr                  --组织3级(片区)  
               ,level4_org_id                     --组织4级(小片)  
               ,level4_org_descr                  --组织4级(小片)  
               ,level5_org_id                     --组织5级(公司)  
               ,level5_org_descr                  --组织5级(公司)  
               ,level6_org_id                     --组织6级(OU)  
               ,level6_org_descr                  --组织6级(OU)  
               ,level7_org_id                     --组织7级(库存组织)
               ,level7_org_descr                  --组织7级(库存组织)
               ,level1_businesstype_id            --业态1级      
               ,level1_businesstype_name          --业态1级      
               ,level2_businesstype_id            --业态2级      
               ,level2_businesstype_name          --业态2级      
               ,level3_businesstype_id            --业态3级      
               ,level3_businesstype_name          --业态3级      
               ,level4_businesstype_id            --业态4级      
               ,level4_businesstype_name          --业态4级      
               ,production_line_id                --产线    
               ,production_line_descr             --产线
               ,breed_type_id                     --养殖模式
               ,breed_type_descr                  --养殖模式
               ,recyle_type_id                    --回收类型
               ,recyle_type_descr                 --回收类型
               ,cust_id                           --客户ID
               ,cust_name                         --客户名称
               ,contract_no                       --合同号(批次)
               ,contract_date                     --合同日期
               ,mature_date                       --到期日
               ,salesrep_id                       --销售员ID
               ,salesrep_name                     --销售员名称
               ,invoice_type                      --应收账款类型
               ,invoice_type_name                 --应收账款类型
               ,'3' currency_id                   --币种(母币)
               ,'CNY' currency_descr              --币种
               ,case when loc_currency_id='CNY' then loc_ar_amount
                else round(a2.conversion_rate*loc_ar_amount,2) end ar_amt  --应该账款余额
               ,case when loc_currency_id='CNY' then loc_deposit_amount
                else round(a2.conversion_rate*loc_deposit_amount,2) end usage_deposit_amt  --可用保证金余额
          FROM $TMP_DWP_BIRD_AR_DD_3 a1
          LEFT JOIN (SELECT from_currency,
                            to_currency,
                            conversion_rate
                       FROM mreport_global.dmd_fin_period_currency_rate_mm
                      WHERE conversion_period='$OP_MONTH'
                        AND to_currency='CNY') a2
            ON (a1.loc_currency_id=a2.from_currency)
         WHERE op_day='$GL_DAY') t1
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_AR_DD_5='TMP_DWP_BIRD_AR_DD_5'

CREATE_TMP_DWP_BIRD_AR_DD_5="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_AR_DD_5(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级      
  ,production_line_id           string     --产线    
  ,production_line_descr        string     --产线
  ,breed_type_id                string     --养殖模式
  ,breed_type_descr             string     --养殖模式
  ,recyle_type_id               string     --回收类型
  ,recyle_type_descr            string     --回收类型
  ,cust_id                      string     --客户ID
  ,cust_name                    string     --客户名称
  ,contract_no                  string     --合同号(批次)
  ,contract_date                string     --合同日期
  ,mature_date                  string     --到期日
  ,salesrep_id                  string     --销售员ID
  ,salesrep_name                string     --销售员名称
  ,invoice_type                 string     --应收账款类型
  ,invoice_type_name            string     --应收账款类型
  ,currency_id                  string     --原币种       
  ,currency_descr               string     --本位币种
  ,ar_amt                       string     --应该账款余额
  ,ar_begin_amt                 string     --月初应收账款余额
  ,ar_end_amt                   string     --期末应收账款余额
  ,ar_due_amt                   string     --其中截止到期日金额
  ,o_due_days                   string     --距离到期日天数
  ,ar_due_end_amt               string     --其中：期末应收账款余额中已逾期金额
  ,due_days                     string     --逾期天数
  ,ar_0_30                      string     --账龄分析0-30
  ,ar_30_60                     string     --账龄分析30-60
  ,ar_60_90                     string     --账龄分析60-90
  ,ar_90                        string     --账龄分析90以上
  ,usage_deposit_amt            string     --可用保证金余额
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>关联合同及相关信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_AR_DD_5="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_AR_DD_5 PARTITION(op_day='$GL_DAY')
SELECT month_id                             --期间(月份)    
       ,day_id                              --期间(日)     
       ,level1_org_id                       --组织1级(股份)  
       ,level1_org_descr                    --组织1级(股份)  
       ,level2_org_id                       --组织2级(片联)  
       ,level2_org_descr                    --组织2级(片联)  
       ,level3_org_id                       --组织3级(片区)  
       ,level3_org_descr                    --组织3级(片区)  
       ,level4_org_id                       --组织4级(小片)  
       ,level4_org_descr                    --组织4级(小片)  
       ,level5_org_id                       --组织5级(公司)  
       ,level5_org_descr                    --组织5级(公司)  
       ,level6_org_id                       --组织6级(OU)  
       ,level6_org_descr                    --组织6级(OU)  
       ,level7_org_id                       --组织7级(库存组织)
       ,level7_org_descr                    --组织7级(库存组织)
       ,level1_businesstype_id              --业态1级      
       ,level1_businesstype_name            --业态1级      
       ,level2_businesstype_id              --业态2级      
       ,level2_businesstype_name            --业态2级      
       ,level3_businesstype_id              --业态3级      
       ,level3_businesstype_name            --业态3级      
       ,level4_businesstype_id              --业态4级      
       ,level4_businesstype_name            --业态4级      
       ,production_line_id                  --产线    
       ,production_line_descr               --产线
       ,breed_type_id                       --养殖模式
       ,breed_type_descr                    --养殖模式
       ,recyle_type_id                      --回收类型
       ,recyle_type_descr                   --回收类型
       ,cust_id                             --客户ID
       ,cust_name                           --客户名称
       ,contract_no                         --合同号(批次)
       ,contract_date                       --合同日期
       ,mature_date                         --到期日
       ,salesrep_id                         --销售员ID
       ,salesrep_name                       --销售员名称
       ,invoice_type                        --应收账款类型
       ,invoice_type_name                   --应收账款类型
       ,currency_id                         --币种       
       ,currency_descr                      --币种 
       ,sum(coalesce(ar_amt,0))                              --应收账款余额
       ,sum(coalesce(ar_begin_amt,0))                        --月初应收账款余额
       ,sum(coalesce(ar_end_amt,0))                          --期末应收账款余额
       ,sum(coalesce(ar_due_amt,0))                          --其中截止到期日金额
       ,sum(coalesce(o_due_days,0))                          --距离到期日天数
       ,sum(coalesce(ar_due_end_amt,0))                      --其中：期末应收账款余额中已逾期金额
       ,sum(coalesce(due_days,0))                            --逾期天数
       ,sum(coalesce(ar_0_30,0))                             --账龄分析0-30
       ,sum(coalesce(ar_30_60,0))                            --账龄分析30-60
       ,sum(coalesce(ar_60_90,0))                            --账龄分析60-90
       ,sum(coalesce(ar_90,0))                               --账龄分析90以上
       ,sum(coalesce(usage_deposit_amt,0))                   --可用保证金余额
  FROM (SELECT month_id                            --期间(月份)    
               ,day_id                             --期间(日)     
               ,level1_org_id                      --组织1级(股份)  
               ,level1_org_descr                   --组织1级(股份)  
               ,level2_org_id                      --组织2级(片联)  
               ,level2_org_descr                   --组织2级(片联)  
               ,level3_org_id                      --组织3级(片区)  
               ,level3_org_descr                   --组织3级(片区)  
               ,level4_org_id                      --组织4级(小片)  
               ,level4_org_descr                   --组织4级(小片)  
               ,level5_org_id                      --组织5级(公司)  
               ,level5_org_descr                   --组织5级(公司)  
               ,level6_org_id                      --组织6级(OU)  
               ,level6_org_descr                   --组织6级(OU)  
               ,level7_org_id                      --组织7级(库存组织)
               ,level7_org_descr                   --组织7级(库存组织)
               ,level1_businesstype_id             --业态1级      
               ,level1_businesstype_name           --业态1级      
               ,level2_businesstype_id             --业态2级      
               ,level2_businesstype_name           --业态2级      
               ,level3_businesstype_id             --业态3级      
               ,level3_businesstype_name           --业态3级      
               ,level4_businesstype_id             --业态4级      
               ,level4_businesstype_name           --业态4级      
               ,production_line_id                 --产线    
               ,production_line_descr              --产线
               ,breed_type_id                      --养殖模式
               ,breed_type_descr                   --养殖模式
               ,recyle_type_id                     --回收类型
               ,recyle_type_descr                  --回收类型
               ,cust_id                            --客户ID
               ,cust_name                          --客户名称
               ,contract_no                        --合同号(批次)
               ,contract_date                      --合同日期
               ,mature_date                        --到期日
               ,salesrep_id                        --销售员ID
               ,salesrep_name                      --销售员名称
               ,invoice_type                       --应收账款类型
               ,invoice_type_name                  --应收账款类型
               ,currency_id                        --原币种       
               ,currency_descr                     --本位币种
               ,ar_amt                             --应收账款余额
               ,0 ar_begin_amt                     --月初应收账款余额
               ,ar_amt ar_end_amt                  --期末应收账款余额
               ,case when datediff('$FORMAT_DAY', mature_date)<=0 then ar_amt else '0' end ar_due_amt  --其中截止到期日金额
               ,case when datediff('$FORMAT_DAY', mature_date)<=0 then datediff(mature_date, '$FORMAT_DAY')
                else '0' end o_due_days            --距离到期日天数
               ,case when datediff('$FORMAT_DAY', mature_date)>0 then ar_amt else '0' end ar_due_end_amt --其中：期末应收账款余额中已逾期金额
               ,case when datediff('$FORMAT_DAY', mature_date)>0 then datediff('$FORMAT_DAY', mature_date)
                else '0' end due_days              --逾期天数
               ,case when datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))<=30 then ar_amt
                else '0' end ar_0_30               --账龄分析0-30
               ,case when datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))>30 and datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))<=60
                     then ar_amt
                else '0' end ar_30_60              --账龄分析30-60
               ,case when datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))>60 and datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))<=90
                     then ar_amt
                else '0' end ar_60_90              --账龄分析60-90
               ,case when datediff('$FORMAT_DAY', concat(substr(contract_date,1,4),'-',substr(contract_date,5,2),'-',substr(contract_date,7,2)))>=90 then ar_amt
                else '0' end ar_90                 --账龄分析90以上
               ,usage_deposit_amt                  --可用保证金余额
          FROM $TMP_DWP_BIRD_AR_DD_4
         WHERE op_day='$GL_DAY'
        UNION ALL
        SELECT '$OP_MONTH' month_id                --期间(月份)    
               ,'$GL_DAY' day_id                   --期间(日)     
               ,level1_org_id                       --组织1级(股份)  
               ,level1_org_descr                    --组织1级(股份)  
               ,level2_org_id                       --组织2级(片联)  
               ,level2_org_descr                    --组织2级(片联)  
               ,level3_org_id                       --组织3级(片区)  
               ,level3_org_descr                    --组织3级(片区)  
               ,level4_org_id                       --组织4级(小片)  
               ,level4_org_descr                    --组织4级(小片)  
               ,level5_org_id                       --组织5级(公司)  
               ,level5_org_descr                    --组织5级(公司)  
               ,level6_org_id                       --组织6级(OU)  
               ,level6_org_descr                    --组织6级(OU)  
               ,level7_org_id                       --组织7级(库存组织)
               ,level7_org_descr                    --组织7级(库存组织)
               ,level1_businesstype_id              --业态1级      
               ,level1_businesstype_name            --业态1级      
               ,level2_businesstype_id              --业态2级      
               ,level2_businesstype_name            --业态2级      
               ,level3_businesstype_id              --业态3级      
               ,level3_businesstype_name            --业态3级      
               ,level4_businesstype_id              --业态4级      
               ,level4_businesstype_name            --业态4级      
               ,production_line_id                  --产线    
               ,production_line_descr               --产线
               ,breed_type_id                       --养殖模式
               ,breed_type_descr                    --养殖模式
               ,recyle_type_id                      --回收类型
               ,recyle_type_descr                   --回收类型
               ,cust_id                             --客户ID
               ,cust_name                           --客户名称
               ,batch_id contract_no                --合同号(批次)
               ,contract_date                       --合同日期
               ,mature_date                         --到期日
               ,salesrep_id                         --销售员ID
               ,salesrep_name                       --销售员名称
               ,ar_type_id invoice_type             --应收账款类型
               ,ar_type_descr invoice_type_name     --应收账款类型
               ,currency_id                         --币种       
               ,currency_descr                      --币种
               ,0 ar_amt                            --应收账款余额
               ,ar_end_amt ar_begin_amt             --月初应收账款余额
               ,0 ar_end_amt                        --期末应收账款余额
               ,0 ar_due_amt                        --其中截止到期日金额
               ,0 o_due_days                        --距离到期日天数
               ,0 ar_due_end_amt                    --其中：期末应收账款余额中已逾期金额
               ,0 due_days                          --逾期天数
               ,0 ar_0_30                           --账龄分析0-30
               ,0 ar_30_60                          --账龄分析30-60
               ,0 ar_60_90                          --账龄分析60-90
               ,0 ar_90                             --账龄分析90以上
               ,0 usage_deposit_amt                 --可用保证金余额
          FROM dwp_bird_ar_dd
         WHERE op_day='$LAST_MONTH_END'
           AND ar_type_id='INV') t1
 GROUP BY month_id                             --期间(月份)    
       ,day_id                              --期间(日)     
       ,level1_org_id                       --组织1级(股份)  
       ,level1_org_descr                    --组织1级(股份)  
       ,level2_org_id                       --组织2级(片联)  
       ,level2_org_descr                    --组织2级(片联)  
       ,level3_org_id                       --组织3级(片区)  
       ,level3_org_descr                    --组织3级(片区)  
       ,level4_org_id                       --组织4级(小片)  
       ,level4_org_descr                    --组织4级(小片)  
       ,level5_org_id                       --组织5级(公司)  
       ,level5_org_descr                    --组织5级(公司)  
       ,level6_org_id                       --组织6级(OU)  
       ,level6_org_descr                    --组织6级(OU)  
       ,level7_org_id                       --组织7级(库存组织)
       ,level7_org_descr                    --组织7级(库存组织)
       ,level1_businesstype_id              --业态1级      
       ,level1_businesstype_name            --业态1级      
       ,level2_businesstype_id              --业态2级      
       ,level2_businesstype_name            --业态2级      
       ,level3_businesstype_id              --业态3级      
       ,level3_businesstype_name            --业态3级      
       ,level4_businesstype_id              --业态4级      
       ,level4_businesstype_name            --业态4级      
       ,production_line_id                  --产线    
       ,production_line_descr               --产线
       ,breed_type_id                       --养殖模式
       ,breed_type_descr                    --养殖模式
       ,recyle_type_id                      --回收类型
       ,recyle_type_descr                   --回收类型
       ,cust_id                             --客户ID
       ,cust_name                           --客户名称
       ,contract_no                         --合同号(批次)
       ,contract_date                       --合同日期
       ,mature_date                         --到期日
       ,salesrep_id                         --销售员ID
       ,salesrep_name                       --销售员名称
       ,invoice_type                        --应收账款类型
       ,invoice_type_name                   --应收账款类型
       ,currency_id                         --币种       
       ,currency_descr                      --币种
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWP_BIRD_AR_DD='DWP_BIRD_AR_DD'

CREATE_DWP_BIRD_AR_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_AR_DD(
  month_id                          string      --期间(月份)           
  ,day_id                           string      --期间(日)            
  ,level1_org_id                    string      --组织1级(股份)         
  ,level1_org_descr                 string      --组织1级(股份)         
  ,level2_org_id                    string      --组织2级(片联)         
  ,level2_org_descr                 string      --组织2级(片联)         
  ,level3_org_id                    string      --组织3级(片区)         
  ,level3_org_descr                 string      --组织3级(片区)         
  ,level4_org_id                    string      --组织4级(小片)         
  ,level4_org_descr                 string      --组织4级(小片)         
  ,level5_org_id                    string      --组织5级(公司)         
  ,level5_org_descr                 string      --组织5级(公司)         
  ,level6_org_id                    string      --组织6级(OU)         
  ,level6_org_descr                 string      --组织6级(OU)         
  ,level7_org_id                    string      --组织7级(库存组织)       
  ,level7_org_descr                 string      --组织7级(库存组织)       
  ,level1_businesstype_id           string      --业态1级             
  ,level1_businesstype_name         string      --业态1级             
  ,level2_businesstype_id           string      --业态2级             
  ,level2_businesstype_name         string      --业态2级             
  ,level3_businesstype_id           string      --业态3级             
  ,level3_businesstype_name         string      --业态3级             
  ,level4_businesstype_id           string      --业态4级             
  ,level4_businesstype_name         string      --业态4级             
  ,production_line_id               string      --产线               
  ,production_line_descr            string      --产线               
  ,breed_type_id                    string      --养殖模式             
  ,breed_type_descr                 string      --养殖模式             
  ,recyle_type_id                   string      --回收类型             
  ,recyle_type_descr                string      --回收类型             
  ,cust_id                          string      --客户ID             
  ,cust_name                        string      --客户名称             
  ,batch_id                         string      --批次号              
  ,contract_date                    string      --合同日期
  ,mature_date                      string      --到期日
  ,salesrep_id                      string      --销售员
  ,salesrep_name                    string      --销售员名称
  ,ar_type_id                       string      --应收账款类型
  ,ar_type_descr                    string      --应收账款类型
  ,currency_id                      string      --币种       
  ,currency_descr                   string      --币种
  ,ar_begin_amt                     string      --月初应收账款余额         
  ,ar_end_amt                       string      --期末应收账款余额         
  ,ar_due_amt                       string      --其中截止到期日金额        
  ,o_due_days                       string      --距离到期日天数          
  ,ar_due_end_amt                   string      --其中：期末应收账款余额中已逾期金额          
  ,due_days                         string      --逾期天数             
  ,ar_0_30                          string      --账龄分析0-30         
  ,ar_30_60                         string      --账龄分析30-60        
  ,ar_60_90                         string      --账龄分析60-90        
  ,ar_90                            string      --账龄分析90以上         
  ,usable_deposit                   string      --可用保证金
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_AR_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_AR_DD PARTITION(op_day='$GL_DAY')
SELECT t1.month_id                                --期间(月份)           
       ,t1.day_id                                 --期间(日)            
       ,t1.level1_org_id                          --组织1级(股份)         
       ,t1.level1_org_descr                       --组织1级(股份)         
       ,t1.level2_org_id                          --组织2级(片联)         
       ,t1.level2_org_descr                       --组织2级(片联)         
       ,t1.level3_org_id                          --组织3级(片区)         
       ,t1.level3_org_descr                       --组织3级(片区)         
       ,t1.level4_org_id                          --组织4级(小片)         
       ,t1.level4_org_descr                       --组织4级(小片)         
       ,t1.level5_org_id                          --组织5级(公司)         
       ,t1.level5_org_descr                       --组织5级(公司)         
       ,t1.level6_org_id                          --组织6级(OU)         
       ,t1.level6_org_descr                       --组织6级(OU)         
       ,t1.level7_org_id                          --组织7级(库存组织)       
       ,t1.level7_org_descr                       --组织7级(库存组织)       
       ,t1.level1_businesstype_id                 --业态1级             
       ,t1.level1_businesstype_name               --业态1级             
       ,t1.level2_businesstype_id                 --业态2级             
       ,t1.level2_businesstype_name               --业态2级             
       ,t1.level3_businesstype_id                 --业态3级             
       ,t1.level3_businesstype_name               --业态3级             
       ,t1.level4_businesstype_id                 --业态4级             
       ,t1.level4_businesstype_name               --业态4级             
       ,t1.production_line_id                     --产线               
       ,t1.production_line_descr                  --产线               
       ,t1.breed_type_id                          --养殖模式             
       ,t1.breed_type_descr                       --养殖模式             
       ,t1.recyle_type_id                         --回收类型             
       ,t1.recyle_type_descr                      --回收类型             
       ,t1.cust_id                                --客户ID             
       ,t1.cust_name                              --客户名称             
       ,t1.contract_no batch_id                   --批次号              
       ,t1.contract_date                          --合同日期
       ,t1.mature_date                            --到期日
       ,t1.salesrep_id                            --销售员
       ,t1.salesrep_name                          --销售员名称
       ,t1.invoice_type                           --应收账款类型
       ,t1.invoice_type_name                      --应收账款类型
       ,t1.currency_id                            --币种       
       ,t1.currency_descr                         --币种
       ,t1.ar_begin_amt                           --月初应收账款余额         
       ,t1.ar_end_amt                             --期末应收账款余额         
       ,t1.ar_due_amt                             --其中截止到期日金额        
       ,t1.o_due_days                             --距离到期日天数          
       ,t1.ar_due_end_amt                         --其中：期末应收账款余额中已逾期金额          
       ,t1.due_days                               --逾期天数             
       ,t1.ar_0_30                                --账龄分析0-30         
       ,t1.ar_30_60                               --账龄分析30-60        
       ,t1.ar_60_90                               --账龄分析60-90        
       ,t1.ar_90                                  --账龄分析90以上         
       ,t2.usage_deposit_amt usable_deposit       --可用保证金
  FROM (SELECT month_id                                --期间(月份)           
               ,day_id                                 --期间(日)            
               ,level1_org_id                          --组织1级(股份)         
               ,level1_org_descr                       --组织1级(股份)         
               ,level2_org_id                          --组织2级(片联)         
               ,level2_org_descr                       --组织2级(片联)         
               ,level3_org_id                          --组织3级(片区)         
               ,level3_org_descr                       --组织3级(片区)         
               ,level4_org_id                          --组织4级(小片)         
               ,level4_org_descr                       --组织4级(小片)         
               ,level5_org_id                          --组织5级(公司)         
               ,level5_org_descr                       --组织5级(公司)         
               ,level6_org_id                          --组织6级(OU)         
               ,level6_org_descr                       --组织6级(OU)         
               ,level7_org_id                          --组织7级(库存组织)       
               ,level7_org_descr                       --组织7级(库存组织)       
               ,level1_businesstype_id                 --业态1级             
               ,level1_businesstype_name               --业态1级             
               ,level2_businesstype_id                 --业态2级             
               ,level2_businesstype_name               --业态2级             
               ,level3_businesstype_id                 --业态3级             
               ,level3_businesstype_name               --业态3级             
               ,level4_businesstype_id                 --业态4级             
               ,level4_businesstype_name               --业态4级             
               ,production_line_id                     --产线               
               ,production_line_descr                  --产线               
               ,breed_type_id                          --养殖模式             
               ,breed_type_descr                       --养殖模式             
               ,recyle_type_id                         --回收类型             
               ,recyle_type_descr                      --回收类型             
               ,cust_id                                --客户ID             
               ,cust_name                              --客户名称             
               ,contract_no                            --批次号              
               ,contract_date                          --合同日期
               ,mature_date                            --到期日
               ,salesrep_id                            --销售员
               ,salesrep_name                          --销售员名称
               ,invoice_type                           --应收账款类型
               ,invoice_type_name                      --应收账款类型
               ,currency_id                            --币种       
               ,currency_descr                         --币种
               ,round(ar_begin_amt,4) ar_begin_amt     --月初应收账款余额         
               ,round(ar_end_amt,4) ar_end_amt         --期末应收账款余额         
               ,round(ar_due_amt,4) ar_due_amt         --其中截止到期日金额        
               ,round(o_due_days,4) o_due_days         --距离到期日天数          
               ,round(ar_due_end_amt,4) ar_due_end_amt --其中：期末应收账款余额中已逾期金额          
               ,round(due_days,4) due_days             --逾期天数             
               ,round(ar_0_30,4) ar_0_30               --账龄分析0-30         
               ,round(ar_30_60,4) ar_30_60             --账龄分析30-60        
               ,round(ar_60_90,4) ar_60_90             --账龄分析60-90        
               ,round(ar_90,4) ar_90                   --账龄分析90以上 
          FROM $TMP_DWP_BIRD_AR_DD_5
         WHERE op_day='$GL_DAY'
           AND invoice_type='INV') t1
  LEFT JOIN (SELECT cust_id
                    ,contract_no
                    ,round(usage_deposit_amt,4) usage_deposit_amt
               FROM $TMP_DWP_BIRD_AR_DD_5
              WHERE op_day='$GL_DAY'
                AND invoice_type='PMT') t2
    ON (t1.cust_id=t2.cust_id
    AND t1.contract_no=t2.contract_no)
  LEFT JOIN (SELECT salesperson_id
                    ,salesperson_name
               FROM mreport_global.dim_salesperson
              WHERE 1=0) t3
    ON (t1.salesrep_id=t3.salesperson_id)
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

    $CREATE_TMP_DWP_BIRD_AR_DD_0;
    $INSERT_TMP_DWP_BIRD_AR_DD_0;
    $CREATE_TMP_DWP_BIRD_AR_DD_1;
    $INSERT_TMP_DWP_BIRD_AR_DD_1;
    $CREATE_TMP_DWP_BIRD_AR_DD_2;
    $INSERT_TMP_DWP_BIRD_AR_DD_2;
    $CREATE_TMP_DWP_BIRD_AR_DD_3;
    $INSERT_TMP_DWP_BIRD_AR_DD_3;
    $CREATE_TMP_DWP_BIRD_AR_DD_4;
    $INSERT_TMP_DWP_BIRD_AR_DD_4;
    $CREATE_TMP_DWP_BIRD_AR_DD_5;
    $INSERT_TMP_DWP_BIRD_AR_DD_5;
    $CREATE_DWP_BIRD_AR_DD;
    $INSERT_DWP_BIRD_AR_DD;
" -v

