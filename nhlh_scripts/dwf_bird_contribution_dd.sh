#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwf_bird_contribution_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度利润贡献表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwf_bird_contribution_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_CONTRIBUTION_DD_0='TMP_DWF_BIRD_CONTRIBUTION_DD_0'

CREATE_TMP_DWF_BIRD_CONTRIBUTION_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_CONTRIBUTION_DD_0(
  period_id                   string   --账期(期间)
  ,org_id                     string   --组织ID(OU级)
  ,inv_org_id                 string   --库存组织ID
  ,currency_id                string   --币种ID
  ,currency_descr             string   --币种名称
  ,busi_type                  string   --业态类型
  ,production_line_id         string   --产线        
  ,production_line_descr      string   --产线
  ,breed_type_id              string   --养殖模式      
  ,breed_type_descr           string   --养殖模式      
  ,sales_province             string   --销售区域(省)   
  ,customer_id                string   --客户ID      
  ,customer_name              string   --客户名称      
  ,contract_no                string   --合同号       
  ,order_no                   string   --订单号       
  ,customer_channel_id        string   --客户渠道编码    
  ,customer_channel_descr     string   --客户渠道描述    
  ,salesman_id                string   --业务员ID     
  ,salesman_name              string   --业务员名称
  ,item_code                  string   --物料编码      
  ,item_name                  string   --物料名称      
  ,sales_type_id              string   --销售类型编码    
  ,sales_type_descr           string   --销售类型描述   
  ,total_profits_amt          string   --总利润(元)    
  ,sales_nozq_weight          string   --销量(kg)    
  ,sales_lczq_qty             string   --销量（只） 
  ,MAIN_INCOME                string   --主营业务收入(元)
  ,main_prod_income           string   --主产品销售收入(元)
  ,byproduct_income           string   --副产品收入(元)  
  ,main_material_cost         string   --主要材料(元)   
  ,carriage_cost              string   --运费(元)     
  ,feeding_cost               string   --饲料成本(元)   
  ,packing_material_cost      string   --包装材料(元)   
  ,excipient_material_cost    string   --辅料材料(元)   
  ,drugs_cost                 string   --兽药成本(元)   
  ,seed_cost                  string   --苗种成本(元)   
  ,direct_labor_cost          string   --直接人工(元)   
  ,fuel_cost                  string   --燃料(元)     
  ,water_power_cost           string   --水电(元)     
  ,manufacture_change_cost    string   --制造费用-变动(元)
  ,manufacture_fixed_cost     string   --制造费用-固定(元)
  ,other_material_cost        string   --其他材料(元)   
  ,other_cost                 string   --其他费用(元)   
  ,foster_cost                string   --寄养费(元)    
  ,input_tax                  string   --进项税(元)    
  ,management_cost            string   --管理费用(元)   
  ,sales_change_cost          string   --销售费用-变动(元)
  ,sales_fixed_cost           string   --销售费用-固定(元)
  ,financing_cost             string   --财务费用(元)   
  ,marginal_contribution_cost string   --边际贡献(元)   
  ,store_down_loss            string   --存货跌价损失    
  ,ar_bad_loss                string   --应收坏账损失    
  ,OTHER_LOSSES_ASSET         string   --其他减值损  
  ,other_business_income      string   --其他业务收入    
  ,other_business_cost        string   --其他业务成本    
  ,outter_business_income     string   --营业外收入     
  ,outter_business_cost       string   --营业外支出 
  ,EXT_CON_AMT                string   --提取保险合同准备金净额
  ,CHANGE_IN_FAIR_VALUE       string   --公允价值变动收益
  ,INVESTMENT_INCOME          string   --投资收益
  ,ASSET_DISPOSIT_INCOME      string   --资产处置收益
  ,OTHER_INCOME               string   --其他收益
  ,BY_PROD_COST               string   --副产品成本
  ,BIO_ASSETS_DEC             string   --生物资产折旧
  ,INV_RESALE                 string   --存货减值转销
  ,OPERATING_TAX              string   --税金及附加


)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_CONTRIBUTION_DD_0="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_CONTRIBUTION_DD_0 PARTITION(op_day='$OP_DAY')
SELECT t1.period_id                                         --账期(期间)
       ,t1.org_id                                           --组织id(ou级)
       ,t1.inv_org_id                                       --库存组织id
       ,t1.currency_type currency_id                        --币种id
       ,case when currency_type ='3' then '母币'
             when currency_type ='2' then '本位币'
        else null end  currency_descr                       --币种名称
       ,t1.bus_type busi_type                               --业态类型
       ,case when t1.product_line='10' then '1'
             when t1.product_line='20' then '2'
        else null end production_line_id                    --产线
       ,case when t1.product_line='10' then '鸡线'
             when t1.product_line='20' then '鸭线'
        else null end production_line_descr                 --产线
       ,case when qw03.contracttype_grp='代养' then '1'
             when qw03.contracttype_grp='放养' then '2'
        else null end breed_type_id                         --养殖模式
       ,qw03.contracttype_grp breed_type_descr              --养殖模式
       ,a.province_descr sales_province                     --销售区域(省)     
       ,t1.cust_id customer_id                              --客户id
       ,EBS_C.CUSTOMER_DESCR customer_name                  --客户名称   
       ,regexp_replace(t1.contract_num,'\011','') contract_no  --合同号
       ,t1.order_number order_no                            --订单号
       ,CASE WHEN order_source = 'CRM' THEN crm_c.id_cust_chan            
             WHEN order_source = 'EBS' THEN NULL            --EBS客户表没有渠道ID
        ELSE NULL END  customer_channel_id                  --客户渠道编码
       ,CASE WHEN order_source = 'CRM' THEN crm_c.code_cust_chan          
             WHEN order_source = 'EBS' THEN ebs_c.customer_channels
        ELSE NULL END customer_channel_descr                --客户渠道描述
       ,t1.sales_id salesman_id                             --业务员id
       ,s.salesperson_name salesman_name                    --业务员名称
       ,t1.inventory_item_id                  item_code     --物料编码
       ,t1.inventory_item_desc                item_name     --物料名称
       ,HA.CUSTOMER_TYPE_ID    sales_type_id                --销售类型编码 
       ,HA.CUSTOMER_TYPE_DESCR sales_type_descr              --销售类型描述
       ,income - cost_amount_t- selling_expense_fixed - selling_expense_change
         - fin_expense - admini_expense - operating_tax - ar_losses_asset- other_losses_asset
         + non_income - non_expense + change_in_fair_value  + investment_income + other_income     
         + asset_disposit_income - cost_amount20 total_profits_amt --总利润(元)
       ,case when t1.material_segment1_id in ('15','45') 
               or (t1.material_segment1_id = '35' 
              and t1.material_segment2_id in ('31','32')) --and t1.bus_type <> 132011
        then invoice_trans_qty else 0 end sales_nozq_weight    --销量(kg): 不含种禽
       ,case when t1.material_segment1_id in ('25') 
               or (t1.material_segment1_id = '35'
              and t1.material_segment2_id in ('01','02','03','04','05','06')) --and t1.bus_type in (132011,132020,135020)
        then invoice_trans_qty else 0 end sales_lczq_qty       --销量（只）: 冷藏、禽旺：换算为只  种禽为只
       ,case when substr(subject_id,1,4)='6001' 
             then income else 0 end main_income       --主营业务收入（元) 
       ,case when substr(subject_id,1,4)='6001' 
             then income else 0 end 
        - case when src_type = '02' 
                 then income else 0 end main_prod_income  --主产品销售收入(元) 主营-副产
       ,case when src_type = '02' 
                 then income else 0 end byproduct_income  --副产品收入(元) 
       ,t1.cost_amount01 main_material_cost               --主要材料(元)
       ,t1.cost_amount02 carriage_cost                    --运费(元)
       ,t1.cost_amount03 feeding_cost                     --饲料成本(元)
       ,t1.cost_amount04 packing_material_cost            --包装材料(元)
       ,t1.cost_amount05 excipient_material_cost          --辅料材料(元)
       ,t1.cost_amount06 drugs_cost                       --兽药成本(元)
       ,t1.cost_amount07 seed_cost                        --苗种成本(元)
       ,t1.cost_amount08 direct_labor_cost                --直接人工(元)
       ,t1.cost_amount09 fuel_cost                        --燃料(元)
       ,t1.cost_amount10 water_power_cost                 --水电(元)
       ,t1.cost_amount12 manufacture_change_cost          --制造费用-变动(元)
       ,t1.cost_amount13 manufacture_fixed_cost           --制造费用-固定(元)
       ,t1.cost_amount14 other_material_cost              --其他材料(元)
       ,t1.cost_amount15 other_cost                       --其他费用(元)
       ,t1.cost_amount16 foster_cost                      --寄养费(元)
       ,t1.cost_amount17 input_tax                        --进项税(元)
       ,t1.admini_expense management_cost                 --管理费用(元)
       ,t1.selling_expense_change sales_change_cost       --销售费用-变动(元)
       ,t1.selling_expense_fixed sales_fixed_cost         --销售费用-固定(元)
       ,t1.fin_expense financing_cost                     --财务费用(元)
       ,case when substr(subject_id,1,4)='6001' 
                 then income else 0 end
        -t1.cost_amount01-t1.cost_amount02-t1.cost_amount03-t1.cost_amount04
        -t1.cost_amount05-t1.cost_amount06-t1.cost_amount07-t1.cost_amount08
        -t1.cost_amount09-t1.cost_amount10-t1.cost_amount12-t1.cost_amount14
        -t1.cost_amount15-t1.cost_amount16+t1.cost_amount17-t1.selling_expense_change        
        marginal_contribution_cost                        --边际贡献(元)
       ,t1.cost_amount20        store_down_loss           --存货跌价损失
       ,t1.ar_losses_asset ar_bad_loss                    --应收坏账损失
       ,t1.other_losses_asset other_losses_asset          --其他减值损失
       ,case when substr(subject_id,1,4)='6051'
         then income else 0 end other_business_income     --其他业务收入
       ,case when substr(subject_id,1,4)='6402' 
        then cost_amount_t else 0 end other_business_cost --其他业务成本
       ,t1.non_income outter_business_income              --营业外收入
       ,t1.non_expense outter_business_cost               --营业外支出
       ,net_provision_insurance_contracts ext_con_amt     --提取保险合同准备金净额 
       ,change_in_fair_value                              --公允价值变动收益
       ,investment_income                                 --投资收益
       ,asset_disposit_income                             --资产处置收益
       ,other_income                                      --其他收益
       ,cost_amount18                                     --副产品成本
       ,cost_amount19                                     --生物资产折旧
       ,cost_amount31                                     --存货减值转销
       ,operating_tax                                     --税金及附加
  FROM (SELECT a1.period_id                                         --账期(期间)
               ,a1.org_id                                           --组织id(ou级)
               ,a1.inv_org_id                                       --库存组织id
               ,a1.currency_type                                    --币种id
               ,a1.bus_type                                         --业态类型
               ,a1.product_line                                     --产线  
               ,a1.cust_id                                          --客户id
               ,a1.cust_address_id
               ,a1.order_source                                     --订单来源   
               ,a1.contract_num                                     --合同号
               ,a1.order_number                                     --订单号
               ,a1.sales_id                                         --业务员id
               ,a1.inventory_item_id                                --物料编码
               ,a2.inventory_item_desc                              --物料名称
               ,a1.income
               ,a1.cost_amount_t
               ,a1.selling_expense_fixed
               ,a1.selling_expense_change
               ,a1.admini_expense
               ,a1.operating_tax
               ,a1.non_income
               ,a1.non_expense
               ,a1.change_in_fair_value
               ,a1.investment_income
               ,a1.other_income     
               ,a1.asset_disposit_income
               ,a2.material_segment1_id
               ,a2.material_segment2_id
               ,a1.invoice_trans_qty
               ,a1.subject_id
               ,a1.src_type
               ,a1.cost_amount01                      --主要材料(元)
               ,a1.cost_amount02                      --运费(元)
               ,a1.cost_amount03                      --饲料成本(元)
               ,a1.cost_amount04                      --包装材料(元)
               ,a1.cost_amount05                      --辅料材料(元)
               ,a1.cost_amount06                      --兽药成本(元)
               ,a1.cost_amount07                      --苗种成本(元)
               ,a1.cost_amount08                      --直接人工(元)
               ,a1.cost_amount09                      --燃料(元)
               ,a1.cost_amount10                      --水电(元)
               ,a1.cost_amount12                      --制造费用-变动(元)
               ,a1.cost_amount13                      --制造费用-固定(元)
               ,a1.cost_amount14                      --其他材料(元)
               ,a1.cost_amount15                      --其他费用(元)
               ,a1.cost_amount16                      --寄养费(元)
               ,a1.cost_amount17                      --进项税(元)
               ,a1.cost_amount18                      --副产品成本
               ,a1.cost_amount19                      --生物资产折旧
               ,a1.cost_amount20                      --存货跌价损失
               ,a1.fin_expense                        --财务费用(元)
               ,a1.cost_amount31                      --存货跌价损失
               ,a1.ar_losses_asset                    --应收坏账损失
               ,a1.other_losses_asset                 --其他减值损失
               ,a1.net_provision_insurance_contracts  --提取保险合同准备金净额
          FROM (SELECT *
                  FROM dmd_fin_exps_profits 
                 WHERE bus_type in ('132011','132012','132020','134040','135010','135020','137000')
                  -- AND OP_MONTH='$OP_MONTH'
                   AND substr(order_number,1,2) not in('AP','AR','GL')) a1
         INNER JOIN (SELECT *
                       FROM mreport_global.dwu_dim_material_new
                      WHERE material_segment1_id in('15','25','45')
                         OR concat(material_segment1_id,material_segment2_id) in('3501','3502','3503','3504','3505','3506','3531','3532')) a2
            ON (a1.inventory_item_id=a2.inventory_item_id
            AND a1.inv_org_id=a2.inv_org_id)
        UNION ALL
        SELECT a1.period_id                                         --账期(期间)
               ,a1.org_id                                           --组织id(ou级)
               ,a1.inv_org_id                                       --库存组织id
               ,a1.currency_type                                    --币种id
               ,a1.bus_type                                         --业态类型
               ,a1.product_line                                     --产线  
               ,a1.cust_id                                          --客户id
               ,a1.cust_address_id
               ,a1.order_source                                     --订单来源   
               ,a1.contract_num                                     --合同号
               ,a1.order_number                                     --订单号
               ,a1.sales_id                                         --业务员id
               ,a1.inventory_item_id                                --物料编码
               ,a2.inventory_item_desc                              --物料名称
               ,a1.income
               ,a1.cost_amount_t
               ,a1.selling_expense_fixed
               ,a1.selling_expense_change
               ,a1.admini_expense
               ,a1.operating_tax
               ,a1.non_income
               ,a1.non_expense
               ,a1.change_in_fair_value
               ,a1.investment_income
               ,a1.other_income     
               ,a1.asset_disposit_income
               ,a2.material_segment1_id
               ,a2.material_segment2_id
               ,a1.invoice_trans_qty
               ,a1.subject_id
               ,a1.src_type
               ,a1.cost_amount01                      --主要材料(元)
               ,a1.cost_amount02                      --运费(元)
               ,a1.cost_amount03                      --饲料成本(元)
               ,a1.cost_amount04                      --包装材料(元)
               ,a1.cost_amount05                      --辅料材料(元)
               ,a1.cost_amount06                      --兽药成本(元)
               ,a1.cost_amount07                      --苗种成本(元)
               ,a1.cost_amount08                      --直接人工(元)
               ,a1.cost_amount09                      --燃料(元)
               ,a1.cost_amount10                      --水电(元)
               ,a1.cost_amount12                      --制造费用-变动(元)
               ,a1.cost_amount13                      --制造费用-固定(元)
               ,a1.cost_amount14                      --其他材料(元)
               ,a1.cost_amount15                      --其他费用(元)
               ,a1.cost_amount16                      --寄养费(元)
               ,a1.cost_amount17                      --进项税(元)
               ,a1.cost_amount18                      --副产品成本
               ,a1.cost_amount19                      --生物资产折旧
               ,a1.cost_amount20                      --存货跌价损失
               ,a1.fin_expense                        --财务费用(元)
               ,a1.cost_amount31                      --存货跌价损失
               ,a1.ar_losses_asset                    --应收坏账损失
               ,a1.other_losses_asset                 --其他减值损失
               ,a1.net_provision_insurance_contracts  --提取保险合同准备金净额
          FROM (SELECT *
                  FROM dmd_fin_exps_profits 
                 WHERE substr(order_number,1,2) in('AP','AR','GL')
                   --AND OP_MONTH='$OP_MONTH'
                   ) a1
          LEFT JOIN (SELECT *
                       FROM mreport_global.dwu_dim_material_new) a2
            ON (a1.inventory_item_id=a2.inventory_item_id
            AND a1.inv_org_id=a2.inv_org_id)) t1
LEFT JOIN mreport_global.dwu_dim_customer ebs_c
       ON (t1.cust_id = ebs_c.cust_account_id)
LEFT JOIN (SELECT ebs.cust_account_id,
                  ebs.account_number,
                  crm.id_cust_chan,
                  crm.customer_descr,
                  crm.code_cust_chan
             FROM mreport_global.dwu_dim_customer          ebs
                 ,mreport_global.dwu_dim_crm_customer      crm
            WHERE EBS.ACCOUNT_NUMBER = CUSTOMER_ACCOUNT_ID) CRM_C
       ON (t1.cust_id = crm_c.cust_account_id)
LEFT JOIN mreport_global.dim_customer_site a
       ON (t1.cust_address_id = a.customer_site_id)
LEFT JOIN (SELECT *
             FROM dwu_qw_contract_dd
            WHERE op_day='$OP_DAY') QW03
       ON (t1.contract_num = qw03.contractnumber)
LEFT JOIN (SELECT res.salesrep_id
                 ,jrs.resource_name salesperson_name
             FROM MREPORT_GLOBAL.ods_ebs_jtf_rs_salesreps res
             LEFT JOIN MREPORT_GLOBAL.ods_ebs_jtf_rs_defresources_v jrs
               ON (jrs.resource_id = res.resource_id)
            WHERE jrs.resource_name is not null
            group by res.salesrep_id
                    ,jrs.resource_name) S
      ON (s.salesrep_id = t1.sales_id)
LEFT JOIN (select hca.CUST_ACCOUNT_ID,
                  hca.ACCOUNT_NUMBER,
                  hca.ACCOUNT_NAME,
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '1' ELSE '2' END CUSTOMER_TYPE_ID, --I-1 内部 R-2 外部
                  CASE WHEN hca.CUSTOMER_TYPE = 'I'
                  THEN '内部' ELSE '外部' END CUSTOMER_TYPE_DESCR
            from MREPORT_GLOBAL.ODS_EBS_HZ_CUST_ACCOUNTS hca) HA
       ON (t1.cust_id = HA.cust_account_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_CONTRIBUTION_DD_1='TMP_DWF_BIRD_CONTRIBUTION_DD_1'

CREATE_TMP_DWF_BIRD_CONTRIBUTION_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_CONTRIBUTION_DD_1(
  period_id                   string   --账期(期间)
  ,org_id                     string   --组织ID(OU级)
  ,inv_org_id                 string   --库存组织ID
  ,currency_id                string   --币种ID
  ,currency_descr             string   --币种名称
  ,busi_type                  string   --业态类型
  ,production_line_id         string   --产线        
  ,production_line_descr      string   --产线
  ,breed_type_id              string   --养殖模式      
  ,breed_type_descr           string   --养殖模式      
  ,sales_province             string   --销售区域(省)   
  ,customer_id                string   --客户ID      
  ,customer_name              string   --客户名称      
  ,contract_no                string   --合同号       
  ,order_no                   string   --订单号       
  ,customer_channel_id        string   --客户渠道编码    
  ,customer_channel_descr     string   --客户渠道描述    
  ,salesman_id                string   --业务员ID     
  ,salesman_name              string   --业务员名称
  ,item_code                  string   --物料编码      
  ,item_name                  string   --物料名称      
  ,sales_type_id              string   --销售类型编码    
  ,sales_type_descr           string   --销售类型描述 
  ,total_profits_amt          string   --总利润(元)    
  ,sales_nozq_weight          string   --销量(kg)    
  ,sales_lczq_qty             string   --销量（只）     
  ,main_prod_income           string   --主产品销售收入(元)
  ,byproduct_income           string   --副产品收入(元)  
  ,main_material_cost         string   --主要材料(元)   
  ,carriage_cost              string   --运费(元)     
  ,feeding_cost               string   --饲料成本(元)   
  ,packing_material_cost      string   --包装材料(元)   
  ,excipient_material_cost    string   --辅料材料(元)   
  ,drugs_cost                 string   --兽药成本(元)   
  ,seed_cost                  string   --苗种成本(元)   
  ,direct_labor_cost          string   --直接人工(元)   
  ,fuel_cost                  string   --燃料(元)     
  ,water_power_cost           string   --水电(元)     
  ,manufacture_change_cost    string   --制造费用-变动(元)
  ,manufacture_fixed_cost     string   --制造费用-固定(元)
  ,other_material_cost        string   --其他材料(元)   
  ,other_cost                 string   --其他费用(元)   
  ,foster_cost                string   --寄养费(元)    
  ,input_tax                  string   --进项税(元)    
  ,management_cost            string   --管理费用(元)   
  ,sales_change_cost          string   --销售费用-变动(元)
  ,sales_fixed_cost           string   --销售费用-固定(元)
  ,financing_cost             string   --财务费用(元)   
  ,marginal_contribution_cost string   --边际贡献(元)   
  ,store_down_loss            string   --存货跌价损失    
  ,ar_bad_loss                string   --应收坏账损失    
  ,OTHER_LOSSES_ASSET         string   --其他减值损  
  ,other_business_income      string   --其他业务收入    
  ,other_business_cost        string   --其他业务成本    
  ,outter_business_income     string   --营业外收入     
  ,outter_business_cost       string   --营业外支出
  ,EXT_CON_AMT                string   --提取保险合同准备金净额
  ,CHANGE_IN_FAIR_VALUE       string   --公允价值变动收益
  ,INVESTMENT_INCOME          string   --投资收益
  ,ASSET_DISPOSIT_INCOME      string   --资产处置收益
  ,OTHER_INCOME               string   --其他收益 
  ,MAIN_INCOME                string   --主营业务收入(元)
  ,BY_PROD_COST               string   --副产品成本
  ,BIO_ASSETS_DEC             string   --生物资产折旧
  ,INV_RESALE                 string   --存货减值转销
  ,OPERATING_TAX              string   --税金及附加
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_CONTRIBUTION_DD_1="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_CONTRIBUTION_DD_1 PARTITION(op_day='$OP_DAY')
SELECT t1.period_id                           --账期(期间)
       ,t1.org_id                             --组织ID(OU级)
       ,t1.inv_org_id                         --库存组织ID
       ,t1.currency_id                        --币种ID
       ,t1.currency_descr                     --币种名称
       ,t1.busi_type                          --业态类型
       ,t1.production_line_id                 --产线        
       ,t1.production_line_descr              --产线
       ,t1.breed_type_id                      --养殖模式      
       ,t1.breed_type_descr                   --养殖模式      
       ,t1.sales_province                     --销售区域(省)   
       ,t2.customer_code                      --客户ID      
       ,t2.customer_name                      --客户名称      
       ,t1.contract_no                        --合同号       
       ,t1.order_no                           --订单号       
       ,t1.customer_channel_id                --客户渠道编码    
       ,t1.customer_channel_descr             --客户渠道描述    
       ,t1.salesman_id                        --业务员ID     
       ,t1.salesman_name                      --业务员名称
       ,t1.item_code                          --物料编码      
       ,t1.item_name                          --物料名称      
       ,t1.sales_type_id                      --销售类型编码    
       ,t1.sales_type_descr                   --销售类型描述   
       ,t1.total_profits_amt                  --总利润(元)    
       ,t1.sales_nozq_weight                  --销量(kg)    
       ,t1.sales_lczq_qty                     --销量（只）     
       ,t1.main_prod_income                   --主产品销售收入(元)
       ,t1.byproduct_income                   --副产品收入(元)  
       ,t1.main_material_cost                 --主要材料(元)   
       ,t1.carriage_cost                      --运费(元)     
       ,t1.feeding_cost                       --饲料成本(元)   
       ,t1.packing_material_cost              --包装材料(元)   
       ,t1.excipient_material_cost            --辅料材料(元)   
       ,t1.drugs_cost                         --兽药成本(元)   
       ,t1.seed_cost                          --苗种成本(元)   
       ,t1.direct_labor_cost                  --直接人工(元)   
       ,t1.fuel_cost                          --燃料(元)     
       ,t1.water_power_cost                   --水电(元)     
       ,t1.manufacture_change_cost            --制造费用-变动(元)
       ,t1.manufacture_fixed_cost             --制造费用-固定(元)
       ,t1.other_material_cost                --其他材料(元)   
       ,t1.other_cost                         --其他费用(元)   
       ,t1.foster_cost                        --寄养费(元)    
       ,t1.input_tax                          --进项税(元)    
       ,t1.management_cost                    --管理费用(元)   
       ,t1.sales_change_cost                  --销售费用-变动(元)
       ,t1.sales_fixed_cost                   --销售费用-固定(元)
       ,t1.financing_cost                     --财务费用(元)   
       ,t1.marginal_contribution_cost         --边际贡献(元)   
       ,t1.store_down_loss                    --存货跌价损失    
       ,t1.ar_bad_loss                        --应收坏账损失    
       ,t1.OTHER_LOSSES_ASSET                 --其他减值损  
       ,t1.other_business_income              --其他业务收入    
       ,t1.other_business_cost                --其他业务成本    
       ,t1.outter_business_income             --营业外收入     
       ,t1.outter_business_cost               --营业外支出 
       ,EXT_CON_AMT                         --提取保险合同准备金净额
       ,CHANGE_IN_FAIR_VALUE                  --公允价值变动收益
       ,INVESTMENT_INCOME                     --投资收益
       ,ASSET_DISPOSIT_INCOME                 --资产处置收益
       ,OTHER_INCOME                          --其他收益 
       ,MAIN_INCOME                   --主营业务收入(元)
       ,BY_PROD_COST                  --副产品成本
       ,BIO_ASSETS_DEC                --生物资产折旧
       ,INV_RESALE                    --存货减值转销
       ,OPERATING_TAX                 --税金及附加
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_CONTRIBUTION_DD_0
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT customer_id,
                    customer_code,
                    customer_descr customer_name,
                    null customer_channel_id,
                    customer_channels customer_channel_descr
               FROM mreport_global.dim_customer) t2
    ON (t1.customer_id=t2.customer_id)
  LEFT JOIN (SELECT customer_account_id customer_id,
                    account_number customer_code,
                    customer_descr customer_name,
                    code_cust_chan customer_channel_id,
                    cust_chan_type customer_channel_descr
               FROM mreport_global.dwu_dim_crm_customer) t3
    ON (t1.customer_id=t3.customer_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_CONTRIBUTION_DD='DWF_BIRD_CONTRIBUTION_DD'

CREATE_DWF_BIRD_CONTRIBUTION_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_CONTRIBUTION_DD(
  period_id                   string   --账期(期间)
  ,org_id                     string   --组织ID(OU级)
  ,inv_org_id                 string   --库存组织ID
  ,currency_id                string   --币种ID
  ,currency_descr             string   --币种名称
  ,busi_type                  string   --业态类型
  ,production_line_id         string   --产线        
  ,production_line_descr      string   --产线
  ,breed_type_id              string   --养殖模式      
  ,breed_type_descr           string   --养殖模式      
  ,sales_province             string   --销售区域(省)   
  ,customer_id                string   --客户ID      
  ,customer_name              string   --客户名称      
  ,contract_no                string   --合同号       
  ,order_no                   string   --订单号       
  ,customer_channel_id        string   --客户渠道编码    
  ,customer_channel_descr     string   --客户渠道描述    
  ,salesman_id                string   --业务员ID     
  ,salesman_name              string   --业务员名称
  ,item_code                  string   --物料编码      
  ,item_name                  string   --物料名称      
  ,sales_type_id              string   --销售类型编码    
  ,sales_type_descr           string   --销售类型描述  
  ,total_profits_amt          string   --总利润(元)    
  ,sales_nozq_weight          string   --销量(kg)    
  ,sales_lczq_qty             string   --销量（只）     
  ,main_prod_income           string   --主产品销售收入(元)
  ,byproduct_income           string   --副产品收入(元)  
  ,main_material_cost         string   --主要材料(元)   
  ,carriage_cost              string   --运费(元)     
  ,feeding_cost               string   --饲料成本(元)   
  ,packing_material_cost      string   --包装材料(元)   
  ,excipient_material_cost    string   --辅料材料(元)   
  ,drugs_cost                 string   --兽药成本(元)   
  ,seed_cost                  string   --苗种成本(元)   
  ,direct_labor_cost          string   --直接人工(元)   
  ,fuel_cost                  string   --燃料(元)     
  ,water_power_cost           string   --水电(元)     
  ,manufacture_change_cost    string   --制造费用-变动(元)
  ,manufacture_fixed_cost     string   --制造费用-固定(元)
  ,other_material_cost        string   --其他材料(元)   
  ,other_cost                 string   --其他费用(元)   
  ,foster_cost                string   --寄养费(元)    
  ,input_tax                  string   --进项税(元)    
  ,management_cost            string   --管理费用(元)   
  ,sales_change_cost          string   --销售费用-变动(元)
  ,sales_fixed_cost           string   --销售费用-固定(元)
  ,financing_cost             string   --财务费用(元)   
  ,marginal_contribution_cost string   --边际贡献(元)   
  ,store_down_loss            string   --存货跌价损失    
  ,ar_bad_loss                string   --应收坏账损失    
  ,OTHER_LOSSES_ASSET          string   --其他减值损  
  ,other_business_income      string   --其他业务收入    
  ,other_business_cost        string   --其他业务成本    
  ,outter_business_income     string   --营业外收入     
  ,outter_business_cost       string   --营业外支出 
  ,EXT_CON_AMT                string   --提取保险合同准备金净额
  ,CHANGE_IN_FAIR_VALUE       string   --公允价值变动收益
  ,INVESTMENT_INCOME          string   --投资收益
  ,ASSET_DISPOSIT_INCOME      string   --资产处置收益
  ,OTHER_INCOME               string   --其他收益
  ,MAIN_INCOME                string   --主营业务收入
  ,BY_PROD_COST               string   --副产品成本
  ,BIO_ASSETS_DEC             string   --生物资产折旧
  ,INV_RESALE                 string   --存货减值转销
  ,OPERATING_TAX              string   --税金及附加 
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_CONTRIBUTION_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_CONTRIBUTION_DD PARTITION(op_day='$OP_DAY')
SELECT period_id                        --账期(期间)
       ,org_id                          --组织ID(OU级)
       ,inv_org_id                      --库存组织ID
       ,currency_id                     --币种ID
       ,currency_descr                  --币种名称
       ,busi_type                       --业态类型
       ,production_line_id              --产线        
       ,production_line_descr           --产线
       ,breed_type_id                   --养殖模式      
       ,breed_type_descr                --养殖模式      
       ,sales_province                  --销售区域(省)   
       ,customer_id                     --客户ID      
       ,customer_name                   --客户名称      
       ,contract_no                     --合同号       
       ,order_no                        --订单号       
       ,customer_channel_id             --客户渠道编码    
       ,customer_channel_descr          --客户渠道描述    
       ,salesman_id                     --业务员ID     
       ,salesman_name                   --业务员名称
       ,item_code                       --物料编码      
       ,item_name                       --物料名称      
       ,sales_type_id                   --销售类型编码    
       ,sales_type_descr                --销售类型描述  
       ,total_profits_amt               --总利润(元)    
       ,sales_nozq_weight               --销量(kg)    
       ,sales_lczq_qty                  --销量（只）     
       ,main_prod_income                --主产品销售收入(元)
       ,byproduct_income                --副产品收入(元)  
       ,main_material_cost              --主要材料(元)   
       ,carriage_cost                   --运费(元)     
       ,feeding_cost                    --饲料成本(元)   
       ,packing_material_cost           --包装材料(元)   
       ,excipient_material_cost         --辅料材料(元)   
       ,drugs_cost                      --兽药成本(元)   
       ,seed_cost                       --苗种成本(元)   
       ,direct_labor_cost               --直接人工(元)   
       ,fuel_cost                       --燃料(元)     
       ,water_power_cost                --水电(元)     
       ,manufacture_change_cost         --制造费用-变动(元)
       ,manufacture_fixed_cost          --制造费用-固定(元)
       ,other_material_cost             --其他材料(元)   
       ,other_cost                      --其他费用(元)   
       ,foster_cost                     --寄养费(元)    
       ,input_tax                       --进项税(元)    
       ,management_cost                 --管理费用(元)   
       ,sales_change_cost               --销售费用-变动(元)
       ,sales_fixed_cost                --销售费用-固定(元)
       ,financing_cost                  --财务费用(元)   
       ,marginal_contribution_cost      --边际贡献(元)   
       ,store_down_loss                 --存货跌价损失    
       ,ar_bad_loss                     --应收坏账损失    
       ,OTHER_LOSSES_ASSET              --其他减值损  
       ,other_business_income           --其他业务收入    
       ,other_business_cost             --其他业务成本    
       ,outter_business_income          --营业外收入     
       ,outter_business_cost            --营业外支出
       ,EXT_CON_AMT                   --提取保险合同准备金净额
       ,CHANGE_IN_FAIR_VALUE            --公允价值变动收益
       ,INVESTMENT_INCOME               --投资收益
       ,ASSET_DISPOSIT_INCOME           --资产处置收益
       ,OTHER_INCOME                    --其他收益
       ,MAIN_INCOME                   --主营业务收入
       ,BY_PROD_COST                  --副产品成本
       ,BIO_ASSETS_DEC                --生物资产折旧
       ,INV_RESALE                    --存货减值转销
       ,OPERATING_TAX                 --税金及附加
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_CONTRIBUTION_DD_0
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWF_BIRD_CONTRIBUTION_DD_0;
    $INSERT_TMP_DWF_BIRD_CONTRIBUTION_DD_0;
    $CREATE_TMP_DWF_BIRD_CONTRIBUTION_DD_1;
    $INSERT_TMP_DWF_BIRD_CONTRIBUTION_DD_1;
    $CREATE_DWF_BIRD_CONTRIBUTION_DD;
    $INSERT_DWF_BIRD_CONTRIBUTION_DD;
"  -v