#!/bin/bash

######################################################################
#
# 程    序: dwf_bird_unit_profits_dd.sh
# 创建时间: 2018年04月18日
# 创 建 者: ch
# 参数:
#    参数1: 日期[yyyymmdd]
# 补充说明:
# 功    能: 禽产业单只利润
# 修改说明:
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwf_bird_unit_profits_dd.sh 20180101"
    exit 1
fi



###########################################################################################
## 处理 基础指标 禽旺\养殖户利润 CG01通过合同号关联CW19
## 变量声明
TMP_DWF_BIRD_UNIT_PROFITS_DD_00='TMP_DWF_BIRD_UNIT_PROFITS_DD_00'

CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_00="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIT_PROFITS_DD_00(
  period_id               string     --期间
  ,org_id                 string     --组织
  ,bus_type               string     --业态
  ,product_line           string     --产线
  ,contract_no            string     --合同号
  ,contracttype_grp       string     --合同类型分组 放养、代养
  ,guarantees_market      string     --保值保底市场
  ,income                 string     --收入
  ,cost_amount_t          string     --总成本
  ,selling_expense_fixed  string     --销售费用-固定
  ,selling_expense_change string     --销售费用-变动
  ,fin_expense            string     --财务费用
  ,admini_expense         string     --管理费用
  ,operating_tax          string     --税金及附加
  ,ar_losses_asset        string     --应收坏账损失 
  ,other_losses_asset     string     --其他减值损失 
  ,non_income             string     --营业外收入
  ,non_expense            string     --营业外支出
  ,change_in_fair_value   string     --公允价值变动收益
  ,investment_income      string     --投资收益
  ,other_income           string     --其他收益
  ,asset_disposit_income  string     --资产处置收益 
  ,profit                 string     --利润
  ,base_amount            string     --本位币金额 发票行金额
  ,quantity_main          string     --主数量 采购数量 cg01013
  ,buy_amt                string     --采购含税金额 cg01019
  ,killed_qty             string     --辅助数量 宰杀数量 回收只数
  ,put_qty                string     --投放只数
  ,out_quantity           string     --出库数量 xs02025
  ,risk_get_way           string     --风险金收取途径：投放/回收
  ,risk_rate              string     --风险金系数
  ,material_used_amt      string     --饲料耗用金额
  ,risk_amt               string     --销售风险金金额
  ,drugs_amt              string     --养殖户承担药金额
  ,sales_amt              string     --销售总金额
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_00="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIT_PROFITS_DD_00 PARTITION(op_day='$OP_DAY')
SELECT cg01.period_id,
       cg01.org_id,
       cw19.bus_type,
       case when cg01.material_code='3501000002' then '10'
            when cg01.material_code='3502000002' then '20'
       else null end product_line,
       cg01.contract_no,                         --合同号
       qw03.contracttype_grp,                    --合同类型分组 放养、代养
       qw03.guarantees_market,                   --保值保底市场
       coalesce(cw19.income,0),                  --收入
       coalesce(cw19.cost_amount_t,0),           --总成本
       coalesce(cw19.selling_expense_fixed,0),   --销售费用-固定
       coalesce(cw19.selling_expense_change,0),  --销售费用-变动
       coalesce(cw19.fin_expense,0),             --财务费用
       coalesce(cw19.admini_expense,0),          --管理费用
       coalesce(cw19.operating_tax,0),           --税金及附加
       coalesce(cw19.ar_losses_asset,0),         --应收坏账损失
       coalesce(cw19.other_losses_asset,0),      --其他减值损失
       coalesce(cw19.non_income,0),              --营业外收入
       coalesce(cw19.non_expense,0),             --营业外支出
       coalesce(cw19.change_in_fair_value,0),    --公允价值变动收益
       coalesce(cw19.investment_income,0),       --投资收益
       coalesce(cw19.other_income,0),            --其他收益
       coalesce(cw19.asset_disposit_income,0),   --资产处置收益
       CASE WHEN qw03.guarantees_market = '市场' THEN 0
       ELSE
       coalesce(cw19.income - cw19.cost_amount_t- cw19.selling_expense_fixed - cw19.selling_expense_change
         - cw19.fin_expense - cw19.admini_expense - cw19.operating_tax - cw19.ar_losses_asset- cw19.other_losses_asset
         + cw19.non_income - cw19.non_expense + cw19.change_in_fair_value  + cw19.investment_income + cw19.other_income
         + cw19.asset_disposit_income - cost_amount20,0)
       END profit,          --利润
       coalesce(cw07.base_amount,0) base_amount,      --本位币金额 发票行金额
       cg01.quantity_main quantity_main,              --主数量   CG01013
       case when cg01.loc_currency_id = 'CNY'
            then cg01.buy_amt
       else cg01.buy_amt * cur.conversion_rate end buy_amt,     --采购含税金额 CG01019
       0 killed_qty,                                  --辅助数量 宰杀数量 回收只数
       coalesce(qw03.qty,0) put_qty,                  --投放只数
       coalesce(xs02.out_quantity,0) out_quantity,    --出库数量      xs02025
       qw12.risk_get_way,                             --风险金收取途径：投放/回收
       coalesce(qw12.risk_rate,0) risk_rate,          --风险金系数
       xs02.material_used_amt,                        --饲料耗用金额
       xs02.risk_amt,                                 --销售风险金金额
       xs02.drugs_amt,                                --养殖户承担药金额
       xs02.sales_amt                                 --销售总金额
  FROM (SELECT substr(transaction_date,1,6) period_id
               ,org_id
               ,material_code
               ,contract_no
               ,loc_currency_id
               ,sum(price_with_tax*quantity_main) buy_amt  --采购金额(含税)
               ,sum(quantity_main) quantity_main
               ,sum(secondary_qty) secondary_qty
          FROM mreport_poultry.dwu_cg_buy_list_cg01_dd
         WHERE op_day='$OP_DAY'
           AND material_code in('3501000002','3502000002')
           AND release_num like 'BWP%'
           AND cancel_flag in('CLOSED','OPEN')
           AND quantity_received>0
         GROUP BY substr(transaction_date,1,6)
               ,org_id
               ,material_code
               ,contract_no
               ,loc_currency_id) cg01
 INNER JOIN (SELECT *
               FROM  mreport_poultry.dwu_qw_contract_dd
              WHERE op_day = '$OP_DAY'
                AND guarantees_market!='市场') qw03
    ON (cg01.contract_no = qw03.contractnumber)
  LEFT JOIN (SELECT from_currency,
                    to_currency,
                    conversion_rate,
                    conversion_period
               FROM mreport_global.dmd_fin_period_currency_rate_mm
              WHERE to_currency='CNY') cur
         on (cg01.loc_currency_id=cur.from_currency
        and substr(cg01.period_id,1,6) = cur.conversion_period)
  LEFT JOIN (SELECT sum(case when t.base_currency_code = 'CNY'
                             then t.base_amount
                        else t.base_amount * c.conversion_rate end) base_amount,
                    line_desc
               FROM (SELECT *
                       FROM mreport_poultry.dwu_qw_invoice_dd
                      WHERE distribution_account = '5002010110'
                        AND op_day = '$OP_DAY') t
               LEFT JOIN (SELECT from_currency,
                                 to_currency,
                                 conversion_rate,
                                 conversion_period
                            FROM mreport_global.dmd_fin_period_currency_rate_mm
                           WHERE to_currency='CNY') c
                 ON (t.base_currency_code=c.from_currency
                 AND t.period_name = c.conversion_period)
              GROUP BY line_desc) cw07
    ON (cg01.contract_no = cw07.line_desc)
  LEFT JOIN (SELECT a1.contract_num
                    ,a2.bus_type
                    --,substr(a1.period_id,1,6) period_id
                    ,sum(coalesce(a1.income,0)) income                                 --收入
                    ,sum(coalesce(a1.cost_amount_t,0)) cost_amount_t                   --总成本
                    ,sum(coalesce(a1.selling_expense_fixed,0)) selling_expense_fixed   --销售费用-固定
                    ,sum(coalesce(a1.selling_expense_change,0)) selling_expense_change --销售费用-变动
                    ,sum(coalesce(a1.fin_expense,0)) fin_expense                       --财务费用
                    ,sum(coalesce(a1.admini_expense,0)) admini_expense                 --管理费用
                    ,sum(coalesce(a1.operating_tax,0)) operating_tax                   --税金及附加
                    ,sum(coalesce(a1.ar_losses_asset,0)) ar_losses_asset               --应收坏账损失
                    ,sum(coalesce(a1.other_losses_asset,0)) other_losses_asset         --其他减值损失
                    ,sum(coalesce(a1.non_income,0)) non_income                         --营业外收入
                    ,sum(coalesce(a1.non_expense,0)) non_expense                       --营业外支出
                    ,sum(coalesce(a1.change_in_fair_value,0)) change_in_fair_value     --公允价值变动收益
                    ,sum(coalesce(a1.investment_income,0)) investment_income           --投资收益
                    ,sum(coalesce(a1.other_income,0)) other_income                     --其他收益
                    ,sum(coalesce(a1.asset_disposit_income,0)) asset_disposit_income   --资产处置收益
                    ,sum(coalesce(a1.cost_amount20,0)) cost_amount20
               FROM  mreport_poultry.dmd_fin_exps_profits a1
              INNER JOIN (SELECT m.org_id,
                                 org.level4_bus_type bus_type
                            FROM mreport_global.ods_ebs_cux_bi_ar_ou_inv_mapping M
                            LEFT JOIN mreport_global.ods_ebs_cux_org_structures_all ORG
                              ON (m.organization_id = org.level7_org_id)
                           GROUP BY m.org_id,org.level4_bus_type) a2
                 ON (a1.org_id = a2.org_id)
              WHERE a1.currency_type = '3'                                            --仅取母币数据
              GROUP BY a1.contract_num
                    ,a2.bus_type
                    --,substr(a1.period_id,1,6)
                    ) cw19
    ON (cg01.contract_no = cw19.contract_num
   -- AND cg01.period_id=cw19.period_id
    )
  LEFT JOIN (SELECT cust_po_num,                    --合同号
                    sum(case when wl01ebs.material_segment1_id='15' and material_segment2_id = '02'
                              and xs.loc_currency_id='CNY'
                             then loc_amount
                             when wl01ebs.material_segment1_id='15' and material_segment2_id = '02'
                              and xs.loc_currency_id!='CNY'
                             then loc_amount * c.conversion_rate else 0 end) material_used_amt,  --饲料耗用金额
                    sum(case when material_segment1_id = '25'
                              and xs.loc_currency_id='CNY'
                             then loc_amount
                             when material_segment1_id = '25'
                              and xs.loc_currency_id !='CNY'
                             then loc_amount * c.conversion_rate
                             else 0 end) risk_amt,                           --amt5:风险金（qw12大表风险金标准*只数,有标志以投放或回收计算风险金）
                    sum(case when sycdf in ('养殖户承担','养殖户承担_退货')
                              and wl01ebs.material_segment1_id='65'
                              and xs.loc_currency_id='CNY'
                             then loc_amount
                             when sycdf in ('养殖户承担','养殖户承担_退货')
                             and wl01ebs.material_segment1_id='65'
                              and xs.loc_currency_id!='CNY'
                             then loc_amount * c.conversion_rate else 0 end) drugs_amt,         --药金额
                    sum(case when xs.loc_currency_id='CNY' then loc_amount
                             when xs.loc_currency_id!='CNY' then loc_amount * c.conversion_rate
                        else 0 end) sales_amt,                               --销售总金额
                    sum(coalesce(out_quantity,0)) out_quantity               --出库数量      xs02025
               FROM (SELECT *
                       FROM  mreport_poultry.dwu_xs_other_sale_dd
                      WHERE op_day = '$OP_DAY') xs
               LEFT JOIN mreport_global.dwu_dim_material_new wl01ebs
                      ON (xs.material_id = wl01ebs.inventory_item_id
                     AND xs.inv_org_id  = wl01ebs.inv_org_id)
               LEFT JOIN (SELECT from_currency,
                                 to_currency,
                                 conversion_rate,
                                 conversion_period
                           FROM mreport_global.dmd_fin_period_currency_rate_mm
                          WHERE to_currency='CNY') c
                      ON (xs.loc_currency_id=c.from_currency
                     AND substr(xs.period_id,1,4) = c.conversion_period)
              GROUP BY cust_po_num) xs02
    ON (cg01.contract_no = xs02.cust_po_num)
  LEFT JOIN (SELECT a2.org_id
                    ,case when a1.kpi_type='鸡' then '3501000002'
                          when a1.kpi_type='鸭' then '3502000002'
                     else null end production_line_id
                    ,a1.risk_get_way
                    ,a1.risk_rate
               FROM (SELECT *
                       FROM  mreport_poultry.dwu_qw_qw12_dd
                      WHERE op_day='$OP_DAY') a1
              INNER JOIN (SELECT b1.account_ou_id org_id
                                 ,b1.org_id co_org_id       --合作社组织
                            FROM (SELECT org_id
                                         ,account_ou_id
                                         ,last_update_date
                                    FROM mreport_global.ods_ebs_cux_3_gl_coop_account) b1
                           INNER JOIN (SELECT account_ou_id
                                              ,max(last_update_date) last_update_date
                                         FROM mreport_global.ods_ebs_cux_3_gl_coop_account
                                        GROUP BY account_ou_id) b2
                              ON (b1.account_ou_id=b2.account_ou_id
                              AND b1.last_update_date=b2.last_update_date)) a2
                 ON (a1.org_id=a2.co_org_id)) qw12
    ON (cg01.org_id = qw12.org_id
    AND cg01.material_code=qw12.production_line_id)
"

###########################################################################################
## 处理 基础指标 冷藏 利润 CG01通过ou关联CW19
## 变量声明
TMP_DWF_BIRD_UNIT_PROFITS_DD_01='TMP_DWF_BIRD_UNIT_PROFITS_DD_01'

CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_01="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIT_PROFITS_DD_01(
  period_id               string     --期间
  ,org_id                 string     --组织
  ,bus_type               string     --业态
  ,product_line           string     --产线
  ,contract_no            string     --合同号
  ,contracttype_grp       string     --合同类型分组 放养、代养
  ,guarantees_market      string     --保值保底市场
  ,income                 string     --收入
  ,cost_amount_t          string     --总成本
  ,selling_expense_fixed  string     --销售费用-固定
  ,selling_expense_change string     --销售费用-变动
  ,fin_expense            string     --财务费用
  ,admini_expense         string     --管理费用
  ,operating_tax          string     --税金及附加
  ,ar_losses_asset        string     --应收坏账损失 
  ,other_losses_asset     string     --其他减值损失 
  ,non_income             string     --营业外收入
  ,non_expense            string     --营业外支出
  ,change_in_fair_value   string     --公允价值变动收益
  ,investment_income      string     --投资收益
  ,other_income           string     --其他收益
  ,asset_disposit_income  string     --资产处置收益 
  ,profit                 string     --利润
  ,base_amount            string     --本位币金额 发票行金额
  ,quantity_main          string     --主数量 采购数量 cg01013
  ,buy_amt                string     --采购含税金额 cg01019
  ,killed_qty             string     --冷藏合同宰杀量
  ,mkt_killed_qty         string     --冷藏市场宰杀量
  ,put_qty                string     --投放只数
  ,out_quantity           string     --出库数量 xs02025
  ,risk_get_way           string     --风险金收取途径：投放/回收
  ,risk_rate              string     --风险金系数
  ,material_used_amt      string     --饲料耗用金额
  ,risk_amt               string     --销售风险金金额
  ,drugs_amt              string     --养殖户承担药金额
  ,sales_amt              string     --销售总金额
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_01="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIT_PROFITS_DD_01 PARTITION(op_day='$OP_DAY')
SELECT cg01.period_id,
       cg01.org_id,
       M.BUS_TYPE,
      cg01.product_line,
       '' contract_no,               --合同号
       '' contracttype_grp,          --合同类型分组 放养、代养
       '' guarantees_market,         --保值保底市场
       coalesce(cw19.income,0),                  --收入
       coalesce(cw19.cost_amount_t,0),           --总成本
       coalesce(cw19.selling_expense_fixed,0),   --销售费用-固定
       coalesce(cw19.selling_expense_change,0),  --销售费用-变动
       coalesce(cw19.fin_expense,0),             --财务费用
       coalesce(cw19.admini_expense,0),          --管理费用
       coalesce(cw19.operating_tax,0),           --税金及附加
       coalesce(cw19.ar_losses_asset,0),         --应收坏账损失
       coalesce(cw19.other_losses_asset,0),      --其他减值损失
       coalesce(cw19.non_income,0),              --营业外收入
       coalesce(cw19.non_expense,0),             --营业外支出
       coalesce(cw19.change_in_fair_value,0),    --公允价值变动收益
       coalesce(cw19.investment_income,0),       --投资收益
       coalesce(cw19.other_income,0),            --其他收益
       coalesce(cw19.asset_disposit_income,0),   --资产处置收益
       coalesce(cw19.income - cw19.cost_amount_t- cw19.selling_expense_fixed - cw19.selling_expense_change
         - cw19.fin_expense - cw19.admini_expense - cw19.operating_tax - cw19.ar_losses_asset- cw19.other_losses_asset
         + cw19.non_income - cw19.non_expense + cw19.change_in_fair_value  + cw19.investment_income + cw19.other_income
         + cw19.asset_disposit_income - cw19.cost_amount20,0) profit, --利润
       0 base_amount,                            --本位币金额 发票行金额
       0 quantity_main,                          --主数量   CG01013
       0 buy_amt,                                --采购含税金额 CG01019
       cg01.contract_killed_qty,                 --冷藏合同宰杀量
       cg01.mkt_killed_qty   ,                   --冷藏市场宰杀量
       0 put_qty,                                --投放只数
       0 out_quantity,                           --出库数量      xs02025
       0 risk_get_way,                           --风险金收取途径：投放/回收
       0 risk_rate,                              --风险金系数
       0 material_used_amt,                      --饲料耗用金额
       0 risk_amt,                               --销售风险金金额
       0 drugs_amt,                              --养殖户承担药金额
       0 sales_amt                               --销售总金额
  FROM (SELECT substr(t.period_id,1,6) period_id
               ,t.org_id
               ,case when t.material_code='3501000002' then '10'
                     when t.material_code='3502000002' then '20'
                      else null end product_line
               ,sum(case when qw03.guarantees_market in ('保值','保底')
                         then coalesce(secondary_qty,0)
                    else 0 end) contract_killed_qty
               ,sum(case when qw03.guarantees_market='市场'
                         then coalesce(secondary_qty,0)
                    else 0 end) mkt_killed_qty
               ,sum(secondary_qty) killed_qty
          FROM mreport_poultry.dwu_cg_buy_list_cg01_dd t
          LEFT JOIN (SELECT *
                       FROM mreport_poultry.dwu_qw_contract_dd
                      WHERE op_day = '$OP_DAY') qw03
            ON (contract_no = contractnumber)
         WHERE t.op_day='$OP_DAY'
           AND t.material_code in('3501000002','3502000002')
           AND t.release_num like 'BWP%'
           AND t.cancel_flag in('CLOSED','OPEN')
           AND t.quantity_received>0
         GROUP BY substr(t.period_id,1,6)
               ,t.org_id
               ,t.material_code) cg01
  LEFT JOIN (SELECT substr(period_id,1,6) period_id
                    ,org_id
                    ,product_line
                    ,sum(coalesce(income,0)) income                                 --收入
                    ,sum(coalesce(cost_amount_t,0)) cost_amount_t                   --总成本
                    ,sum(coalesce(selling_expense_fixed,0)) selling_expense_fixed   --销售费用-固定
                    ,sum(coalesce(selling_expense_change,0)) selling_expense_change --销售费用-变动
                    ,sum(coalesce(fin_expense,0)) fin_expense                       --财务费用
                    ,sum(coalesce(admini_expense,0)) admini_expense                 --管理费用
                    ,sum(coalesce(operating_tax,0)) operating_tax                   --税金及附加
                    ,sum(coalesce(ar_losses_asset,0)) ar_losses_asset               --应收坏账损失
                    ,sum(coalesce(other_losses_asset,0)) other_losses_asset         --其他减值损失
                    ,sum(coalesce(non_income,0)) non_income                         --营业外收入
                    ,sum(coalesce(non_expense,0)) non_expense                       --营业外支出
                    ,sum(coalesce(change_in_fair_value,0)) change_in_fair_value     --公允价值变动收益
                    ,sum(coalesce(investment_income,0)) investment_income           --投资收益
                    ,sum(coalesce(other_income,0)) other_income                     --其他收益
                    ,sum(coalesce(asset_disposit_income,0)) asset_disposit_income   --资产处置收益
                    ,sum(coalesce(cost_amount20,0)) cost_amount20
               FROM dmd_fin_exps_profits
              WHERE currency_type = '3'                --仅取母币数据
                AND bus_type NOT IN ('134020','134030')--排除食品深加工
              GROUP BY substr(period_id,1,6)
                    ,org_id
                    ,product_line) cw19
    ON (cg01.period_id = cw19.period_id
   AND cg01.org_id = cw19.org_id
   AND cg01.product_line = cw19.product_line)
  INNER JOIN (SELECT m.org_id,
                     m.organization_id inv_org_id,
                     org.level4_bus_type bus_type
                FROM mreport_global.ods_ebs_cux_bi_ar_ou_inv_mapping M
                LEFT JOIN mreport_global.ods_ebs_cux_org_structures_all ORG
                  ON (m.organization_id = org.level7_org_id)
                 AND org.level4_bus_type = '132020') m    --取禽屠宰的ou
    ON (cg01.org_id = m.org_id)
"


###########################################################################################
## 处理 基础指标 饲料 利润 CG01通过合同号关联饲料表
## 变量声明
TMP_DWF_BIRD_UNIT_PROFITS_DD_02='TMP_DWF_BIRD_UNIT_PROFITS_DD_02'

CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_02="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIT_PROFITS_DD_02(
  period_id               string     --期间
  ,org_id                 string     --组织
  ,bus_type               string     --业态
  ,product_line           string     --产线
  ,contract_no            string     --合同号
  ,contracttype_grp       string     --合同类型分组 放养、代养
  ,guarantees_market      string     --保值保底市场
  ,income                 string     --收入
  ,cost_amount_t          string     --总成本
  ,selling_expense_fixed  string     --销售费用-固定
  ,selling_expense_change string     --销售费用-变动
  ,fin_expense            string     --财务费用
  ,admini_expense         string     --管理费用
  ,operating_tax          string     --税金及附加
  ,ar_losses_asset        string     --应收坏账损失 
  ,other_losses_asset     string     --其他减值损失 
  ,non_income             string     --营业外收入
  ,non_expense            string     --营业外支出
  ,change_in_fair_value   string     --公允价值变动收益
  ,investment_income      string     --投资收益
  ,other_income           string     --其他收益
  ,asset_disposit_income  string     --资产处置收益 
  ,profit                 string     --利润
  ,base_amount            string     --本位币金额 发票行金额
  ,quantity_main          string     --主数量 采购数量 cg01013
  ,buy_amt                string     --采购含税金额 cg01019
  ,killed_qty             string     --辅助数量 宰杀数量 回收只数
  ,put_qty                string     --投放只数
  ,out_quantity           string     --出库数量 xs02025
  ,risk_get_way           string     --风险金收取途径：投放/回收
  ,risk_rate              string     --风险金系数
  ,material_used_amt      string     --饲料耗用金额
  ,risk_amt               string     --销售风险金金额
  ,drugs_amt              string     --养殖户承担药金额
  ,sales_amt              string     --销售总金额
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_02="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIT_PROFITS_DD_02 PARTITION(op_day='$OP_DAY')
SELECT cg01.period_id,
       cg01.org_id,
       m.bus_type,
       case when cg01.material_code='3501000002' then '10'
            when cg01.material_code='3502000002' then '20'
       else null end product_line,
       cg01.contract_no,               --合同号
       qw03.contracttype_grp,          --合同类型分组 放养、代养
       qw03.guarantees_market,         --保值保底市场
       0 income,                  --收入
       0 cost_amount_t,           --总成本
       0 selling_expense_fixed,   --销售费用-固定
       0 selling_expense_change,  --销售费用-变动
       0 fin_expense,             --财务费用
       0 admini_expense,          --管理费用
       0 operating_tax,           --税金及附加
       0 ar_losses_asset,         --应收坏账损失
       0 other_losses_asset,      --其他减值损失
       0 non_income,              --营业外收入
       0 non_expense,             --营业外支出
       0 change_in_fair_value,    --公允价值变动收益
       0 investment_income,       --投资收益
       0 other_income,            --其他收益
       0 asset_disposit_income,   --资产处置收益
       CASE WHEN qw03.guarantees_market = '市场' THEN 0
       ELSE coalesce(SL.TOTAL_PROFIT,0)  
       END profit,          --利润
       0 base_amount,      --本位币金额 发票行金额
       0 quantity_main,              --主数量   CG01013
       0 buy_amt,     --采购含税金额 CG01019
       0 killed_qty,                 --辅助数量 宰杀数量 回收只数
       0 put_qty,                  --投放只数
       0 out_quantity,    --出库数量      xs02025
       0 risk_get_way,                             --风险金收取途径：投放/回收
       0 risk_rate,                                --风险金系数
       0 material_used_amt,                        --饲料耗用金额
       0 risk_amt,                                 --销售风险金金额
       0 drugs_amt,                                --养殖户承担药金额
       0 sales_amt                                 --销售总金额
  FROM (SELECT substr(period_id,1,6) period_id
               ,org_id
               ,material_id
               ,bus_type
               ,material_code
               ,contract_no
               ,loc_currency_id
               ,sum(price_with_tax*quantity_main) buy_amt  --采购金额(含税)
               ,sum(quantity_main) quantity_main
               ,sum(secondary_qty) secondary_qty
          FROM mreport_poultry.dwu_cg_buy_list_cg01_dd
         WHERE op_day='$OP_DAY'
           AND material_code in('3501000002','3502000002')
           AND release_num like 'BWP%'
           AND cancel_flag in('CLOSED','OPEN')
           AND quantity_received>0
         GROUP BY substr(period_id,1,6)
               ,org_id
               ,material_id
               ,bus_type
               ,material_code
               ,contract_no
               ,loc_currency_id) cg01
  LEFT JOIN (SELECT contract_no,
                    SUM(COALESCE(T2.YB_STANDARD_AMT,0)   --厂价收入
                        +COALESCE(T2.YB_M_DISCOUNT,0)+COALESCE(T2.YB_Q_DISCOUNT,0)+COALESCE(T2.YB_Y_DISCOUNT,0)+COALESCE(T2.YB_O_DISCOUNT,0)+COALESCE(T2.YB_S_DISCOUNT,0) --期间折扣
                        +COALESCE(T2.YB_XCZK_AMT_DISCOUNT,0)    ----现折
                        -COALESCE(T2.YB_TAX,0)           ---税收
                        -COALESCE(T2.CMPNT_COST_1_G,0)-COALESCE(T2.CMPNT_COST_2_G,0)-COALESCE(T2.CMPNT_COST_3_G,0)-COALESCE(T2.CMPNT_COST_4_G,0)
                        -COALESCE(T2.CMPNT_COST_5_G,0)-COALESCE(T2.CMPNT_COST_6_G,0)-COALESCE(T2.CMPNT_COST_7_G,0)-COALESCE(T2.CMPNT_COST_8_G,0)
                        -COALESCE(T2.CMPNT_COST_9_G,0)-COALESCE(T2.MAIN_INCOME_COST_G,0)
                        -COALESCE(T2.SELLING_EXPENSE_CHANGE,0)-COALESCE(T2.SELLING_EXPENSE_CHANGE_G,0)       --销售费用-变动（元）
                        -COALESCE(T2.SELLING_EXPENSE_FIXED,0)-COALESCE(T2.SELLING_EXPENSE_FIXED_G,0)         ---销售费用-固定 
                        -COALESCE(T2.ADMINI_EXPENSE,0)-COALESCE(T2.ADMINI_EXPENSE_G,0)                       --管理费用
                        -COALESCE(T2.FIN_EXPENSE,0)-COALESCE(T2.FIN_EXPENSE_G,0)                             --财务费用
                        -COALESCE(T2.OPERATING_TAX,0)-COALESCE(T2.OPERATING_TAX_G,0)                         --营业税金
                        -COALESCE(T2.LOSSES_ASSET,0)-COALESCE(T2.LOSSES_ASSET_G,0)                           --资产减值
                        +COALESCE(T2.OTHER_REVENUE,0)+COALESCE(T2.OTHER_REVENUE_G,0)-COALESCE(T2.OTHER_COSTS,0)-COALESCE(T2.OTHER_COSTS_G,0)  ---其他业务利润
                        +COALESCE(T2.NON_INCOME,0)+COALESCE(T2.NON_INCOME_G,0)-COALESCE(T2.NON_EXPENSE,0)-COALESCE(T2.NON_EXPENSE_G,0) ---营业外利润
                        +COALESCE(T2.CHANGE_IN_FAIR_VALUE,0)+COALESCE(T2.CHANGE_IN_FAIR_VALUE_G,0)   -- 公允价值变动收益
                        +COALESCE(T2.INVESTMENT_INCOME,0)+COALESCE(T2.INVESTMENT_INCOME_G,0) -- 投资收益
                        +COALESCE(T2.OTHER_INCOME,0)+COALESCE(T2.OTHER_INCOME_G,0)  -- 其他收益
                        +COALESCE(T2.ASSET_DISPOSE,0)+COALESCE(T2.ASSET_DISPOSE_G,0))   -- 资产处置收益
                        AS  TOTAL_PROFIT --总利润，待补充
               FROM mreport_feed.dwu_finance_budget_restore
              WHERE 1=1
              
              GROUP BY contract_no
                    ) SL
    ON (cg01.contract_no = SL.contract_no)
  LEFT JOIN (SELECT *
               FROM dwu_qw_contract_dd
              WHERE op_day = '$OP_DAY') qw03
    ON (cg01.contract_no = qw03.contractnumber)
  JOIN (SELECT M.ORG_ID,
                    M.ORGANIZATION_ID INV_ORG_ID,
                    ORG.LEVEL4_BUS_TYPE BUS_TYPE
               FROM MREPORT_GLOBAL.ods_ebs_cux_bi_ar_ou_inv_mapping M
               LEFT JOIN MREPORT_GLOBAL.ods_ebs_cux_org_structures_all ORG
                 ON M.ORGANIZATION_ID = ORG.LEVEL7_ORG_ID
                AND ORG.LEVEL4_BUS_TYPE = '132020') m
    ON CG01.ORG_ID = M.ORG_ID
"

###########################################################################################
## 处理延伸指标
## 变量声明
TMP_DWF_BIRD_UNIT_PROFITS_DD_1='TMP_DWF_BIRD_UNIT_PROFITS_DD_1'

CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_UNIT_PROFITS_DD_1(
  period_id               string --期间
  ,org_id                 string --组织
  ,bus_type               string --业态
  ,product_line           string --产线
  ,contract_no            string --合同号
  ,contracttype_grp       string --合同类型分组 放养、代养
  ,guarantees_market      string --保值保底市场
  ,income                 string --收入
  ,cost_amount_t          string --总成本
  ,selling_expense_fixed  string --销售费用-固定
  ,selling_expense_change string --销售费用-变动
  ,fin_expense            string --财务费用
  ,admini_expense         string --管理费用
  ,operating_tax          string --税金及附加
  ,ar_losses_asset        string --应收坏账损失 
  ,other_losses_asset     string --其他减值损失 
  ,non_income             string --营业外收入
  ,non_expense            string --营业外支出
  ,change_in_fair_value   string --公允价值变动收益
  ,investment_income      string --投资收益
  ,other_income           string --其他收益
  ,asset_disposit_income  string --资产处置收益 
  ,profit                 string --利润
  ,base_amount            string --本位币金额 发票行金额
  ,quantity_main          string --主数量 采购数量 cg01013
  ,killed_qty             string --辅助数量 宰杀数量 回收只数
  ,put_qty                string --投放只数
  ,out_quantity           string --出库数量 xs02025
  ,risk_get_way           string --风险金收取途径：投放/回收
  ,risk_rate              string --风险金系数
  ,contract_killed_qty    string --冷藏合同宰杀量(只)
  ,mkt_killed_qty         string --冷藏市场宰杀量(只)
  ,zq_profits_amt         string --种禽利润总额
  ,qw_profits_amt         string --禽旺利润总额
  ,material_profits_amt   string --饲料利润总额
  ,cold_profits_amt       string --冷藏利润总额
  ,amt1                   string --合同采购金额（cg01013*cg01019)
  ,amt2                   string --饲料耗用金额
  ,amt3                   string --药金额
  ,amt4                   string  
  ,amt5                   string --风险金
  ,farmer_profits_amt     string --养殖户利润总额
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_1="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_UNIT_PROFITS_DD_1 PARTITION(op_day='$OP_DAY')
select period_id,               --期间
       org_id,                  --组织
       bus_type,                --业态
       product_line,            --产线
       contract_no,             --合同号
       contracttype_grp,        --合同类型分组 放养、代养
       guarantees_market,       --保值保底市场
       income,                  --收入 
       cost_amount_t,           --总成本
       selling_expense_fixed,   --销售费用-固定
       selling_expense_change,  --销售费用-变动
       fin_expense,             --财务费用
       admini_expense,          --管理费用
       operating_tax,           --税金及附加
       ar_losses_asset,         --应收坏账损失 
       other_losses_asset,      --其他减值损失 
       non_income,              --营业外收入
       non_expense,             --营业外支出
       change_in_fair_value,    --公允价值变动收益
       investment_income,       --投资收益
       other_income,            --其他收益
       asset_disposit_income,   --资产处置收益 
       profit,                  --利润
       base_amount,             --本位币金额 发票行金额
       quantity_main ,          --主数量 采购数量 cg01013
       killed_qty ,             --辅助数量 宰杀数量 回收只数
       put_qty,                 --投放只数
       out_quantity ,           --出库数量 xs02025
       risk_get_way ,           --风险金收取途径：投放/回收
       risk_rate,               --风险金系数
       0 contract_killed_qty,                  --冷藏合同宰杀量(只)
       0 mkt_killed_qty,                       --冷藏市场宰杀量(只)
       case when bus_type = 132011 then profit
            else 0 end zq_profits_amt,                       --种禽利润总额 目前取不到值，先不管
       case when bus_type = '132020' then 0
       else profit end qw_profits_amt,                       --禽旺利润总额 剔除禽屠宰业态
       0 material_profits_amt,                               --饲料利润总额
       0 cold_profits_amt,                                   --冷藏利润总额
       buy_amt amt1,                                         --合同采购金额（CG01013*CG01019)
                                                             --AMT2:XS02037饲料耗用金额(XS02实物类别一级为15二级为02的物料的XS02025*XS02032)
       material_used_amt amt2,                               --饲料耗用金额
                                                             --AMT3:药金额(合同号下兽药承担方XS02042为养殖户承担的XS02订单的XS02025*XS02032）
       drugs_amt amt3,                                       --药金额
                                                             --AMT4:XS02表S实物类别一级为25的XS02025*XS02032
       risk_amt amt4,                                        --amt5:风险金（qw12大表风险金标准*只数,有标志以投放或回收计算风险金）
       case when risk_get_way = '投放' then risk_rate * put_qty
            when risk_get_way = '回收' then risk_rate * killed_qty
            else 0 end amt5,                                 --风险金
       case when contracttype_grp = '代养' then base_amount  --发票行金额(借方科目为农业生产成本-寄养费)
            when contracttype_grp = '放养' and risk_get_way='投放'
            then buy_amt-material_used_amt-drugs_amt-risk_amt-risk_rate*put_qty
            when contracttype_grp = '放养' and risk_get_way='回收'
            then buy_amt-material_used_amt-drugs_amt-risk_amt-risk_rate*killed_qty
            when contracttype_grp = '放养'
            then buy_amt-material_used_amt-drugs_amt-risk_amt
       else 0 end farmer_profits_amt                         --养殖户利润总额
  FROM $TMP_DWF_BIRD_UNIT_PROFITS_DD_00 --禽旺、养殖户
 WHERE op_day = '$OP_DAY'
 UNION ALL
 select period_id,               --期间
       org_id,                  --组织
       bus_type,                --业态
       product_line,            --产线
       contract_no,             --合同号
       contracttype_grp,        --合同类型分组 放养、代养
       guarantees_market,       --保值保底市场
       income,                  --收入 
       cost_amount_t,           --总成本
       selling_expense_fixed,   --销售费用-固定
       selling_expense_change,  --销售费用-变动
       fin_expense,             --财务费用
       admini_expense,          --管理费用
       operating_tax,           --税金及附加
       ar_losses_asset,         --应收坏账损失 
       other_losses_asset,      --其他减值损失 
       non_income,              --营业外收入
       non_expense,             --营业外支出
       change_in_fair_value,    --公允价值变动收益
       investment_income,       --投资收益
       other_income,            --其他收益
       asset_disposit_income,   --资产处置收益 
       profit,                  --利润
       base_amount,             --本位币金额 发票行金额
       quantity_main ,          --主数量 采购数量 cg01013
       killed_qty ,             --辅助数量 宰杀数量 回收只数
       put_qty,                 --投放只数
       out_quantity ,           --出库数量 xs02025
       risk_get_way ,           --风险金收取途径：投放/回收
       risk_rate,               --风险金系数
       killed_qty contract_killed_qty,                         --冷藏合同宰杀量(只)
       mkt_killed_qty,                                         --冷藏市场宰杀量(只)
       0 zq_profits_amt,                                       --种禽利润总额
       0 qw_profits_amt,                                       --禽旺利润总额 剔除禽屠宰业态
       0 material_profits_amt,                                 --饲料利润总额
       profit cold_profits_amt,                                --冷藏利润总额
       0 amt1,                                                 --合同采购金额（CG01013*CG01019)
                                                               --AMT2:XS02037饲料耗用金额(XS02实物类别一级为15二级为02的物料的XS02025*XS02032)
       0 amt2,                                                 --饲料耗用金额
                                                               --AMT3:药金额(合同号下兽药承担方XS02042为养殖户承担的XS02订单的XS02025*XS02032）
       0 amt3,                                                 --药金额
                                                               --AMT4:XS02表S实物类别一级为25的XS02025*XS02032
       0 amt4,                                                 --amt5:风险金（qw12大表风险金标准*只数,有标志以投放或回收计算风险金）
       0 amt5,                                                 --风险金
       0 farmer_profits_amt                                    --养殖户利润总额
  FROM $TMP_DWF_BIRD_UNIT_PROFITS_DD_01 --冷藏
 WHERE op_day = '$OP_DAY'
 UNION ALL
 select period_id,               --期间
       org_id,                  --组织
       bus_type,                --业态
       product_line,            --产线
       contract_no,             --合同号
       contracttype_grp,        --合同类型分组 放养、代养
       guarantees_market,       --保值保底市场
       income,                  --收入 
       cost_amount_t,           --总成本
       selling_expense_fixed,   --销售费用-固定
       selling_expense_change,  --销售费用-变动
       fin_expense,             --财务费用
       admini_expense,          --管理费用
       operating_tax,           --税金及附加
       ar_losses_asset,         --应收坏账损失 
       other_losses_asset,      --其他减值损失 
       non_income,              --营业外收入
       non_expense,             --营业外支出
       change_in_fair_value,    --公允价值变动收益
       investment_income,       --投资收益
       other_income,            --其他收益
       asset_disposit_income,   --资产处置收益 
       profit,                  --利润
       base_amount,             --本位币金额 发票行金额
       quantity_main ,          --主数量 采购数量 cg01013
       killed_qty ,             --辅助数量 宰杀数量 回收只数
       put_qty,                 --投放只数
       out_quantity ,           --出库数量 xs02025
       risk_get_way ,           --风险金收取途径：投放/回收
       risk_rate,               --风险金系数
       0 contract_killed_qty,                  --冷藏合同宰杀量(只)
       0 mkt_killed_qty,                       --冷藏市场宰杀量(只)
       0 zq_profits_amt,                       --种禽利润总额
       0 qw_profits_amt,                       --禽旺利润总额 剔除禽屠宰业态
       profit material_profits_amt,            --饲料利润总额
       0 cold_profits_amt,                     --冷藏利润总额
       0 amt1,                                 --合同采购金额（CG01013*CG01019)
                                               --AMT2:XS02037饲料耗用金额(XS02实物类别一级为15二级为02的物料的XS02025*XS02032)
       0 amt2,                                 --饲料耗用金额
                                               --AMT3:药金额(合同号下兽药承担方XS02042为养殖户承担的XS02订单的XS02025*XS02032）
       0 amt3,                                 --药金额
                                               --AMT4:XS02表S实物类别一级为25的XS02025*XS02032
       0 amt4,                                 --amt5:风险金（qw12大表风险金标准*只数,有标志以投放或回收计算风险金）
       0 amt5,                                 --风险金
       0 farmer_profits_amt                    --养殖户利润总额
  FROM $TMP_DWF_BIRD_UNIT_PROFITS_DD_02 --饲料
 WHERE op_day = '$OP_DAY'
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_UNIT_PROFITS_DD='DWF_BIRD_UNIT_PROFITS_DD'

CREATE_DWF_BIRD_UNIT_PROFITS_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_UNIT_PROFITS_DD(
  month_id                       string    --期间(月)
  ,day_id                        string    --期间(日)
  ,level1_org_id                 string    --组织1级(股份)
  ,level1_org_descr              string    --组织1级(股份)
  ,level2_org_id                 string    --组织2级(片联)
  ,level2_org_descr              string    --组织2级(片联)
  ,level3_org_id                 string    --组织3级(片区)
  ,level3_org_descr              string    --组织3级(片区)
  ,level4_org_id                 string    --组织4级(小片)
  ,level4_org_descr              string    --组织4级(小片)
  ,level5_org_id                 string    --组织5级(公司)
  ,level5_org_descr              string    --组织5级(公司)
  ,level6_org_id                 string    --组织6级(ou)
  ,level6_org_descr              string    --组织6级(ou)
  ,level7_org_id                 string    --组织7级(库存组织)
  ,level7_org_descr              string    --组织7级(库存组织)
  ,level1_businesstype_id        string    --业态1级
  ,level1_businesstype_name      string    --业态1级
  ,level2_businesstype_id        string    --业态2级
  ,level2_businesstype_name      string    --业态2级
  ,level3_businesstype_id        string    --业态3级
  ,level3_businesstype_name      string    --业态3级
  ,level4_businesstype_id        string    --业态4级
  ,level4_businesstype_name      string    --业态4级
  ,production_line_id            string    --产线id
  ,production_line_descr         string    --产线
  ,contract_no                   string --合同号
  ,contracttype_grp              string --合同类型分组 放养、代养
  ,guarantees_market             string --保值保底市场
  ,income                        string --收入
  ,cost_amount_t                 string --总成本
  ,selling_expense_fixed         string --销售费用-固定
  ,selling_expense_change        string --销售费用-变动
  ,fin_expense                   string --财务费用
  ,admini_expense                string --管理费用
  ,operating_tax                 string --税金及附加
  ,ar_losses_asset               string --应收坏账损失 
  ,other_losses_asset            string --其他减值损失 
  ,non_income                    string --营业外收入
  ,non_expense                   string --营业外支出
  ,change_in_fair_value          string --公允价值变动收益
  ,investment_income             string --投资收益
  ,other_income                  string --其他收益
  ,asset_disposit_income         string --资产处置收益 
  ,profit                        string --利润
  ,base_amount                   string --本位币金额 发票行金额
  ,quantity_main                 string --主数量 采购数量 cg01013
  ,killed_qty                    string --辅助数量 宰杀数量 回收只数
  ,put_qty                       string --投放只数
  ,out_quantity                  string --出库数量 xs02025
  ,risk_get_way                  string --风险金收取途径：投放/回收
  ,risk_rate                     string --风险金系数
  ,contract_killed_qty           string --冷藏合同宰杀量(只)
  ,mkt_killed_qty                string --冷藏市场宰杀量(只)
  ,zq_profits_amt                string --种禽利润总额
  ,qw_profits_amt                string --禽旺利润总额
  ,material_profits_amt          string --饲料利润总额
  ,cold_profits_amt              string --冷藏利润总额
  ,amt1                          string --合同采购金额（cg01013*cg01019)
  ,amt2                          string --饲料耗用金额
  ,amt3                          string --药金额
  ,amt4                          string
  ,amt5                          string --风险金
  ,farmer_profits_amt            string --养殖户利润总额
)
PARTITIONED BY (op_day STRING)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_UNIT_PROFITS_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_UNIT_PROFITS_DD PARTITION(op_day='$OP_DAY')
select substring(t1.period_id, 1, 6) month_id,
       t1.period_id day_id,
       t2.level1_org_id,
       t2.level1_org_descr,
       t2.level2_org_id,
       t2.level2_org_descr,
       t2.level3_org_id,
       t2.level3_org_descr,
       t2.level4_org_id,
       t2.level4_org_descr,
       t2.level5_org_id,
       t2.level5_org_descr,
       t2.level6_org_id,
       t2.level6_org_descr,
       null level7_org_id,
       null level7_org_descr,
       t5.level1_businesstype_id,
       t5.level1_businesstype_name,
       t5.level2_businesstype_id,
       t5.level2_businesstype_name,
       t5.level3_businesstype_id,
       t5.level3_businesstype_name,
       t5.level4_businesstype_id,
       t5.level4_businesstype_name,
       case t1.product_line when 10 then '1' when 20 then '2'
       else null end production_line_id,       --产线
       case t1.product_line when 10 then '鸡线' when 20 then '鸭线'
       else null end production_line_descr,
       contract_no,                            --合同号
       contracttype_grp,                       --合同类型分组 放养、代养
       guarantees_market,                      --保值保底市场
       income,                                 --收入 
       cost_amount_t,                          --总成本
       selling_expense_fixed,                  --销售费用-固定
       selling_expense_change,                 --销售费用-变动
       fin_expense,                            --财务费用
       admini_expense,                         --管理费用
       operating_tax,                          --税金及附加
       ar_losses_asset,                        --应收坏账损失 
       other_losses_asset,                     --其他减值损失 
       non_income,                             --营业外收入
       non_expense,                            --营业外支出
       change_in_fair_value,                   --公允价值变动收益
       investment_income,                      --投资收益
       other_income,                           --其他收益
       asset_disposit_income,                  --资产处置收益 
       profit,                                 --利润
       base_amount,                            --本位币金额 发票行金额
       quantity_main,                          --主数量 采购数量 cg01013
       killed_qty,                             --辅助数量 宰杀数量 回收只数
       put_qty,                                --投放只数
       out_quantity,                           --出库数量 xs02025
       risk_get_way,                           --风险金收取途径：投放/回收
       risk_rate,                              --风险金系数
       contract_killed_qty,                    --冷藏合同宰杀量(只)
       mkt_killed_qty,                         --冷藏市场宰杀量(只)
       zq_profits_amt,                         --种禽利润总额
       qw_profits_amt,                         --禽旺利润总额
       material_profits_amt,                   --饲料利润总额
       cold_profits_amt,                       --冷藏利润总额
       amt1,
       amt2,                                   --饲料耗用金额
       amt3,                                   --药金额
       amt4,
       amt5,                                   --风险金
       farmer_profits_amt                      --养殖户利润总额
  from (SELECT *
          FROM $TMP_DWF_BIRD_UNIT_PROFITS_DD_1
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT level1_org_id
                    ,level1_org_descr
                    ,level2_org_id
                    ,level2_org_descr
                    ,level3_org_id
                    ,level3_org_descr
                    ,level4_org_id
                    ,level4_org_descr
                    ,level5_org_id
                    ,level5_org_descr
                    ,level6_org_id
                    ,level6_org_descr
                    ,org_id
               FROM mreport_global.dim_org_management
              GROUP BY level1_org_id
                    ,level1_org_descr
                    ,level2_org_id
                    ,level2_org_descr
                    ,level3_org_id
                    ,level3_org_descr
                    ,level4_org_id
                    ,level4_org_descr
                    ,level5_org_id
                    ,level5_org_descr
                    ,level6_org_id
                    ,level6_org_descr
                    ,org_id) t2 
    ON (t1.org_id=t2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t5
    ON (T1.bus_type = T5.level4_businesstype_id
    AND 1=0)                                        --去掉业态
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

    $CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_00;
    $INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_00;
    $CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_01;
    $INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_01;
    $CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_02;

    $CREATE_TMP_DWF_BIRD_UNIT_PROFITS_DD_1;
    $INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_1;
    $CREATE_DWF_BIRD_UNIT_PROFITS_DD;
    $INSERT_DWF_BIRD_UNIT_PROFITS_DD;
"  -v
#    $INSERT_TMP_DWF_BIRD_UNIT_PROFITS_DD_02;