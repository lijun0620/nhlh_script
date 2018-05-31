#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwf_bird_material_cost_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 养户档案信息
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwf_bird_material_cost_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_MATERIAL_COST_DD_1='TMP_DWF_BIRD_MATERIAL_COST_DD_1'

CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_MATERIAL_COST_DD_1(
  contract_no                   string      --合同号
  ,farmer_id                    string      --养户号
  ,farmer_name                  string      --养殖单位
  ,production_line_id           string      --产线代码
  ,production_line_descr        string      --产线描述
  ,contract_date                string      --合同日期
  ,m_factory_descr              string      --苗场
  ,recycle_type_id              string      --回收类型
  ,recycle_type_descr           string      --回收类型
  ,breed_type_id                string      --养殖类型
  ,breed_type_descr             string      --养殖类型
  ,distance                     string      --距离
  ,contract_qty                 string      --投放数量
  ,put_weight                   string      --投放重量(kg)
  ,put_cost                     string      --苗投放成本
  ,put_price                    string      --苗投放金额(元)
  ,material_weight              string      --物料重量(kg)
  ,material_amount              string      --物料金额(元)
  ,drugs_cost                   string      --兽药费用
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_1="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_MATERIAL_COST_DD_1 PARTITION(op_day='$OP_DAY')
SELECT t1.contract_no                    --合同号
       ,t1.farmer_id                     --养户号
       ,t1.farmer_name                   --养殖单位
       ,case when t1.production_line_id='CHICHEN' then '1'
             when t1.production_line_id='DUCK' then '2'
        else null end production_line_id           --产线代码
       ,case when t1.production_line_id='CHICHEN' then '鸡线'
             when t1.production_line_id='DUCK' then '鸭线'
        else null end production_line_descr  --产线描述
       ,t1.contract_date                 --合同日期
       ,t1.m_factory_descr               --苗场
       ,case when t1.recycle_type_descr='保值' then '1'
             when t1.recycle_type_descr='保底' then '2'
             when t1.recycle_type_descr='市场' then '3'
        else null end recycle_type_id    --回收类型
       ,t1.recycle_type_descr            --回收类型
       ,case when t1.breed_type_descr='代养' then '1'
             when t1.breed_type_descr='放养' then '2'
        else null end breed_type_id
       ,t1.breed_type_descr              --养殖类型
       ,t1.distance                      --距离
       ,t1.contract_qty                  --投放数量
       ,null put_weight                  --投放重量
       ,t1.put_cost                      --苗投放成本
       ,t1.put_price                     --苗投放单价(元/只)
       ,t2.material_weight               --物料重量(kg)
       ,t2.material_amount               --物料金额(元)
       ,t1.drugs_cost                    --兽药费用
  FROM (SELECT contractnumber contract_no             --合同号
               ,meaning production_line_id            --产线代码
               ,meaning_desc production_line_descr    --产线描述
               ,substr(contract_date,1,10) contract_date  --合同日期
               ,hatchery_name m_factory_descr         --孵化场
               ,org_id                                --OU组织  
               ,inv_org_id                            --库存组织ID
               ,bus_type                              --业态
               ,qty contract_qty                      --合同数量
               ,guarantees_market recycle_type_descr  --保值保底市场
               ,vendor_code farmer_id                 --养户号
               ,vendor_name farmer_name               --养殖单位
               ,contracttype_grp breed_type_descr     --合同类型分组
               ,mediamount*qty drugs_cost             --兽药费用               
               ,chicksalemoney*qty put_cost           --投放成本
               ,price put_price                       --苗投放单价
               ,distance                              --距离
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT cust_po_num contract_no             --合同号
                    ,sum(primary_quantity) material_weight --物料重量
                    ,sum(order_price*primary_quantity) material_amount
               FROM dwu_xs_other_sale_dd a1
              INNER JOIN (SELECT inventory_item_id item_id
                            FROM mreport_global.dim_material
                           WHERE level2_material_id='1503'
                           GROUP BY inventory_item_id) a2
                 ON (a1.material_id=a2.item_id)
              WHERE a1.op_day='$OP_DAY'
              GROUP BY a1.cust_po_num) t2
    ON (t1.contract_no=t2.contract_no)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_MATERIAL_COST_DD_2='TMP_DWF_BIRD_MATERIAL_COST_DD_2'

CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_MATERIAL_COST_DD_2(
  contract_no                       string      --合同号
  ,farmer_id                        string      --养户号
  ,farmer_name                      string      --养殖单位
  ,production_line_id               string      --产线代码
  ,production_line_descr            string      --产线描述
  ,contract_date                    string      --合同日期
  ,recycle_date                     string      --回收日期
  ,m_factory_descr                  string      --苗场
  ,recycle_type_id                  string      --回收类型
  ,recycle_type_descr               string      --回收类型
  ,breed_type_id                    string      --养殖类型
  ,breed_type_descr                 string      --养殖类型
  ,distance                         string      --距离
  ,level1_org_id                    string      --组织1级
  ,level1_org_descr                 string      --组织1级
  ,level2_org_id                    string      --组织2级
  ,level2_org_descr                 string      --组织2级
  ,level3_org_id                    string      --组织3级
  ,level3_org_descr                 string      --组织3级
  ,level4_org_id                    string      --组织4级
  ,level4_org_descr                 string      --组织4级
  ,level5_org_id                    string      --组织5级
  ,level5_org_descr                 string      --组织5级
  ,level6_org_id                    string      --组织6级
  ,level6_org_descr                 string      --组织6级
  ,level7_org_id                    string      --组织7级
  ,level7_org_descr                 string      --组织7级
  ,level1_businesstype_id           string      --业态1级
  ,level1_businesstype_name         string      --业态1级
  ,level2_businesstype_id           string      --业态2级
  ,level2_businesstype_name         string      --业态2级
  ,level3_businesstype_id           string      --业态3级
  ,level3_businesstype_name         string      --业态3级
  ,level4_businesstype_id           string      --业态4级
  ,level4_businesstype_name         string      --业态4级
  ,material_id                      string      --物料编码
  ,material_descr                   string      --物料名称
  ,cancel_flag                      string      --结算标志(CLOSED-关闭(已结算), OPEN-打开)
  ,contract_qty                     string      --投放数量
  ,put_cost                         string      --苗投放成本
  ,put_amount                       string      --苗投放金额(元)
  ,material_weight                  string      --物料重量(kg)
  ,material_amount                  string      --物料金额(元)
  ,recycle_qty                      string      --回收数量(支)
  ,recycle_weight                   string      --回收重量(kg)
  ,recycle_cost                     string      --回收成本
  ,base_recycle_cost                string      --基本回收成本
  ,drugs_cost                       string      --兽药费用
  ,other_subsidy_cost               string      --其他项目补贴(元)
  ,kpi_type                         string      --指标类型(采购, 投放)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_2="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_MATERIAL_COST_DD_2 PARTITION(op_day='$OP_DAY')
SELECT t2.contract_no                         --合同号
       ,t2.farmer_id                          --养户号
       ,t2.farmer_name                        --养殖单位
       ,t2.production_line_id                 --产线代码
       ,t2.production_line_descr              --产线描述
       ,t2.contract_date                      --合同日期
       ,t1.recycle_date                       --回收日期
       ,t2.m_factory_descr                    --苗场
       ,t2.recycle_type_id                    --回收类型
       ,t2.recycle_type_descr                 --回收类型
       ,t2.breed_type_id                      --养殖类型
       ,t2.breed_type_descr                   --养殖类型
       ,t2.distance                           --距离
       ,t4.level1_org_id                      --组织1级
       ,t4.level1_org_descr                   --组织1级
       ,t4.level2_org_id                      --组织2级
       ,t4.level2_org_descr                   --组织2级
       ,t4.level3_org_id                      --组织3级
       ,t4.level3_org_descr                   --组织3级
       ,t4.level4_org_id                      --组织4级
       ,t4.level4_org_descr                   --组织4级
       ,t4.level5_org_id                      --组织5级
       ,t4.level5_org_descr                   --组织5级
       ,t4.level6_org_id                      --组织6级
       ,t4.level6_org_descr                   --组织6级
       ,t5.level7_org_id                      --组织7级
       ,t5.level7_org_descr                   --组织7级
       ,t6.level1_businesstype_id             --业态1级
       ,t6.level1_businesstype_name           --业态1级
       ,t6.level2_businesstype_id             --业态2级
       ,t6.level2_businesstype_name           --业态2级
       ,t6.level3_businesstype_id             --业态3级
       ,t6.level3_businesstype_name           --业态3级
       ,t6.level4_businesstype_id             --业态4级
       ,t6.level4_businesstype_name           --业态4级
       ,t1.material_id                        --物料编码
       ,t1.material_descr                     --物料名称
       ,t1.cancel_flag                        --结算标志(CLOSED-关闭(已结算), OPEN-打开)
       ,0 contract_qty                        --投放数量
       ,0 put_cost                            --苗投放成本
       ,t2.put_price*t1.recycle_qty put_amount--苗投放金额
       ,t2.material_weight                    --物料重量(kg)
       ,t2.material_amount                    --物料金额(元)
       ,t1.recycle_qty                        --回收数量(支)
       ,t1.recycle_weight                     --回收重量(kg)
       ,t1.recycle_cost                       --回收成本(元)
       ,t7.recycle_cost base_recycle_cost     --基本回收成本(元)
       ,0 drugs_cost                          --兽药费用
       ,t1.other_subsidy_cost                 --其他项目补贴(元)
       ,'BUY_BACK' kpi_type                   --指标类型(采购)
  FROM (SELECT a1.contract_no
               ,a1.org_id
               ,a1.inv_org_id
               ,a1.bus_type
               ,a1.material_id
               ,a1.material_descr
               ,a1.recycle_date
               ,a1.cancel_flag
               ,sum(a1.recycle_qty) recycle_qty
               ,sum(a1.recycle_weight) recycle_weight
               ,sum(a1.recycle_cost) recycle_cost
               ,sum(coalesce(a2.amount,0) + (coalesce(a3.unit_amount,0)+coalesce(a4.unit_amount)) * a1.recycle_weight) other_subsidy_cost
          FROM (SELECT contract_no                           --合同号
                       ,regexp_replace(release_num,'BWP','') doc_no  --单据号
                       ,org_id                               --采购组织
                       ,inv_org_id                           --库存组织ID
                       ,bus_type                             --业态
                       ,material_code material_id            --物料编码
                       ,material_description material_descr  --物料描述
                       ,substr(transaction_date,1,6) recycle_date  --采购日期(月)
                       ,cancel_flag                           --结算标志(CLOSED-关闭, OPEN-打开)
                       ,secondary_qty recycle_qty             --辅助数量               
                       ,quantity_received recycle_weight      --已接收数量               
                       ,price_with_tax*quantity_received recycle_cost
                  FROM dwu_cg_buy_list_cg01_dd
                 WHERE op_day='$OP_DAY'
                   AND material_code in('3501000002','3502000002')
                   AND release_num like 'BWP%'
                   AND cancel_flag in('CLOSED','OPEN')
                   AND quantity_received>0) a1
          LEFT JOIN (SELECT pith_num contract_no
                            ,cacu_num doc_no
                            ,sum(amount) amount
                       FROM dwu_qw_qw08_dd
                      WHERE op_day='$OP_DAY'
                      GROUP BY pith_num,cacu_num) a2
            ON (a1.contract_no=a2.contract_no
            AND a1.doc_no=a2.doc_no)
          LEFT JOIN (SELECT pith_num contract_no
                            ,doc_num doc_no
                            ,coalesce(failed_flag,0)+coalesce(dermatitis,0)+coalesce(palmar,0)+coalesce(subsides,0)+coalesce(sed_self_ajst,0)+coalesce(airea_ajst,0)+coalesce(no_medicine,0)+coalesce(out_contrat_ajst,0)+coalesce(medcine_fee,0) unit_amount
                       FROM dwu_qw_qw13_dd
                      WHERE op_day='$OP_DAY') a3
            ON (a1.contract_no=a3.contract_no
            AND a1.doc_no=a3.doc_no)
          LEFT JOIN (SELECT contract_pitch_num contract_no
                            ,sum(coalesce(para_value,0)) unit_amount   --补贴单价
                       FROM dwu_qw_qw14_dd
                      WHERE op_day='$OP_DAY'
                        AND contract_para_id in('4','17','16','13','11')
                      GROUP BY contract_pitch_num) a4
            ON (a1.contract_no=a4.contract_no)
        GROUP BY a1.contract_no
               ,a1.org_id
               ,a1.inv_org_id
               ,a1.bus_type
               ,a1.material_id
               ,a1.material_descr
               ,a1.recycle_date
               ,a1.cancel_flag) t1
 INNER JOIN (SELECT *
               FROM $TMP_DWF_BIRD_MATERIAL_COST_DD_1
              WHERE op_day='$OP_DAY') t2
    ON (t1.contract_no=t2.contract_no)
  LEFT JOIN (SELECT level1_org_id,
                    level1_org_descr,
                    level2_org_id,
                    level2_org_descr,
                    level3_org_id,
                    level3_org_descr,
                    level4_org_id,
                    level4_org_descr,
                    level5_org_id,
                    level5_org_descr,
                    level6_org_id,
                    level6_org_descr,
                    org_id
               FROM mreport_global.dim_org_management
              WHERE org_id is not null
              GROUP BY level1_org_id,
                    level1_org_descr,
                    level2_org_id,
                    level2_org_descr,
                    level3_org_id,
                    level3_org_descr,
                    level4_org_id,
                    level4_org_descr,
                    level5_org_id,
                    level5_org_descr,
                    level6_org_id,
                    level6_org_descr,
                    org_id) t4
    ON (t1.org_id=t4.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t5
    ON (t1.inv_org_id=t5.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t6
    ON (t1.bus_type=t6.level4_businesstype_id)
  LEFT JOIN (SELECT pith_no contract_no,
                    sum(coalesce(rt_base_price,'0')*coalesce(buy_weight,'0')) recycle_cost
               FROM dwu_qw_qw11_dd
              WHERE op_day='$OP_DAY'
              GROUP BY pith_no) t7
    ON (t1.contract_no=t7.contract_no)
 WHERE t2.contract_no is not null
UNION ALL
SELECT a1.contract_no                         --合同号
       ,a1.farmer_id                          --养户号
       ,a1.farmer_name                        --养殖单位
       ,a1.production_line_id                 --产线代码
       ,a1.production_line_descr              --产线描述
       ,substr(a1.contract_date,1,10) contract_date    --合同日期
       ,regexp_replace(substr(a1.contract_date,1,10),'-','') recycle_date   --回收日期
       ,a1.m_factory_descr                    --苗场
       ,a1.recycle_type_id                    --回收类型
       ,a1.recycle_type_descr                 --回收类型
       ,a1.breed_type_id                      --养殖类型
       ,a1.breed_type_descr                   --养殖类型
       ,a1.distance                           --距离
       ,a2.level1_org_id                      --组织1级
       ,a2.level1_org_descr                   --组织1级
       ,a2.level2_org_id                      --组织2级
       ,a2.level2_org_descr                   --组织2级
       ,a2.level3_org_id                      --组织3级
       ,a2.level3_org_descr                   --组织3级
       ,a2.level4_org_id                      --组织4级
       ,a2.level4_org_descr                   --组织4级
       ,a2.level5_org_id                      --组织5级
       ,a2.level5_org_descr                   --组织5级
       ,a2.level6_org_id                      --组织6级
       ,a2.level6_org_descr                   --组织6级
       ,a3.level7_org_id                      --组织7级
       ,a3.level7_org_descr                   --组织7级
       ,a4.level1_businesstype_id             --业态1级
       ,a4.level1_businesstype_name           --业态1级
       ,a4.level2_businesstype_id             --业态2级
       ,a4.level2_businesstype_name           --业态2级
       ,a4.level3_businesstype_id             --业态3级
       ,a4.level3_businesstype_name           --业态3级
       ,a4.level4_businesstype_id             --业态4级
       ,a4.level4_businesstype_name           --业态4级
       ,null material_id                      --物料编码
       ,null material_descr                   --物料名称
       ,null cancel_flag                      --结算标志(CLOSED-关闭(已结算), OPEN-打开)
       ,a1.contract_qty                       --投放数量
       ,a1.put_cost                           --苗投放成本
       ,a1.put_amount                         --苗投放金额
       ,'0' material_weight                   --物料重量(kg)
       ,'0' material_amount                   --物料金额(元)
       ,'0' recycle_qty                       --回收数量(支)
       ,'0' recycle_weight                    --回收重量(kg)
       ,'0' recycle_cost                      --回收成本(元)
       ,'0' base_recycle_cost                 --基础回收成本(元)
       ,'0' drugs_cost                        --兽药费用
       ,'0' other_subsidy_cost                --其他项目补贴(元)
       ,'PUT' kpi_type                        --指标类型(投放)
  FROM (SELECT contractnumber contract_no     --合同号
               ,vendor_code farmer_id         --养户号
               ,vendor_name farmer_name       --养殖单位
               ,case when meaning='CHICHEN' then '1'
                     when meaning='DUCK' then '2'
                else null end production_line_id       --产线代码
               ,case when meaning='CHICHEN' then '鸡线'
                     when meaning='DUCK' then '鸭线'
                else null end production_line_descr    --产线描述
               ,contract_date                          --合同日期
               ,hatchery_name m_factory_descr          --孵化场
               ,case when guarantees_market='保值' then '1'
                     when guarantees_market='保底' then '2'
                     when guarantees_market='市场' then '3'
                else null end recycle_type_id          --回收类型
               ,guarantees_market recycle_type_descr   --回收类型
               ,case when contracttype_grp='代养' then '1'
                     when contracttype_grp='放养' then '2'
                else null end breed_type_id
               ,contracttype_grp breed_type_descr      --养殖类型
               ,distance                               --距离
               ,org_id
               ,inv_org_id
               ,bus_type
               ,qty contract_qty
               ,chicksalemoney*qty put_cost            --苗投放成本(元)
               ,'0' put_amount                         --苗投放金额(元)
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY'
           AND guarantees_market in('保值','保底')) a1
  LEFT JOIN (SELECT level1_org_id,
                    level1_org_descr,
                    level2_org_id,
                    level2_org_descr,
                    level3_org_id,
                    level3_org_descr,
                    level4_org_id,
                    level4_org_descr,
                    level5_org_id,
                    level5_org_descr,
                    level6_org_id,
                    level6_org_descr,
                    org_id
               FROM mreport_global.dim_org_management
              WHERE org_id is not null
              GROUP BY level1_org_id,
                    level1_org_descr,
                    level2_org_id,
                    level2_org_descr,
                    level3_org_id,
                    level3_org_descr,
                    level4_org_id,
                    level4_org_descr,
                    level5_org_id,
                    level5_org_descr,
                    level6_org_id,
                    level6_org_descr,
                    org_id) a2
    ON (a1.org_id=a2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) a3
    ON (a1.inv_org_id=a3.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) a4
    ON (a1.bus_type=a4.level4_businesstype_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_MATERIAL_COST_DD_3='TMP_DWF_BIRD_MATERIAL_COST_DD_3'

CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_MATERIAL_COST_DD_3(
  contract_no                       string      --合同号
  ,farmer_id                        string      --养户号
  ,farmer_name                      string      --养殖单位
  ,production_line_id               string      --产线代码
  ,production_line_descr            string      --产线描述
  ,contract_date                    string      --合同日期
  ,recycle_date                     string      --回收日期
  ,m_factory_descr                  string      --苗场
  ,recycle_type_id                  string      --回收类型
  ,recycle_type_descr               string      --回收类型
  ,breed_type_id                    string      --养殖类型
  ,breed_type_descr                 string      --养殖类型
  ,distance                         string      --距离
  ,level1_org_id                    string      --组织1级
  ,level1_org_descr                 string      --组织1级
  ,level2_org_id                    string      --组织2级
  ,level2_org_descr                 string      --组织2级
  ,level3_org_id                    string      --组织3级
  ,level3_org_descr                 string      --组织3级
  ,level4_org_id                    string      --组织4级
  ,level4_org_descr                 string      --组织4级
  ,level5_org_id                    string      --组织5级
  ,level5_org_descr                 string      --组织5级
  ,level6_org_id                    string      --组织6级
  ,level6_org_descr                 string      --组织6级
  ,level7_org_id                    string      --组织7级
  ,level7_org_descr                 string      --组织7级
  ,level1_businesstype_id           string      --业态1级
  ,level1_businesstype_name         string      --业态1级
  ,level2_businesstype_id           string      --业态2级
  ,level2_businesstype_name         string      --业态2级
  ,level3_businesstype_id           string      --业态3级
  ,level3_businesstype_name         string      --业态3级
  ,level4_businesstype_id           string      --业态4级
  ,level4_businesstype_name         string      --业态4级
  ,material_id                      string      --物料编码
  ,material_descr                   string      --物料名称
  ,contract_qty                     string      --投放数量
  ,put_cost                         string      --苗投放成本
  ,put_amount                       string      --苗投放金额
  ,material_weight                  string      --物料重量(kg)
  ,material_amount                  string      --物料金额(元)
  ,recycle_qty                      string      --回收数量(支)
  ,recycle_weight                   string      --回收重量(kg)
  ,recycle_cost                     string      --回收成本
  ,base_recycle_cost                string      --基本回收成本
  ,drugs_cost                       string      --兽药费用
  ,other_subsidy_cost               string      --其他项目补贴(元)
  ,recyle_carriage_cost             string      --回收运费
  ,kpi_type                         string      --指标类型(采购, 投放)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_3="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_MATERIAL_COST_DD_3 PARTITION(op_day='$OP_DAY')
SELECT t1.contract_no                         --合同号
       ,t1.farmer_id                          --养户号
       ,t1.farmer_name                        --养殖单位
       ,t1.production_line_id                 --产线代码
       ,t1.production_line_descr              --产线描述
       ,t1.contract_date                      --合同日期
       ,t1.recycle_date                       --回收日期
       ,t1.m_factory_descr                    --苗场
       ,t1.recycle_type_id                    --回收类型
       ,t1.recycle_type_descr                 --回收类型
       ,t1.breed_type_id                      --养殖类型
       ,t1.breed_type_descr                   --养殖类型
       ,case when t2.distance is null and coalesce(t1.distance,999)<=50
             then t1.recycle_qty
        else t2.distance end distance         --距离
       ,t1.level1_org_id                      --组织1级
       ,t1.level1_org_descr                   --组织1级
       ,t1.level2_org_id                      --组织2级
       ,t1.level2_org_descr                   --组织2级
       ,t1.level3_org_id                      --组织3级
       ,t1.level3_org_descr                   --组织3级
       ,t1.level4_org_id                      --组织4级
       ,t1.level4_org_descr                   --组织4级
       ,t1.level5_org_id                      --组织5级
       ,t1.level5_org_descr                   --组织5级
       ,t1.level6_org_id                      --组织6级
       ,t1.level6_org_descr                   --组织6级
       ,t1.level7_org_id                      --组织7级
       ,t1.level7_org_descr                   --组织7级
       ,t1.level1_businesstype_id             --业态1级
       ,t1.level1_businesstype_name           --业态1级
       ,t1.level2_businesstype_id             --业态2级
       ,t1.level2_businesstype_name           --业态2级
       ,t1.level3_businesstype_id             --业态3级
       ,t1.level3_businesstype_name           --业态3级
       ,t1.level4_businesstype_id             --业态4级
       ,t1.level4_businesstype_name           --业态4级
       ,t1.material_id                        --物料编码
       ,t1.material_descr                     --物料名称
       ,t1.contract_qty                       --投放数量
       ,t1.put_cost                           --投放成本
       ,t1.put_amount                         --投放金额(苗投放金额)
       ,case when t4.breed_type_descr='放养' and t1.kpi_type='BUY_BACK' then t1.material_weight
             when t4.breed_type_descr='代养' and t1.kpi_type='BUY_BACK' then t5.material_weight
        else null end material_weight         --物料重量(kg)
       ,case when t4.breed_type_descr='放养' and t1.kpi_type='BUY_BACK' then t1.material_amount
             when t4.breed_type_descr='代养' and t1.kpi_type='BUY_BACK' then t5.material_amount
        else null end material_amount         --物料金额(元)
       ,t1.recycle_qty                        --回收数量(支)
       ,t1.recycle_weight                     --回收重量(kg)
       ,t1.recycle_cost                       --回收成本
       ,t1.base_recycle_cost                  --基础回收成本
       ,'0' drugs_cost                        --兽药费用
       ,t1.other_subsidy_cost                 --其他项目补贴(元)
       ,case when t1.kpi_type='BUY_BACK' then t2.recyle_carriage_cost
        else '0' end recyle_carriage_cost     --回收运费
       ,t1.kpi_type                           --指标类型(采购, 投放)
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_MATERIAL_COST_DD_2
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT a1.contract_id                         --批次号
                    ,a2.recycle_date
                    ,sum(case when coalesce(a1.mileage,99999)<=50 then coalesce(a1.factnumber,0) else 0 end) distance  --里程(换算为<=50的回收只收)
                    ,sum(a1.freight) recyle_carriage_cost  --运费金额
               FROM (SELECT *
                       FROM dwu_qw_weighfreight_dd
                      WHERE op_day='$OP_DAY') a1
              INNER JOIN (SELECT contract_no                           --合同号
                                 ,regexp_replace(release_num,'BWP','') doc_no  --单据号
                                 ,substr(transaction_date,1,6) recycle_date  --采购日期(月)
                            FROM dwu_cg_buy_list_cg01_dd
                           WHERE op_day='$OP_DAY'
                             AND material_code in('3501000002','3502000002')
                             AND release_num like 'BWP%'
                             AND cancel_flag in('CLOSED','OPEN')
                             AND quantity_received>0) a2
                ON (a1.contract_id=a2.contract_no
                AND a1.callback_id=a2.doc_no)
              GROUP BY a1.contract_id
                    ,a2.recycle_date) t2
    ON (t1.contract_no=t2.contract_id
    AND t1.recycle_date=t2.recycle_date)
  LEFT JOIN (SELECT contractnumber contract_no          --合同号
                    ,contracttype_grp breed_type_descr   --合同类型分组
               FROM dwu_qw_contract_dd
              WHERE op_day='$OP_DAY'
              GROUP BY contractnumber,contracttype_grp) t4
    ON (t1.contract_no=t4.contract_no)
  LEFT JOIN (SELECT a1.breeding_contract,
                    sum(a1.quantity) material_weight,
                    sum(a1.quantity*a3.price) material_amount
               FROM dwu_qw_breed_cost_dd a1
              INNER JOIN (SELECT item_id
                            FROM mreport_global.dim_material
                           WHERE level2_material_id='1503'
                           GROUP BY item_id) a2
                 ON (a1.inventory_item_code=a2.item_id)
               LEFT JOIN (SELECT period_id
                                 ,org_id
                                 ,organization_id inv_org_id
                                 ,material_item_id
                                 ,cost_amount_t price
                            FROM dwu_finance_cost_pric) a3
                 ON (a1.ou_id=a3.org_id
                 AND a1.organization_id=a3.inv_org_id
                 AND substr(regexp_replace(a1.transaction_date,'-',''),1,6)=a3.period_id
                 AND a1.inventory_item_code=a3.material_item_id)
              WHERE op_day='$OP_DAY'
                AND a1.breeding_contract is not null
              GROUP BY a1.breeding_contract) t5
    ON (t1.contract_no=t5.breeding_contract)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_MATERIAL_COST_DD='DWF_BIRD_MATERIAL_COST_DD'

CREATE_DWF_BIRD_MATERIAL_COST_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_MATERIAL_COST_DD(
  contract_no                       string      --合同号
  ,farmer_id                        string      --养户号
  ,farmer_name                      string      --养殖单位
  ,production_line_id               string      --产线代码
  ,production_line_descr            string      --产线描述
  ,contract_date                    string      --合同日期
  ,recycle_date                     string      --回收日期
  ,m_factory_descr                  string      --苗场
  ,recycle_type_id                  string      --回收类型
  ,recycle_type_descr               string      --回收类型
  ,breed_type_id                    string      --养殖类型
  ,breed_type_descr                 string      --养殖类型
  ,distance                         string      --距离
  ,level1_org_id                    string      --组织1级
  ,level1_org_descr                 string      --组织1级
  ,level2_org_id                    string      --组织2级
  ,level2_org_descr                 string      --组织2级
  ,level3_org_id                    string      --组织3级
  ,level3_org_descr                 string      --组织3级
  ,level4_org_id                    string      --组织4级
  ,level4_org_descr                 string      --组织4级
  ,level5_org_id                    string      --组织5级
  ,level5_org_descr                 string      --组织5级
  ,level6_org_id                    string      --组织6级
  ,level6_org_descr                 string      --组织6级
  ,level7_org_id                    string      --组织7级
  ,level7_org_descr                 string      --组织7级
  ,level1_businesstype_id           string      --业态1级
  ,level1_businesstype_name         string      --业态1级
  ,level2_businesstype_id           string      --业态2级
  ,level2_businesstype_name         string      --业态2级
  ,level3_businesstype_id           string      --业态3级
  ,level3_businesstype_name         string      --业态3级
  ,level4_businesstype_id           string      --业态4级
  ,level4_businesstype_name         string      --业态4级
  ,material_id                      string      --物料编码
  ,material_descr                   string      --物料名称
  ,contract_qty                     string      --投放数量
  ,put_cost                         string      --苗投放成本
  ,put_amount                       string      --苗投放金额
  ,material_weight                  string      --物料重量(kg)
  ,material_amount                  string      --物料金额(元)
  ,recycle_qty                      string      --回收数量(支)
  ,recycle_weight                   string      --回收重量(kg)
  ,recycle_cost                     string      --回收成本
  ,base_recycle_cost                string      --基本回收成本
  ,drugs_cost                       string      --兽药费用
  ,other_subsidy_cost               string      --其他项目补贴(元)
  ,recyle_carriage_cost             string      --回收运费
  ,kpi_type                         string      --指标类型(采购, 投放)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_MATERIAL_COST_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_MATERIAL_COST_DD PARTITION(op_day='$OP_DAY')
SELECT contract_no                             --合同号
       ,farmer_id                              --养户号
       ,farmer_name                            --养殖单位
       ,production_line_id                     --产线代码
       ,production_line_descr                  --产线描述
       ,contract_date                          --合同日期
       ,recycle_date                           --回收日期
       ,m_factory_descr                        --苗场
       ,recycle_type_id                        --回收类型
       ,recycle_type_descr                     --回收类型
       ,breed_type_id                          --养殖类型
       ,breed_type_descr                       --养殖类型
       ,distance                               --距离
       ,level1_org_id                          --组织1级
       ,level1_org_descr                       --组织1级
       ,level2_org_id                          --组织2级
       ,level2_org_descr                       --组织2级
       ,level3_org_id                          --组织3级
       ,level3_org_descr                       --组织3级
       ,level4_org_id                          --组织4级
       ,level4_org_descr                       --组织4级
       ,level5_org_id                          --组织5级
       ,level5_org_descr                       --组织5级
       ,level6_org_id                          --组织6级
       ,level6_org_descr                       --组织6级
       ,level7_org_id                          --组织7级
       ,level7_org_descr                       --组织7级
       ,level1_businesstype_id                 --业态1级
       ,level1_businesstype_name               --业态1级
       ,level2_businesstype_id                 --业态2级
       ,level2_businesstype_name               --业态2级
       ,level3_businesstype_id                 --业态3级
       ,level3_businesstype_name               --业态3级
       ,level4_businesstype_id                 --业态4级
       ,level4_businesstype_name               --业态4级
       ,material_id                            --物料编码
       ,material_descr                         --物料名称
       ,coalesce(contract_qty,0)                           --投放数量
       ,coalesce(put_cost,0)                               --苗投放成本
       ,coalesce(put_amount,0)                             --苗投放金额
       ,coalesce(material_weight,0)                        --物料重量(kg)
       ,coalesce(material_amount,0)                        --物料金额(元)
       ,coalesce(recycle_qty,0)                            --回收数量(支)
       ,coalesce(recycle_weight,0)                         --回收重量(kg)
       ,coalesce(recycle_cost,0)                           --回收成本
       ,coalesce(base_recycle_cost,0)                      --基本回收成本
       ,coalesce(drugs_cost,0)                             --兽药费用
       ,coalesce(other_subsidy_cost,0)                     --其他项目补贴(元)
       ,coalesce(recyle_carriage_cost,0)                   --回收运费
       ,kpi_type                               --指标类型(采购, 投放)
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_MATERIAL_COST_DD_3
         WHERE op_day='$OP_DAY'
           AND level2_org_id NOT IN('1015')) t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;

    $CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_1;
    $INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_1;
    $CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_2;
    $INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_2;
    $CREATE_TMP_DWF_BIRD_MATERIAL_COST_DD_3;
    $INSERT_TMP_DWF_BIRD_MATERIAL_COST_DD_3;
    $CREATE_DWF_BIRD_MATERIAL_COST_DD;
    $INSERT_DWF_BIRD_MATERIAL_COST_DD;
"  -v
