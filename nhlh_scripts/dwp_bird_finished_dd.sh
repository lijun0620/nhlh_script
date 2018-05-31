#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_finished_dd.sh                               
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
    echo "输入参数错误，调用示例: dwp_bird_finished_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_FINISHED_DD_1='TMP_DWP_BIRD_FINISHED_DD_1'

CREATE_TMP_DWP_BIRD_FINISHED_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FINISHED_DD_1(
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
  ,level1_org_id                string      --组织1级
  ,level1_org_descr             string      --组织1级
  ,level2_org_id                string      --组织2级
  ,level2_org_descr             string      --组织2级
  ,level3_org_id                string      --组织3级
  ,level3_org_descr             string      --组织3级
  ,level4_org_id                string      --组织4级
  ,level4_org_descr             string      --组织4级
  ,level5_org_id                string      --组织5级
  ,level5_org_descr             string      --组织5级
  ,level6_org_id                string      --组织6级
  ,level6_org_descr             string      --组织6级
  ,level7_org_id                string      --组织7级
  ,level7_org_descr             string      --组织7级
  ,level1_businesstype_id       string      --业态1级
  ,level1_businesstype_name     string      --业态1级
  ,level2_businesstype_id       string      --业态2级
  ,level2_businesstype_name     string      --业态2级
  ,level3_businesstype_id       string      --业态3级
  ,level3_businesstype_name     string      --业态3级
  ,level4_businesstype_id       string      --业态4级
  ,level4_businesstype_name     string      --业态4级
  ,contract_qty                 string      --投放数量
  ,material_weight_qty          string      --物料重量(kg)
  ,drugs_cost                   string      --兽药费用
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FINISHED_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FINISHED_DD_1 PARTITION(op_day='$OP_DAY')
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
       ,t4.level1_org_id                 --组织1级
       ,t4.level1_org_descr              --组织1级
       ,t4.level2_org_id                 --组织2级
       ,t4.level2_org_descr              --组织2级
       ,t4.level3_org_id                 --组织3级
       ,t4.level3_org_descr              --组织3级
       ,t4.level4_org_id                 --组织4级
       ,t4.level4_org_descr              --组织4级
       ,t4.level5_org_id                 --组织5级
       ,t4.level5_org_descr              --组织5级
       ,t4.level6_org_id                 --组织6级
       ,t4.level6_org_descr              --组织6级
       ,t5.level7_org_id                 --组织7级
       ,t5.level7_org_descr              --组织7级              
       ,t6.level1_businesstype_id        --业态1级
       ,t6.level1_businesstype_name      --业态1级
       ,t6.level2_businesstype_id        --业态2级
       ,t6.level2_businesstype_name      --业态2级
       ,t6.level3_businesstype_id        --业态3级
       ,t6.level3_businesstype_name      --业态3级
       ,t6.level4_businesstype_id        --业态4级
       ,t6.level4_businesstype_name      --业态4级
       ,t1.contract_qty                  --投放数量
       ,case when t1.breed_type_descr='放养' then t2.material_weight_qty
             when t1.breed_type_descr='代养' then t7.material_weight
        else 0 end material_weight_qty   --物料重量(kg)
       --,case when t1.breed_type_descr='放养' and t8.description in('合作社','冷藏厂') then t2.material_weight_qty
       --      when t1.breed_type_descr='放养' and t8.description in('饲料厂') then t9.material_weight
       --      when t1.breed_type_descr='代养' then t7.material_weight
       -- else '0' end material_weight_qty --物料重量(kg)
       ,0 drugs_cost                    --兽药费用
  FROM (SELECT contractnumber contract_no                 --合同号
               ,meaning production_line_id                --产线代码
               ,meaning_desc production_line_descr        --产线描述
               ,substr(contract_date,1,10) contract_date  --合同日期
               ,hatchery_name m_factory_descr             --孵化场
               ,org_id                                    --OU组织  
               ,inv_org_id                                --库存组织ID
               ,bus_type                                  --业态
               ,category_desc                             --合同分组类型
               ,qty contract_qty                          --合同数量
               ,guarantees_market recycle_type_descr      --保值保底市场
               ,vendor_code farmer_id                     --养户号
               ,vendor_name farmer_name                   --养殖单位
               ,contracttype_grp breed_type_descr         --合同类型分组
               ,mediamount*qty drugs_cost                 --兽药费用
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT contract_no
                    ,sum(material_weight_qty) material_weight_qty
               FROM (SELECT cust_po_number contract_no             --合同号
                            ,primary_quantity material_weight_qty --物料重量
                       FROM dwu_qw_buy_list_qw04_dd
                      WHERE op_day='$OP_DAY'
                     UNION ALL
                     SELECT b1.contract_no
                            ,b1.material_weight_qty --物料重量
                       FROM (SELECT b11.cust_po_num contract_no
                                    ,b11.ebs_sale_order_no order_no
                                    ,b11.out_quantity material_weight_qty
                               FROM dwu_xs_other_sale_dd b11
                              INNER JOIN (SELECT inventory_item_id item_id
                                            FROM mreport_global.dim_material
                                           WHERE level2_material_id='1503'
                                           GROUP BY inventory_item_id) b12
                                 ON (b11.material_id=b12.item_id)
                              WHERE b11.op_day='$OP_DAY') b1
                       LEFT JOIN (SELECT cust_po_number contract_no
                                         ,order_number order_no
                                    FROM dwu_qw_buy_list_qw04_dd
                                   WHERE op_day='$OP_DAY') b2
                         ON (b1.contract_no=b2.contract_no
                         AND b1.order_no=b2.order_no)
                      WHERE b2.contract_no is null) a
              GROUP BY contract_no) t2
    ON (t1.contract_no=t2.contract_no)
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
              WHERE org_id is not null
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
                    ,org_id) t4
    ON (t1.org_id=t4.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t5
    ON (t1.inv_org_id=t5.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t6
    ON (t1.bus_type=t6.level4_businesstype_id)
  LEFT JOIN (SELECT a1.contract_no
                    ,sum(a1.quantity) material_weight
               FROM (SELECT breeding_contract contract_no
                            ,inventory_item_id                 --物料编码
                            ,quantity                          --物料重量
                       FROM dwu_qw_breed_cost_dd               --qw02
                      WHERE op_day='$OP_DAY'
                        AND breeding_contract is not null) a1
              INNER JOIN (SELECT inventory_item_id
                            FROM mreport_global.dim_material
                           WHERE level1_material_id='15'
                           GROUP BY inventory_item_id) a2
                 ON (a1.inventory_item_id=a2.inventory_item_id)
              GROUP BY a1.contract_no) t7
    ON (t1.contract_no=t7.contract_no)
  --LEFT JOIN (SELECT lookup_code,
  --                  meaning,
  --                  description,
  --                  tag
  --             FROM mreport_global.ods_ebs_fnd_lookup_values
  --            WHERE lookup_type='CUXBWP_TYPE_TRANSFER'
  --              AND language='ZHS') t8
  --  ON (t1.category_desc=t8.meaning)
  --LEFT JOIN (SELECT cust_po_num contract_no                --合同号
  --                  ,sum(primary_quantity) material_weight --物料重量
  --             FROM dwu_xs_other_sale_dd a1
  --            INNER JOIN (SELECT inventory_item_id item_id
  --                          FROM mreport_global.dim_material
  --                         WHERE level2_material_id='1503'
  --                         GROUP BY inventory_item_id) a2
  --               ON (a1.material_id=a2.item_id)
  --            WHERE a1.op_day='$OP_DAY'
  --            GROUP BY a1.cust_po_num) t9
  --  ON (t1.contract_no=t9.contract_no)
  WHERE t4.level2_org_id not in('1015')
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_FINISHED_DD_2='TMP_DWP_BIRD_FINISHED_DD_2'

CREATE_TMP_DWP_BIRD_FINISHED_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FINISHED_DD_2(
  contract_no                       string      --合同号
  ,farmer_id                        string      --养户号
  ,farmer_name                      string      --养殖单位
  ,order_line_no                    string      --回收订单行号
  ,production_line_id               string      --产线代码
  ,production_line_descr            string      --产线描述
  ,contract_date                    string      --合同日期
  ,recycle_date                     string      --回收日期
  ,m_factory_descr                  string      --苗场
  ,recycle_type_id                  string      --回收类型
  ,recycle_type_descr               string      --回收类型
  ,breed_type_id                    string      --养殖类型
  ,breed_type_descr                 string      --养殖类型
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
  ,material_weight_qty              string      --物料重量(kg)
  ,recycle_qty                      string      --回收数量(支)
  ,recycle_weight                   string      --回收重量(kg)
  ,recycle_amt                      string      --回收金额(元)
  ,drugs_cost                       string      --兽药费用
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FINISHED_DD_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FINISHED_DD_2 PARTITION(op_day='$OP_DAY')
SELECT t2.contract_no                         --合同号
       ,t2.farmer_id                          --养户号
       ,t2.farmer_name                        --养殖单位
       ,null order_line_no                    --回收订单行号
       ,t2.production_line_id                 --产线代码
       ,t2.production_line_descr              --产线描述
       ,t2.contract_date                      --合同日期
       ,t1.recycle_date                       --回收日期
       ,t2.m_factory_descr                    --苗场
       ,t2.recycle_type_id                    --回收类型
       ,t2.recycle_type_descr                 --回收类型
       ,t2.breed_type_id                      --养殖类型
       ,t2.breed_type_descr                   --养殖类型
       ,t2.level1_org_id                      --组织1级
       ,t2.level1_org_descr                   --组织1级
       ,t2.level2_org_id                      --组织2级
       ,t2.level2_org_descr                   --组织2级
       ,t2.level3_org_id                      --组织3级
       ,t2.level3_org_descr                   --组织3级
       ,t2.level4_org_id                      --组织4级
       ,t2.level4_org_descr                   --组织4级
       ,t2.level5_org_id                      --组织5级
       ,t2.level5_org_descr                   --组织5级
       ,t2.level6_org_id                      --组织6级
       ,t2.level6_org_descr                   --组织6级
       ,t2.level7_org_id                      --组织7级
       ,t2.level7_org_descr                   --组织7级
       ,t2.level1_businesstype_id             --业态1级
       ,t2.level1_businesstype_name           --业态1级
       ,t2.level2_businesstype_id             --业态2级
       ,t2.level2_businesstype_name           --业态2级
       ,t2.level3_businesstype_id             --业态3级
       ,t2.level3_businesstype_name           --业态3级
       ,t2.level4_businesstype_id             --业态4级
       ,t2.level4_businesstype_name           --业态4级
       ,t1.material_id                        --物料编码
       ,t1.material_descr                     --物料名称
       ,t1.cancel_flag                        --结算标志(CLOSED-关闭(已结算), OPEN-打开)
       ,t2.contract_qty                       --投放数量
       ,t2.material_weight_qty                --物料重量(kg)
       ,t1.recycle_qty                        --回收数量(支)
       ,t1.recycle_weight                     --回收重量(kg)
       ,t1.recycle_amt                        --回收价格(总额)
       ,t2.drugs_cost                         --兽药费用
  FROM (SELECT contract_no                --合同号
               ,material_code material_id --物料编码
               ,material_description material_descr      --物料描述
               ,period_id recycle_date                   --承诺日期
               ,cancel_flag                              --结算标志(CLOSED-关闭, OPEN-打开)
               ,sum(secondary_qty) recycle_qty           --辅助数量
               ,sum(quantity_received) recycle_weight    --已接收数量               
               ,sum(price_with_tax*quantity_received) recycle_amt   --回收金额(含税)
          FROM dwu_cg_buy_list_cg01_dd
         WHERE op_day='$OP_DAY'
           AND material_code in('3501000002','3502000002')
           AND release_num like 'BWP%'
           AND cancel_flag in('OPEN','CLOSED')
         GROUP BY contract_no
               ,material_code
               ,material_description
               ,period_id
               ,cancel_flag
        UNION ALL
        SELECT a1.contract_no
               ,a1.material_id
               ,a1.material_descr
               ,a1.recycle_date
               ,a1.cancel_flag
               ,sum(nvl(a1.recycle_qty,0)) recycle_qty
               ,sum(nvl(a1.recycle_weight,0)) recycle_weight
               ,sum(nvl(a1.recycle_amt,0)) recycle_amt
          FROM (SELECT pith_no contract_no
                       ,cacu_doc_no order_line_no
                       ,item_code material_id  --物料编码
                       ,case when item_code='3501000002' then '合同鸡'
                             when item_code='3502000002' then '合同鸭'
                        else null end material_descr
                       ,regexp_replace(substr(js_date,1,10),'-','') recycle_date
                       ,null cancel_flag
                       ,killed_qty recycle_qty
                       ,buy_weight recycle_weight
                       ,amount recycle_amt     --计价金额(元)
                       ,cacu_doc_no doc_no     --结算单号
                  FROM dwu_qw_qw11_dd
                 WHERE op_day='$OP_DAY'
                   AND pith_no is not null
                   AND item_code in('3501000002','3502000002')
                   AND doc_status in('已完毕','已审核')) a1
                  LEFT JOIN (SELECT contract_no
                                    ,regexp_replace(release_num,'BWP','') doc_no 
                               FROM dwu_cg_buy_list_cg01_dd
                              WHERE op_day='$OP_DAY'
                                AND material_code in('3501000002','3502000002')
                                AND release_num like 'BWP%'
                                AND cancel_flag in('OPEN','CLOSED')) a2
                    ON (a1.contract_no=a2.contract_no
                    AND a1.doc_no=a2.doc_no)
         WHERE a2.contract_no is null
        GROUP BY a1.contract_no
               ,a1.material_id
               ,a1.material_descr
               ,a1.recycle_date
               ,a1.cancel_flag) t1
  LEFT JOIN (SELECT *
               FROM $TMP_DWP_BIRD_FINISHED_DD_1
              WHERE op_day='$OP_DAY') t2
    ON (t1.contract_no=t2.contract_no)
 WHERE t2.contract_no is not null
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWP_BIRD_FINISHED_DD='DWP_BIRD_FINISHED_DD'

CREATE_DWP_BIRD_FINISHED_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_FINISHED_DD(
  contract_no                       string      --合同号
  ,farmer_id                        string      --养户号
  ,farmer_name                      string      --养殖单位
  ,order_line_no                    string      --回收订单行号
  ,production_line_id               string      --产线代码
  ,production_line_descr            string      --产线描述
  ,contract_date                    string      --合同日期
  ,recycle_date                     string      --回收日期
  ,m_factory_descr                  string      --苗场
  ,recycle_type_id                  string      --回收类型
  ,recycle_type_descr               string      --回收类型
  ,breed_type_id                    string      --养殖类型
  ,breed_type_descr                 string      --养殖类型
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
  ,material_weight_qty              string      --物料重量(kg)
  ,recycle_qty                      string      --回收数量(支)
  ,recycle_weight                   string      --回收重量(kg)
  ,recycle_amt                      string      --回收单价
  ,drugs_cost                       string      --兽药费用(总额)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_FINISHED_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_FINISHED_DD PARTITION(op_day='$OP_DAY')
SELECT contract_no                             --合同号
       ,farmer_id                              --养户号
       ,farmer_name                            --养殖单位
       ,order_line_no                          --回收订单行号
       ,production_line_id                     --产线代码
       ,production_line_descr                  --产线描述
       ,contract_date                          --合同日期
       ,recycle_date                           --回收日期
       ,m_factory_descr                        --苗场
       ,recycle_type_id                        --回收类型
       ,recycle_type_descr                     --回收类型
       ,breed_type_id                          --养殖类型
       ,breed_type_descr                       --养殖类型
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
       ,cancel_flag                            --结算标志(CLOSED-关闭(已结算), OPEN-打开)
       ,coalesce(contract_qty,'0')             --投放数量
       ,coalesce(material_weight_qty,'0')      --物料重量(kg)
       ,coalesce(recycle_qty,'0')              --回收数量(支)
       ,coalesce(recycle_weight,'0')           --回收重量(kg)
       ,coalesce(recycle_amt,'0')              --回收金额(元)
       ,coalesce(drugs_cost,'0')               --兽药费用(总额)
  FROM (SELECT *
          FROM $TMP_DWP_BIRD_FINISHED_DD_2
         WHERE op_day='$OP_DAY'
           AND level2_org_id not in('1015')) t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWP_BIRD_FINISHED_DD_1;
    $INSERT_TMP_DWP_BIRD_FINISHED_DD_1;
    $CREATE_TMP_DWP_BIRD_FINISHED_DD_2;
    $INSERT_TMP_DWP_BIRD_FINISHED_DD_2;
    $CREATE_DWP_BIRD_FINISHED_DD;
    $INSERT_DWP_BIRD_FINISHED_DD;
"  -v