#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_spc_mm.sh                               
# 创建时间: 2018年04月17日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度销量售价成本表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_spc_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


###########################################################################################
## 获取XS02表和物料的映射关系
###########################################################################################
## 获取XS02表和物料的映射关系
TMP_DMP_BIRD_SPC_MM_1='TMP_DMP_BIRD_SPC_MM_1'

CREATE_TMP_DMP_BIRD_SPC_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_1(
   org_id                 string,
   bus_type               string,
   product_line           string,
   period_id              string,
   sales_qty              string, --销量
   sales_amt              string, --销售额
   level1_material_id     string,   
   level1_material_descr  string,
   level2_material_id     string,   
   level2_material_descr  string,
   level3_material_id     string,   
   level3_material_descr  string
   
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS orc
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取XS02表对应信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_1 PARTITION(op_month='$OP_MONTH')
   SELECT
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       substr(d1.period_id,1,6),
       SUM(COALESCE(primary_quantity,0)),
       SUM(COALESCE(loc_price,0)*COALESCE(d3.conversion_rate,1)*COALESCE(primary_quantity,0)),
       d2.level1_material_id,
       d2.level1_material_descr,
       d2.level2_material_id,
       d2.level2_material_descr,
       d2.level3_material_id,
       d2.level3_material_descr
   FROM
       (
           SELECT
               *
           FROM
               dwu_xs_other_sale_dd
           WHERE
               op_day='$OP_DAY' and bus_type in('132011','132012')) d1
   INNER JOIN
       mreport_global.dim_material d2
   ON
       d1.material_id =d2.inventory_item_id
   and d2.level5_material_descr LIKE '%雏%'
   and d2.product_recordname='自产'    --这里
  LEFT join 
   (  SELECT
                    from_currency,
                    to_currency,
                    conversion_rate,
                    conversion_period
                FROM
                    mreport_global.dmd_fin_period_currency_rate_mm
                WHERE to_currency='CNY') d3
    on d1.loc_currency_id =d3.from_currency  
   and d3.conversion_period=substr(d1.period_id,1,6) 
   GROUP BY
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       SUBSTR(d1.period_id,1,6),
       d2.level1_material_id,
       d2.level1_material_descr,
       d2.level2_material_id,
       d2.level2_material_descr,
       d2.level3_material_id,
       d2.level3_material_descr"
       
###########################################################################################
## 获取ZQ07表映射关系
TMP_DMP_BIRD_SPC_MM_2='TMP_DMP_BIRD_SPC_MM_2'
CREATE_TMP_DMP_BIRD_SPC_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_2(
   org_id                 string,
   bus_type               string,
   product_line           string,
   period_id              string,
   sales_cost             string, --销售成本
   level1_material_id     string,   
   level2_material_id     string,   
   level3_material_id     string,  
   level3_material_descr  string
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS orc
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取ZQ02物料和CW02表对应信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_2 PARTITION(op_month='$OP_MONTH')
  SELECT
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       d1.period_id,
       sum(coalesce(conversion_rate,1)*(coalesce(loc_cost_amount01,0)+coalesce(loc_cost_amount02,0)+coalesce(loc_cost_amount03,0)+ 
       coalesce(loc_cost_amount04,0)+coalesce(loc_cost_amount05,0)+coalesce(loc_cost_amount06,0)+ 
       coalesce(loc_cost_amount07,0)+coalesce(loc_cost_amount08,0)+coalesce(loc_cost_amount09,0)+ 
       coalesce(loc_cost_amount10,0)+coalesce(loc_cost_amount11,0)+coalesce(loc_cost_amount12,0)+ 
       coalesce(loc_cost_amount13,0)+coalesce(loc_cost_amount14,0)+coalesce(loc_cost_amount15,0)+ 
       coalesce(loc_cost_amount16,0)+coalesce(loc_cost_amount17,0)+coalesce(loc_cost_amount18,0)+ 
       coalesce(loc_cost_amount19,0)+coalesce(loc_cost_amount20,0)+coalesce(loc_cost_amount21,0)+ 
       coalesce(loc_cost_amount22,0)+coalesce(loc_cost_amount23,0)+coalesce(loc_cost_amount24,0)+ 
       coalesce(loc_cost_amount25,0)+coalesce(loc_cost_amount26,0)+coalesce(loc_cost_amount27,0)+ 
       coalesce(loc_cost_amount28,0)+coalesce(loc_cost_amount29,0))),
       d2.level1_material_id,
       d2.level2_material_id,
       d2.level3_material_id,
       d2.level3_material_descr
   FROM
       (
           SELECT
               *
           FROM
               dwu_zq_reality_cost_subject_dd
           WHERE
               op_day='$OP_DAY') d1
   INNER JOIN
       mreport_global.dim_material d2
    ON     d1.material_item_id = d2.item_id and d2.level5_material_descr LIKE '%雏%'
    and   d2.product_recordname='自产'
   LEFT join 
   (  SELECT
                    from_currency,
                    to_currency,
                    conversion_rate,
                    conversion_period
                FROM
                    mreport_global.dmd_fin_period_currency_rate_mm
                WHERE to_currency='CNY') d3
    on d1.loc_currency_id =d3.from_currency  
   and d3.conversion_period=d1.period_id 
   GROUP BY
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       d1.period_id,
       d2.level1_material_id,
       d2.level2_material_id,
       d2.level3_material_id,
       d2.level3_material_descr"

###########################################################################################
## 获取ZQ19表映射关系
TMP_DMP_BIRD_SPC_MM_3='TMP_DMP_BIRD_SPC_MM_3'
CREATE_TMP_DMP_BIRD_SPC_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_3(
   org_id                 string,
   bus_type               string,
   product_line           string,
   period_id              string,
   sales_cost             string, --销售成本
   level1_material_id     string,   
   level2_material_id     string,   
   level3_material_id     string,
   level3_material_descr  string
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS orc
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取获取ZQ19表映射关系>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_3 PARTITION(op_month='$OP_MONTH')
   SELECT
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       substr(d1.period_id,1,6),
       SUM(COALESCE(selling_expense_fixed,0)+COALESCE(selling_expense_change,0) +COALESCE(fin_expense,
       0)+COALESCE(admini_expense,0)), ---期间费用
       d2.level1_material_id,
       d2.level2_material_id,
       d2.level3_material_id,
       d2.level3_material_descr
   FROM
       (
           SELECT
               *
           FROM
               dmd_fin_exps_profits
           WHERE
               1=1
           AND currency_type='3' and bus_type IN('132011','132012')) d1
   INNER JOIN
       mreport_global.dim_material d2
   ON
       d1.inventory_item_id =d2.inventory_item_id
   AND d2.level5_material_descr LIKE '%雏%'
   GROUP BY
       d1.org_id,
       d1.bus_type,
       d1.product_line,
       substr(d1.period_id,1,6),
       d2.level1_material_id,
       d2.level2_material_id,
       d2.level3_material_id,
       d2.level3_material_descr"
          
###########################################################################################
## 获取ZQ09和ZQ08表映射关系
TMP_DMP_BIRD_SPC_MM_4='TMP_DMP_BIRD_SPC_MM_4'
CREATE_TMP_DMP_BIRD_SPC_MM_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_4(
   org_id                 string,
   bus_type               string,
   product_line           string,
   period_id              string,
   sales_cost             string, --成本-本月预算
   qty                    string, --销量-本月预算
   level1_material_id        string,
   level1_material_descr     string,
   level2_material_id        string,
   level2_material_descr     string,
   level3_material_id        string,
   level3_material_descr     string,
   org_code               string
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS orc
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>成本-本月预算>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_4 PARTITION(op_month='$OP_MONTH')
SELECT
       d1.org_id,
       d1.bus_type,
       d2.product_line,
       d1.period_id,
       sum(qty*price), ---预算成本费用
       sum(qty),
       d3.level1_material_id,     
       d3.level1_material_descr,  
       d3.level2_material_id,     
       d3.level2_material_descr,  
       d3.level3_material_id,     
       d3.level3_material_descr,  
       d1.org_code
   FROM
       (
             SELECT
             product_item_code,
             org_id,
             bus_type,
             regexp_replace(period_name,'-','') period_id,
             SUM(plan_cubs) qty,
             org_code
         FROM
             DWU_ZQ_ZQ08_DD
         WHERE
             op_day='$OP_DAY'
         GROUP BY
             product_item_code,
             org_id,
             bus_type,
             period_name,
             org_code             
           ) d1
   INNER JOIN (
        SELECT
             item_code,
             org_id,
             bus_type,
             regexp_replace(period_id,'-','') period_id,
         product_line,
             SUM((coalesce(dep_cub_cost,0)+coalesce(dep_feed_cost,0)+coalesce(dep_drugs_cost,0)
             +coalesce(dep_direct_hr_cost,0)+coalesce(dep_utiltity_bill,0)+coalesce(dep_loan_fee,0)
             +coalesce(dep_base_feed_fee,0)++coalesce(dep_energy_cost,0)
             +coalesce(dep_indirect_hr_cost,0)+coalesce(dep_other,0)+coalesce(born_feed_cost,0)
             +coalesce(born_drugs_cost,0)+coalesce(born_direct_hr_cost,0)+coalesce(born_utiltity_bill,0)
             +coalesce(born_loan_fee,0)+coalesce(born_base_feed_fee,0)+coalesce(born_energy_cost,0)
             +coalesce(born_indirect_hr_cost,0)+coalesce(born_other,0)+coalesce(born_sub_product_cost,0)
             +coalesce(borning_drugs_cost,0)+coalesce(borning_direct_hr_cost,0)+coalesce(borning_utiltity_bill,0)
             +coalesce(borning_loan_fee,0)+coalesce(borning_base_feed_fee,0)+coalesce(borning_energy_cost,0)
             +coalesce(borning_indirect_hr_cost,0)+coalesce(borning_other,0)+coalesce(borning_packing,0)
             +coalesce(borning_sub_product_cost,0))*coalesce(rate_b,1)) price
         FROM
             DWU_ZQ_ZQ09_DD
         WHERE
             op_day='$OP_DAY'
         GROUP BY
             item_code,
             org_id,
             bus_type,
             period_id,
         product_line
   )d2
   on d1.org_id=d2.org_id and d1.bus_type=d2.bus_type 
     and d1.period_id=d2.period_id and d1.product_item_code=d2.item_code 
     and d1.bus_type IN('132011','132012')
   INNER JOIN
       mreport_global.dim_material d3
      ON d2.item_code =d3.item_id and d3.level5_material_descr LIKE '%雏%'
      and  d3.product_recordname='自产'
   GROUP BY
       d1.org_id,
       d1.bus_type,
       d2.product_line,
       d1.period_id,
       d3.level1_material_id,     
       d3.level1_material_descr,  
       d3.level2_material_id,     
       d3.level2_material_descr,  
       d3.level3_material_id,     
       d3.level3_material_descr, 
       d1.org_code
       ;
   "        
   
###########################################################################################
## 获取预测和实际数据的映射关系
##关联zq15表，取出数量和金额

TMP_DMP_BIRD_SPC_MM_10='TMP_DMP_BIRD_SPC_MM_10'

CREATE_TMP_DMP_BIRD_SPC_MM_10="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_10(
     org_id                    string,     --ou_id
     bus_type                  string,     --业态
     product_line              string,     --产线
     period_id                 string,     --间隔
     kpi_type_id               string,     --数据类别
     kpi_type_descr            string,     --数据类别描述
     sales_qty                 string,     --销售数量
     sales_amt                 string,     --销售金额
     sales_cost                string,     --销售成本
     level1_material_id        string,
     level1_material_descr     string,
     level2_material_id        string,
     level2_material_descr     string,
     level3_material_id        string,
     level3_material_descr     string
)                                         
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"
 
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>与zq15并，做临时表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_10="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_10 PARTITION(op_month='$OP_MONTH')
 SELECT
      coalesce(d1.org_id,m4.org_id,'') org_id ,
      coalesce(d1.bus_type,m4.bus_type,'') bus_type,
      coalesce(d1.product_line,m4.product_line,'') product_line,
      coalesce(d1.period_id,m4.period_id,'') period_id,
      '1'                           kpi_type_id,
      '正常计算指标'                kpi_type_descr,
      nvl(d1.sales_qty,0)   sales_qty, --销量(只)
      nvl(d1.sales_amt,0)  sales_amt , --销售金额
      nvl(m4.seed_cost,0)+nvl(m4.packing_amount,0)+nvl(m4.direct_manual_amount,0)+nvl(m4.drugs_amount,0)+
      nvl(m4.depreciation_amount,0)+nvl(m4.expend_amount,0)+nvl(m4.water_electry_amount,0)+nvl(m4.indirect_amount,0)
      +nvl(m4.other_amount,0)+nvl(m4.byproduct_amount,0)+nvl(m4.manage_amount,0)+nvl(m4.sale_amount,0)
      +nvl(m4.financial_amount,0))   sales_cost,  --销售成本
      coalesce(d1.level1_material_id,'')level1_material_id ,
      coalesce(d1.level1_material_descr,'') level1_material_descr,
      coalesce(d1.level2_material_id,'') level2_material_id,
      coalesce(d1.level2_material_descr,'') level2_material_descr,
      coalesce(d1.level3_material_id,'') level3_material_id,
      coalesce(d1.level3_material_descr,m4.level3_material_descr,'') level3_material_descr
  FROM
      (select * from TMP_DMP_BIRD_SPC_MM_1 where op_month='$OP_MONTH') d1
 full join (
select
     period_id
    ,org_id
    ,org_name
    ,currency_id
    ,bus_type
    ,product_line
    ,category_id
    ,split(category_desc,'\\\\.')[2] level3_material_descr
    ,cost_type_id
    ,cost_type_desc
    ,seed_quantity
    ,seed_cost
    ,packing_amount
    ,direct_manual_amount
    ,drugs_amount
    ,depreciation_amount
    ,expend_amount
    ,water_electry_amount
    ,indirect_amount
    ,other_amount
    ,byproduct_amount
    ,manage_amount
    ,sale_amount
    ,financial_amount
 from DWU_ZQ_MANAGE_PRODUCT_COST_DD t1 where t1.op_day = '$OP_DAY' and cost_type_desc = '本月'
 ) m4
         on d1.period_id = m4.period_id
        and d1.org_id = m4.org_id
        and d1.level3_material_descr =m4.level3_material_descr
        and d1.product_line = m4.product_line
 ;
"
###########################################################################################
## 获取预测和实际数据的映射关系

TMP_DMP_BIRD_SPC_MM_5='TMP_DMP_BIRD_SPC_MM_5'

CREATE_TMP_DMP_BIRD_SPC_MM_5="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SPC_MM_5(
     org_id                    string,     --ou_id
     bus_type                  string,     --业态
     product_line              string,     --产线
     period_id                 string,     --间隔
     kpi_type_id               string,     --数据类别
     kpi_type_descr            string,     --数据类别描述
     sales_qty                 string,     --销售数量
     sales_amt                 string,     --销售金额
     sales_cost                string,     --销售成本
     level1_material_id        string,
     level1_material_descr     string,
     level2_material_id        string,
     level2_material_descr     string,
     level3_material_id        string,
     level3_material_descr     string
)                                         
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
;
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>合并实际和预算数据>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SPC_MM_5="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SPC_MM_5 PARTITION(op_month='$OP_MONTH')
  SELECT
      coalesce(d1.org_id,'') org_id ,
      coalesce(d1.bus_type,'') bus_type,
      coalesce(d1.product_line,'') product_line,
      coalesce(d1.period_id,'') period_id,
      '1'                           kpi_type_id,
      '正常计算指标'                kpi_type_descr,
      nvl(d1.sales_qty,0)   sales_qty, --销量(只)
      nvl(d1.sales_amt,0)  sales_amt , --销售金额
      (nvl(d2.sales_cost,0)+nvl(d3.sales_cost,0)+nvl(d1.sales_cost,0))   sales_cost,  --销售成本
      coalesce(d1.level1_material_id,'')level1_material_id ,
      coalesce(d1.level1_material_descr,'') level1_material_descr,
      coalesce(d1.level2_material_id,'') level2_material_id,
      coalesce(d1.level2_material_descr,'') level2_material_descr,
      coalesce(d1.level3_material_id,'') level3_material_id,
      coalesce(d1.level3_material_descr,'') level3_material_descr
  FROM
      (select * from INSERT_TMP_DMP_BIRD_SPC_MM_10 where op_month='$OP_MONTH') d1
  left JOIN
      (select * from TMP_DMP_BIRD_SPC_MM_2 where op_month='$OP_MONTH') d2
  on d1.org_id =d2.org_id 
     and d1.bus_type=d2.bus_type     
     and d1.product_line=d2.product_line
     and d1.period_id=d2.period_id    
  left JOIN
      (select * from TMP_DMP_BIRD_SPC_MM_3 where op_month='$OP_MONTH') d3
  on d3.org_id =d2.org_id 
     and d3.bus_type=d2.bus_type     
     and d3.product_line=d2.product_line
     and d3.period_id=d2.period_id    
  where (d1.level3_material_descr = d2.level3_material_descr 
  or d1.level3_material_descr = d3.level3_material_descr)
UNION ALL
 SELECT
      coalesce(d3.org_id,m4.org_id,'') org_id ,
      coalesce(d3.bus_type,m4.bus_type,'') bus_type,
      coalesce(d3.product_line,m4.product_line,'') product_line,
      coalesce(d3.period_id,m4.period_id,'') period_id,
      '0'                         kpi_type_id,
      '预算指标'                  kpi_type_descr,
      nvl(d2.qty,0) sales_qty, --销售预算
       0               sales_amt,
      (coalesce(d1.cost,0)+d3.sales_cost) sales_cost , --销售成本
       coalesce(d3.level1_material_id,'')level1_material_id ,
       coalesce(d3.level1_material_descr,'') level1_material_descr,
       coalesce(d3.level2_material_id,'') level2_material_id,
       coalesce(d3.level2_material_descr,'') level2_material_descr,
       coalesce(d3.level3_material_id,'') level3_material_id,
       coalesce(d3.level3_material_descr,'') level3_material_descr
FROM  
(
select 
      coalesce(d2.org_id,m4.org_id,'') org_id ,
      coalesce(d2.bus_type,m4.bus_type,'') bus_type,
      coalesce(d2.product_line,m4.product_line,'') product_line,
      coalesce(d2.period_id,m4.period_id,'') period_id,
      '0'                         kpi_type_id,
      '预算指标'                  kpi_type_descr,
      nvl(d2.qty,0) sales_qty, --销售预算
       0               sales_amt,
      (nvl(d2.sales_cost,0)+nvl(m4.seed_cost,0)+nvl(m4.packing_amount,0)+nvl(m4.direct_manual_amount,0)+nvl(m4.drugs_amount,0)+
         nvl(m4.depreciation_amount,0)+nvl(m4.expend_amount,0)+nvl(m4.water_electry_amount,0)+nvl(m4.indirect_amount,0)
         +nvl(m4.other_amount,0)+nvl(m4.byproduct_amount,0)+nvl(m4.manage_amount,0)+nvl(m4.sale_amount,0)
         +nvl(m4.financial_amount,0)) sales_cost , --销售成本
      coalesce(d2.level1_material_id,'')level1_material_id ,
      coalesce(d2.level1_material_descr,'') level1_material_descr,
      coalesce(d2.level2_material_id,'') level2_material_id,
      coalesce(d2.level2_material_descr,'') level2_material_descr,
      coalesce(d2.level3_material_id,'') level3_material_id,
      coalesce(d2.level3_material_descr,m4.level3_material_descr,'') level3_material_descr
 from (
      (
        SELECT
            *
        FROM
            TMP_DMP_BIRD_SPC_MM_4
        WHERE
            op_month='$OP_MONTH') d2
 full join (
select
     period_id
    ,org_id
    ,org_name
    ,currency_id
    ,bus_type
    ,product_line
    ,category_id
    ,split(category_desc,'\\\\\.')[2] level3_material_descr
    ,cost_type_id
    ,cost_type_desc
    ,seed_quantity
    ,seed_cost
    ,packing_amount
    ,direct_manual_amount
    ,drugs_amount
    ,depreciation_amount
    ,expend_amount
    ,water_electry_amount
    ,indirect_amount
    ,other_amount
    ,byproduct_amount
    ,manage_amount
    ,sale_amount
    ,financial_amount
 from DWU_ZQ_MANAGE_PRODUCT_COST_DD t1 where t1.op_day = '$OP_DAY' and cost_type_desc = '本月'
 ) m4
         on d2.period_id = m4.period_id
        and d2.org_id = m4.org_id
        and d2.level3_material_descr =m4.level3_material_descr
        and d2.product_line = m4.product_line
) d3
LEFT JOIN 
  (
        SELECT
            SUM(fee_j_amount) cost,
            short_code,
            '10'                               product_line,
            regexp_replace(period_code,'-','') period_id
        FROM
            DWU_ZQ_ZQ13_DD
        WHERE
            op_day='$OP_DAY'
        AND SUBSTR(fee_segment,1,4) IN(6601,6602,6603)
        GROUP BY
            short_code,
            period_code
        UNION ALL
        SELECT
            SUM(fee_y_amount) cost,
            short_code,
            '20'                               product_line,
            regexp_replace(period_code,'-','') period_id
        FROM
            DWU_ZQ_ZQ13_DD
        WHERE
            op_day='$OP_DAY'
        AND SUBSTR(fee_segment,1,4) IN(6601,6602,6603)
        GROUP BY
            short_code,
            period_code )d1
ON
    d1.product_line =d3.product_line
AND d1.short_code=d3.org_code
AND d1.period_id=d3.period_id 
;
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_SPC_MM='DMP_BIRD_SPC_MM'

CREATE_DMP_BIRD_SPC_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SPC_MM(
   month_id                    string    --期间(月)
  ,day_id                      string    --期间(日)
  ,level1_org_id               string    --组织1级(股份)  
  ,level1_org_descr            string    --组织1级(股份)  
  ,level2_org_id               string    --组织2级(片联)  
  ,level2_org_descr            string    --组织2级(片联)  
  ,level3_org_id               string    --组织3级(片区)  
  ,level3_org_descr            string    --组织3级(片区)  
  ,level4_org_id               string    --组织4级(小片)  
  ,level4_org_descr            string    --组织4级(小片)  
  ,level5_org_id               string    --组织5级(公司)  
  ,level5_org_descr            string    --组织5级(公司)  
  ,level6_org_id               string    --组织6级(OU)  
  ,level6_org_descr            string    --组织6级(OU)  
  ,level7_org_id               string    --组织7级(库存组织)
  ,level7_org_descr            string    --组织7级(库存组织)
  ,level1_businesstype_id      string    --业态1级
  ,level1_businesstype_name    string    --业态1级
  ,level2_businesstype_id      string    --业态2级
  ,level2_businesstype_name    string    --业态2级
  ,level3_businesstype_id      string    --业态3级
  ,level3_businesstype_name    string    --业态3级
  ,level4_businesstype_id      string    --业态4级
  ,level4_businesstype_name    string    --业态4级
  ,production_line_id          string    --产线ID
  ,production_line_descr       string    --产线名称
  ,kpi_type_id                 string    --0-预算指标  1-正常计算指标
  ,kpi_type_descr              string    --预算指标，正常计算指标
  ,level1_material_id          string    --物料1级id
  ,level1_material_descr       string    --物料1级描述
  ,level2_material_id          string    --物料2级id
  ,level2_material_descr       string    --物料2级描述
  ,level3_material_id          string    --物料3级id
  ,level3_material_descr       string    --物料3级描述
  ,level4_material_id          string    --物料4级id
  ,level4_material_descr       string    --物料4级描述
  ,sales_qty                   string   --销售数量(元)
  ,sales_amt                   string   --销售金额(元)
  ,sales_cost                  string   --销售成本(元),
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SPC_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_SPC_MM PARTITION(op_month='$OP_MONTH')
SELECT
        t1.period_id  as   month_id             --month_id
       ,''    as  day_id  
       ,case when t6.level1_org_id    is null then coalesce(t7.level1_org_id,'-1')      else coalesce(t6.level1_org_id,'-1')       end as level1_org_id      --一级组织编码
       ,case when t6.level1_org_descr is null then coalesce(t7.level1_org_descr,'缺失') else coalesce(t6.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
       ,case when t6.level2_org_id    is null then coalesce(t7.level2_org_id,'-1')      else coalesce(t6.level2_org_id,'-1')       end as level2_org_id      --二级组织编码
       ,case when t6.level2_org_descr is null then coalesce(t7.level2_org_descr,'缺失') else coalesce(t6.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
       ,case when t6.level3_org_id    is null then coalesce(t7.level3_org_id,'-1')      else coalesce(t6.level3_org_id,'-1')       end as level3_org_id      --三级组织编码
       ,case when t6.level3_org_descr is null then coalesce(t7.level3_org_descr,'缺失') else coalesce(t6.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
       ,case when t6.level4_org_id    is null then coalesce(t7.level4_org_id,'-1')      else coalesce(t6.level4_org_id,'-1')       end as level4_org_id      --四级组织编码
       ,case when t6.level4_org_descr is null then coalesce(t7.level4_org_descr,'缺失') else coalesce(t6.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
       ,case when t6.level5_org_id    is null then coalesce(t7.level5_org_id,'-1')      else coalesce(t6.level5_org_id,'-1')       end as level5_org_id      --五级组织编码
       ,case when t6.level5_org_descr is null then coalesce(t7.level5_org_descr,'缺失') else coalesce(t6.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
       ,case when t6.level6_org_id    is null then coalesce(t7.level6_org_id,'-1')      else coalesce(t6.level6_org_id,'-1')       end as level6_org_id      --六级组织编码
       ,case when t6.level6_org_descr is null then coalesce(t7.level6_org_descr,'缺失') else coalesce(t6.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
       ,'' as level7_org_id                   --组织7级
       ,'' as level7_org_descr                --组织7级
       ,t5.level1_businesstype_id             --业态1级
       ,t5.level1_businesstype_name           --业态1级
       ,t5.level2_businesstype_id             --业态2级
       ,t5.level2_businesstype_name           --业态2级
       ,t5.level3_businesstype_id             --业态3级
       ,t5.level3_businesstype_name           --业态3级
       ,t5.level4_businesstype_id             --业态4级
       ,t5.level4_businesstype_name           --业态4级
       ,case when t1.product_line='10' then '1'
             when t1.product_line='20' then '2'
             else '-1' end              as product_line                    --产线ID
       ,case when t1.product_line='10' then '鸡线'
             when t1.product_line='20' then '鸭线' 
             else '缺省' end as production_line_descr           --产线名称
       ,t1.kpi_type_id                         --0-预算指标  1-正常计算指标
       ,t1.kpi_type_descr  
       ,t1.level1_material_id                 --物料1级id
       ,t1.level1_material_descr              --物料1级描述
       ,t1.level2_material_id                 --物料2级id
       ,t1.level2_material_descr              --物料2级描述
       ,t1.level3_material_id                 --物料3级id
       ,t1.level3_material_descr              --物料3级描述
       ,'' level4_material_id                 --物料4级id
       ,'' level4_material_descr              --物料4级描述
       ,t1.sales_qty                          --销售数量
       ,t1.sales_amt                          --销售金额
       ,t1.sales_cost                         --销售成本
       ,'$CREATE_TIME' create_time              --创建日期
 from  
 (
  select * from TMP_DMP_BIRD_SPC_MM_5 where op_month='$OP_MONTH'
 )t1
left join mreport_global.dim_org_management t6 
     on t1.org_id=t6.org_id  
     and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
     on t1.org_id=t7.org_id 
     and t1.bus_type=t7.bus_type_id 
     and t7.attribute5='2'
LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_businesstype
        WHERE  level4_businesstype_name IS NOT NULL) t5
     ON  (t1.bus_type=t5.level4_businesstype_id)

"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_SPC_MM_1;
    $INSERT_TMP_DMP_BIRD_SPC_MM_1;
    $CREATE_TMP_DMP_BIRD_SPC_MM_2;
    $INSERT_TMP_DMP_BIRD_SPC_MM_2;
    $CREATE_TMP_DMP_BIRD_SPC_MM_3;
    $INSERT_TMP_DMP_BIRD_SPC_MM_3;
    $CREATE_TMP_DMP_BIRD_SPC_MM_4;
    $INSERT_TMP_DMP_BIRD_SPC_MM_4;
    $CREATE_TMP_DMP_BIRD_SPC_MM_10;
    $INSERT_TMP_DMP_BIRD_SPC_MM_10;
    $CREATE_TMP_DMP_BIRD_SPC_MM_5;
    $INSERT_TMP_DMP_BIRD_SPC_MM_5;
    $CREATE_DMP_BIRD_SPC_MM;
    $INSERT_DMP_BIRD_SPC_MM;
"  -v