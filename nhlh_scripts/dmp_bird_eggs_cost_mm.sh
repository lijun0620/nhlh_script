#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_eggs_cost_mm.sh                               
# 创建时间: 2018年04月20日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 种蛋生产成本
# 修改说明:     3504001004   鸭养殖合格蛋  3503001004   鸡养殖合格蛋                                                    
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_eggs_cost_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 获取CW02表和物料的映射关系
DMP_BIRD_EGGS_COST_MM_1='DMP_BIRD_EGGS_COST_MM_1'

CREATE_DMP_BIRD_EGGS_COST_MM_1="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_EGGS_COST_MM_1(
   org_id                 string,
   bus_type               string,
   product_line           string,
   period_id              string,
   item_code              string,
   qty                    string,      --产蛋期产合格蛋
   total_qty              string,       --合格蛋数量
   actual_ebs_qualified   string,  
   cost_amount11_loc      string,
   level1_material_id     string,   
   level1_material_descr  string,
   level2_material_id     string,   
   level2_material_descr  string,
   level3_material_id     string,   
   level3_material_descr  string
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取ZQ01物料和CW02表对应信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_EGGS_COST_MM_1="
INSERT OVERWRITE TABLE $DMP_BIRD_EGGS_COST_MM_1 PARTITION(op_month='$OP_MONTH')
 SELECT
    d1.org_id,                                        --期间    
    d1.bus_type,                                      --ou_id 
    d1.product_line,                                  --业态
    d1.period_id,                                     --产线
    d1.qualified_egg_code,                            --物料
    case when (stage = '产蛋期')  then nvl(d1.qualified_egg,0)-nvl(d1.fh_check,0)  else 0 end,  --产蛋期产合格蛋
    nvl(d1.qualified_egg,0)-nvl(d1.fh_check,0),           --产合格蛋
    nvl(d1.actual_ebs_qualified,0) ,   --合格蛋（预测）
    nvl(d2.cost_amount_t,0),           --种蛋成本-总额
    '',--d4.level1_material_id,             --物料1级id
    '',--d4.level1_material_descr,          --物料1级描述
    '',--d4.level2_material_id,             --物料2级id
    '',--d4.level2_material_descr,          --物料2级描述
    '',--d4.level3_material_id,             --物料3级id
    d1.dict_item_name_xb          generation_name     --系别
from (select * from DWU_QYZ_NLBP_BIRD_PRODFEED_HEAD_DD where op_day = '$OP_DAY')  d1
left join dwu_finance_cost_pric d2 
  on d1.org_id = d2.org_id       
  and substr(d1.period_id,1,6) = d2.period_id  
  and d1.qualified_egg_code = d2.MATERIAL_ITEM_ID
--INNER JOIN
--    mreport_global.dim_material d4
--ON
--    substr(d1.qualified_egg_code,1,10)=d4.item_id
;
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_EGGS_COST_MM='DMP_BIRD_EGGS_COST_MM'

CREATE_DMP_BIRD_EGGS_COST_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_EGGS_COST_MM(
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
  ,currency_type_id            string    --币种
  ,currency_type_descr         string    --币种描述
  ,level1_material_id          string    --物料1级id
  ,level1_material_descr       string    --物料1级描述
  ,level2_material_id          string    --物料2级id
  ,level2_material_descr       string    --物料2级描述
  ,level3_material_id          string    --物料3级id
  ,level3_material_descr       string    --物料3级描述
  ,level4_material_id          string    --物料4级id
  ,level4_material_descr       string    --物料4级描述
  ,kpi_type_id                 string    --0-预算指标  1-正常计算指标
  ,kpi_type_descr              string    --预算指标，正常计算指标
  ,eggs_qty                    string    --种蛋产量(个)
  ,eggs_cost                   string    --种蛋成本总额
  ,eggs_mtl_cost               string    --种蛋成本-饲料成本总额(元)
  ,eggs_drugs_cost             string    --种蛋成本-药品成本总额
  ,eggs_direct_labor_cost      string    --种蛋成本-直接人工总额
  ,eggs_depreciated_cost       string    --种蛋成本-生物资产折旧总额
  ,dp_chicks_cost              string    --生物资产折旧-种雏成本总额
  ,dp_mtl_cost                 string    --生物资产折旧- 饲料成本总额
  ,dp_drugs_cost               string    --生物资产折旧-药品成本总额
  ,dp_direct_labor_cost        string    --生物资产折旧-人工成本总额
  ,dp_mf_rental_cost           string    --生物资产折旧- 制造费用-折旧租赁费总额
  ,dp_mf_padding_cost          string    --生物资产折旧- 制造费用- 垫料费用总额
  ,dp_mf_consum_cost           string    --生物资产折旧- 制造费用-能耗费总额
  ,dp_mf_water_power_cost      string    --生物资产折旧- 制造费用-水电费总额
  ,dp_mf_indirect_labor_cost   string    --生物资产折旧- 制造费用- 间接人工总额
  ,dp_mf_other_cost            string    --生物资产折旧- 制造费用- 其他总额
  ,dp_byproduct_income         string    --生物资产折旧-副产品总额
  ,eggs_mf_rental_cost         string    --农业制造费用-折旧租赁费总额
  ,eggs_mf_consum_cost         string    --农业制造费用-能耗费总额
  ,eggs_mf_water_power_cost    string    --农业制造费用- 水电费总额
  ,eggs_mf_padding_cost        string    --农业制造费用-垫料费用总额
  ,eggs_mf_indirect_labor_cost string    --农业制造费用-间接人工总额
  ,eggs_mf_other_cost          string    --农业制造费用-其他总额
  ,eggs_byproduct_income       string    --种蛋成本-副产品收入总额
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_EGGS_COST_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_EGGS_COST_MM PARTITION(op_month='$OP_MONTH')
  SELECT
        t1.period_id       as   month_id         --month_id
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
       ,''                                       --组织7级
       ,''                                       --组织7级
       ,t5.level1_businesstype_id                --业态1级
       ,t5.level1_businesstype_name              --业态1级
       ,t5.level2_businesstype_id                --业态2级
       ,t5.level2_businesstype_name              --业态2级
       ,t5.level3_businesstype_id                --业态3级
       ,t5.level3_businesstype_name              --业态3级
       ,t5.level4_businesstype_id                --业态4级
       ,t5.level4_businesstype_name              --业态4级
       ,case when t1.product_line='10' then '1'
             when t1.product_line='20' then '2' 
             else '-1' end             --产线ID
       ,case when t1.product_line ='10'  then '鸡线'
             when t1.product_line ='20'  then '鸭线'
             else  '缺省'    end                      --产线名称
       ,'CNY'  currency_type_id                  --币种
       ,'母币' currency_type_descr               --币种描述
       ,t1.level1_material_id   
       ,t1.level1_material_descr                 --物料等级
       ,t1.level2_material_id   
       ,t1.level2_material_descr
       ,t1.level3_material_id  
       ,t1.level3_material_descr
       ,'' level4_material_id  
       ,'' level4_material_descr
       ,t1.kpi_type_id                                     --0-预算指标  1-正常计算指标
       ,t1.kpi_type_descr                                  --预算指标，正常计算指标
       ,t1.eggs_qty                                        --种蛋产量
       ,round(t1.eggs_cost,2)                              --种蛋成本-总额   
       ,round(t1.eggs_mtl_cost,2)                          --种蛋成本-饲料成本总额
       ,round(t1.eggs_drugs_cost,2)                        --种蛋成本-药品成本总额
       ,round(t1.eggs_direct_labor_cost,2)                 --种蛋成本-直接人工总额
       ,round(t1.eggs_depreciated_cost,2)                  --种蛋成本-生物资产折旧总额
       ,round(t1.dp_chicks_cost,2)                         --生物资产折旧-种雏成本总额
       ,round(t1.dp_mtl_cost,2)                            --生物资产折旧- 饲料成本总额
       ,round(t1.dp_drugs_cost,2)                          --生物资产折旧-药品成本总额
       ,round(t1.dp_direct_labor_cost,2)                   --生物资产折旧-人工成本总额
       ,round(t1.dp_mf_rental_cost,2)                      --生物资产折旧-折旧租赁费总额
       ,round(t1.dp_mf_padding_cost,2)                     --生物资产折旧-垫料费用总额
       ,round(t1.dp_mf_consum_cost,2)                      --生物资产折旧-能耗费总额
       ,round(t1.dp_mf_water_power_cost,2)                 --生物资产折旧-水电费总额
       ,round(t1.dp_mf_indirect_labor_cost,2)              --生物资产折旧-间接人工总额
       ,round(t1.dp_mf_other_cost,2)                       --生物资产折旧-其他总额
       ,round(t1.borning_sub_dep_cost,2)                   --生物资产折旧-副产品总额
       ,round(t1.eggs_mf_rental_cost,2)                    --农业制造费用-折旧租赁费总额
       ,round(t1.eggs_mf_consum_cost,2)                    --农业制造费用-能耗费总额
       ,round(t1.eggs_mf_water_power_cost,2)               --农业制造费用- 水电费总额
       ,round(t1.eggs_mf_padding_cost,2)                   --农业制造费用-垫料费用总额
       ,round(t1.eggs_mf_indirect_labor_cost,2)            --农业制造费用-间接人工总额
       ,round(t1.eggs_mf_other_cost,2)                     --农业制造费用-其他总额
       ,round(t1.eggs_byproduct_income,2)                  --种蛋成本-副产品收入总额
       ,'$CREATE_TIME'                                     --数据推送时间
from
 (
   SELECT
        d2.org_id,
        d2.product_line ,
        d2.bus_type ,
        substr(d2.period_id,1,6) period_id,
        '1'     as               kpi_type_id,                    --指标类型
        '正常计算指标'           kpi_type_descr,                 --指标类型描述
        sum(d2.total_qty)                   eggs_qty ,                 --种蛋产量
        d2.level1_material_id,   
        d2.level1_material_descr,                                --物料等级
        d2.level2_material_id,   
        d2.level2_material_descr,
        d2.level3_material_id,   
        d2.level3_material_descr,
        sum(nvl(cost_amount11_loc,0)*d2.qty*rate_b)    eggs_cost,     
        sum(nvl(born_feed_cost,0)*d2.qty*rate_b)       eggs_mtl_cost,                                       --种蛋成本-饲料成本总额
        sum(nvl(born_drugs_cost,0)*d2.qty*rate_b)      eggs_drugs_cost,                                     --种蛋成本-药品成本总额
        sum(nvl(born_direct_hr_cost,0)*d2.qty*rate_b)  eggs_direct_labor_cost,                              --种蛋成本-直接人工总额
        sum((nvl(dep_cub_cost,0)+nvl(dep_feed_cost,0)+nvl(dep_drugs_cost,0)
        +nvl(dep_direct_hr_cost,0)+nvl(dep_utiltity_bill,0)
        +nvl(dep_loan_fee,0)+nvl(dep_base_feed_fee,0)+nvl(dep_energy_cost,0)
        +nvl(dep_indirect_hr_cost,0)+nvl(dep_other,0))*d2.qty*rate_b)    eggs_depreciated_cost,             --种蛋成本-生物资产折旧总额
        sum(nvl(dep_cub_cost,0)*d2.qty*rate_b)          dp_chicks_cost,                                     --生物资产折旧-种雏成本总额
        sum(nvl(dep_feed_cost,0)*d2.qty*rate_b)         dp_mtl_cost,                                        --生物资产折旧- 饲料成本总额
        sum(nvl(dep_drugs_cost,0)*d2.qty*rate_b)        dp_drugs_cost,                                      --生物资产折旧-药品成本总额
        sum(nvl(dep_direct_hr_cost,0)*d2.qty*rate_b)    dp_direct_labor_cost ,                              --生物资产折旧-人工成本总额
        sum(nvl(dep_loan_fee,0)*d2.qty*rate_b)          dp_mf_rental_cost,                                  --生物资产折旧-折旧租赁费总额
        sum(nvl(dep_base_feed_fee,0)*d2.qty*rate_b)     dp_mf_padding_cost,                                 --生物资产折旧-垫料费用总额
        sum(nvl(dep_energy_cost,0)*d2.qty*rate_b)       dp_mf_consum_cost,                                  --生物资产折旧-能耗费总额
        sum(nvl(dep_utiltity_bill,0)*d2.qty*rate_b)     dp_mf_water_power_cost,                             --生物资产折旧-水电费总额
        sum(nvl(dep_indirect_hr_cost,0)*d2.qty*rate_b)  dp_mf_indirect_labor_cost,                          --生物资产折旧-间接人工总额
        sum(nvl(dep_other,0)*d2.qty*rate_b)             dp_mf_other_cost,                                   --生物资产折旧-其他总额
        sum(nvl(borning_sub_dep_cost,0)*d2.qty*rate_b)  borning_sub_dep_cost,                               --生物资产折旧-副产品总额
        sum(nvl(born_loan_fee,0)*d2.qty*rate_b)         eggs_mf_rental_cost,                                --农业制造费用-折旧租赁费总额
        sum(nvl(born_energy_cost,0)*d2.qty*rate_b)      eggs_mf_consum_cost,                                --农业制造费用-能耗费总额
        sum(nvl(born_utiltity_bill,0)*d2.qty*rate_b)    eggs_mf_water_power_cost,                           --农业制造费用- 水电费总额
        sum(nvl(born_base_feed_fee,0)*d2.qty*rate_b)    eggs_mf_padding_cost,                               --农业制造费用-垫料费用总额
        sum(nvl(born_indirect_hr_cost,0)*d2.qty*rate_b) eggs_mf_indirect_labor_cost,                        --农业制造费用-间接人工总额
        sum(nvl(born_other,0)*d2.qty*rate_b)            eggs_mf_other_cost,                                 --农业制造费用-其他总额
        sum(nvl(born_sub_product_cost,0)*d2.qty*rate_b) eggs_byproduct_income                               
    FROM
        (
            SELECT
                *
            FROM
                dwu_zq_zq06_dd
            WHERE op_day='$OP_DAY')d1
    INNER JOIN
        (
            SELECT
                *
            FROM
                DMP_BIRD_EGGS_COST_MM_1
            WHERE
                op_month='$OP_MONTH') d2
    ON
        d1.org_id=d2.org_id
    AND d1.item_code=d2.item_code
    AND d1.bus_type=d2.bus_type
    and d1.product_line=d2.product_line
    AND regexp_replace(d1.period_id,'-','')=substr(d2.period_id,1,6)
    group by 
        d2.org_id,
        d2.product_line,
        d2.bus_type,
        substr(d2.period_id,1,6),
        d2.level1_material_id,   
        d2.level1_material_descr,                                --物料等级
        d2.level2_material_id,   
        d2.level2_material_descr,
        d2.level3_material_id,   
        d2.level3_material_descr 
 union all
   SELECT
        d2.org_id,
        d2.product_line ,
        d2.bus_type ,
        substr(d2.period_id,1,6) period_id,
        '0'     as               kpi_type_id,                    --指标类型
        '预算计算指标'           kpi_type_descr,                 --指标类型描述
        sum(d2.actual_ebs_qualified) eggs_qty ,                  --种蛋产量
        d2.level1_material_id,   
        d2.level1_material_descr,                                --物料等级
        d2.level2_material_id,   
        d2.level2_material_descr,
        d2.level3_material_id,   
        d2.level3_material_descr,
        sum(nvl(cost_amount11_loc,0)*d2.qty*rate_b)    eggs_cost,     
        sum(nvl(born_feed_cost,0)*d2.qty*rate_b)       eggs_mtl_cost,                  --种蛋成本-饲料成本总额
        sum(nvl(born_drugs_cost,0)*d2.qty*rate_b)      eggs_drugs_cost,                --种蛋成本-药品成本总额
        sum(nvl(born_direct_hr_cost,0)*d2.qty*rate_b)  eggs_direct_labor_cost,         --种蛋成本-直接人工总额
        sum((nvl(dep_cub_cost,0)+nvl(dep_feed_cost,0)+nvl(dep_drugs_cost,0)
        +nvl(dep_direct_hr_cost,0)+nvl(dep_utiltity_bill,0)
        +nvl(dep_loan_fee,0)+nvl(dep_base_feed_fee,0)+nvl(dep_energy_cost,0)
        +nvl(dep_indirect_hr_cost,0)+nvl(dep_other,0))*d2.qty*rate_b)    eggs_depreciated_cost,             --种蛋成本-生物资产折旧总额
        sum(nvl(dep_cub_cost,0)*d2.qty*rate_b)          dp_chicks_cost,                 --生物资产折旧-种雏成本总额
        sum(nvl(dep_feed_cost,0)*d2.qty*rate_b)         dp_mtl_cost,                    --生物资产折旧- 饲料成本总额
        sum(nvl(dep_drugs_cost,0)*d2.qty*rate_b)        dp_drugs_cost,                  --生物资产折旧-药品成本总额
        sum(nvl(dep_direct_hr_cost,0)*d2.qty*rate_b)    dp_direct_labor_cost ,          --生物资产折旧-人工成本总额
        sum(nvl(dep_loan_fee,0)*d2.qty*rate_b)          dp_mf_rental_cost,              --生物资产折旧-折旧租赁费总额
        sum(nvl(dep_base_feed_fee,0)*d2.qty*rate_b)     dp_mf_padding_cost,             --生物资产折旧-垫料费用总额
        sum(nvl(dep_energy_cost,0)*d2.qty*rate_b)       dp_mf_consum_cost,              --生物资产折旧-能耗费总额
        sum(nvl(dep_utiltity_bill,0)*d2.qty*rate_b)     dp_mf_water_power_cost,         --生物资产折旧-水电费总额
        sum(nvl(dep_indirect_hr_cost,0)*d2.qty*rate_b)  dp_mf_indirect_labor_cost,      --生物资产折旧-间接人工总额
        sum(nvl(dep_other,0)*d2.qty*rate_b)             dp_mf_other_cost,               --生物资产折旧-其他总额
        sum(nvl(borning_sub_product_depre_cost,0)*d2.qty*rate_b)  borning_sub_dep_cost, --生物资产折旧-副产品总额  
        sum(nvl(born_loan_fee,0)*d2.qty*rate_b)         eggs_mf_rental_cost,            --农业制造费用-折旧租赁费总额
        sum(nvl(born_energy_cost,0)*d2.qty*rate_b)    eggs_mf_consum_cost,              --农业制造费用-能耗费总额
        sum(nvl(born_utiltity_bill,0)*d2.qty*rate_b)      eggs_mf_water_power_cost,     --农业制造费用- 水电费总额
        sum(nvl(born_base_feed_fee,0)*d2.qty*rate_b)    eggs_mf_padding_cost,           --农业制造费用-垫料费用总额
        sum(nvl(born_indirect_hr_cost,0)*d2.qty*rate_b) eggs_mf_indirect_labor_cost,    --农业制造费用-间接人工总额
        sum(nvl(born_other,0)*d2.qty*rate_b)            eggs_mf_other_cost,             --农业制造费用-其他总额
        sum(nvl(born_sub_product_cost,0)*d2.qty*rate_b) eggs_byproduct_income       
    FROM
        (
            SELECT
                *
            FROM
                dwu_zq_zq09_dd
            WHERE op_day='$OP_DAY' )d1
    INNER JOIN
        (
            SELECT
                *
            FROM
                DMP_BIRD_EGGS_COST_MM_1
            WHERE
                op_month='$OP_MONTH') d2
    ON
        d1.org_id=d2.org_id
    AND d1.item_code=d2.item_code
    AND d1.bus_type=d2.bus_type
    AND regexp_replace(d1.period_id,'-','')=substr(d2.period_id,1,6)
    AND d1.product_line=d2.product_line
    group by 
        d2.org_id,
          d2.product_line ,
        d2.bus_type ,
        substr(d2.period_id,1,6),
        d2.level1_material_id,   
        d2.level1_material_descr,                                --物料等级
        d2.level2_material_id,   
        d2.level2_material_descr,
        d2.level3_material_id,   
        d2.level3_material_descr
 ) t1
LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_businesstype
        WHERE  level4_businesstype_name IS NOT NULL) t5
     ON  (t1.bus_type=t5.level4_businesstype_id)
left join mreport_global.dim_org_management t6 
     on t1.org_id=t6.org_id  
     and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
     on t1.org_id=t7.org_id 
     and t1.bus_type=t7.bus_type_id 
     and t7.attribute5='2'
 ;
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DMP_BIRD_EGGS_COST_MM_1;
    $INSERT_DMP_BIRD_EGGS_COST_MM_1;
    $CREATE_DMP_BIRD_EGGS_COST_MM;
    $INSERT_DMP_BIRD_EGGS_COST_MM;
"  -v