#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_inv_counting_mm.sh                               
# 创建时间: 2018年04月19日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 存货减值计提表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_inv_counting_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_INV_COUNTING_MM='DMP_BIRD_INV_COUNTING_MM'

CREATE_DMP_BIRD_INV_COUNTING_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_INV_COUNTING_MM(
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
  ,trade_type_id               string    --交易关系
  ,trade_type_descr            string    --描述
  ,currency_type_id            string    --币种
  ,currency_type_descr         string    --币种
  ,series_code                 string    --物料系别
  ,aggs_qty                    string     --库存种蛋数量(枚)
  ,eggs_cost                   string     --库存种蛋成本(元)
  ,on_eggs_qty                 string     --在孵种蛋数量(枚)
  ,on_eggs_cost                string     --在孵种蛋苗种成本(元)
  ,on_hatch_cost               string     --孵化费用(元)
  ,out_hatch_qty               string     --孵化完工（出雏)的种蛋数量
  ,healthy_chicks_qty          string     --入孵蛋健雏数量(只)
  ,predict_sales_amt           string     --预计售价(元)
  ,sales_cost                  string     --销售费用
  ,inv_predict_lost_amt        string     --库存种蛋预计潜亏
  ,on_hatch_predict_lost_amt   string     --在孵种蛋预计潜亏
  ,inv_eggs_lost_up_amt        string     --本月库存种蛋预计潜亏补提
  ,on_hatch_lost_up_amt        string     --在孵种蛋预计潜亏补提 
  ,inv_predict_amt             string     --库存种蛋预计苗量
  ,on_hatch_predict_amt        string     --在孵种蛋预计苗量  
  ,create_time                 string     --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_INV_COUNTING_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_INV_COUNTING_MM PARTITION(op_month='$OP_MONTH')
  SELECT
      t1.period_id   as   month_id      --month_id
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
       ,t1.product_line                       --产线ID
       ,case when t1.product_line='10' then '鸡线' else '鸭线' end as production_line_descr           --产线名称
       ,'' as trade_type_id                   --交易关系
       ,'' as trade_type_descr                --描述
       ,'3' as currency_type_id               --币种
       ,'母币' as currency_type_descr         --币种
       ,t1.series_desc                        --物料系别
       ,t1.inv_egg_num                        --库存种蛋数量
       ,t1.inv_egg_cost                       --库存种蛋成本
       ,t1.hatch_egg_num                      --在孵种蛋数量                                 
       ,t1.hatch_egg_nest_cost                  --在孵种蛋苗种成本
       ,case when t3.conversion_rate is null 
	        then round((t1.unit_hatch_cost*t2.out_hatch_qty),2) 
            else round((t1.unit_hatch_cost*t2.out_hatch_qty*t3.conversion_rate),2) end	   --孵化费用(元)
       ,t2.out_hatch_qty                      --孵化完工（出雏)的种蛋数量
       ,t2.out_hatch_qty*NEST_EGG_RATE        --入孵蛋健雏数量(只)
       ,case when t3.conversion_rate is null then t1.budget_price else  t1.budget_price*t3.conversion_rate end    --预计售价
       ,case when t3.conversion_rate is null then round((t1.unit_sales_cost*t2.out_hatch_qty),2) 
	                                         else round((t1.unit_sales_cost*t2.out_hatch_qty*t3.conversion_rate),2) end                  --销售费用
       ,case when t3.conversion_rate is null then t1.inv_egg_budget_loss    else  t3.conversion_rate*t1.inv_egg_budget_loss end               --库存种蛋预计潜亏
       ,case when t3.conversion_rate is null then t1.hatch_egg_budget_loss  else  t3.conversion_rate*t1.hatch_egg_budget_loss end             --在孵种蛋预计潜亏
       ,case when t3.conversion_rate is null then t1.inv_egg_budget_loss_supp  else  t3.conversion_rate*t1.inv_egg_budget_loss_supp end       --本月库存种蛋预计潜亏补提
       ,case when t3.conversion_rate is null then t1.hatch_egg_budget_loss_supp  else  t3.conversion_rate*t1.hatch_egg_budget_loss_supp end   --在孵种蛋预计潜亏补提
       ,t1.inv_egg_budget_nest             --库存种蛋预计苗量
	   ,t1.hatch_egg_budget_nest
	   ,'$CREATE_TIME'                        --创建时间

from
(select * from dwu_zq_inventory_impairment_dd where op_day='$OP_DAY'  and bus_type in(132011,1320112)) t1
LEFT JOIN (
   SELECT
       org_id,
       bus_type,
       product_line,
       generation_name,
       SUBSTR(period_id,1,6) period_id,
       SUM(big_good_a +middle_good_a+little_good_a+good_b +middle_goob_b +big_parent_a+little_parent_a) healthy_chicks_qty,
       SUM(hatch_egg_number) out_hatch_qty
   FROM
       dwu_zq_hatch_dd
   WHERE
       op_day='$OP_DAY'
   GROUP BY
       org_id,
       bus_type,
       product_line,
       generation_name,
       SUBSTR(period_id,1,6)    
)t2 on  t1.org_id=t2.org_id and t1.bus_type=t2.bus_type and t1.product_line=t2.product_line
 and t1.period_id=t2.period_id and t2.generation_name=t1.series_desc
LEFT join 
   (  SELECT
                    from_currency,
                    to_currency,
                    conversion_rate,
					conversion_period
                FROM
                    mreport_global.dmd_fin_period_currency_rate_mm
                WHERE to_currency='CNY') t3
    on t1.loc_currency_id =t3.from_currency 
   and t3.conversion_period=t1.period_id
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
    $CREATE_DMP_BIRD_INV_COUNTING_MM;
    $INSERT_DMP_BIRD_INV_COUNTING_MM;
"  -v