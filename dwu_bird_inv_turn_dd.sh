#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_inv_turn_dd.sh                               
# 创建时间: 2018年4月15日                                            
# 创 建 者: gl                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 库存周转
# 修改说明:                                                          
######################################################################



OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)
CURRENT_DAY=$(date -d " -0 day" +%Y-%m-%d)
CURRENT_MONTH=$(date -d " -0 day" +%Y%m)
# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_inv_turn_dd.sh 20180101"
    exit 1
fi
############################################################################################
## 建立临时表，用于存放库存的主要信息
TMP_DMP_BIRD_MAIN_INVTURN_DD_1='TMP_DMP_BIRD_MAIN_INVTURN_DD_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,sale_cnt                         string      --销量
      ,store_cnt                        string      --库存
      ,sale_total_amt                   string      --销售总金额
      ,sale_cost_amt                    string      --庫存毛利
      ,paper_store_cnt                  string      --账面超期库存量
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

############################################################################################
##查询库存主要信息并放入以上临时表
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_1 PARTITION(op_day='$OP_DAY')
SELECT
   t1.month_id,                                                --期间月
   t1.day_id,                                                  --期间日
   t1.product_line as production_line_id,                      --产线id
   case  t1.product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                               --产线描述
   t1.bus_type,                                                 --业态
   t1.org_id,                                                   --公司id
   t1.organization_id,                                          --库存组织id
   t1.item_id,                                                  --物料
   t1.total_out_qty as sale_cnt,                                --销量
   (t2.total_finish_num+t2.total_fresh_num) as store_cnt,       --库存
   nvl(t1.sale_total_amt,0),                                     --销售金额
   t2.total_finish_num*(t7.price_with_tax/(1+t5.percentage_rate/100)-t6.cost_amount_t),--庫存毛利                                       
   case when t3.d_total_quantity is not null and datediff(from_unixtime(unix_timestamp(t1.day_id,'yyyymmdd'),'yyyy-mm-dd'),t3.orig_date)>90 then t3.d_total_quantity
   else 0 end as paper_store_cnt                                --账面超期库存量 
FROM
(SELECT 
  org_id,                                                       --公司id
  organization_id,                                              --库存组织id
  bus_type,                                                     --业态
  item_id,                                                      --物料
  substr(out_date,1,6) as month_id,                             --期间（月）
  out_date as day_id,                                           --期间日
  product_line,                                                 --产线
  sum(out_qty) as total_out_qty,                                --总出库数
  sum(out_qty*execute_price)  as sale_total_amt                 --销售总金额
FROM mreport_poultry.dwu_gyl_xs01_dd where op_day='$OP_DAY' 
GROUP BY 
  org_id,
  organization_id,
  bus_type,
  item_id,
  product_line,
  substr(out_date,1,6), 
  out_date
) t1 

LEFT JOIN 
(SELECT
  item_id,
  organization_id,
  sum(nvl(finish_product_stock,0)) as total_finish_num,
  sum(nvl(fresh_normal_sto_count,0)) as total_fresh_num,
  regexp_replace(date_sub(substr(available_stock_time,1,10),1),'-','')  as day_id
FROM mreport_poultry.dwu_xs_xs03_dd WHERE op_day='$OP_DAY'
GROUP BY 
 organization_id,
 item_id,
  regexp_replace(date_sub(substr(available_stock_time,1,10),1),'-',''))t2
  ON(t1.organization_id=t2.organization_id and t1.item_id=t2.item_id and t1.day_id=t2.day_id )
LEFT JOIN
(SELECT  
  org_id,
  organization_id,
  item_id,
  quantity as d_total_quantity,
  substr(orig_date_received,1,10) as orig_date,                         --原始日期

  regexp_replace(substr(cur_date,1,10),'-','') as cur_date_format       --现有日期格式化为年月日
FROM mreport_poultry.dwu_xs_xs04_dd where op_day='$OP_DAY'




) t3
  ON (t1.organization_id=t3.organization_id and t1.item_id=t3.item_id and t1.day_id=t3.cur_date_format)
  
  LEFT  JOIN 
  (SELECT * FROM mreport_global.dwu_dim_material_new)t4
  ON(t2.item_id=t4.inventory_item_id and t2.organization_id=t4.inv_org_id)
  LEFT JOIN
  (SELECT 
   tax_rate_code,
   percentage_rate
   from mreport_global.ods_ebs_zx_rates_b) t5
  ON
  (t4.tax_code=t5.tax_rate_code)
  LEFT JOIN 
  (SELECT 
  period_id,
  inventory_item_id,
  organization_id,
  org_id,
  nvl(cost_amount_t,0) as cost_amount_t
  from mreport_poultry.dwu_finance_cost_pric where period_id='$OP_MONTH')t6
  ON (t2.item_id=t6.inventory_item_id and t2.organization_id=t6.organization_id)
  
  LEFT JOIN 
  (SELECT 
   org_id,
   item_id,
   price_with_tax 
   from mreport_poultry.dwu_gyl_xs07_dd
   where op_day='$OP_DAY'
  )t7
  ON(t1.organization_id=t2.organization_id and t1.org_id=t7.org_id and t2.item_id=t7.item_id)
"
 
 
 ####################################建立一张报表用于存放最终需求字段
 DMP_BIRD_INV_TURN_DD='dmp_bird_inv_turn_dd'
 CREATE_DMP_BIRD_INV_TURN_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_INV_TURN_DD(
   month_id                 string                                    --期间(月份)
  ,day_id                   string                                    --期间(日)
  ,level1_org_id            string                                    --组织1级(股份)
  ,level1_org_descr         string                                    --组织1级(股份)
  ,level2_org_id            string                                    --组织2级(片联)
  ,level2_org_descr         string                                    --组织2级(片联)
  ,level3_org_id            string                                    --组织3级(片区)
  ,level3_org_descr         string                                    --组织3级(片区)
  ,level4_org_id            string                                    --组织4级(小片)
  ,level4_org_descr         string                                    --组织4级(小片)
  ,level5_org_id            string                                    --组织5级(公司)
  ,level5_org_descr         string                                    --组织5级(公司)
  ,level6_org_id            string                                    --组织6级(OU)
  ,level6_org_descr         string                                    --组织6级(OU)
  ,level7_org_id            string                                    --组织7级(库存组织)
  ,level7_org_descr         string                                    --组织7级(库存组织)
  ,level1_businesstype_id   string                                    --业态1级
  ,level1_businesstype_name string                                    --业态1级
  ,level2_businesstype_id   string                                    --业态2级
  ,level2_businesstype_name string                                    --业态2级
  ,level3_businesstype_id   string                                    --业态3级
  ,level3_businesstype_name string                                    --业态3级
  ,level4_businesstype_id   string                                    --业态4级
  ,level4_businesstype_name string                                    --业态4级
  ,production_line_id       string                                    --产线
  ,production_line_descr    string                                    --产线
  ,level1_prod_id           string                                    --产品线1级
  ,level1_prod_descr        string                                    --产品线1级
  ,level2_prod_id           string                                    --产品线2级
  ,level2_prod_descr        string                                    --产品线2级
  ,level1_prodtype_id       string                                    --产品分类1级
  ,level1_prodtype_descr    string                                    --产品分类1级
  ,level2_prodtype_id       string                                    --产品分类2级
  ,level2_prodtype_descr    string                                    --产品分类2级
  ,level3_prodtype_id       string                                    --产品分类3级
  ,level3_prodtype_descr    string                                    --产品分类3级
  ,inventory_item_id        string                                    --物料品名ID
  ,inventory_item_desc      string                                    --物料品名名称
  ,sale_cnt                 string                                    --销量
  ,store_cnt                string                                    --库存
  ,sale_total_amt           string                                    --销售金额
  ,sale_cost_amt            string                                    --销售成本
  ,paper_store_cnt          string                                    --账面超期库存量
  ,create_time              string                                    --数据推送时间
)      
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"
 #################################################################插入报表数据
 INSERT_DMP_BIRD_INV_TURN_DD="
 INSERT OVERWRITE TABLE $DMP_BIRD_INV_TURN_DD PARTITION(op_day='$OP_DAY')
 SELECT
   t1.month_id,                                                 --期间（月）
   t1.day_id,                                                   --期间(日)
   t2.level1_org_id,                                            --组织一级(股份)id
   t2.level1_org_descr,                                         --组织一级(股份)
   t2.level2_org_id,                                            --组织二级id(片联)
   t2.level2_org_descr,                                         --组织二级(片联)
   t2.level3_org_id,                                            --组织三级id(片区)
   t2.level3_org_descr,                                         --组织三级(片区)
   t2.level4_org_id,                                            --组织四级id(小片)
   t2.level4_org_descr,                                         --组织四级(小片)
   t2.level5_org_id,                                            --组织五级id(公司)
   t2.level5_org_descr,                                         --组织五级(公司)
   t2.level6_org_id,                                            --组织六级id(OU)
   t2.level6_org_descr,                                         --组织六级(OU)
   t2.level7_org_id,                                            --组织七级id(库存)
   t2.level7_org_descr,                                          --组织七级(库存) 
   t3.level1_businesstype_id,                                   --业态1级
   t3.level1_businesstype_name,                                 --业态1级
   t3.level2_businesstype_id,                                   --业态2级
   t3.level2_businesstype_name,                                 --业态2级
   t3.level3_businesstype_id,                                   --业态3级
   t3.level3_businesstype_name,                                 --业态3级
   t3.level4_businesstype_id,                                   --业态4级
   t3.level4_businesstype_name,                                 --业态4级
   t1.production_line_id,                                       --产线id
   t1.production_line_descr,                                    --产线描述
   t5.prd_line_cate_id as level1_prod_id,                       --产品线一级Id
   t5.prd_line_cate as level1_prod_descr,                       --产品线一级
   t5.sub_prd_line_tp_id as level2_prod_id,                     --产品线二级
   t5.sub_prd_line_tp   as level2_prod_descr,                   --产品线二级
   t5.first_lv_tp_id as level1_prodtype_id,                     --产品分类1级id
   t5.first_lv_tp as level1_prodtype_descr,                     --产品分类1级
   t5.scnd_lv_tp_id as level2_prodtype_id,                      --产品分类2级id
   t5.scnd_lv_tp as level2_prodtype_descr,                      --产品分类2级
   t5.thrd_lv_tp_id as level3_prodtype_id,                      --产品分类3级id
   t5.thrd_lv_tp as level3_prodtype_descr,                      --产品分类3级
   t4.inventory_item_id,                                        --物料id
   t4.inventory_item_desc,                                      --物料品名描述
   nvl(t1.sale_cnt,0),                                          --销量
   nvl(t1.store_cnt,0),                                         --库存
   nvl(t1.sale_total_amt,0),                                    --销售金额
   nvl(t1.sale_cost_amt,0),                                     --庫存毛利
   t1.paper_store_cnt,                                          --账面超期库存量
   $CREATE_TIME
   FROM
     (SELECT * FROM $TMP_DMP_BIRD_MAIN_INVTURN_DD_1 where op_day='$OP_DAY')t1
  LEFT JOIN
     (SELECT
         inv_org_id,
         level1_org_id, 
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
         level7_org_id,
         level7_org_descr
       FROM  mreport_global.dim_org_inv_management)t2
  ON(t1.organization_id=t2.inv_org_id)
  LEFT JOIN
  (SELECT 
  level1_businesstype_id,
  level1_businesstype_name,
  level2_businesstype_id,
  level2_businesstype_name,
  level3_businesstype_id,
  level3_businesstype_name,
  level4_businesstype_id,
  level4_businesstype_name 
FROM mreport_global.dim_org_businesstype) t3
  ON (t1.bus_type=t3.level4_businesstype_id)
  LEFT JOIN 
  (SELECT 
  inventory_item_code,
  inventory_item_desc,
  inventory_item_id,
  inv_org_id 
FROM mreport_global.dwu_dim_material_new)t4
  ON (t1.item_id=t4.inventory_item_id
  AND t1.organization_id=t4.inv_org_id)
LEFT JOIN
  (SELECT
  item_code,
  prd_line_cate_id,
  prd_line_cate,
  sub_prd_line_tp_id,
  sub_prd_line_tp,
  first_lv_tp_id,
  first_lv_tp,
  scnd_lv_tp_id,
  scnd_lv_tp,
  thrd_lv_tp_id,  
  thrd_lv_tp
FROM mreport_global.dim_crm_item)t5
  ON (t4.inventory_item_code=t5.item_code)
"  
 echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_1;
    $CREATE_DMP_BIRD_INV_TURN_DD;
    $INSERT_DMP_BIRD_INV_TURN_DD;
    "  -v 



