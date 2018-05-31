#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_fy_est_dd.sh                               
# 创建时间: 2018年04月12日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺放养测算底表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_duck_charge_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 获取禽旺放养测算信息
TMP_DMP_BIRD_FY_EST_DD_1='TMP_DMP_BIRD_FY_EST_DD_1'

CREATE_TMP_DMP_BIRD_FY_EST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_FY_EST_DD_1(
   org_id      string,
   day_id      string,
   bus_type    string,
   material_id string,
   inv_org_id  string,
   level2_material_id string,
   level2_material_descr string,
   sale_cnt  decimal(16,2),
   sale_cost decimal(16,2),
   buy_cost  decimal(16,2),
   d_profit  decimal(16,2),
   profit    decimal(16,2),
   pre_sale_cnt decimal(16,2),
   pre_sale_cost decimal(16,2),
   pre_buy_cost  decimal(16,2),
   pre_d_profit  decimal(16,2),
   pre_profit    decimal(16,2),
   pre_month_sale_cnt decimal(16,2),
   pre_month_sale_cost decimal(16,2)
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取禽旺放养测算信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_FY_EST_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_FY_EST_DD_1 PARTITION(op_day='$OP_DAY')
SELECT
     t1.org_id                                                                                   --ou_id
    ,t1.day_id                                                                                   --day_id
    ,t1.bus_type                                                                                 --业态Id
    ,t1.material_id                                                                              --物料编码
    ,t1.inv_org_id                                                                               --库存组织id
	,t4.level2_material_id                                                                          --物料2级id
    ,t4.level2_material_descr                                                                       --物料2级名称
    ,t2.out_quantity                                                       as sale_cnt          --销量
    ,t2.loc_std_price                                                      as sale_cost         --售价
    ,case when level2_material_id ='1503'
       then  t1.unit_selling_price else  t3.cost end                     as buy_cost          --采购价
    ,case when level2_material_id ='1503' then (t2.loc_std_price -t1.unit_selling_price)
    else  (t2.loc_std_price - t3.cost) end                                as d_profit         --吨利润
    ,case when level2_material_id ='1503' then 
    t2.out_quantity *(t2.loc_std_price -t1.unit_selling_price) 
    else  t2.out_quantity* (t2.loc_std_price -t3.cost) end            as profit           --利润
    , 0.00 as pre_sale_cnt                                                                     --测算日至月底:销量
    , 0.00 as pre_sale_cost                                                                    --测算日至月底:售价
    , 0.00 as pre_buy_cost                                                                     --测算日至月底:采购价
    , 0.00 as pre_d_profit                                                                     --测算日至月底:吨利润
    , 0.00 as pre_profit                                                                       --测算日至月底:利润
    , 0.00 as pre_month_sale_cnt                                                               --预计全月:销量
    , 0.00 as pre_month_sale_cost                                                              --预计全月:售价
	FROM
    (
        SELECT
            period_id as day_id,
            org_id,
            bus_type,
            material_id,
            inv_org_id,
            cust_po_number,
            SUM( unit_selling_price*primary_quantity)/SUM(primary_quantity) unit_selling_price
        FROM
            dwu_qw_buy_list_qw04_dd
        WHERE
            op_day ='$OP_DAY'
        GROUP BY
            org_id,
            bus_type,
            material_id,
            period_id,
            inv_org_id,
            cust_po_number) t1
INNER JOIN
(select * from  mreport_global.dim_material  
where level2_material_id in ('2501','2502','1503','6501','6502','6503','6504','6505'))
t4 ON t1.material_id = t4.inventory_item_id
INNER JOIN
    (
        SELECT
            inv_org_id,
            material_id,
            cust_po_num                                        cust_po_number,
            SUM(out_quantity)                                  out_quantity,
            SUM(loc_std_price*out_quantity) /SUM(out_quantity) loc_std_price
        FROM
            dwu_xs_other_sale_dd
        GROUP BY
            inv_org_id,
            material_id,
            cust_po_num ) t2
ON
    t1.inv_org_id=t2.inv_org_id
AND t1.material_id=t2.material_id
AND t1.cust_po_number=t2.cust_po_number
LEFT JOIN
    (
        SELECT
            inv_org_id,
            material_id,
            contract_no,
            SUM(quantity_main*price_with_tax)/SUM(quantity_main) cost
        FROM
            dwu_cg_buy_list_cg01_dd
        GROUP BY
            inv_org_id,
            material_id,
            contract_no ) t3
ON
    t1.inv_org_id=t3.inv_org_id
AND t1.material_id=t3.material_id
AND t1.cust_po_number=t3.contract_no
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_FY_EST_DD='DMP_BIRD_FY_EST_DD'

CREATE_DMP_BIRD_FY_EST_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_FY_EST_DD(
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
  ,level2_material_id          string    --物料2级id
  ,level2_material_descr       string    --物料2级名称
  ,sale_cnt                decimal(16,2) --测算已实现:销量
  ,sale_cost               decimal(16,2) --测算已实现:售价
  ,buy_cost                decimal(16,2) --测算已实现:采购价
  ,pre_sale_cnt            decimal(16,2) --测算日至月底:销量
  ,pre_sale_cost           decimal(16,2) --测算日至月底:售价
  ,pre_buy_cost            decimal(16,2) --测算日至月底:采购价
  ,pre_d_profit            decimal(16,2) --测算日至月底:吨利润
  ,pre_profit              decimal(16,2) --测算日至月底:利润
  ,pre_month_sale_cnt      decimal(16,2) --预计全月:销量
  ,pre_month_sale_cost     decimal(16,2) --预计全月:售价
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_FY_EST_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_FY_EST_DD PARTITION(op_day='$OP_DAY')
SELECT
        substr(t1.day_id,1,6) as   month_id   --month_id          
       ,t1.day_id                             --day_id
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
       ,'' as level7_org_id                   --组织7级
       ,'' as level7_org_descr                --组织7级
       ,t3.level1_businesstype_id             --业态1级
       ,t3.level1_businesstype_name           --业态1级
       ,t3.level2_businesstype_id             --业态2级
       ,t3.level2_businesstype_name           --业态2级
       ,t3.level3_businesstype_id             --业态3级
       ,t3.level3_businesstype_name           --业态3级
       ,t3.level4_businesstype_id             --业态4级
       ,t3.level4_businesstype_name           --业态4级
       ,t1.level2_material_id                 --物料2级id
       ,t1.level2_material_descr              --物料2级名称
       ,t1.sale_cnt                           --测算已实现:销量
       ,t1.sale_cost                          --测算已实现:售价
       ,t1.buy_cost                           --测算已实现:采购价
       ,t1.pre_sale_cnt                       --测算日至月底:销量
       ,t1.pre_sale_cost                      --测算日至月底:售价
       ,t1.pre_buy_cost                       --测算日至月底:采购价
       ,t1.pre_d_profit                       --测算日至月底:吨利润
       ,t1.pre_profit                         --测算日至月底:利润
       ,t1.pre_month_sale_cnt                 --预计全月:销量
       ,t1.pre_month_sale_cost                --预计全月:售价
	   ,'$CREATE_TIME' as create_time         --创建时间
FROM
    (select * from TMP_DMP_BIRD_FY_EST_DD_1 where op_day='$OP_DAY') t1
LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_management
        WHERE org_id IS NOT NULL) t2
     ON ( t1.org_id=t2.org_id)
LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_businesstype
        WHERE  level4_businesstype_name IS NOT NULL) t3
     ON  (t1.bus_type=t3.level4_businesstype_id)
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_FY_EST_DD_1;
    $INSERT_TMP_DMP_BIRD_FY_EST_DD_1;
    $CREATE_DMP_BIRD_FY_EST_DD;
    $INSERT_DMP_BIRD_FY_EST_DD;
"  -v