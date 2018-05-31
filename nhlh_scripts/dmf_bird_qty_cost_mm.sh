#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_qty_cost_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰残次品数据表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_qty_cost_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)



## 建立临时表，生成残次品的销售中间表--和物料残次品物料ID关联
TMP_DMF_BIRD_QTY_COST_MM_1='TMP_DMF_BIRD_QTY_COST_MM_1'

CREATE_TMP_DMF_BIRD_QTY_COST_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_QTY_COST_MM_1(
       period_id                      string, 
       org_id                        string,  
       item_id                       string, 
       organization_id               string, 
       bus_type                      string, 
       product_line                  string, 
       material_segment5_desc        string, 
       material_segment4_desc        string, 
       is_d_product                  string, 
       primary_quantity              string, 
       primary_quantity_5            string, 
       primary_quantity_4            string, 
       primary_quantity_2            string 
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## CW23  取成本系数
## DIM_CRM_ITEM   去除副产品
## DWU_ORDER_INCOME  CW01  SRC_TYPE='01'   销售订单收入   每个类型分开计算
## 过滤残次品的清单
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_QTY_COST_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_QTY_COST_MM_1 PARTITION(op_day='$OP_DAY')
select distinct * from 
(select  
        substr(period_id,1,6)
       ,org_id
       ,item_id
       ,organization_id
       ,bus_type
       ,product_line
       ,material_segment5_desc
       ,material_segment4_desc
       ,is_d_product
       ,sum(primary_quantity) over(partition by m1.org_id,m1.product_line,m1.item_id,m1.bus_type,substr(period_id,1,6)) primary_quantity
       ,sum(m1.primary_quantity) over(partition by m1.org_id,m1.product_line,m1.material_segment5_desc,m1.bus_type,substr(period_id,1,6)) primary_quantity_5
       ,sum(m1.primary_quantity) over(partition by m1.org_id,m1.product_line,m1.material_segment4_desc,m1.bus_type,substr(period_id,1,6)) primary_quantity_4
       ,sum(m1.primary_quantity) over(partition by m1.org_id,m1.product_line,substr(period_id,1,6)) primary_quantity_2 from 
 (SELECT   
           t1.period_id                     
           ,t1.org_id                                          
           ,t1.item_id                       
           ,t1.organization_id                           
           ,t1.bus_type                                
           ,t1.product_line
           ,t2.material_segment5_desc
           ,t2.material_segment4_desc
           ,t2.is_d_product                                                
           ,sum(t1.primary_quantity) primary_quantity                                 
   FROM  mreport_poultry.dwu_tz_storage_transation02_dd t1
   left  join mreport_global.dwu_dim_material_new t2 on t1.item_id=t2.inventory_item_id and t1.organization_id=t2.inv_org_id
   left  join mreport_global.dim_crm_item t3 on t2.inventory_item_code=t3.item_code
   WHERE t1.OP_DAY='$OP_DAY'
     and (t3.prd_line_cate_id <> '1-16BW2M' or t3.prd_line_cate_id is null)   --副产品剔除
     and t1.item_id not in ('781343','27561','781345')
     and (t1.item_source <> '外部' or t1.item_source is null)
      group by           t1.period_id                     
           ,t1.org_id                                          
           ,t1.item_id                       
           ,t1.organization_id                           
           ,t1.bus_type                                
           ,t1.product_line 
           ,t2.material_segment5_desc
           ,t2.material_segment4_desc
           ,t2.is_d_product) m1
     ) n1
"





## 生成单价的临时表
TMP_DMF_BIRD_QTY_COST_MM_2='TMP_DMF_BIRD_QTY_COST_MM_2'

CREATE_TMP_DMF_BIRD_QTY_COST_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_QTY_COST_MM_2(
       month_id                                string,
       inv_org_id                              string, 
       org_id                                  string,  
       inventory_item_id                       string, 
       material_segment5_desc                  string, 
       defective_prods_val                     string, 
       quality_prods_val                       string, 
       defective_prods_amt                     string, 
       defective_prods_cnt                     string, 
       quality_prods_amt                       string, 
       quality_prods_cnt                       string,
       quality_prods_amt_4                     string, 
       quality_prods_cnt_4                     string   
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"



echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_QTY_COST_MM_2="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_QTY_COST_MM_2 PARTITION(op_day='$OP_DAY')
select
 month_id
,inv_org_id
,org_id
,inventory_item_id
,material_segment5_desc
,prc1/prt defective_prods_val
,prc2/prt quality_prods_val
,loc_income defective_prods_amt
,ordered_qty defective_prods_cnt
,aab quality_prods_amt
,aaa quality_prods_cnt
,ccb quality_prods_amt_4
,cca quality_prods_cnt_4
from
( select
   t3.month_id
  ,t1.inv_org_id
  ,t1.org_id
  ,t1.inventory_item_id
  ,t4.percentage_rate/100+1 prt
  ,t2.material_segment4_desc
  ,t2.material_segment5_desc
  ,t1.loc_income
  ,t1.ordered_qty
  ,t2.is_d_product
  ,t3.prc1
  ,t3.prc2
  ,sum(case when t2.is_d_product='Y' then 0 else t1.loc_income end) over(partition by t1.inv_org_id,t1.org_id,t2.material_segment5_desc) aab
  ,sum(case when t2.is_d_product='Y' then 0 else t1.ordered_qty end) over(partition by t1.inv_org_id,t1.org_id,t2.material_segment5_desc) aaa
  ,sum(case when t2.is_d_product='Y' then 0 else t1.loc_income end) over(partition by t1.inv_org_id,t1.org_id,t2.material_segment4_desc) ccb
  ,sum(case when t2.is_d_product='Y' then 0 else t1.ordered_qty end) over(partition by t1.inv_org_id,t1.org_id,t2.material_segment4_desc) cca
from
(select period_id,inv_org_id,org_id,inventory_item_id,sum(loc_income) loc_income,sum(ordered_qty) ordered_qty from 
(select period_id,inv_org_id,org_id,inventory_item_id,loc_income,ordered_qty from DWU_ORDER_INCOME
where op_month='$OP_MONTH' and SRC_TYPE='01'
union all
select substr(period_id,1,6) period_id,organization_id inv_org_id,org_id,item_id inventory_item_id,'0' loc_income,'0' ordered_qty from mreport_poultry.dwu_tz_storage_transation02_dd where op_day='$OP_DAY'
 group by substr(period_id,1,6),org_id,item_id,organization_id ) b1
 group by period_id,inv_org_id,org_id,inventory_item_id) t1
left join mreport_global.dwu_dim_material_new t2 
on (t1.inventory_item_id=t2.inventory_item_id and t1.inv_org_id=t2.inv_org_id)
left join
 (select 
        substr(regexp_replace(a1.date_to,'-',''),1,6) month_id,
        a1.item_id,
        a1.prc prc1,
        max(case when a2.is_d_product='Y' then 0 else a1.prc end) over (partition by a1.date_to,a2.material_segment5_desc) prc2
 from 
(select item_id,prc,date_to from DWU_CW_CW23_DD where op_day='$OP_DAY') a1
left join (select distinct material_segment5_desc,is_d_product,inventory_item_id from mreport_global.dwu_dim_material_new) a2 on a2.inventory_item_id=a1.item_id) t3
 on (t1.inventory_item_id=t3.item_id and t1.period_id=t3.month_id)
left join
  (select a1.inventory_item_id,a1.inv_org_id,a2.percentage_rate
  from (select * from mreport_global.dwu_dim_material_new where inv_org_desc like '%_生产库存组织' and material_segment3_desc not like '%半成品%') a1
  left join (select tax_rate_code,percentage_rate from  mreport_global.ODS_EBS_ZX_rates_b group by tax_rate_code,percentage_rate) a2
  where a1.tax_code=a2.tax_rate_code) t4
 on (t1.inventory_item_id=t4.inventory_item_id and t1.inv_org_id=t4.inv_org_id)
) n1
"



###########################################################################################
## 将数据转换至目标表
## 变量声明
DMF_BIRD_QTY_COST_MM='DMF_BIRD_QTY_COST_MM'

CREATE_DMF_BIRD_QTY_COST_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_QTY_COST_MM(
      month_id                           string         --期间(月份)    
     ,day_id                             string         --期间(日)     
     ,level1_org_id                      string         --组织1级(股份)  
     ,level1_org_descr                   string         --组织1级(股份)  
     ,level2_org_id                      string         --组织2级(片联)  
     ,level2_org_descr                   string         --组织2级(片联)  
     ,level3_org_id                      string         --组织3级(片区)  
     ,level3_org_descr                   string         --组织3级(片区)  
     ,level4_org_id                      string         --组织4级(小片)  
     ,level4_org_descr                   string         --组织4级(小片)  
     ,level5_org_id                      string         --组织5级(公司)  
     ,level5_org_descr                   string         --组织5级(公司)  
     ,level6_org_id                      string         --组织6级(OU)  
     ,level6_org_descr                   string         --组织6级(OU)  
     ,level7_org_id                      string         --组织7级(库存组织)
     ,level7_org_descr                   string         --组织7级(库存组织)
     ,level1_businesstype_id             string         --业态1级id
     ,level1_businesstype_name           string         --业态1级
     ,level2_businesstype_id             string         --业态2级id
     ,level2_businesstype_name           string         --业态2级
     ,level3_businesstype_id             string         --业态3级id
     ,level3_businesstype_name           string         --业态3级
     ,level4_businesstype_id             string         --业态4级id
     ,level4_businesstype_name           string         --业态4级
     ,production_line_id                 string         --产线        
     ,production_line_descr              string         --产线        
     ,level1_material_id                 string         --物料1级      
     ,level1_material_descr              string         --物料1级      
     ,level2_material_id                 string         --物料2级      
     ,level2_material_descr              string         --物料2级      
     ,level3_material_id                 string         --物料3级      
     ,level3_material_descr              string         --物料3级      
     ,level4_material_id                 string         --物料4级      
     ,level4_material_descr              string         --物料4级      
     ,level5_material_id                 string         --物料5级      
     ,level5_material_descr              string         --物料5级      
     ,level6_material_id                 string         --物料6级      
     ,level6_material_descr              string         --物料6级 
     ,inventory_item_id                  string         --物料品名ID
     ,inventory_item_desc                string         --物料品名名称
     ,defective_prods_weight             string         --次品产量（kg）  
     ,same_prod_weight                   string         --该部位总产量(kg)
     ,total_prod_weight                  string         --总产量(kg) 
     ,quality_prods_cnt                  string         --正品销量
     ,quality_prods_amt                  string         --正品总金额(元)  
     ,quality_prods_val                  string         --正品分配数值
     ,defective_prods_cnt                string         --次品销售
     ,defective_prods_amt                string         --次品总金额(元) 
     ,defective_prods_val                string         --次品分配数值
     ,create_time                        string         --数据推送时间    
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_QTY_COST_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_QTY_COST_MM PARTITION(op_month='$OP_MONTH')
SELECT '$OP_MONTH' month_id               --期间(月份) 
       ,substr(t1.period_id,1,6)   day_id                --期间(月) 
       ,t3.level1_org_id                        --组织1级(股份)  
       ,t3.level1_org_descr                     --组织1级(股份)  
       ,t3.level2_org_id                        --组织2级(片联)  
       ,t3.level2_org_descr                     --组织2级(片联)  
       ,t3.level3_org_id                        --组织3级(片区)  
       ,t3.level3_org_descr                     --组织3级(片区)  
       ,t3.level4_org_id                        --组织4级(小片)  
       ,t3.level4_org_descr                     --组织4级(小片)  
       ,t3.level5_org_id                        --组织5级(公司)  
       ,t3.level5_org_descr                     --组织5级(公司)  
       ,t3.level6_org_id                        --组织6级(OU)
       ,t3.level6_org_descr                     --组织6级(OU)
       ,t4.level7_org_id                        --组织7级(库存组织)
       ,t4.level7_org_descr                     --组织7级(库存组织)
       ,t7.level1_businesstype_id                     --业态1级id
       ,t7.level1_businesstype_name                   --业态1级
       ,t7.level2_businesstype_id                     --业态2级id
       ,t7.level2_businesstype_name                   --业态2级
       ,t7.level3_businesstype_id                     --业态3级id
       ,t7.level3_businesstype_name                   --业态3级
       ,t7.level4_businesstype_id                     --业态4级id
       ,t7.level4_businesstype_name                   --业态4级
       ,case when t1.product_line='10' then '1'
             when t1.product_line='20' then '2'
             else '' end production_line_id           --产线id
       ,case when t1.product_line='10' then '鸡线'
             when t1.product_line='20' then '鸭线'
             else '缺失' end production_line_descr    --产线定义
       ,t5.material_segment1_id                 --物料1级
       ,t5.material_segment1_desc               --
       ,concat(t5.material_segment1_id,t5.material_segment2_id) material_segment2_id                 
                                                --物料2级
       ,t5.material_segment2_desc               --
       ,concat(t5.material_segment1_id,t5.material_segment2_id,t5.material_segment3_id) material_segment3_id
                                               --物料3级
       ,t5.material_segment3_desc               --
       ,concat(t5.material_segment1_id,t5.material_segment2_id,t5.material_segment3_id,t5.material_segment4_id) material_segment4_id
                                                --物料4级
       ,t5.material_segment4_desc               --
       ,concat(t5.material_segment1_id,t5.material_segment2_id,t5.material_segment3_id,t5.material_segment4_id,t5.material_segment5_id) material_segment5_id
                                                --物料5级
       ,t5.material_segment5_desc               --
       ,concat(t5.material_segment1_id,t5.material_segment2_id,t5.material_segment3_id,t5.material_segment4_id,t5.material_segment5_id,t5.material_segment6_id) material_segment6_id
                                                --物料6级
       ,t5.material_segment6_desc               --
       ,t5.inventory_item_id                    --
       ,t5.inventory_item_desc                  --物料品名名称
       ,t1.primary_quantity   defective_prods_weight   --次品产量（kg）
       ,case when t5.material_segment4_desc in ('鸡头类','鸡爪类','鸭掌类','鸭舌类') then t1.primary_quantity_4
             else t1.primary_quantity_5 end same_prod_weight         --该部位总产量(kg)
       ,t1.primary_quantity_2  total_prod_weight        --总产量(kg)
       ,case when t5.material_segment4_desc in ('鸡头类','鸡爪类','鸭掌类','鸭舌类') then t2.quality_prods_cnt_4
             else t2.quality_prods_cnt end quality_prods_cnt
       ,case when t5.material_segment4_desc in ('鸡头类','鸡爪类','鸭掌类','鸭舌类') then t2.quality_prods_amt_4
             else t2.quality_prods_amt end quality_prods_amt
       ,t2.quality_prods_val
       ,t2.defective_prods_cnt
       ,t2.defective_prods_amt
       ,t2.defective_prods_val
       ,'$CREATE_TIME' create_time
  FROM (select * from $TMP_DMF_BIRD_QTY_COST_MM_1 where op_day='$OP_DAY') t1
  LEFT JOIN (select * from $TMP_DMF_BIRD_QTY_COST_MM_2 where op_day='$OP_DAY') t2
      ON (t1.item_id=t2.inventory_item_id and substr(t1.period_id,1,6)=t2.month_id and t1.org_id=t2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t3
    ON (t1.org_id=t3.org_id and t1.bus_type=t3.bus_type_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t4
    ON (t1.organization_id=t4.inv_org_id)
  LEFT JOIN (SELECT * 
               FROM mreport_global.dwu_dim_material_new) t5
    ON (t1.item_id=t5.inventory_item_id and t1.organization_id=t5.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t7
    ON (t1.bus_type=t7.level4_businesstype_id)
WHERE t1.is_d_product='Y'       --选出残次品品类
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMF_BIRD_QTY_COST_MM_1;
    $INSERT_TMP_DMF_BIRD_QTY_COST_MM_1;
    $CREATE_TMP_DMF_BIRD_QTY_COST_MM_2;
    $INSERT_TMP_DMF_BIRD_QTY_COST_MM_2;
    $CREATE_DMF_BIRD_QTY_COST_MM;
    $INSERT_DMF_BIRD_QTY_COST_MM;
"  -v