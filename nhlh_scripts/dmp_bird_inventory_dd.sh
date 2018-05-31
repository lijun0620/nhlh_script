#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_inventory_dd.sh                               
# 创建时间: 2018年4月18日                                            
# 创 建 者: gl                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 供销存
# 修改说明:                                                          
######################################################################


OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_inventory_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 建立临时表，用于存放TZ01的产量
TMP_DMP_BIRD_INVENTORY_DD_1='tmp_dmp_bird_inventory_dd_1'

CREATE_TMP_DMP_BIRD_INVENTORY_DD_1="
CREATE TABLE IF NOT EXISTS TMP_DMP_BIRD_INVENTORY_DD_1(    
      day_id                          string       --日期
      ,org_id                          string      --组织6级ouid  
      ,organization_id                 string      --库存组织Id	  
      ,bus_type                        string      --业态4级
      ,production_line_id              string      --产线id
	  ,item_id                         string      --成品物料Id
	 

      ,value                           string      --当日成品入库数

)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_INVENTORY_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_1 PARTITION(op_day='$OP_DAY')
SELECT  
    period_id,
     org_id,
	 inv_org_id,
     bus_type,
     product_line,
     final_inventory_item_id, 
	
    
      sum(trans_qty) as value
     from mreport_poultry.dwu_qtz_fresh_goods_dd
 WHERE op_day='$OP_DAY' and TRANS_TYPE_NAME not like '%利润分析鲜品杂项入库%'
  
 GROUP BY
     period_id,
      org_id,
	  inv_org_id,
      bus_type,
      product_line,
      final_inventory_item_id
	 
  
"	 

###########################################################################################
## 建立临时表，用于存放TZ02的鲜品产量
TMP_DMP_BIRD_INVENTORY_DD_2='tmp_dmp_bird_inventory_dd_2'

CREATE_TMP_DMP_BIRD_INVENTORY_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_INVENTORY_DD_2(    
      day_id                          string       --日期
      ,org_id                          string      --组织6级ouid    
	  ,organization_id                 string      --库存组织Id
      ,bus_type                        string      --业态4级
	  
      ,production_line_id              string      --产线id
      ,item_id                         string      --物料id

      
      ,value                           string      --当日鲜品入库数
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_INVENTORY_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_2 PARTITION(op_day='$OP_DAY')
SELECT
     period_id as day_id,  
     org_id,
	 organization_id,
     bus_type,
     product_line, 
	 item_id,

   
     sum(nvl(primary_quantity,0)) as value
  FROM

   mreport_poultry.dwu_tz_storage_transation02_dd where op_day='$OP_DAY' 
   AND subinventory_code like '%XP%' 
 
  GROUP BY
     period_id,  
     org_id,
	 organization_id,
     bus_type,
     product_line,
	 item_id

    
"



###########################################################################################
## 建立临时表，用于存放xs01的销量
TMP_DMP_BIRD_INVENTORY_DD_3='tmp_dmp_bird_inventory_dd_3'
CREATE_TMP_DMP_BIRD_INVENTORY_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_INVENTORY_DD_3(    
      day_id                          string      --日期
      ,org_id                          string      --组织6级ouid    
	  ,organization_id                 string      --库存组织Id
      ,bus_type                        string      --业态4级
      ,production_line_id              string      --产线id
      ,item_id                         string      --物料Id
	  

      ,value                           string      --当日销量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_INVENTORY_DD_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_3 PARTITION(op_day='$OP_DAY')
SELECT
     approve_date,  
     org_id,
	 organization_id,
     bus_type,
     product_line, 
     item_id,
	
   
     sum(nvl(out_main_qty,0)) as value
 FROM

     mreport_poultry.dwu_gyl_xs01_dd where op_day='$OP_DAY'

 
 GROUP BY
     approve_date,  
     org_id,
	 organization_id,
     bus_type,
     product_line, 
	 item_id

   
    "
	
###########################################################################################
## 建立临时表，用于存放xs03的库存量

TMP_DMP_BIRD_INVENTORY_DD_4='tmp_dmp_bird_inventory_dd_4'
CREATE_TMP_DMP_BIRD_INVENTORY_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_INVENTORY_DD_4(    
      day_id                           string      --日期
      ,org_id                          string      --组织6级ouid    
	  ,organization_id                 string      --库存组织Id
      ,bus_type                        string      --业态4级
      ,production_line_id              string      --产线id
	  ,item_id                         string      --物料id

      
      ,value                           string      --当日库存量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"

############################################################插入数据
INSERT_TMP_DMP_BIRD_INVENTORY_DD_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_4 PARTITION(op_day='$OP_DAY')
SELECT 
     t1.day_id,
     t4.ou_org_id,
	 t1.organization_id,
     t1.bus_type,
     t1.product_line,
     t1.item_id,
	
     t1.store_cnt as value 
FROM
(SELECT 
      regexp_replace(date_sub(substr(available_stock_time,1,10),1),'-','') as day_id,
      organization_id,
      item_id,
      bus_type,
      product_line ,
	  
       sum(nvl(finish_product_stock,0)+nvl(fresh_normal_sto_count,0)) as store_cnt
      
 FROM mreport_poultry.dwu_xs_xs03_dd  WHERE op_day<=regexp_replace(CURRENT_DATE,'-','')
  GROUP BY      
       regexp_replace(date_sub(substr(available_stock_time,1,10),1),'-',''),
       organization_id,
       item_id,
       bus_type,
       product_line
       	   
 ) t1 

  

LEFT JOIN
(SELECT inv_org_id,level7_org_id,level7_org_descr,ou_org_id FROM mreport_global.dim_org_inv_management)t4
 ON (t1.organization_id=t4.inv_org_id)

	
	
"
  
  #######################################建立临时表，用于存放产量
  TMP_DMP_BIRD_INVENTORY_DD_5='tmp_dmp_bird_inventory_dd_5'
  CREATE_TMP_DMP_BIRD_INVENTORY_DD_5="
  CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_INVENTORY_DD_5(    
      day_id                           string      --日期
      ,org_id                          string      --组织6级ouid  
      ,organization_id                 string      --库存组织Id	  
      ,bus_type                        string      --业态4级
      ,production_line_id              string      --产线id
      ,item_id                         string      --物料id
	 
    
      ,value                           string      --当日产量
   )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS ORC"
   
   
   
   #插入数据
   INSERT_TMP_DMP_BIRD_INVENTORY_DD_5="
   INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_5 PARTITION(op_day='$OP_DAY')
   SELECT 
       t1.day_id,
	   t1.org_id,
	   t1.organization_id,
       t1.bus_type,
       t1.production_line_id,
       t1.item_id,
	
     
       sum(cp+fresh)as value   
   FROM 
     (select 
	   day_id,
	   org_id,
	   organization_id,
       bus_type,
       production_line_id,
       item_id,
	   
       value as fresh,   
	   0 as cp
	 from $TMP_DMP_BIRD_INVENTORY_DD_1 where op_day='$OP_DAY'
	  UNION ALL
	   select  
	   day_id,
	   org_id,
	   organization_id,
       bus_type,
       production_line_id,
	   item_id,
	 
	   0 as fresh,
	   value as cp
       from $TMP_DMP_BIRD_INVENTORY_DD_2 where op_day='$OP_DAY'
	 )t1
	 GROUP BY
	   t1.day_id,
	   t1.org_id,
	   t1.organization_id,
       t1.bus_type,
       t1.production_line_id,
       t1.item_id
	  
    
   "
	
  #######################################################################创建临时表存放3张表的union all结果
  TMP_DMP_BIRD_INVENTORY_DD_6='tmp_dmp_bird_inventory_dd_6'
  CREATE_TMP_DMP_BIRD_INVENTORY_DD_6="
  CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_INVENTORY_DD_6(    
       day_id                          string      --日期
      ,org_id                          string      --组织6级ouid
      ,organization_id                 string      --库存组织Id	  
      ,bus_type                        string      --业态4级
      ,production_line_id              string      --产线id
	  ,tz_item_id                      string      --业务原表当中的物料id
	  ,item_id                         string      --物料Id
	  ,item_code                       string      --物料code
	  ,item_descr                      string      --物料描述
      ,level1_prod_id                  string      --产品线一级id
      ,level1_prod_descr               string      --产品线一级
      ,level2_prod_id                  string      --产品线二级id
      ,level2_prod_descr               string      --产品线二级
      ,level1_prodtype_id              string      --产品分类一级id
      ,level1_prodtype_descr           string      --产品分类一级
      ,level2_prodtype_id              string      --产品分类二级id
      ,level2_prodtype_descr           string      --产品分类二级
      ,level3_prodtype_id              string      --产品分类三级id
      ,level3_prodtype_descr           string      --产品分类三级
      ,prod_cnt		                   string      --产量	
      ,sale_cnt	                       string      --销量
      ,store_cnt		               string      --业务库存	
    )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS ORC"
    ###############################################插入数据
    INSERT_TMP_DMP_BIRD_INVENTORY_DD_6="
    INSERT OVERWRITE TABLE $TMP_DMP_BIRD_INVENTORY_DD_6 PARTITION(op_day='$OP_DAY')
    SELECT
	t4.day_id,
	t4.org_id,
	t4.organization_id,
    t4.bus_type,
    t4.production_line_id,
	t4.item_id,
	ebs.inventory_item_id,
	ebs.inventory_item_code,
	ebs.inventory_item_desc,
    t5.prd_line_cate_id,
    t5.prd_line_cate,
	t5.sub_prd_line_tp_id,
    t5.sub_prd_line_tp,	   
	t5.first_lv_tp_id,  
	t5.first_lv_tp,
    t5.scnd_lv_tp_id, 
    t5.scnd_lv_tp,
    t5.thrd_lv_tp_id, 
    t5.thrd_lv_tp,
	sum(t4.prod_cnt),
	sum(t4.sale_cnt),
    sum(t4.store_cnt)	   	
	FROM	  
	  (SELECT 
	   day_id,
	   org_id,
	   organization_id,
       bus_type,
       production_line_id,
	   item_id,
	
     
	   0 as prod_cnt,
	   0 as sale_cnt,
       value as store_cnt   	 
	   from $TMP_DMP_BIRD_INVENTORY_DD_4 where op_day='$OP_DAY'
	   union all
	   SELECT 
	   
	   day_id,
	   org_id,
	   organization_id,
       bus_type,
       production_line_id,
       item_id,
	
     
	   0 as prod_cnt,
	   value as sale_cnt,
       0 as store_cnt   	 
	   from $TMP_DMP_BIRD_INVENTORY_DD_3 where op_day='$OP_DAY'
	   union all
	   SELECT 

	   day_id,
	   org_id,
	   organization_id,
       bus_type,
       production_line_id,
	   item_id,
	 
     
	   value as prod_cnt,
	   0 as sale_cnt,
       0 as store_cnt 
	   from $TMP_DMP_BIRD_INVENTORY_DD_5 where op_day='$OP_DAY'
      )t4
	  LEFT JOIN
	  (SELECT * FROM mreport_global.dwu_dim_material_new where inv_org_id='115')ebs
	  ON(t4.item_id=ebs.inventory_item_id)
	  
	
	  LEFT JOIN
      (SELECT 
	  item_id,
      item_code,
      prd_line_cate,
      prd_line_cate_id,
      sub_prd_line_tp,
      sub_prd_line_tp_id,
      first_lv_tp_id,
      first_lv_tp,
      scnd_lv_tp_id,
      scnd_lv_tp,
      thrd_lv_tp_id,
      thrd_lv_tp
       FROM mreport_global.dim_crm_item )t5
  ON(t4.item_id=ebs.inventory_item_id and ebs.inventory_item_code=t5.item_code)
	  
      GROUP BY
	t4.day_id,
	t4.org_id,
	t4.organization_id,
    t4.bus_type,
    t4.production_line_id,
	t4.item_id,
	ebs.inventory_item_id,
	ebs.inventory_item_code,
	ebs.inventory_item_desc,
    t5.prd_line_cate_id,
    t5.prd_line_cate,
	t5.sub_prd_line_tp_id,
    t5.sub_prd_line_tp,	   
	t5.first_lv_tp_id,  
	t5.first_lv_tp,
    t5.scnd_lv_tp_id, 
    t5.scnd_lv_tp,
    t5.thrd_lv_tp_id, 
    t5.thrd_lv_tp
    "
  ###########################################################################################
  ## 建立报表，用于存放最终的需求指标
  DMP_BIRD_INVENTORY_DD='dmp_bird_inventory_dd'
  CREATE_DMP_BIRD_INVENTORY_DD="
  CREATE TABLE IF NOT EXISTS $DMP_BIRD_INVENTORY_DD(
    month_id	             string	   --期间(月)
   ,day_id		             string	   --期间(日)
   ,level1_org_id	         string    --组织1级(股份)	
   ,level1_org_descr	     string    --组织1级(股份)		
   ,level2_org_id	         string    --组织2级(片联)	
   ,level2_org_descr	     string    --组织2级(片联)		
   ,level3_org_id	         string    --组织3级(片区)	
   ,level3_org_descr         string    --组织3级(片区)
   ,level4_org_id	         string    --组织4级(小片)
   ,level4_org_descr         string    --组织4级(小片)
   ,level5_org_id            string    --组织5级(公司)
   ,level5_org_descr	     string    --组织5级(公司)
   ,level6_org_id	         string    --组织6级(OU)
   ,level6_org_descr	     string    --组织6级(OU)
   ,level7_org_id	         string    --组织7级(库存组织)	
   ,level7_org_descr	     string    --组织7级(库存组织)
   ,level1_businesstype_id   string    --业态1级id
   ,level1_businesstype_name string    --业态1级
   ,level2_businesstype_id   string    --业态2级
   ,level2_businesstype_name string    --业态2级	
   ,level3_businesstype_id   string    --业态3级	
   ,level3_businesstype_name string    --业态3级	
   ,level4_businesstype_id   string    --业态4级	
   ,level4_businesstype_name string    --业态4级	
   ,production_line_id	     string    --产线
   ,production_line_descr    string    --产线	
   ,level1_prod_id	         string    --产品线一级
   ,level1_prod_descr	     string    --产品线1级
   ,level2_prod_id           string    --产品线2级	
   ,level2_prod_descr	     string    --产品线2级	
   ,level1_prodtype_id	     string    --产品分类1级
   ,level1_prodtype_descr    string    --产品分类1级
   ,level2_prodtype_id	     string    --产品分类2级
   ,level2_prodtype_descr    string    --产品分类2级
   ,level3_prodtype_id	     string    --产品分类3级
   ,level3_prodtype_descr    string    --产品分类3级
   ,inventory_item_id        string    --物料Id
   ,inventory_item_desc      string    --物料名称
   ,prod_cnt		         string    --产量	
   ,sale_cnt	             string    --销量
   ,store_cnt		         string    --业务库存	
   ,create_time	             string    --数据推送时间	
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE"

###############################################插入数据
INSERT_DMP_BIRD_INVENTORY_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_INVENTORY_DD PARTITION(op_day='$OP_DAY')
SELECT
  substr(t1.day_id,1,6) as month_id,
  t1.day_id, 
  case when t5.level1_org_id    is null then coalesce(t6.level1_org_id,'-1') else coalesce(t5.level1_org_id,'-1')  end as level1_org_id,                --一级组织编码
  case when t5.level1_org_descr is null then coalesce(t6.level1_org_descr,'缺失') else coalesce(t5.level1_org_descr,'缺失')  end as level1_org_descr,   --一级组织描述
  case when t5.level2_org_id is null    then coalesce(t6.level2_org_id,'-1') else coalesce(t5.level2_org_id,'-1')  end as level2_org_id,                --二级组织编码
  case when t5.level2_org_descr is null then coalesce(t6.level2_org_descr,'缺失') else coalesce(t5.level2_org_descr,'缺失')  end as level2_org_descr,   --二级组织描述
  case when t5.level3_org_id    is null then coalesce(t6.level3_org_id,'-1') else coalesce(t5.level3_org_id,'-1')  end as level3_org_id,                --三级组织编码
  case when t5.level3_org_descr is null then coalesce(t6.level3_org_descr,'缺失') else coalesce(t5.level3_org_descr,'缺失')  end as level3_org_descr,   --三级组织描述
  case when t5.level4_org_id    is null then coalesce(t6.level4_org_id,'-1') else coalesce(t5.level4_org_id,'-1')  end as level4_org_id,                --四级组织编码
  case when t5.level4_org_descr is null then coalesce(t6.level4_org_descr,'缺失') else coalesce(t5.level4_org_descr,'缺失')  end as level4_org_descr,   --四级组织描述
  case when t5.level5_org_id    is null then coalesce(t6.level5_org_id,'-1') else coalesce(t5.level5_org_id,'-1')  end as level5_org_id,                --五级组织编码
  case when t5.level5_org_descr is null then coalesce(t6.level5_org_descr,'缺失') else coalesce(t5.level5_org_descr,'缺失')  end as level5_org_descr,   --五级组织描述
  case when t5.level6_org_id    is null then coalesce(t6.level6_org_id,'-1') else coalesce(t5.level6_org_id,'-1')  end as level6_org_id,                --六级组织编码
  case when t5.level6_org_descr is null then coalesce(t6.level6_org_descr,'缺失') else coalesce(t5.level6_org_descr,'缺失')  end as level6_org_descr,   --六级组织描述
  t3.level7_org_id,
  t3.level7_org_descr,
  t2.level1_businesstype_id,
  t2.level1_businesstype_name,
  t2.level2_businesstype_id,
  t2.level2_businesstype_name,
  t2.level3_businesstype_id,
  t2.level3_businesstype_name,
  t2.level4_businesstype_id,
  t2.level4_businesstype_name,
  case t1.production_line_id
  when '10' then '1'
  when '20' then '2'
  else '-1'
  end as production_line_id,
  case t1.production_line_id
  when '10' then '鸡线'
  when '20' then '鸭线'
  else '缺失'
  end as production_line_descr,
  t1.level1_prod_id,                 
  t1.level1_prod_descr,              
  t1.level2_prod_id,                 
  t1.level2_prod_descr,              
  t1.level1_prodtype_id,             
  t1.level1_prodtype_descr,          
  t1.level2_prodtype_id,             
  t1.level2_prodtype_descr,          
  t1.level3_prodtype_id,             
  t1.level3_prodtype_descr, 
  t1.item_id,  
  t1.item_descr,
 
  t1.prod_cnt,
  t1.sale_cnt,
  t1.store_cnt,
  $CREATE_TIME
  
 FROM 
 (select * from $TMP_DMP_BIRD_INVENTORY_DD_6 where op_day='$OP_DAY')t1
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
 FROM mreport_global.dim_org_businesstype)t2
 ON (t1.bus_type=t2.level4_businesstype_id)
 LEFT JOIN 
 (SELECT * FROM mreport_global.dim_org_inv_management)t3
 ON(t1.organization_id=t3.inv_org_id)
 
  
  
  LEFT JOIN
   (select * from mreport_global.dim_org_management)t5
   ON(t1.org_id=t5.org_id and t5.attribute5='1')
   LEFT JOIN
    (select * from mreport_global.dim_org_management)t6
   ON(t1.org_id=t6.org_id and t6.attribute5='2' and t1.bus_type=t6.bus_type_id)
  
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_INVENTORY_DD_1;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_1;
    $CREATE_TMP_DMP_BIRD_INVENTORY_DD_2;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_2;
    $CREATE_TMP_DMP_BIRD_INVENTORY_DD_3;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_3;
    $CREATE_TMP_DMP_BIRD_INVENTORY_DD_4;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_4;
	$CREATE_TMP_DMP_BIRD_INVENTORY_DD_5;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_5;
	$CREATE_TMP_DMP_BIRD_INVENTORY_DD_6;
    $INSERT_TMP_DMP_BIRD_INVENTORY_DD_6;
    $CREATE_DMP_BIRD_INVENTORY_DD;
    $INSERT_DMP_BIRD_INVENTORY_DD;
    "  -v

############################################################插入数据

   
   
  


   
   
  

