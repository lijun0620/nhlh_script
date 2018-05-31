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
## 建立临时表，用于计算销量
TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1='TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --销量
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

############################################################################################
##查询库存主要信息并放入以上临时表
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1 PARTITION(op_day='$OP_DAY')
SELECT 
 t1.approve_month,
 t1.approve_date,
 t1.production_line_id,
 t1.production_line_descr,
 t1.bus_type,
 t1.org_id,
 t1.organization_id,
 t1.item_id,
 sum(sale_cnt)
 FROM
(SELECT
   substr(approve_date,1,6) as approve_month,                                    --期间月
  approve_date as approve_date,                                                --期间日
   product_line as production_line_id,                      --产线id
   case  product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                            --产线描述
   bus_type as bus_type,                                     --业态
   org_id  as org_id,                                        --公司id
   organization_id as organization_id,                       --库存组织id
   item_id as item_id,                                       --物料
   sum(nvl(out_main_qty,0)) as sale_cnt                           --销量
 
  FROM
  mreport_poultry.dwu_gyl_xs01_dd where op_day='$OP_DAY' and approve_date is not null
  GROUP BY

  substr(approve_date,1,6),                                    
   approve_date,                                                
   product_line,                     
   case  product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end,                          
   bus_type,                                                 
   org_id,                                                   
   organization_id,                                         
   item_id         
  UNION ALL
  SELECT 
    substr(transaction_date,1,6) as approve_month,
	transaction_date as approve_date,
	product_line as production_line_id,
    case  product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                            --产线描述
   bus_type as bus_type,                                     --业态
   org_id  as org_id,                                        --公司id
   organization_id as organization_id,                       --库存组织id
   material_id as item_id,       
   sum(nvl(primary_quantity,0)*-1) as sale_cnt

   FROM DWU_ZQ_KC01_DD where tr_tp_name='Direct Org Transfer' and op_day='$OP_DAY'
   GROUP BY   

   substr(transaction_date,1,6),
	transaction_date,
	product_line,
    case  product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end,                            
   bus_type,                                     
   org_id,                                        
   organization_id,                       
   material_id       
  
  )t1
  GROUP BY
  t1.approve_month,
  t1.approve_date,
  t1.production_line_id,
  t1.production_line_descr,
  t1.org_id,
  t1.bus_type,
  t1.organization_id,
  t1.item_id  
                                                    
"

############################################################################################
## 建立临时表，用于计算销售总金额
TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1='TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --销售总金额
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

############################################################################################
##
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1 PARTITION(op_day='$OP_DAY')
SELECT
   substr(approve_date,1,6),                                    --期间月
   approve_date,                                                --期间日
   product_line as production_line_id,                      --产线id
   case product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                            --产线描述
   bus_type,                                                 --业态
   org_id,                                                   --公司id
   organization_id,                                          --库存组织id
   item_id,                                                  --物料
   sum(nvl(out_qty,0)*nvl(execute_price,0))                  --销售金额
  
FROM
  mreport_poultry.DWU_GYL_XS01_DD where op_day='$OP_DAY' 
  GROUP BY
  substr(approve_date,1,6),                                    
   approve_date,                                                
   product_line,                     
   case  product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end,                          
   bus_type,                                                 
   org_id,                                                   
   organization_id,                                         
   item_id                                   
"


############################################################################################
## 建立临时表，用于计算库存量
TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1='tmp_dmp_bird_main_invturn_dd_3_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --时点库存量
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

############################################################################################
##查询库存主要信息并放入以上临时表
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1 PARTITION(op_day='$OP_DAY')
SELECT
  substr(regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),1,6),
  regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-','')  as day_id,
  t1.product_line as production_line_id,                           --产线id
   case  t1.product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                               --产线描述
   t1.bus_type,
   t2.ou_org_id,
 
   t1.organization_id,
   t1.item_id,
  sum(nvl(t1.finish_product_stock,0))

 
FROM 
(SELECT * from mreport_poultry.dwu_xs_xs03_dd )t1
LEFT JOIN
(SELECT inv_org_id,level7_org_descr,level7_org_id,ou_org_id FROM mreport_global.dim_org_inv_management)t2
ON(t1.organization_id=t2.inv_org_id )

GROUP BY
 substr(regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),1,6),
  regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),
  t1.product_line,                                                 
   case  t1.product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end ,                                                                  
   t1.bus_type,
   t2.ou_org_id,
   t1.item_id,
   t1.organization_id
"


#############################算库存总量  
TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1='tmp_dmp_bird_main_invturn_dd_4_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --库存量
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

############################################################################################
##查询库存主要信息并放入以上临时表
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1 PARTITION(op_day='$OP_DAY')
SELECT
  substr(regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),1,6),
  regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-','')  as day_id,
  t1.product_line as production_line_id,                           --产线id
   case  t1.product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end  as production_line_descr,                               --产线描述
   t1.bus_type,
   t2.ou_org_id,
 
   t1.organization_id,
   t1.item_id,
  sum(nvl(t1.fresh_normal_sto_count,0)+nvl(t1.finish_product_stock,0))

 
FROM 
(SELECT * from mreport_poultry.dwu_xs_xs03_dd)t1
LEFT JOIN
(SELECT inv_org_id,level7_org_descr,level7_org_id,ou_org_id FROM mreport_global.dim_org_inv_management)t2
ON(t1.organization_id=t2.inv_org_id)


GROUP BY
 substr(regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),1,6),
  regexp_replace(date_sub(substr(t1.available_stock_time,1,10),1),'-',''),
  t1.product_line,                                                 
   case  t1.product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失'                                                      
   end ,                                                                  
   t1.bus_type,
   t2.ou_org_id,
   t1.item_id,
   t1.organization_id
"


## 建立临时表，用于计算库存毛利
TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1='TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --库存毛利
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"


############################################################################################
##插入数据
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1 PARTITION(op_day='$OP_DAY')
SELECT
  t1.month_id,
  t1.day_id,
  t1.production_line_id,
  t1.production_line_descr,
  t1.bus_type,
  t1.org_id,
  t1.organization_id,
  t1.item_id,
  round(t1.value*(nvl(t5.price_with_tax,0)/(1+nvl(t3.percentage_rate,0)/100)-nvl(t4.cost_amount_t,0)),4)                                            
FROM
(SELECT * FROM $TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1 where op_day='$OP_DAY')t1
  
  LEFT  JOIN 
  (SELECT * FROM mreport_global.dwu_dim_material_new)t2
  ON(t1.item_id=t2.inventory_item_id and t1.organization_id=t2.inv_org_id)
  LEFT JOIN
  (SELECT 
   tax_rate_code,
   percentage_rate
   from mreport_global.ods_ebs_zx_rates_b) t3
  ON
  (t1.item_id=t2.inventory_item_id and t1.organization_id=t2.inv_org_id and t2.tax_code=t3.tax_rate_code)
  LEFT JOIN 
  (SELECT org_id,organization_id,regexp_replace(period_code,'-','') as period_code,item_id,max(nvl(item_cost,0)) as cost_amount_t from mreport_poultry.dwu_cw_cw20_dd where op_day='$OP_DAY'
   GROUP BY 
   org_id,
   organization_id,
   regexp_replace(period_code,'-',''),
   item_id
  )t4
  ON (t1.item_id=t4.item_id and t1.organization_id=t4.organization_id and t1.month_id=t4.period_code)
  
  LEFT JOIN 
  (SELECT
   t5_2.period_id as period_id,
   t5_2.item_id as item_id,
   t5_1.price_with_tax,
   t5_2.org_id 
   
   FROM  
     (
	 SELECT
	 period_id as period_id,
   org_id as org_id,
   item_id as item_id,
   nvl(price_with_tax,0) as price_with_tax,
   to_date(update_date) as day_id,
   source
   from mreport_poultry.dwu_gyl_xs07_dd
   where op_day='$OP_DAY')t5_1
   
   JOIN
   (SELECT period_id,max(to_date(update_date)) as day_id,org_id,item_id,source from mreport_poultry.dwu_gyl_xs07_dd
   where op_day='$OP_DAY' GROUP BY period_id,org_id,item_id,source)t5_2
   ON(t5_1.day_id=t5_2.day_id and t5_1.period_id=t5_2.period_id and t5_1.org_id=t5_2.org_id and t5_1.item_id=t5_2.item_id and t5_1.source=t5_2.source)
  )t5
  ON(t1.org_id=t5.org_id and t1.item_id=t5.item_id and t1.month_id=regexp_replace(t5.period_id,'-',''))
"
 
 ###################算超期库存
TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1='tmp_dmp_bird_main_invturn_dd_6_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1(
       month_id                         string      --期间(月份)
      ,day_id                           string      --期间(日)
      ,production_line_id               string      --产线id
      ,production_line_descr            string      --产线描述
      ,bus_type                         string      --业态
      ,org_id                           string      --公司id
      ,organization_id                  string      --库存组织id
      ,item_id                          string      --物料id
      ,value                            string      --超期库存
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

 INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1="
 INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1 PARTITION(op_day='$OP_DAY')
 SELECT 
   t1.month_id,
   t1.day_id,
   t1.product_line,
   t1.production_line_descr,
   t1.bus_type,
   t1.org_id,
   t1.organization_id,
   t1.item_id,
   t1.paper_store_cnt
   FROM

 (SELECT
   regexp_replace(substr(cur_date,1,7),'-','') as month_id,               --现有日期月
   regexp_replace(cur_date,'-','') as day_id,                                                  --现有日期日
   product_line as product_line,                                              --产线id
   case product_line 
   when '10' then '鸡线'
   when '20' then '鸭线'
   else '缺失' 
   end as production_line_descr,                                                      --产线描述
   bus_type,                                                 --业态
   org_id,                                                   --公司id
   organization_id,                                          --库存组织id
   item_id,                                                  --物料
   case when quantity is not null and datediff(cur_date,to_date(orig_date_received))>=90 then quantity
   else 0 end as paper_store_cnt                             --账面超期库存量 
 
   FROM
   mreport_poultry.dwu_xs_xs04_dd where op_day='$OP_DAY' and product_line is not null and bus_type='132020'
   )t1
   LEFT JOIN
   (SELECT * FROM mreport_global.dim_org_inv_management)t2
   ON(t1.organization_id=t2.inv_org_id)
 
 
   where t2.level7_org_descr not like '%非生产%'
   
   
   "




  ##################################union all所有临时表
  TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1='tmp_dmp_bird_main_invturn_dd_7_1'

CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1(
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
      ,sale_total_amt                   string      --销售金额
      ,sale_cost_amt                    string      --库存毛利
      ,paper_store_cnt                  string      --账面超期库存量
     
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1="
 INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1 PARTITION(op_day='$OP_DAY')
 SELECT
    tmp.month_id,
	tmp.day_id,
	tmp.production_line_id,
	tmp.production_line_descr,
	tmp.bus_type, 
	tmp.org_id,
	tmp.organization_id ,
	tmp.item_id,
    sum(tmp.sale_cnt),
	sum(tmp.store_cnt),
	sum(tmp.sale_total_amt),
	sum(tmp.sale_cost_amt),
	sum(tmp.paper_store_cnt)
 FROM 
  (SELECT 
	   month_id,
	   day_id,
	   production_line_id,
	   production_line_descr,
	   bus_type, 
	   org_id,
	   organization_id ,
	   item_id,
	   value as sale_cnt,
       0 as store_cnt ,
       0 as sale_total_amt,	 
       0 as sale_cost_amt,
       0 as paper_store_cnt
	   from $TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1 where op_day='$OP_DAY'
	   union all
	   SELECT
	    month_id,
	   day_id,
	   production_line_id,
	   production_line_descr,
	   bus_type, 
	   org_id,
	   organization_id ,
	   item_id,
	   0 as sale_cnt,
       0 as store_cnt ,
       value as sale_total_amt,	 
       0 as sale_cost_amt,
       0 as paper_store_cnt
	   from $TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1 where op_day='$OP_DAY'
	   union all
	   SELECT
	   month_id,
	   day_id,
	   production_line_id,
	   production_line_descr,
	   bus_type, 
	   org_id,
	   organization_id ,
	   item_id,
	   0 as sale_cnt,
       value as store_cnt ,
       0 as sale_total_amt,	 
       0 as sale_cost_amt,
       0 as paper_store_cnt
	   from $TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1 where op_day='$OP_DAY'
	   union all
	      
	   SELECT
	   month_id,
	   day_id,
	   production_line_id,
	   production_line_descr,
	   bus_type, 
	   org_id,
	   organization_id ,
	   item_id,
	   0 as sale_cnt,
       0 as store_cnt ,
       0 as sale_total_amt,	 
       value as sale_cost_amt,
       0 as paper_store_cnt
	   from $TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1 where op_day='$OP_DAY'
	   union all	   
	   SELECT
	   month_id,
	   day_id,
	   production_line_id,
	   production_line_descr,
	   bus_type, 
	   org_id,
	   organization_id ,
	   item_id,
	   0 as sale_cnt,
       0 as store_cnt ,
       0 as sale_total_amt,	 
       0 as sale_cost_amt,
       value as paper_store_cnt
	   from $TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1 where op_day='$OP_DAY')tmp
	   GROUP BY
	    tmp.month_id,
	    tmp.day_id,
	    tmp.production_line_id,
	    tmp.production_line_descr,
	    tmp.bus_type, 
	    tmp.org_id,
	    tmp.organization_id ,
	    tmp.item_id   
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
   t1.month_id,
   t1.day_id,
   case when t2.level1_org_id    is null then coalesce(t6.level1_org_id,'-1') else coalesce(t2.level1_org_id,'-1')  end as level1_org_id,                
   case when t2.level1_org_descr is null then coalesce(t6.level1_org_descr,'缺失') else coalesce(t2.level1_org_descr,'缺失')  end as level1_org_descr,   
   case when t2.level2_org_id    is null then coalesce(t6.level2_org_id,'-1') else coalesce(t2.level2_org_id,'-1')  end as level2_org_id,                
   case when t2.level2_org_descr is null then coalesce(t6.level2_org_descr,'缺失') else coalesce(t2.level2_org_descr,'缺失')  end as level2_org_descr,   
   case when t2.level3_org_id    is null then coalesce(t6.level3_org_id,'-1') else coalesce(t2.level3_org_id,'-1')  end as level3_org_id,                
   case when t2.level3_org_descr is null then coalesce(t6.level3_org_descr,'缺失') else coalesce(t2.level3_org_descr,'缺失')  end as level3_org_descr,
   case when t2.level4_org_id    is null then coalesce(t6.level4_org_id,'-1') else coalesce(t2.level4_org_id,'-1')  end as level4_org_id,                
   case when t2.level4_org_descr is null then coalesce(t6.level4_org_descr,'缺失') else coalesce(t2.level4_org_descr,'缺失')  end as level4_org_descr,   
   case when t2.level5_org_id    is null then coalesce(t6.level5_org_id,'-1') else coalesce(t2.level5_org_id,'-1')  end as level5_org_id,                
   case when t2.level5_org_descr is null then coalesce(t6.level5_org_descr,'缺失') else coalesce(t2.level5_org_descr,'缺失')  end as level5_org_descr,   
   case when t2.level6_org_id    is null then coalesce(t6.level6_org_id,'-1') else coalesce(t2.level6_org_id,'-1')  end as level6_org_id,        
   case when t2.level6_org_descr is null then coalesce(t6.level6_org_descr,'缺失') else coalesce(t2.level6_org_descr,'缺失')  end as level6_org_descr,
   t7.level7_org_id,
   t7.level7_org_descr,
   t3.level1_businesstype_id,                                   
   t3.level1_businesstype_name,                                
   t3.level2_businesstype_id,                          
   t3.level2_businesstype_name,                                 
   t3.level3_businesstype_id,                                  
   t3.level3_businesstype_name,
   t3.level4_businesstype_id,   
   t3.level4_businesstype_name,                                 
   case t1.production_line_id when '10' then '1'
   when '20' then '2'
   else '-1'
   end as production_line_id,                                     
   t1.production_line_descr,                                    
   t5.prd_line_cate_id as level1_prod_id,                       
   t5.prd_line_cate as level1_prod_descr,                       
   t5.sub_prd_line_tp_id as level2_prod_id,                     
   t5.sub_prd_line_tp   as level2_prod_descr,                   
   t5.first_lv_tp_id as level1_prodtype_id,                     
   t5.first_lv_tp as level1_prodtype_descr,                     
   t5.scnd_lv_tp_id as level2_prodtype_id,                   
   t5.scnd_lv_tp as level2_prodtype_descr,                      
   t5.thrd_lv_tp_id as level3_prodtype_id,                     
   t5.thrd_lv_tp as level3_prodtype_descr,                    
   t4.inventory_item_id,                                        
   t4.inventory_item_desc,                                      
   t1.sale_cnt,                                                 
   t1.store_cnt,                                                 
   t1.sale_total_amt,                                           
   t1.sale_cost_amt,                                            
   t1.paper_store_cnt,                                        
   '$CREATE_TIME'
   FROM
     (SELECT * FROM TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1 where op_day='$OP_DAY')t1
  LEFT JOIN
     (SELECT   * FROM  mreport_global.dim_org_management)t2
	   
  ON(t1.org_id=t2.org_id and t2.attribute5='1')
  LEFT JOIN 
     (SELECT * FROM mreport_global.dim_org_management)t6
   ON(t1.org_id=t6.org_id and t1.bus_type=t6.bus_type_id and t6.attribute5='2')
  LEFT JOIN 
     ( SELECT * FROM mreport_global.dim_org_inv_management)t7
   ON(t1.organization_id=t7.inv_org_id)	 
  
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
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_1_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_2_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_3_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_4_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_5_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_6_1;
    $CREATE_TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1;
    $INSERT_TMP_DMP_BIRD_MAIN_INVTURN_DD_7_1;
    $CREATE_DMP_BIRD_INV_TURN_DD;
    $INSERT_DMP_BIRD_INV_TURN_DD;
    "  -v 





