#!/bin/bash

######################################################################
#                                                                    
# 程    序: DMP_BIRD_EIGHT_FIVE_DD.sh                               
# 创建时间: 2018年04月20日                                            
# 创 建 者: fwj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 八大类5日考核
# 修改说明:                                                          
######################################################################

OP_DAY=$1


# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_eight_five_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWU_BIRD_EIGHT_FIVE_DD_1='TMP_DWU_BIRD_EIGHT_FIVE_DD_1'

CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_EIGHT_FIVE_DD_1 (
	day_id							string		--日期
	,org_id							string		--公司ID
	,item_id                        string      --物料ID
	,organization_id                string      --库存组织ID
	,bus_type                       string      --业态
	,product_line                   string      --产线
	,primary_quantity               string      --主要数量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_1="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_EIGHT_FIVE_DD_1 PARTITION (op_day = '$OP_DAY')
SELECT
	t1.period_id day_id				--日期			
	,t1.org_id						--公司ID	
	,t1.item_id                     --物料ID
	,t1.organization_id             --库存组织ID
	,t1.bus_type                    --业态
	,t1.product_line                --产线
	,nvl(t1.primary_quantity,0)     --主要数量
FROM (select * from mreport_poultry.dwu_tz_storage_transation02_dd where op_day = '$OP_DAY') t1
join mreport_global.dim_crm_item t2
	on (
		t1.item_code = t2.item_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t3
	on (
		t1.item_code = t3.sale_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t4
	on (
		t2.thrd_lv_tp_code = t4.sale_code
	)
where (t4.index_type != 'CUX_BI_PRDCT CTGRY EXCLSS' and t3.index_type is NULL)
	  or (t3.index_type != 'CUX_BI_MATERIAL EXCLUSION ITEM' and t4.index_type is null) 
	  or (t3.index_type is null and t4.index_type is null)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWU_BIRD_EIGHT_FIVE_DD_2='TMP_DWU_BIRD_EIGHT_FIVE_DD_2'

CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_2="
CREATE TABLE IF NOT EXISTS 	$TMP_DWU_BIRD_EIGHT_FIVE_DD_2(
	day_id							string		--日期
	,item_id                        string      --物料ID
	,organization_id                string      --库存组织ID
	,bus_type                       string      --业态
	,product_line                   string      --产线
	,finish_product_stock           string      --时点库存
	,fresh_normal_sto_count			string		--半成品入库量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_2="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_EIGHT_FIVE_DD_2 PARTITION (op_day = '$OP_DAY')
SELECT
	from_unixtime(unix_timestamp(t1.available_stock_time),'yyyyMMdd') day_id
	,t1.item_id
	,t1.organization_id
	,t1.bus_type
	,t1.product_line
	,nvl(t1.finish_product_stock,0)
	,nvl(t1.fresh_normal_sto_count,0)
FROM (select * from mreport_poultry.dwu_xs_xs03_dd where op_day = '$OP_DAY') t1
left join mreport_global.dwu_dim_material_new t2
	on (
		t1.item_id = t2.inventory_item_id
		and t1.organization_id = t2.inv_org_id  
	)
join mreport_global.dim_crm_item t3
	on (
		t2.inventory_item_code = t3.item_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t4
	on (
		t2.inventory_item_code = t4.sale_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t5
	on (
		t3.thrd_lv_tp_code = t5.sale_code
	)
where (t5.index_type != 'CUX_BI_PRDCT CTGRY EXCLSS' and t4.index_type is NULL)
	  or (t4.index_type != 'CUX_BI_MATERIAL EXCLUSION ITEM' and t5.index_type is null) 
	  or (t5.index_type is null and t4.index_type is null)
"
	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWU_BIRD_EIGHT_FIVE_DD_3='TMP_DWU_BIRD_EIGHT_FIVE_DD_3'

CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_3="	
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_EIGHT_FIVE_DD_3 (
	day_id							string		--日期
	,org_id							string		--公司ID
	,item_id                        string      --物料ID
	,organization_id                string      --库存组织ID
	,bus_type                       string      --业态
	,product_line                   string      --产线
	,out_qty               	string      --出库主数量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC	
"

	
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_3="	
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_EIGHT_FIVE_DD_3 PARTITION (op_day = '$OP_DAY')
SELECT	
	t1.out_date											--日期
	,t1.org_id											--公司ID
	,t1.item_id                    						--物料ID
	,t1.organization_id            						--库存组织ID
	,t1.bus_type                   						--业态
	,t1.product_line               						--产线
	,nvl(t1.out_qty,0)               						--出库主数量
FROM (select * from mreport_poultry.dwu_gyl_xs01_dd where op_day = '$OP_DAY' and out_date is not null) t1             
join mreport_global.dim_crm_item t2
	on (
		t1.item_code = t2.item_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t3
	on (
		t1.item_code = t3.sale_code
	)
left join MREPORT_GLOBAL.DWU_DIM_GYL_XS08 t4
	on (
		t2.thrd_lv_tp_code = t4.sale_code
	)
where (t4.index_type != 'CUX_BI_PRDCT CTGRY EXCLSS' and t3.index_type is NULL)
	  or (t3.index_type != 'CUX_BI_MATERIAL EXCLUSION ITEM' and t4.index_type is null) 
	  or (t3.index_type is null and t4.index_type is null)
"
	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWU_BIRD_EIGHT_FIVE_DD='DWU_BIRD_EIGHT_FIVE_DD'

CREATE_DWU_BIRD_EIGHT_FIVE_DD="	
CREATE TABLE IF NOT EXISTS $DWU_BIRD_EIGHT_FIVE_DD (
	day_id							string		--日期 
	,item_id                        string      --物料ID
	,organization_id                string      --库存组织ID
	,bus_type                       string      --业态
	,product_line                   string      --产线
	,out_qty               			string      --出库数量
	,finish_product_stock           string      --时点库存
	,fresh_normal_sto_count			string		--半成品入库量
	,primary_quantity               string      --主要数量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC	
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWU_BIRD_EIGHT_FIVE_DD="
INSERT OVERWRITE TABLE $DWU_BIRD_EIGHT_FIVE_DD PARTITION (op_day = '$OP_DAY')
SELECT	
	t1.day_id
	,t1.item_id
	,t1.organization_id
	,t1.bus_type
	,t1.product_line
	,sum(t1.out_qty)
	,sum(t1.finish_product_stock)
	,sum(t1.fresh_normal_sto_count)
	,sum(t1.primary_quantity)
FROM (select 
		day_id					
		,item_id         
		,organization_id 
		,bus_type        
		,product_line    
		,primary_quantity
		,'0' out_qty
		,'0' fresh_normal_sto_count
		,'0' finish_product_stock
	from $TMP_DWU_BIRD_EIGHT_FIVE_DD_1 where op_day = '$OP_DAY'
	union all
	select 
		day_id					
		,item_id                
		,organization_id        
		,bus_type               
		,product_line
		,'0' primary_quantity
		,'0' out_qty            
		,fresh_normal_sto_count
		,finish_product_stock
	from $TMP_DWU_BIRD_EIGHT_FIVE_DD_2 where op_day = '$OP_DAY'
	union all
	select 
		day_id					
		,item_id                
		,organization_id        
		,bus_type               
		,product_line
		,'0' primary_quantity
		,out_qty            
		,'0' fresh_normal_sto_count
		,'0' finish_product_stock
	from $TMP_DWU_BIRD_EIGHT_FIVE_DD_3 where op_day = '$OP_DAY'
) t1
group by 
	t1.day_id
	,t1.item_id
	,t1.organization_id
	,t1.bus_type
	,t1.product_line
"	



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_1='TMP_DMP_BIRD_EIGHT_FIVE_DD_1'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_1 (
	day_id					string		--日期
	,item_id                string      --物料ID
	,organization_id        string      --库存组织ID
	,bus_type               string      --业态
	,product_line           string      --产线
	,day_prod_cnt         	string      --日产量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_1 PARTITION (op_day = '$OP_DAY')
SELECT
	day_id	 									--日期
	,item_id                             		--物料ID
	,organization_id                     		--库存组织ID
	,bus_type                            		--业态
	,product_line                        		--产线
	,sum(primary_quantity) day_prod_cnt     	--日产量
FROM $DWU_BIRD_EIGHT_FIVE_DD WHERE op_day = '$OP_DAY'
group by
	day_id	
    ,item_id        
    ,organization_id
    ,bus_type       
    ,product_line   
"	

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_2='TMP_DMP_BIRD_EIGHT_FIVE_DD_2'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_2 (
	day_id					string		--日期
	,item_id                string      --物料ID
	,organization_id        string      --库存组织ID
	,bus_type               string      --业态
	,product_line           string      --产线
	,day_sale_cnt         	string      --日销量
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_2 PARTITION (op_day = '$OP_DAY')
SELECT
	day_id	 									--日期
	,item_id                             							--物料ID
	,organization_id                     							--库存组织ID
	,bus_type                            							--业态
	,product_line                        							--产线
	,sum(out_qty) day_sale_cnt     							--日销量
FROM $DWU_BIRD_EIGHT_FIVE_DD WHERE op_day = '$OP_DAY'
group by
	day_id	
    ,item_id        
    ,organization_id
    ,bus_type       
    ,product_line   
"
	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_3='TMP_DMP_BIRD_EIGHT_FIVE_DD_3'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_3 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,prod_sale1_5						string		--1-5日销量
	,prod_sale6_10						string		--6-10日销量
	,prod_sale11_15						string		--11-15日销量
	,prod_sale16_20						string		--16-20日销量
	,prod_sale21_25						string		--21-25日销量
	,prod_sale26_31						string		--26-31日销量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_3 PARTITION(op_day = '$OP_DAY')
SELECT
	t1.day_id																																					--日期 	                        																														--公司id
	,t2.product_line                                      																										--产线
	,t2.bus_type																																				--业态
	,t2.item_id																																					--物料id																																		--销售机构id
	,t2.organization_id          																																--库存组织ID																																		--销售员
	,SUM(case when substring(t1.day_id,7,2) >= '01' and substring(t1.day_id,7,2) <= '05' and t1.day_id >= t2.day_id then t2.day_sale_cnt 
	          when substring(t1.day_id,7,2) >= '06' and substring(t2.day_id,7,2) >= '01' and substring(t2.day_id,7,2) <= '05' then t2.day_sale_cnt 
			  else 0 end) prod_sale1_5	--1-5日销量
	,SUM(case when substring(t1.day_id,7,2) >= '06' and substring(t1.day_id,7,2) <= '10' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '06' then t2.day_sale_cnt 
			  when substring(t1.day_id,7,2) >= '11' and substring(t2.day_id,7,2) >= '06' and substring(t2.day_id,7,2) <= '10' then t2.day_sale_cnt
			  else 0 end) prod_sale6_10	--6-10日销量
	,SUM(case when substring(t1.day_id,7,2) >= '11' and substring(t1.day_id,7,2) <= '15' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '11' then t2.day_sale_cnt 
			  when substring(t1.day_id,7,2) >= '16' and substring(t2.day_id,7,2) >= '11' and substring(t2.day_id,7,2) <= '15' then t2.day_sale_cnt
			  else 0 end) prod_sale11_15 --11-15日销量
	,SUM(case when substring(t1.day_id,7,2) >= '16' and substring(t1.day_id,7,2) <= '20' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '16' then t2.day_sale_cnt 
			  when substring(t1.day_id,7,2) >= '21' and substring(t2.day_id,7,2) >= '16' and substring(t2.day_id,7,2) <= '20' then t2.day_sale_cnt
			  else 0 end) prod_sale16_20 --16-20日销量
	,SUM(case when substring(t1.day_id,7,2) >= '21' and substring(t1.day_id,7,2) <= '25' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '21' then t2.day_sale_cnt 
			  when substring(t1.day_id,7,2) >= '26' and substring(t2.day_id,7,2) >= '21' and substring(t2.day_id,7,2) <= '25' then t2.day_sale_cnt
	          else 0 end) prod_sale21_25 --21-25日销量
	,SUM(case when substring(t1.day_id,7,2) >= '26' and substring(t1.day_id,7,2) <= '31' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '26' then t2.day_sale_cnt 
			  else 0 end) prod_sale26_31 --26-31日销量
FROM (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
LEFT JOIN $TMP_DMP_BIRD_EIGHT_FIVE_DD_2 t2
	ON (
		t2.op_day = '$OP_DAY'
		and t1.month_id = substring(t2.day_id,1,6)
	)
GROUP BY
	t1.day_id
	,t1.month_id                          								
	,t2.product_line                                      				
	,t2.bus_type														
	,t2.item_id																											
	,t2.organization_id          										
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_4='TMP_DMP_BIRD_EIGHT_FIVE_DD_4'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_4 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,prod_cnt1_5						string		--1-5日产量
	,prod_cnt6_10						string		--6-10日产量
	,prod_cnt11_15						string		--11-15日产量
	,prod_cnt16_20						string		--16-20日产量
	,prod_cnt21_25						string		--21-25日产量
	,prod_cnt26_31						string		--26-31日产量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_4 PARTITION(op_day = '$OP_DAY')
SELECT
	t1.day_id																																					--日期 	                        																														--公司id
	,t2.product_line                                      																										--产线
	,t2.bus_type																																				--业态
	,t2.item_id																																					--物料id																																		--销售机构id
	,t2.organization_id          																																--库存组织ID																																		--销售员
	,SUM(case when substring(t1.day_id,7,2) >= '01' and substring(t1.day_id,7,2) <= '05' and t1.day_id >= t2.day_id then t2.day_prod_cnt 
	          when substring(t1.day_id,7,2) >= '06' and substring(t2.day_id,7,2) >= '01' and substring(t2.day_id,7,2) <= '05' then t2.day_prod_cnt 
			  else 0 end) prod_cnt1_5	--1-5日产量
	,SUM(case when substring(t1.day_id,7,2) >= '06' and substring(t1.day_id,7,2) <= '10' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '06' then t2.day_prod_cnt 
			  when substring(t1.day_id,7,2) >= '11' and substring(t2.day_id,7,2) >= '06' and substring(t2.day_id,7,2) <= '10' then t2.day_prod_cnt
			  else 0 end) prod_cnt6_10	--6-10日产量
	,SUM(case when substring(t1.day_id,7,2) >= '11' and substring(t1.day_id,7,2) <= '15' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '11' then t2.day_prod_cnt 
			  when substring(t1.day_id,7,2) >= '16' and substring(t2.day_id,7,2) >= '11' and substring(t2.day_id,7,2) <= '15' then t2.day_prod_cnt
			  else 0 end) prod_cnt11_15 --11-15日产量
	,SUM(case when substring(t1.day_id,7,2) >= '16' and substring(t1.day_id,7,2) <= '20' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '16' then t2.day_prod_cnt 
			  when substring(t1.day_id,7,2) >= '21' and substring(t2.day_id,7,2) >= '16' and substring(t2.day_id,7,2) <= '20' then t2.day_prod_cnt
			  else 0 end) prod_cnt16_20 --16-20日产量
	,SUM(case when substring(t1.day_id,7,2) >= '21' and substring(t1.day_id,7,2) <= '25' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '21' then t2.day_prod_cnt 
			  when substring(t1.day_id,7,2) >= '26' and substring(t2.day_id,7,2) >= '21' and substring(t2.day_id,7,2) <= '25' then t2.day_prod_cnt
	          else 0 end) prod_cnt21_25 --21-25日产量
	,SUM(case when substring(t1.day_id,7,2) >= '26' and substring(t1.day_id,7,2) <= '31' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '26' then t2.day_prod_cnt 
			  else 0 end) prod_cnt26_31 --26-31日产量
FROM (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
LEFT JOIN $TMP_DMP_BIRD_EIGHT_FIVE_DD_1 t2
	ON (
		t2.op_day = '$OP_DAY'
		and t1.month_id = substring(t2.day_id,1,6)
	)
GROUP BY
	t1.day_id
	,t1.month_id                           								
	,t2.product_line                                      				
	,t2.bus_type														
	,t2.item_id																										
	,t2.organization_id 
"

	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_5='TMP_DMP_BIRD_EIGHT_FIVE_DD_5'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_5="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_5 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,day_store							string		--日业务库存
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"



## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_5="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_5 PARTITION(op_day = '$OP_DAY')
select
	day_id																	--日期
	,product_line                                                           --产线
	,bus_type                                                               --业态
	,item_id		                                                        --物料ID
	,organization_id                                                        --库存组织ID
	,SUM(finish_product_stock + fresh_normal_sto_count)                     --日业务库存
from $DWU_BIRD_EIGHT_FIVE_DD where op_day = '$OP_DAY'
group by
	day_id      
    ,product_line   
    ,bus_type       
    ,item_id		 
	,organization_id
"
	
	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_6='TMP_DMP_BIRD_EIGHT_FIVE_DD_6'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_6="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_6 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,month_prod_cnt						string		--月产量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_6="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_6 PARTITION(op_day = '$OP_DAY')
select
	t1.day_id						--日期
	,t2.product_line               --产线
	,t2.bus_type                   --业态
	,t2.item_id                    --物料ID
	,t2.organization_id            --库存组织ID
	,sum(case when t1.day_id >= t2.day_id then t2.day_prod_cnt
		else 0 end)  month_prod_cnt             --月产量
from (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_EIGHT_FIVE_DD_1 t2
	on (
		t2.op_day = '$OP_DAY'
		and t1.month_id = substring(t2.day_id,1,6)
	)
group by
	t1.day_id
	,t1.month_id						
	,t2.item_id        
	,t2.organization_id
	,t2.bus_type       
	,t2.product_line    
"
	
	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_7='TMP_DMP_BIRD_EIGHT_FIVE_DD_7'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_7="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_7 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,month_sale_cnt						string 		--月销量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_7="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_7 PARTITION(op_day = '$OP_DAY')
select
	t1.day_id						--日期
	,t2.product_line               --产线
	,t2.bus_type                   --业态
	,t2.item_id                    --物料ID
	,t2.organization_id            --库存组织ID
	,sum(case when t1.day_id >= t2.day_id then t2.day_sale_cnt
		else 0 end)  month_sale_cnt             --月销量
from (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_EIGHT_FIVE_DD_2 t2
	on (
		t2.op_day = '$OP_DAY'
		and t1.month_id = substring(t2.day_id,1,6)
	)
group by
	t1.day_id			
	,t1.month_id	
	,t2.item_id        
	,t2.organization_id
	,t2.bus_type       
	,t2.product_line   
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_8='TMP_DMP_BIRD_EIGHT_FIVE_DD_8'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_8="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_8 (
	day_id								string		--日期
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料ID
	,organization_id                    string      --库存组织ID
	,store_1							string		--1日业务库存
	,store_5							string		--5日业务库存
	,store_6							string		--6日业务库存
	,store_10							string		--10日业务库存
	,store_11							string		--11日业务库存
	,store_15							string		--15日业务库存
	,store_16							string		--16日业务库存
	,store_20							string		--20日业务库存
	,store_21							string		--21日业务库存
	,store_25							string		--25日业务库存
	,store_26							string		--26日业务库存
	,store_end							string		--月末业务库存
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_8="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_8 PARTITION(op_day = '$OP_DAY')
select
	t1.day_id																	--日期
	,t2.product_line                                                           --产线
	,t2.bus_type                                                               --业态
	,t2.item_id		                                                        --物料ID
	,t2.organization_id                                                        --库存组织ID
	,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '01' then nvl(t2.day_store,0) 
		  else 0 end store_1																--1日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '05' then nvl(t2.day_store,0) 
		  else 0 end store_5	                                                            --5日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '06' then nvl(t2.day_store,0) 
		  else 0 end store_6	                                                            --6日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '10' then nvl(t2.day_store,0) 
		  else 0 end store_10	                                                            --10日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '11' then nvl(t2.day_store,0) 
		  else 0 end store_11	                                                            --11日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '15' then nvl(t2.day_store,0) 
		  else 0 end store_15	                                                            --15日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '16' then nvl(t2.day_store,0) 
		  else 0 end store_16	                                                            --16日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '20' then nvl(t2.day_store,0) 
		  else 0 end store_20	                                                            --20日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '21' then nvl(t2.day_store,0) 
		  else 0 end store_21	                                                            --21日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '25' then nvl(t2.day_store,0) 
		  else 0 end store_25	                                                            --25日业务库存
    ,case when t1.day_id >= t2.day_id and substring(t2.day_id,7,2) = '26' then nvl(t2.day_store,0) 
		  else 0 end store_26	                                                            --26日业务库存
    ,case when (t1.day_id >= t2.day_id and (substr(t1.day_id,1,4)%4=0  and substr(t1.day_id,1,4)%100!=0) or substr(t1.day_id,1,4)%400=0) 
		    then (case when substr(t1.day_id,5,2)  in ('01','03','05','07','08','10','12') and substring(t2.day_id,7,2) = '31' then nvl(t2.day_store,0)
				 when substr(t1.day_id,5,2) = '02' and substring(t2.day_id,7,2) = '29' then nvl(t2.day_store,0)
				 when substr(t1.day_id,5,2)  in ('04','06','09','11') and substring(t2.day_id,7,2) = '30' then nvl(t2.day_store,0)
				 end)
		  when t1.day_id >= t2.day_id and substr(t1.day_id,5,2)  in ('01','03','05','07','08','10','12') and substring(t2.day_id,7,2) = '31' then nvl(t2.day_store,0)
		  when t1.day_id >= t2.day_id and substr(t1.day_id,5,2) = '02' and substring(t2.day_id,7,2) = '28' then nvl(t2.day_store,0)
		  when t1.day_id >= t2.day_id and substr(t1.day_id,5,2)  in ('04','06','09','11') and substring(t2.day_id,7,2) = '30' then nvl(t2.day_store,0)
		  else 0 end store_end	                                                            --月末业务库存
from (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_EIGHT_FIVE_DD_5 t2
	on (
		t2.op_day = '$OP_DAY'
		and t1.month_id = substring(t2.day_id,1,6)
	)
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_9='TMP_DMP_BIRD_EIGHT_FIVE_DD_9'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_9="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_9 (
	day_id                          string      --期间(日)
	,organization_id                string      --库存组织
	,bus_type         				string      --业态
	,product_line             		string      --产线
	,item_id          				string      --物料id
	,prod_cnt1_5                    string      --1-5日产量
	,prod_sale1_5                   string      --1-5日销量
	,store_1                        string      --1日业务库存
	,store_5                        string      --5日业务库存
	,prod_cnt6_10                   string      --6-10日产量
	,prod_sale6_10                  string      --6-10日销量
	,store_6                        string      --6日业务库存
	,store_10                       string      --10日业务库存
	,prod_cnt11_15                  string      --11-15日产量
	,prod_sale11_15                 string      --11-15日销量
	,store_11                       string      --11日业务库存
	,store_15                       string      --15日业务库存
	,prod_cnt16_20                  string      --16-20日产量
	,prod_sale16_20                 string      --16-20日销量
	,store_16                       string      --16日业务库存
	,store_20                       string      --20日业务库存
	,prod_cnt21_25                  string      --21-25日产量
	,prod_sale21_25                 string      --21-25日销量
	,store_21                       string      --21日业务库存
	,store_25                       string      --25日业务库存
	,prod_cnt26_30                  string      --26-月末日产量
	,prod_sale26_30                 string      --26-月末日销量
	,store_26                       string      --26日业务库存
	,store_30                       string      --月末业务库存
	,month_prod_cnt                 string      --月产量
	,month_sale_cnt                 string      --月销量
)PARTITIONED BY (op_day string)     
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_9="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_9 PARTITION(op_day = '$OP_DAY')
select
	t1.day_id           
	,t1.organization_id 
	,t1.bus_type        
	,t1.product_line    
	,t1.item_id         
	,sum(t1.prod_cnt1_5)     
	,sum(t1.prod_sale1_5)    
	,sum(t1.store_1)         
	,sum(t1.store_5)         
	,sum(t1.prod_cnt6_10)    
	,sum(t1.prod_sale6_10)   
	,sum(t1.store_6)         
	,sum(t1.store_10)        
	,sum(t1.prod_cnt11_15)   
	,sum(t1.prod_sale11_15)  
	,sum(t1.store_11)        
	,sum(t1.store_15)        
	,sum(t1.prod_cnt16_20)   
	,sum(t1.prod_sale16_20)  
	,sum(t1.store_16)        
	,sum(t1.store_20)        
	,sum(t1.prod_cnt21_25)   
	,sum(t1.prod_sale21_25)  
	,sum(t1.store_21)        
	,sum(t1.store_25)        
	,sum(t1.prod_cnt26_31)   
	,sum(t1.prod_sale26_31)  
	,sum(t1.store_26)        
	,sum(t1.store_end)        
	,sum(t1.month_prod_cnt)  
	,sum(t1.month_sale_cnt)  
from (select 
		day_id			
		,product_line    
		,bus_type        
		,item_id		
		,organization_id 
		,prod_cnt1_5	
		,prod_cnt6_10	
		,prod_cnt11_15	
		,prod_cnt16_20	
		,prod_cnt21_25	
		,prod_cnt26_31
		,'0' prod_sale1_5	
		,'0' prod_sale6_10	
		,'0' prod_sale11_15	
		,'0' prod_sale16_20	
		,'0' prod_sale21_25	
		,'0' prod_sale26_31
		,'0' store_1
		,'0' store_5
		,'0' store_6
		,'0' store_10
		,'0' store_11
		,'0' store_15
		,'0' store_16
		,'0' store_20
		,'0' store_21
		,'0' store_25
		,'0' store_26
		,'0' store_end	
		,'0' month_prod_cnt
		,'0' month_sale_cnt		
	 from $TMP_DMP_BIRD_EIGHT_FIVE_DD_4 where op_day = '$OP_DAY'
	 union all
	 select 
		day_id			
		,product_line    
		,bus_type        
		,item_id		
		,organization_id 
		,'0' prod_cnt1_5	
		,'0' prod_cnt6_10	
		,'0' prod_cnt11_15	
		,'0' prod_cnt16_20	
		,'0' prod_cnt21_25	
		,'0' prod_cnt26_31
		,prod_sale1_5	
		,prod_sale6_10	
		,prod_sale11_15	
		,prod_sale16_20	
		,prod_sale21_25	
		,prod_sale26_31
		,'0' store_1
		,'0' store_5
		,'0' store_6
		,'0' store_10
		,'0' store_11
		,'0' store_15
		,'0' store_16
		,'0' store_20
		,'0' store_21
		,'0' store_25
		,'0' store_26
		,'0' store_end	
		,'0' month_prod_cnt
		,'0' month_sale_cnt		
	 from $TMP_DMP_BIRD_EIGHT_FIVE_DD_3 where op_day = '$OP_DAY'
	 union all
	 select 
		day_id			
		,product_line    
		,bus_type        
		,item_id		
		,organization_id 
		,'0' prod_cnt1_5	
		,'0' prod_cnt6_10	
		,'0' prod_cnt11_15	
		,'0' prod_cnt16_20	
		,'0' prod_cnt21_25	
		,'0' prod_cnt26_31
		,'0' prod_sale1_5	
		,'0' prod_sale6_10	
		,'0' prod_sale11_15	
		,'0' prod_sale16_20	
		,'0' prod_sale21_25	
		,'0' prod_sale26_31
		,store_1
		,store_5
		,store_6
		,store_10
		,store_11
		,store_15
		,store_16
		,store_20
		,store_21
		,store_25
		,store_26
		,store_end	
		,'0' month_prod_cnt
		,'0' month_sale_cnt		
	 from $TMP_DMP_BIRD_EIGHT_FIVE_DD_8 where op_day = '$OP_DAY'
	 union all
	 select 
		day_id			
		,product_line    
		,bus_type        
		,item_id		
		,organization_id 
		,'0' prod_cnt1_5	
		,'0' prod_cnt6_10	
		,'0' prod_cnt11_15	
		,'0' prod_cnt16_20	
		,'0' prod_cnt21_25	
		,'0' prod_cnt26_31
		,'0' prod_sale1_5	
		,'0' prod_sale6_10	
		,'0' prod_sale11_15	
		,'0' prod_sale16_20	
		,'0' prod_sale21_25	
		,'0' prod_sale26_31
		,'0' store_1
		,'0' store_5
		,'0' store_6
		,'0' store_10
		,'0' store_11
		,'0' store_15
		,'0' store_16
		,'0' store_20
		,'0' store_21
		,'0' store_25
		,'0' store_26
		,'0' store_end
		,month_prod_cnt
		,'0' month_sale_cnt		
	 from $TMP_DMP_BIRD_EIGHT_FIVE_DD_6 where op_day = '$OP_DAY'
	 union all
	 select 
		day_id			
		,product_line    
		,bus_type        
		,item_id		
		,organization_id 
		,'0' prod_cnt1_5	
		,'0' prod_cnt6_10	
		,'0' prod_cnt11_15	
		,'0' prod_cnt16_20	
		,'0' prod_cnt21_25	
		,'0' prod_cnt26_31
		,'0' prod_sale1_5	
		,'0' prod_sale6_10	
		,'0' prod_sale11_15	
		,'0' prod_sale16_20	
		,'0' prod_sale21_25	
		,'0' prod_sale26_31
		,'0' store_1
		,'0' store_5
		,'0' store_6
		,'0' store_10
		,'0' store_11
		,'0' store_15
		,'0' store_16
		,'0' store_20
		,'0' store_21
		,'0' store_25
		,'0' store_26
		,'0' store_end
		,'0' month_prod_cnt
		,month_sale_cnt		
	 from $TMP_DMP_BIRD_EIGHT_FIVE_DD_7 where op_day = '$OP_DAY'	 
	 ) t1
group by 
	t1.day_id			
	,t1.product_line    
	,t1.bus_type        
	,t1.item_id		
	,t1.organization_id
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_10='TMP_DMP_BIRD_EIGHT_FIVE_DD_10'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_10="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_10 (
	day_id                          string      --期间(日)
	,organization_id                string      --库存组织
	,bus_type         				string      --业态
	,product_line             		string      --产线
	,item_id          				string      --物料id
	,level4_sale_id                 string      --销售组织4级
	,level4_manager                 string      --销售组织4级责任人
	,level5_sale_id                 string      --销售组织5级
	,level5_manager					string		--销售组织5级责任人
	,prod_cnt1_5                    string      --1-5日产量
	,prod_sale1_5                   string      --1-5日销量
	,store_1                        string      --1日业务库存
	,store_5                        string      --5日业务库存
	,prod_cnt6_10                   string      --6-10日产量
	,prod_sale6_10                  string      --6-10日销量
	,store_6                        string      --6日业务库存
	,store_10                       string      --10日业务库存
	,prod_cnt11_15                  string      --11-15日产量
	,prod_sale11_15                 string      --11-15日销量
	,store_11                       string      --11日业务库存
	,store_15                       string      --15日业务库存
	,prod_cnt16_20                  string      --16-20日产量
	,prod_sale16_20                 string      --16-20日销量
	,store_16                       string      --16日业务库存
	,store_20                       string      --20日业务库存
	,prod_cnt21_25                  string      --21-25日产量
	,prod_sale21_25                 string      --21-25日销量
	,store_21                       string      --21日业务库存
	,store_25                       string      --25日业务库存
	,prod_cnt26_30                  string      --26-月末日产量
	,prod_sale26_30                 string      --26-月末日销量
	,store_26                       string      --26日业务库存
	,store_30                       string      --月末业务库存
	,month_prod_cnt                 string      --月产量
	,month_sale_cnt                 string      --月销量
)PARTITIONED BY (op_day string)     
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_10="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_10 PARTITION(op_day = '$OP_DAY')
select 
	t2.day_id           
	,t2.organization_id 
	,t2.bus_type        
	,t2.product_line    
	,t2.item_id         
	,case when t2.product_line = '10' then t1.chicken_sale_company
		  when t2.product_line = '20' then t1.duck_sale_company
		  end  
	,case when t2.product_line = '10' then t1.chicken_sale_person
		  when t2.product_line = '20' then t1.duck_sale_person 
		  end   
	,case when t2.product_line = '10' then t1.chicken_sale_area1 
		  when t2.product_line = '20' then t1.duck_sale_area1 
		  end  
	,case when t2.product_line = '10' then t1.chicken_area1_person 
		  when t2.product_line = '20' then t1.duck_area1_person 
		  end 
	,t2.prod_cnt1_5     
	,t2.prod_sale1_5    
	,t2.store_1         
	,t2.store_5         
	,t2.prod_cnt6_10    
	,t2.prod_sale6_10   
	,t2.store_6         
	,t2.store_10        
	,t2.prod_cnt11_15   
	,t2.prod_sale11_15  
	,t2.store_11        
	,t2.store_15        
	,t2.prod_cnt16_20   
	,t2.prod_sale16_20  
	,t2.store_16        
	,t2.store_20        
	,t2.prod_cnt21_25   
	,t2.prod_sale21_25  
	,t2.store_21        
	,t2.store_25        
	,t2.prod_cnt26_30   
	,t2.prod_sale26_30  
	,t2.store_26        
	,t2.store_30        
	,t2.month_prod_cnt  
	,t2.month_sale_cnt  
from (select * from $TMP_DMP_BIRD_EIGHT_FIVE_DD_9 where op_day = '$OP_DAY') t2
left join (select * from mreport_poultry.DWU_SALES_INV_ORG_REF_DD where op_day = '$OP_DAY') t1
	on t1.organization_id = t2.organization_id
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_11='TMP_DMP_BIRD_EIGHT_FIVE_DD_11'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_11="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_11 (
	day_id                          string      --期间(日)
	,organization_id                string      --库存组织
	,bus_type         				string      --业态
	,product_line             		string      --产线
	,item_id          				string      --物料id
	,level4_sale_id                 string      --销售组织4级
	,level4_manager                 string      --销售组织4级责任人
	,level5_sale_id                 string      --销售组织5级
	,level5_manager					string		--销售组织5级责任人
	,prod_cnt1_5                    string      --1-5日产量
	,prod_sale1_5                   string      --1-5日销量
	,store_1                        string      --1日业务库存
	,store_5                        string      --5日业务库存
	,prod_cnt6_10                   string      --6-10日产量
	,prod_sale6_10                  string      --6-10日销量
	,store_6                        string      --6日业务库存
	,store_10                       string      --10日业务库存
	,prod_cnt11_15                  string      --11-15日产量
	,prod_sale11_15                 string      --11-15日销量
	,store_11                       string      --11日业务库存
	,store_15                       string      --15日业务库存
	,prod_cnt16_20                  string      --16-20日产量
	,prod_sale16_20                 string      --16-20日销量
	,store_16                       string      --16日业务库存
	,store_20                       string      --20日业务库存
	,prod_cnt21_25                  string      --21-25日产量
	,prod_sale21_25                 string      --21-25日销量
	,store_21                       string      --21日业务库存
	,store_25                       string      --25日业务库存
	,prod_cnt26_30                  string      --26-月末日产量
	,prod_sale26_30                 string      --26-月末日销量
	,store_26                       string      --26日业务库存
	,store_30                       string      --月末业务库存
	,month_prod_cnt                 string      --月产量
	,month_sale_cnt                 string      --月销量
)PARTITIONED BY (op_day string)     
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_11="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_11 PARTITION(op_day = '$OP_DAY')
select 
	t2.day_id           
	,t2.organization_id 
	,t2.bus_type        
	,t2.product_line    
	,t2.item_id         
	,case when t2.product_line = '10' then t1.chicken_sale_company
		  when t2.product_line = '20' then t1.duck_sale_company
		  end  
	,case when t2.product_line = '10' then t1.chicken_sale_person
		  when t2.product_line = '20' then t1.duck_sale_person 
		  end   
	,case when t2.product_line = '10' then t1.chicken_sale_area2 
		  when t2.product_line = '20' then t1.duck_sale_area2 
		  end  
	,case when t2.product_line = '10' then t1.chicken_area2_person 
		  when t2.product_line = '20' then t1.duck_area2_person 
		  end 
	,'0'     
	,'0'   
	,'0'         
	,'0'         
	,'0'    
	,'0'   
	,'0'         
	,'0'        
	,'0'   
	,'0'  
	,'0'        
	,'0'        
	,'0'   
	,'0'  
	,'0'        
	,'0'        
	,'0'   
	,'0'  
	,'0'        
	,'0'       
	,'0'   
	,'0'  
	,'0'        
	,'0'        
	,'0'
	,'0'  
from (select * from $TMP_DMP_BIRD_EIGHT_FIVE_DD_9 where op_day = '$OP_DAY') t2
left join (select * from mreport_poultry.DWU_SALES_INV_ORG_REF_DD where op_day = '$OP_DAY') t1
	on t1.organization_id = t2.organization_id
where t1.chicken_sale_area2 is not null or t1.duck_sale_area2 is not null
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_EIGHT_FIVE_DD_12='TMP_DMP_BIRD_EIGHT_FIVE_DD_12'

CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_12="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_EIGHT_FIVE_DD_12 (
	day_id                          string      --期间(日)
	,organization_id                string      --库存组织
	,bus_type         				string      --业态
	,product_line             		string      --产线
	,item_id          				string      --物料id
	,level4_sale_id                 string      --销售组织4级
	,level4_manager                 string      --销售组织4级责任人
	,level5_sale_id                 string      --销售组织5级
	,level5_manager					string		--销售组织5级责任人
	,prod_cnt1_5                    string      --1-5日产量
	,prod_sale1_5                   string      --1-5日销量
	,store_1                        string      --1日业务库存
	,store_5                        string      --5日业务库存
	,prod_cnt6_10                   string      --6-10日产量
	,prod_sale6_10                  string      --6-10日销量
	,store_6                        string      --6日业务库存
	,store_10                       string      --10日业务库存
	,prod_cnt11_15                  string      --11-15日产量
	,prod_sale11_15                 string      --11-15日销量
	,store_11                       string      --11日业务库存
	,store_15                       string      --15日业务库存
	,prod_cnt16_20                  string      --16-20日产量
	,prod_sale16_20                 string      --16-20日销量
	,store_16                       string      --16日业务库存
	,store_20                       string      --20日业务库存
	,prod_cnt21_25                  string      --21-25日产量
	,prod_sale21_25                 string      --21-25日销量
	,store_21                       string      --21日业务库存
	,store_25                       string      --25日业务库存
	,prod_cnt26_30                  string      --26-月末日产量
	,prod_sale26_30                 string      --26-月末日销量
	,store_26                       string      --26日业务库存
	,store_30                       string      --月末业务库存
	,month_prod_cnt                 string      --月产量
	,month_sale_cnt                 string      --月销量
)PARTITIONED BY (op_day string)     
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_12="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_EIGHT_FIVE_DD_12 PARTITION(op_day = '$OP_DAY')
select 
	t1.day_id          
	,t1.organization_id
	,t1.bus_type       
	,t1.product_line   
	,t1.item_id        
	,t1.level4_sale_id 
	,t1.level4_manager 
	,t1.level5_sale_id 
	,t1.level5_manager	
	,t1.prod_cnt1_5    
	,t1.prod_sale1_5   
	,t1.store_1        
	,t1.store_5        
	,t1.prod_cnt6_10   
	,t1.prod_sale6_10  
	,t1.store_6        
	,t1.store_10       
	,t1.prod_cnt11_15  
	,t1.prod_sale11_15 
	,t1.store_11       
	,t1.store_15       
	,t1.prod_cnt16_20  
	,t1.prod_sale16_20 
	,t1.store_16       
	,t1.store_20       
	,t1.prod_cnt21_25  
	,t1.prod_sale21_25 
	,t1.store_21       
	,t1.store_25       
	,t1.prod_cnt26_30  
	,t1.prod_sale26_30 
	,t1.store_26       
	,t1.store_30       
	,t1.month_prod_cnt 
	,t1.month_sale_cnt 
from (select * from $TMP_DMP_BIRD_EIGHT_FIVE_DD_10 where op_day = '$OP_DAY' 
	union all select * from $TMP_DMP_BIRD_EIGHT_FIVE_DD_11 where op_day = '$OP_DAY') t1
"








	
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_EIGHT_FIVE_DD='DMP_BIRD_EIGHT_FIVE_DD'

CREATE_DMP_BIRD_EIGHT_FIVE_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_EIGHT_FIVE_DD (
	month_id						string		--期间(月份)
	,day_id                         string      --期间(日)
	,level1_org_id                  string      --组织1级(股份)
	,level1_org_descr               string      --组织1级(股份)
	,level2_org_id                  string      --组织2级(片联)
	,level2_org_descr               string      --组织2级(片联)
	,level3_org_id                  string      --组织3级(片区)
	,level3_org_descr               string      --组织3级(片区)
	,level4_org_id                  string      --组织4级(小片)
	,level4_org_descr               string      --组织4级(小片)
	,level5_org_id                  string      --组织5级(公司)
	,level5_org_descr               string      --组织5级(公司)
	,level6_org_id                  string      --组织6级(OU)
	,level6_org_descr               string      --组织6级(OU)
	,level7_org_id                  string      --组织7级(库存组织)
	,level7_org_descr               string      --组织7级(库存组织)
	,level1_businesstype_id         string      --业态1级
	,level1_businesstype_name       string      --业态1级
	,level2_businesstype_id         string      --业态2级
	,level2_businesstype_name       string      --业态2级
	,level3_businesstype_id         string      --业态3级
	,level3_businesstype_name       string      --业态3级
	,level4_businesstype_id         string      --业态4级
	,level4_businesstype_name       string      --业态4级
	,level1_sale_id                 string      --销售组织1级
	,level1_sale_descr              string      --销售组织1级
	,level2_sale_id                 string      --销售组织2级
	,level2_sale_descr              string      --销售组织2级
	,level3_sale_id                 string      --销售组织3级
	,level3_sale_descr              string      --销售组织3级
	,level4_sale_id                 string      --销售组织4级
	,level4_sale_descr              string      --销售组织4级
	,level5_sale_id                 string      --销售组织5级
	,level5_sale_descr              string      --销售组织5级
	,level4_manager                 string      --销售组织责任人4级
	,level5_manager					string		--销售组织责任人5级
	,production_line_id             string      --产线
	,production_line_descr          string      --产线
	,level1_prod_id                 string      --产品线1级
	,level1_prod_descr              string      --产品线1级
	,level2_prod_id                 string      --产品线2级
	,level2_prod_descr              string      --产品线2级
	,level1_prodtype_id             string      --产品分类1级
	,level1_prodtype_descr          string      --产品分类1级
	,level2_prodtype_id             string      --产品分类2级
	,level2_prodtype_descr          string      --产品分类2级
	,level3_prodtype_id             string      --产品分类3级
	,level3_prodtype_descr          string      --产品分类3级
	,prod_cnt1_5                    string      --1-5日产量
	,prod_sale1_5                   string      --1-5日销量
	,store_1                        string      --1日业务库存
	,store_5                        string      --5日业务库存
	,prod_cnt6_10                   string      --6-10日产量
	,prod_sale6_10                  string      --6-10日销量
	,store_6                        string      --6日业务库存
	,store_10                       string      --10日业务库存
	,prod_cnt11_15                  string      --11-15日产量
	,prod_sale11_15                 string      --11-15日销量
	,store_11                       string      --11日业务库存
	,store_15                       string      --15日业务库存
	,prod_cnt16_20                  string      --16-20日产量
	,prod_sale16_20                 string      --16-20日销量
	,store_16                       string      --16日业务库存
	,store_20                       string      --20日业务库存
	,prod_cnt21_25                  string      --21-25日产量
	,prod_sale21_25                 string      --21-25日销量
	,store_21                       string      --21日业务库存
	,store_25                       string      --25日业务库存
	,prod_cnt26_30                  string      --26-月末日产量
	,prod_sale26_30                 string      --26-月末日销量
	,store_26                       string      --26日业务库存
	,store_30                       string      --月末业务库存
	,month_prod_cnt                 string      --月产量
	,month_sale_cnt                 string      --月销量
	,create_time					string		--数据推送时间
)PARTITIONED BY (op_day string)     
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_EIGHT_FIVE_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_EIGHT_FIVE_DD PARTITION(op_day = '$OP_DAY')
select
	substring(t1.day_id,1,6) month_id						--期间(月份)
	,t1.day_id                                              --期间(日)
	,t6.level1_org_id       								--一级组织编码
	,t6.level1_org_descr    								--一级组织描述
	,t6.level2_org_id is    								--二级组织编码
	,t6.level2_org_descr    								--二级组织描述
	,t6.level3_org_id       								--三级组织编码
	,t6.level3_org_descr    								--三级组织描述
	,t6.level4_org_id       								--四级组织编码
	,t6.level4_org_descr    								--四级组织描述
	,t6.level5_org_id       								--五级组织编码
	,t6.level5_org_descr    								--五级组织描述
	,t6.level6_org_id       								--六级组织编码
	,t6.level6_org_descr    								--六级组织描述
	,t6.level7_org_id                                       --组织7级(库存组织)
	,t6.level7_org_descr                                    --组织7级(库存组织)
	,t8.level1_businesstype_id                              --业态1级
	,t8.level1_businesstype_name                            --业态1级
	,t8.level2_businesstype_id                              --业态2级
	,t8.level2_businesstype_name                            --业态2级
	,t8.level3_businesstype_id                              --业态3级
	,t8.level3_businesstype_name                            --业态3级
	,t8.level4_businesstype_id                              --业态4级
	,t8.level4_businesstype_name                            --业态4级
	,t9.first_sale_org_code                                 --销售组织1级
	,t9.first_sale_org_name                                 --销售组织1级
	,t9.second_sale_org_code                                --销售组织2级
	,t9.second_sale_org_name                                --销售组织2级
	,t9.three_sale_org_code                                 --销售组织3级
	,t9.three_sale_org_name                                 --销售组织3级
	,t9.four_sale_org_code                                  --销售组织4级
	,t9.four_sale_org_name                                  --销售组织4级
	,t9.five_sale_org_code                                  --销售组织5级
	,t9.five_sale_org_name                                  --销售组织5级
	,t9.four_sale_org_manage                                --销售组织责任人4级
	,t9.five_sale_org_manage                                --销售组织责任人5级
	,case when t1.product_line = '10' then '1'
			   when t1.product_line = '20' then '2'
		  else '-1' end					        			--产线
  	,case when t1.product_line = '10' then '鸡'
			   when t1.product_line = '20' then '鸭'
		  else '缺省' end  							  		--产线
	,t11.prd_line_cate_id	                                --产品线1级
	,t11.prd_line_cate					                    --产品线1级
	,t11.sub_prd_line_tp_id		                        	--产品线2级
	,t11.sub_prd_line_tp                                    --产品线2级
	,t11.first_lv_tp_id                                     --产品分类1级
	,t11.first_lv_tp                                   		--产品分类1级
	,t11.scnd_lv_tp_id                                      --产品分类2级
	,t11.scnd_lv_tp                                    		--产品分类2级
	,t11.thrd_lv_tp_id                                      --产品分类3级
	,t11.thrd_lv_tp                                    		--产品分类3级
	,t1.prod_cnt1_5                                         --1-5日产量
	,t1.prod_sale1_5                                        --1-5日销量
	,t1.store_1                                             --1日业务库存
	,t1.store_5                                             --5日业务库存
	,t1.prod_cnt6_10                                        --6-10日产量
	,t1.prod_sale6_10                                       --6-10日销量
	,t1.store_6                                             --6日业务库存
	,t1.store_10                                            --10日业务库存
	,t1.prod_cnt11_15                                       --11-15日产量
	,t1.prod_sale11_15                                      --11-15日销量
	,t1.store_11                                            --11日业务库存
	,t1.store_15                                            --15日业务库存
	,t1.prod_cnt16_20                                       --16-20日产量
	,t1.prod_sale16_20                                      --16-20日销量
	,t1.store_16                                            --16日业务库存
	,t1.store_20                                            --20日业务库存
	,t1.prod_cnt21_25                                       --21-25日产量
	,t1.prod_sale21_25                                      --21-25日销量
	,t1.store_21                                            --21日业务库存
	,t1.store_25                                            --25日业务库存
	,t1.prod_cnt26_30                                       --26-月末日产量
	,t1.prod_sale26_30                                      --26-月末日销量
	,t1.store_26                                            --26日业务库存
	,t1.store_30                                           	--月末业务库存
	,t1.month_prod_cnt                                      --月产量
	,t1.month_sale_cnt                                      --月销量
	,'$CREATE_TIME' create_time                             --数据推送时间
from (select * from $TMP_DMP_BIRD_EIGHT_FIVE_DD_12 where op_day = '$OP_DAY') t1
left join mreport_global.dim_org_inv_management t6
	ON t1.organization_id=t6.inv_org_id
left join mreport_global.dim_org_businesstype t8
	ON t1.bus_type = t8.level4_businesstype_id
left join mreport_global.dwu_dim_xs_org t9
	ON t1.level4_sale_id = t9.four_sale_org_code and t1.level5_sale_id = t9.five_sale_org_code
left join mreport_global.dwu_dim_material_new t10
	on (
		t1.item_id = t10.inventory_item_id  
		and t1.organization_id = t10.inv_org_id
	)
left join mreport_global.dim_crm_item t11
	 on t10.inventory_item_code = t11.item_code   
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
	$CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_1;
    $INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_1;
    $CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_2;
    $INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_2;
    $CREATE_TMP_DWU_BIRD_EIGHT_FIVE_DD_3;
    $INSERT_TMP_DWU_BIRD_EIGHT_FIVE_DD_3;
    $CREATE_DWU_BIRD_EIGHT_FIVE_DD;
    $INSERT_DWU_BIRD_EIGHT_FIVE_DD;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_1;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_1;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_2;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_2;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_3;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_3;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_4;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_4;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_5;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_5;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_6;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_6;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_7;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_7;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_8;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_8;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_9;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_9;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_10;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_10;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_11;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_11;
	$CREATE_TMP_DMP_BIRD_EIGHT_FIVE_DD_12;
    $INSERT_TMP_DMP_BIRD_EIGHT_FIVE_DD_12;
	$CREATE_DMP_BIRD_EIGHT_FIVE_DD;
    $INSERT_DMP_BIRD_EIGHT_FIVE_DD;
"  -v 




 