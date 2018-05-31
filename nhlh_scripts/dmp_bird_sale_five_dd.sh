#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_sale_five_dd.sh                               
# 创建时间: 2018年04月17日                                            
# 创 建 者: fwj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 每5日节拍销售任务量跟踪
# 修改说明:                                                          
######################################################################

OP_DAY=$1
# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)
# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_sale_five_dd.sh 20180101"
    exit 1
fi


###########################################################################################
## 将数据从大表转换至目标表 
## 清单表
## 变量声明
DWU_BIRD_SALE_FIVE_DD='DWU_BIRD_SALE_FIVE_DD'

CREATE_DWU_BIRD_SALE_FIVE_DD="
CREATE TABLE IF NOT EXISTS $DWU_BIRD_SALE_FIVE_DD(
	org_id                             	string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料编码
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,out_date							string		--出库日期
	,out_qty							string		--出库量
	,resource_name						string		--销售员
)                      
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWU_BIRD_SALE_FIVE_DD="
INSERT OVERWRITE TABLE $DWU_BIRD_SALE_FIVE_DD PARTITION(op_day = '$OP_DAY')
SELECT   									
	org_id                            										--公司id
	,product_line                                      						--产线
	,bus_type																--业态
	,item_id																--物料id
	,fifth_org_id															--销售机构id
	,organization_id          												--库存组织ID
	,out_date																--出库日期
	,out_qty																--出库主数量
	,resource_name															--销售员
from mreport_poultry.dwu_gyl_xs01_dd
WHERE op_day='$OP_DAY'  AND out_date IS NOT NULL;
"


###########################################################################################
## 将数据从大表转换至目标表 
## 时间维表
## 变量声明
TMP_DMP_BIRD_SALE_FIVE_DD_0='TMP_DMP_BIRD_SALE_FIVE_DD_0'

CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_FIVE_DD_0 (
	day_id								string		--日期
	,stage								string		--阶段
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_FIVE_DD_0 
SELECT
	day_id				--日期
	,case when substring(day_id,7,2) >= '01' and substring(day_id,7,2) <= '05' then '1'
		  when substring(day_id,7,2) >= '06' and substring(day_id,7,2) <= '10' then '2'
		  when substring(day_id,7,2) >= '11' and substring(day_id,7,2) <= '15' then '3'
		  when substring(day_id,7,2) >= '16' and substring(day_id,7,2) <= '20' then '4'
		  when substring(day_id,7,2) >= '21' and substring(day_id,7,2) <= '25' then '5'
		  else '6' end stage												--阶段
FROM  mreport_global.dim_day 
where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')

"




###########################################################################################
## 将数据从大表转换至目标表 
## 每日数据聚合
## 变量声明
TMP_DMP_BIRD_SALE_FIVE_DD_1='TMP_DMP_BIRD_SALE_FIVE_DD_1'

CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_FIVE_DD_1 (
	day_id								string		--日期
	,stage								string		--阶段
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料编码
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,resource_name						string		--销售员
	,cnt_sale_dd						string		--每日销量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_FIVE_DD_1 PARTITION(op_day = '$OP_DAY')
SELECT
	out_date day_id				--日期
	,case when substring(out_date,7,2) >= '01' and substring(out_date,7,2) <= '05' then '1'
		  when substring(out_date,7,2) >= '06' and substring(out_date,7,2) <= '10' then '2'
		  when substring(out_date,7,2) >= '11' and substring(out_date,7,2) <= '15' then '3'
		  when substring(out_date,7,2) >= '16' and substring(out_date,7,2) <= '20' then '4'
		  when substring(out_date,7,2) >= '21' and substring(out_date,7,2) <= '25' then '5'
		  else '6' end stage												--阶段
	,org_id                            										--公司id
	,product_line                                      						--产线
	,bus_type																--业态
	,item_id																--物料id
	,fifth_org_id															--销售机构id
	,organization_id          												--库存组织ID
	,resource_name															--销售员
	,SUM(nvl(out_qty,0)) cnt_sale_dd											--每日销量
FROM $DWU_BIRD_SALE_FIVE_DD
where op_day = '$OP_DAY' and out_date is not null
GROUP BY
	out_date
	,org_id                            								
	,product_line                                      				
	,bus_type														
	,item_id														
	,fifth_org_id													
	,organization_id          										
	,resource_name;
"

###########################################################################################
## 将数据从大表转换至目标表 
## 每5日数据聚合
## 变量声明
TMP_DMP_BIRD_SALE_FIVE_DD_2='TMP_DMP_BIRD_SALE_FIVE_DD_2'

CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_FIVE_DD_2 (
	day_id						    string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料编码
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,resource_name						string		--销售员
	,cnt_sale1_5						string		--1-5日销量
	,cnt_sale6_10						string		--6-10日销量
	,cnt_sale11_15						string		--11-15日销量
	,cnt_sale16_20						string		--16-20日销量
	,cnt_sale21_25						string		--21-25日销量
	,cnt_sale26_31						string		--26-31日销量
)
PARTITIONED BY (op_day STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_FIVE_DD_2 PARTITION(op_day = '$OP_DAY')
SELECT
	t1.day_id																																					--日期 	
	,'缺矢'                            																														--公司id
	,t2.product_line                                      																										--产线
	,t2.bus_type																																				--业态
	,'缺矢'																																					--物料id
	,t2.fifth_org_id																																			--销售机构id
	,'缺矢'          																																--库存组织ID
	,t2.resource_name																																			--销售员
	,SUM(case when substring(t1.day_id,7,2) >= '01' and substring(t1.day_id,7,2) <= '05' and t1.day_id >= t2.day_id then t2.cnt_sale_dd 
	          when substring(t1.day_id,7,2) >= '06' and substring(t2.day_id,7,2) >= '01' and substring(t2.day_id,7,2) <= '05' then t2.cnt_sale_dd 
			  else 0 end) cnt_sale1_5	--1-5日销量
	,SUM(case when substring(t1.day_id,7,2) >= '06' and substring(t1.day_id,7,2) <= '10' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '06' then t2.cnt_sale_dd 
			  when substring(t1.day_id,7,2) >= '11' and substring(t2.day_id,7,2) >= '06' and substring(t2.day_id,7,2) <= '10' then t2.cnt_sale_dd
			  else 0 end) cnt_sale6_10	--6-10日销量
	,SUM(case when substring(t1.day_id,7,2) >= '11' and substring(t1.day_id,7,2) <= '15' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '11' then t2.cnt_sale_dd 
			  when substring(t1.day_id,7,2) >= '16' and substring(t2.day_id,7,2) >= '11' and substring(t2.day_id,7,2) <= '15' then t2.cnt_sale_dd
			  else 0 end) cnt_sale11_15 --11-15日销量
	,SUM(case when substring(t1.day_id,7,2) >= '16' and substring(t1.day_id,7,2) <= '20' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '16' then t2.cnt_sale_dd 
			  when substring(t1.day_id,7,2) >= '21' and substring(t2.day_id,7,2) >= '16' and substring(t2.day_id,7,2) <= '20' then t2.cnt_sale_dd
			  else 0 end) cnt_sale16_20 --16-20日销量
	,SUM(case when substring(t1.day_id,7,2) >= '21' and substring(t1.day_id,7,2) <= '25' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '21' then t2.cnt_sale_dd 
			  when substring(t1.day_id,7,2) >= '26' and substring(t2.day_id,7,2) >= '21' and substring(t2.day_id,7,2) <= '25' then t2.cnt_sale_dd
	          else 0 end) cnt_sale21_25 --21-25日销量
	,SUM(case when substring(t1.day_id,7,2) >= '26' and substring(t1.day_id,7,2) <= '31' and t1.day_id >= t2.day_id and substring(t2.day_id,7,2) >= '26' then t2.cnt_sale_dd 
			  else 0 end) cnt_sale26_31 --26-31日销量
FROM  $TMP_DMP_BIRD_SALE_FIVE_DD_0  t1
LEFT JOIN $TMP_DMP_BIRD_SALE_FIVE_DD_1 t2
	ON (
		t2.op_day = '$OP_DAY' and substring(t1.day_id,1,6) = substring(t2.day_id,1,6)
	)
GROUP BY
	t1.day_id
	,t1.stage
	,'缺矢'                            								
	,t2.product_line                                      				
	,t2.bus_type														
	,'缺矢'														
	,t2.fifth_org_id													
	,'缺矢'         										
	,t2.resource_name;
"

###########################################################################################
## 将数据从大表转换至目标表 
## 每5日数据聚合报表
## 变量声明
TMP_DMP_BIRD_SALE_FIVE_DD_3='TMP_DMP_BIRD_SALE_FIVE_DD_3'

CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_FIVE_DD_3 (
	month_id								string			--期间(月份)
	,day_id                                 string          --期间(日)
	,level1_org_id                          string          --组织1级(股份)
	,level1_org_descr                       string          --组织1级(股份)
	,level2_org_id                          string          --组织2级(片联)
	,level2_org_descr                       string          --组织2级(片联)
	,level3_org_id                          string          --组织3级(片区)
	,level3_org_descr                       string          --组织3级(片区)
	,level4_org_id                          string          --组织4级(小片)
	,level4_org_descr                       string          --组织4级(小片)
	,level5_org_id                          string          --组织5级(公司)
	,level5_org_descr                       string          --组织5级(公司)
	,level6_org_id                          string          --组织6级(OU)
	,level6_org_descr                       string          --组织6级(OU)
	,level7_org_id                          string          --组织7级(库存组织)
	,level7_org_descr                       string          --组织7级(库存组织)
	,level1_businesstype_id                 string          --业态1级
	,level1_businesstype_name               string          --业态1级
	,level2_businesstype_id                 string          --业态2级
	,level2_businesstype_name               string          --业态2级
	,level3_businesstype_id                 string          --业态3级
	,level3_businesstype_name               string          --业态3级
	,level4_businesstype_id                 string          --业态4级
	,level4_businesstype_name               string          --业态4级
	,level1_sale_id                         string          --销售组织1级
	,level1_sale_descr                      string          --销售组织1级
	,level2_sale_id                         string          --销售组织2级
	,level2_sale_descr                      string          --销售组织2级
	,level3_sale_id                         string          --销售组织3级
	,level3_sale_descr                      string          --销售组织3级
	,level4_sale_id                         string          --销售组织4级
	,level4_sale_descr                      string          --销售组织4级
	,level5_sale_id                         string          --销售组织5级
	,level5_sale_descr                      string          --销售组织5级
	,production_line_id                     string          --产线
	,production_line_descr                  string          --产线
	,level1_prod_id                         string          --产品线1级
	,level1_prod_descr                      string          --产品线1级
	,level2_prod_id                         string          --产品线2级
	,level2_prod_descr                      string          --产品线2级
	,business_id                            string          --业务员ID
	,business_name                          string          --业务员名称
	,obj_sale1_5                            string          --1-5日销售任务量
	,obj_sale6_10                           string          --6-10日销售任务量
	,obj_sale11_15                          string          --11-15日销售任务量
	,obj_sale16_20                          string          --16-20日销售任务量
	,obj_sale21_25                          string          --21-25日销售任务量
	,obj_sale26_31                          string          --26-31日销售任务量
	,cnt_sale1_5                            string          --1-5日销量
	,cnt_sale6_10                           string          --6-10日销量
	,cnt_sale11_15                          string          --11-15日销量
	,cnt_sale16_20                          string          --16-20日销量
	,cnt_sale21_25                          string          --21-25日销量
	,cnt_sale26_31                          string          --26-31日销量
)PARTITIONED BY (op_day string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"                       

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_FIVE_DD_3 PARTITION(op_day = '$OP_DAY') 
SELECT
	substring(t1.day_id,1,6)			 						 --月份
	,t1.day_id									 --日期
	,'缺矢'               --一级组织编码
    ,'缺矢'   --一级组织描述
    ,'缺矢'                --二级组织编码
    ,'缺矢'   --二级组织描述
    ,'缺矢'                --三级组织编码
    ,'缺矢'   --三级组织描述
    ,'缺矢'                --四级组织编码
    ,'缺矢'   --四级组织描述
    ,'缺矢'                --五级组织编码
    ,'缺矢'   --五级组织描述
    ,'缺矢'                --六级组织编码
    ,'缺矢'   --六级组织描述
	,'缺矢'                          	 --组织7级(库存组织)
	,'缺矢'                         --组织7级(库存组织)
	,t4.level1_businesstype_id                   --业态1级
	,t4.level1_businesstype_name                 --业态1级
	,t4.level2_businesstype_id                   --业态2级
	,t4.level2_businesstype_name                 --业态2级
	,t4.level3_businesstype_id                   --业态3级
	,t4.level3_businesstype_name                 --业态3级
	,t4.level4_businesstype_id                   --业态4级
	,t4.level4_businesstype_name                 --业态4级
	,t5.first_sale_org_code                      --销售组织1级
	,t5.first_sale_org_name                      --销售组织1级
	,t5.second_sale_org_code                     --销售组织2级
	,t5.second_sale_org_name                     --销售组织2级
	,t5.three_sale_org_code                      --销售组织3级
	,t5.three_sale_org_name                      --销售组织3级
	,t5.four_sale_org_code                       --销售组织4级
	,t5.four_sale_org_name                       --销售组织4级
	,t5.five_sale_org_code                       --销售组织5级
	,t5.five_sale_org_name                       --销售组织5级
	,case when t1.product_line = '10' then '1'
			   when t1.product_line = '20' then '2'
		  else '-1' end	             	             --产线
	,case when t1.product_line = '10' then '鸡'
			   when t1.product_line = '20' then '鸭'
		  else '缺省' end  							 --产线
	,'缺矢'					         --产品线1级
	,'缺矢'					             --产品线1级
	,'缺矢' 				         --产品线2级
	,'缺矢'	                 --产品线2级
	,split(t1.resource_name,',')[0]              --业务员ID
	,t1.resource_name                               --业务员名称
	,''                               			 --1-5日销售任务量
	,''                                			 --6-10日销售任务量
	,''                               			 --11-15日销售任务量
	,''                               			 --16-20日销售任务量
	,''                               			 --21-25日销售任务量
	,''                               			 --26-31日销售任务量
	,t1.cnt_sale1_5                              --1-5日销量
	,t1.cnt_sale6_10                             --6-10日销量
	,t1.cnt_sale11_15                            --11-15日销量
	,t1.cnt_sale16_20                            --16-20日销量
	,t1.cnt_sale21_25                            --21-25日销量
	,t1.cnt_sale26_31                            --26-31日销量
FROM (select * from $TMP_DMP_BIRD_SALE_FIVE_DD_2 where op_day = '$OP_DAY') t1
left join mreport_global.dim_org_businesstype t4
	ON t1.bus_type = t4.level4_businesstype_id
left join mreport_global.dwu_dim_xs_org t5
	ON t1.fifth_org_id = t5.sale_org_code
"

###########################################################################################
## 将数据从大表转换至目标表 
## sg02表数据
## 变量声明
TMP_DMP_BIRD_SALE_FIVE_DD_4='TMP_DMP_BIRD_SALE_FIVE_DD_4'

CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_FIVE_DD_4 (
	month_id								string			--期间(月份)
	,day_id                                 string          --期间(日)
	,level1_org_id                          string          --组织1级(股份)
	,level1_org_descr                       string          --组织1级(股份)
	,level2_org_id                          string          --组织2级(片联)
	,level2_org_descr                       string          --组织2级(片联)
	,level3_org_id                          string          --组织3级(片区)
	,level3_org_descr                       string          --组织3级(片区)
	,level4_org_id                          string          --组织4级(小片)
	,level4_org_descr                       string          --组织4级(小片)
	,level5_org_id                          string          --组织5级(公司)
	,level5_org_descr                       string          --组织5级(公司)
	,level6_org_id                          string          --组织6级(OU)
	,level6_org_descr                       string          --组织6级(OU)
	,level7_org_id                          string          --组织7级(库存组织)
	,level7_org_descr                       string          --组织7级(库存组织)
	,level1_businesstype_id                 string          --业态1级
	,level1_businesstype_name               string          --业态1级
	,level2_businesstype_id                 string          --业态2级
	,level2_businesstype_name               string          --业态2级
	,level3_businesstype_id                 string          --业态3级
	,level3_businesstype_name               string          --业态3级
	,level4_businesstype_id                 string          --业态4级
	,level4_businesstype_name               string          --业态4级
	,level1_sale_id                         string          --销售组织1级
	,level1_sale_descr                      string          --销售组织1级
	,level2_sale_id                         string          --销售组织2级
	,level2_sale_descr                      string          --销售组织2级
	,level3_sale_id                         string          --销售组织3级
	,level3_sale_descr                      string          --销售组织3级
	,level4_sale_id                         string          --销售组织4级
	,level4_sale_descr                      string          --销售组织4级
	,level5_sale_id                         string          --销售组织5级
	,level5_sale_descr                      string          --销售组织5级
	,production_line_id                     string          --产线
	,production_line_descr                  string          --产线
	,level1_prod_id                         string          --产品线1级
	,level1_prod_descr                      string          --产品线1级
	,level2_prod_id                         string          --产品线2级
	,level2_prod_descr                      string          --产品线2级
	,business_id                            string          --业务员ID
	,business_name                          string          --业务员名称
	,obj_sale1_5                            string          --1-5日销售任务量
	,obj_sale6_10                           string          --6-10日销售任务量
	,obj_sale11_15                          string          --11-15日销售任务量
	,obj_sale16_20                          string          --16-20日销售任务量
	,obj_sale21_25                          string          --21-25日销售任务量
	,obj_sale26_31                          string          --26-31日销售任务量
)PARTITIONED BY (op_day string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"                       

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_FIVE_DD_4 PARTITION(op_day = '$OP_DAY') 
SELECT
	t3.month_id			 --月份
	,t3.day_id									 --日期
	,'缺矢'                         --组织1级(股份)
	,'缺矢'                      --组织1级(股份)
	,'缺矢'                         --组织2级(片联)
	,'缺矢'                      --组织2级(片联)
	,'缺矢'                        --组织3级(片区)
	,'缺矢'                      --组织3级(片区)
	,'缺矢'                         --组织4级(小片)
	,'缺矢'                      --组织4级(小片)
	,'缺矢'                         --组织5级(公司)
	,'缺矢'                      --组织5级(公司)
	,'缺矢'                         --组织6级(OU)
	,'缺矢'                      --组织6级(OU)
	,'缺矢'                       	 --组织7级(库存组织)
	,'缺矢'                      --组织7级(库存组织)
	,t2.level1_businesstype_id                --业态1级
	,t2.level1_businesstype_name              --业态1级
	,t2.level2_businesstype_id                --业态2级
	,t2.level2_businesstype_name              --业态2级
	,t2.level3_businesstype_id                --业态3级
	,t2.level3_businesstype_name              --业态3级
	,t2.level4_businesstype_id                --业态4级
	,t2.level4_businesstype_name              --业态4级
	,t4.first_sale_org_code                       --销售组织1级
	,t4.first_sale_org_name                       --销售组织1级
	,t4.second_sale_org_code                      --销售组织2级
	,t4.second_sale_org_name                      --销售组织2级
	,t4.three_sale_org_code                       --销售组织3级
	,t4.three_sale_org_name                       --销售组织3级
	,t4.four_sale_org_code                        --销售组织4级
	,t4.four_sale_org_name                        --销售组织4级
	,t4.five_sale_org_code                        --销售组织5级
	,t4.five_sale_org_name                        --销售组织5级
	,case when t1.PRODUCT_LINE_CODE = '10' then '1'
			   when t1.PRODUCT_LINE_CODE = '20' then '2'
		  else '-1' end					        --产线代码
	,case when t1.PRODUCT_LINE_CODE = '10' then '鸡'
			   when t1.PRODUCT_LINE_CODE = '20' then '鸭'
		  else '缺省' end  							  --产线     	 		  
	,'缺矢'                        --产品线1级
	,'缺矢'                     --产品线1级
	,'缺矢'                        --产品线2级
	,'缺矢'                     --产品线2级
	,split(t1.agent_name,',')[0]                           --业务员ID
	,t1.agent_name                         --业务员名称
	,t1.task_1                			      --1-5日销售任务量
	,t1.task_2                  			  --6-10日销售任务量
	,t1.task_3                 			      --11-15日销售任务量
	,t1.task_4                 			      --16-20日销售任务量
	,t1.task_5                 			      --21-25日销售任务量
	,t1.task_6                 			      --26-31日销售任务量
FROM (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','') ) t3	
left join mreport_global.dwu_dim_oe_sale_task_all t1
	on regexp_replace(t1.period_name,'-','') = t3.month_id
left join mreport_global.dim_org_businesstype t2
	ON t1.type = t2.level4_businesstype_id
left join mreport_global.dwu_dim_xs_org t4
	ON t1.sale_org_code_segments = t4.sale_org_code
"

###########################################################################################
## 将数据从大表转换至目标表 
## 每5日数据聚合最终报表
## 变量声明
DMP_BIRD_SALE_FIVE_DD='DMP_BIRD_SALE_FIVE_DD'

CREATE_DMP_BIRD_SALE_FIVE_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SALE_FIVE_DD (
	month_id								string			--期间(月份)
	,day_id                                 string          --期间(日)
	,level1_org_id                          string          --组织1级(股份)
	,level1_org_descr                       string          --组织1级(股份)
	,level2_org_id                          string          --组织2级(片联)
	,level2_org_descr                       string          --组织2级(片联)
	,level3_org_id                          string          --组织3级(片区)
	,level3_org_descr                       string          --组织3级(片区)
	,level4_org_id                          string          --组织4级(小片)
	,level4_org_descr                       string          --组织4级(小片)
	,level5_org_id                          string          --组织5级(公司)
	,level5_org_descr                       string          --组织5级(公司)
	,level6_org_id                          string          --组织6级(OU)
	,level6_org_descr                       string          --组织6级(OU)
	,level7_org_id                          string          --组织7级(库存组织)
	,level7_org_descr                       string          --组织7级(库存组织)
	,level1_businesstype_id                 string          --业态1级
	,level1_businesstype_name               string          --业态1级
	,level2_businesstype_id                 string          --业态2级
	,level2_businesstype_name               string          --业态2级
	,level3_businesstype_id                 string          --业态3级
	,level3_businesstype_name               string          --业态3级
	,level4_businesstype_id                 string          --业态4级
	,level4_businesstype_name               string          --业态4级
	,level1_sale_id                         string          --销售组织1级
	,level1_sale_descr                      string          --销售组织1级
	,level2_sale_id                         string          --销售组织2级
	,level2_sale_descr                      string          --销售组织2级
	,level3_sale_id                         string          --销售组织3级
	,level3_sale_descr                      string          --销售组织3级
	,level4_sale_id                         string          --销售组织4级
	,level4_sale_descr                      string          --销售组织4级
	,level5_sale_id                         string          --销售组织5级
	,level5_sale_descr                      string          --销售组织5级
	,production_line_id                     string          --产线
	,production_line_descr                  string          --产线
	,level1_prod_id                         string          --产品线1级
	,level1_prod_descr                      string          --产品线1级
	,level2_prod_id                         string          --产品线2级
	,level2_prod_descr                      string          --产品线2级
	,business_id                            string          --业务员ID
	,business_name                          string          --业务员名称
	,obj_sale1_5                            string          --1-5日销售任务量
	,obj_sale6_10                           string          --6-10日销售任务量
	,obj_sale11_15                          string          --11-15日销售任务量
	,obj_sale16_20                          string          --16-20日销售任务量
	,obj_sale21_25                          string          --21-25日销售任务量
	,obj_sale26_31                          string          --26-31日销售任务量
	,cnt_sale1_5                            string          --1-5日销量
	,cnt_sale6_10                           string          --6-10日销量
	,cnt_sale11_15                          string          --11-15日销量
	,cnt_sale16_20                          string          --16-20日销量
	,cnt_sale21_25                          string          --21-25日销量
	,cnt_sale26_31                          string          --26-31日销量
	,balc_1_5                               string          --1-5日任务结余
	,balc_6_10                              string          --6-10日任务结余
	,balc_11_15                             string          --11-15日任务结余
	,balc_16_20                              string          --16-20日任务结余
	,balc_21_25                             string          --21-25日任务结余
	,balc_26_30                             string          --26-月末日任务结余
	,create_time							string			--数据推送时间
)PARTITIONED BY (op_day string) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"                       

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SALE_FIVE_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_SALE_FIVE_DD PARTITION(op_day = '$OP_DAY') 
SELECT
	nvl(t1.month_id,t2.month_id)			 --月份
	,nvl(t1.day_id,t2.day_id)									 --日期
	,nvl(t1.level1_org_id,t2.level1_org_id)                         --组织1级(股份)
	,nvl(t1.level1_org_descr,t2.level1_org_descr)                      --组织1级(股份)
	,nvl(t1.level2_org_id,t2.level2_org_id)                         --组织2级(片联)
	,nvl(t1.level2_org_descr,t2.level2_org_descr)                      --组织2级(片联)
	,nvl(t1.level3_org_id,t2.level3_org_id)                         --组织3级(片区)
	,nvl(t1.level3_org_descr,t2.level3_org_descr)                      --组织3级(片区)
	,nvl(t1.level4_org_id,t2.level4_org_id)                         --组织4级(小片)
	,nvl(t1.level4_org_descr,t2.level4_org_descr)                      --组织4级(小片)
	,nvl(t1.level5_org_id,t2.level5_org_id)                         --组织5级(公司)
	,nvl(t1.level5_org_descr,t2.level5_org_descr)                      --组织5级(公司)
	,nvl(t1.level6_org_id,t2.level6_org_id)                         --组织6级(OU)
	,nvl(t1.level6_org_descr,t2.level6_org_descr)                      --组织6级(OU)
	,nvl(t1.level7_org_id,t2.level7_org_id)                       	 --组织7级(库存组织)
	,nvl(t1.level7_org_descr,t2.level7_org_descr)                      --组织7级(库存组织)
	,nvl(t1.level1_businesstype_id,t2.level1_businesstype_id)                --业态1级
	,nvl(t1.level1_businesstype_name,t2.level1_businesstype_name)              --业态1级
	,nvl(t1.level2_businesstype_id,t2.level2_businesstype_id)                --业态2级
	,nvl(t1.level2_businesstype_name,t2.level2_businesstype_name)              --业态2级
	,nvl(t1.level3_businesstype_id,t2.level3_businesstype_id)                --业态3级
	,nvl(t1.level3_businesstype_name,t2.level3_businesstype_name)              --业态3级
	,nvl(t1.level4_businesstype_id,t2.level4_businesstype_id)                --业态4级
	,nvl(t1.level4_businesstype_name,t2.level4_businesstype_name)              --业态4级
	,nvl(t1.level1_sale_id,t2.level1_sale_id)                        --销售组织1级
	,nvl(t1.level1_sale_descr,t2.level1_sale_descr)                     --销售组织1级
	,nvl(t1.level2_sale_id,t2.level2_sale_id)                        --销售组织2级
	,nvl(t1.level2_sale_descr,t2.level2_sale_descr)                     --销售组织2级
	,nvl(t1.level3_sale_id,t2.level3_sale_id)                        --销售组织3级
	,nvl(t1.level3_sale_descr,t2.level3_sale_descr)                     --销售组织3级
	,nvl(t1.level4_sale_id,t2.level4_sale_id)                        --销售组织4级
	,nvl(t1.level4_sale_descr,t2.level4_sale_descr)                     --销售组织4级
	,nvl(t1.level5_sale_id,t2.level5_sale_id)                       --销售组织5级
	,nvl(t1.level5_sale_descr,t2.level5_sale_descr)                     --销售组织5级
	,nvl(t1.production_line_id,t2.production_line_id)                    --产线
	,nvl(t1.production_line_descr,t2.production_line_descr)     	 		  --产线
	,nvl(t1.level1_prod_id,t2.level1_prod_id)                        --产品线1级
	,nvl(t1.level1_prod_descr,t2.level1_prod_descr)                     --产品线1级
	,nvl(t1.level2_prod_id,t2.level2_prod_id)                        --产品线2级
	,nvl(t1.level2_prod_descr,t2.level2_prod_descr)                     --产品线2级
	,nvl(t1.business_id,t2.business_id)                           --业务员ID
	,nvl(t1.business_name,t2.business_name)                         --业务员名称
	,nvl(t2.obj_sale1_5,0)                 			      --1-5日销售任务量
	,nvl(t2.obj_sale6_10,0)                  			  --6-10日销售任务量
	,nvl(t2.obj_sale11_15,0)                 			      --11-15日销售任务量
	,nvl(t2.obj_sale16_20,0)                 			      --16-20日销售任务量
	,nvl(t2.obj_sale21_25,0)                 			      --21-25日销售任务量
	,nvl(t2.obj_sale26_31,0)                 			      --26-31日销售任务量
	,nvl(t1.cnt_sale1_5,0)                              --1-5日销量
	,nvl(t1.cnt_sale6_10,0)                             --6-10日销量
	,nvl(t1.cnt_sale11_15,0)                            --11-15日销量
	,nvl(t1.cnt_sale16_20,0)                            --16-20日销量
	,nvl(t1.cnt_sale21_25,0)                            --21-25日销量
	,nvl(t1.cnt_sale26_31,0)                            --26-31日销量
	,''                                    		 --1-5日任务结余
	,''                                   		 --6-10日任务结余
	,''                                  		 --11-15日任务结余
	,''                                  		 --16-20日任务结余
	,''                                  		 --21-25日任务结余
	,''											 --26-月末日任务结余
	,'$CREATE_TIME' create_time					 --数据推送时间
FROM (select * from $TMP_DMP_BIRD_SALE_FIVE_DD_3 where op_day = '$OP_DAY') t1
full outer join (select * from $TMP_DMP_BIRD_SALE_FIVE_DD_4 where op_day = '$OP_DAY') t2
	on (
		t1.month_id = t2.month_id
		and t1.day_id = t2.day_id
		and t1.business_name = t2.business_name
		and t1.level1_sale_id  = t2.level1_sale_id
		and t1.level2_sale_id  = t2.level2_sale_id
		and t1.level3_sale_id  = t2.level3_sale_id
		and t1.level4_sale_id  = t2.level4_sale_id
		and t1.level5_sale_id  = t2.level5_sale_id 
		and t1.production_line_id = t2.production_line_id
	)

	
	
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DWU_BIRD_SALE_FIVE_DD;
    $INSERT_DWU_BIRD_SALE_FIVE_DD;
	$CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_0;
    $INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_0;
    $CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_1;
    $INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_1;
    $CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_2;
    $INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_2;
	$CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_3;
    $INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_3;
	$CREATE_TMP_DMP_BIRD_SALE_FIVE_DD_4;
    $INSERT_TMP_DMP_BIRD_SALE_FIVE_DD_4;
    $CREATE_DMP_BIRD_SALE_FIVE_DD;
    $INSERT_DMP_BIRD_SALE_FIVE_DD;
"  -v