#!/bin/bash

######################################################################
#                                                                    
# 程    序: DMP_BIRD_SALE_MKT_MM.sh                               
# 创建时间: 2018年04月16日                                            
# 创 建 者: fwj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 区域市场销量情况
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}
OP_LAST_MONTH=$(date -d "$OP_DAY -1 months" "+%Y%m" )
OP_LAST_YEAR_MONTH=$(date -d "$OP_DAY -1 years" "+%Y%m" )
# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_sale_mkt_mm.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_0='TMP_DMP_BIRD_SALE_MKT_MM_0'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_0(
     day_id								string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料编码
	,organization_id                    string      --库存组织ID
	,cust_id							string		--客户ID
	,day_sale_cnt						string		--日销量 
	,day_sale_amt						string		--日销售金额 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC     
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_0 PARTITION(op_month='$OP_MONTH')
SELECT 
	approve_date day_id		    												--月份   									
	,org_id                            										--公司id
	,product_line                                      						--产线
	,bus_type																--业态
	,item_id																--物料id
	,organization_id          												--库存组织ID
	,agent_code	cust_id													--客户ID
	,sum(nvl(out_main_qty,0)) day_sale_cnt 										--日销量
	,sum(nvl(out_main_qty,0) * nvl(loc_execute_price,0))	day_sale_amt			--日销售金额 
from mreport_poultry.dwu_gyl_xs01_dd t1
where op_day='$OP_DAY' and approve_date IS NOT NULL
group by 
    approve_date
	,organization_id                 
	,org_id                            
	,product_line                                      
	,bus_type
	,item_id
	,agent_code	;
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_1='TMP_DMP_BIRD_SALE_MKT_MM_1'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_1(
     month_id							string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,item_id							string      --物料编码
	,organization_id                    string      --库存组织ID
	,cust_id							string		--客户ID
	,month_sale_cnt						string		--本月销量 
	,month_sale_amt						string		--本月销售金额 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC     
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.month_id									    							--月份
	,t2.org_id                            										--公司id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.item_id																	--物料id
	,t2.organization_id          												--库存组织ID
	,t2.cust_id																	--客户ID
	,sum(day_sale_cnt) month_sale_cnt 									--本月销量
	,sum(day_sale_amt) month_sale_amt									--本月销售金额 
from (select distinct month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_SALE_MKT_MM_0 t2
	on (
		t2.op_month = '$OP_MONTH'
		and t1.month_id = substring(t2.day_id,1,6)
	)
group by 
    t1.month_id    
	,t2.org_id                          
	,t2.product_line                                    
	,t2.bus_type			
	,t2.item_id				
	,t2.organization_id     
	,t2.cust_id				
"





###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_4='TMP_DMP_BIRD_SALE_MKT_MM_4'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_4(
    month_id							string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,cust_id							string		--客户ID
	,month_sale_cnt						string		--本月销量 
	,month_sale_amt						string		--本月销售金额 
	,l_month_sale_cnt					string		--上月销量 
	,l_month_sale_amt					string		--上月销售金额
	,l_year_sale_cnt					string		--去年同期销量 
	,l_year_sale_amt					string		--去年同期销售金额
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC     
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_4 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.month_id									    							--月份
	,t1.org_id																	--公司
	,t1.product_line                                      						--产线
	,t1.bus_type																--业态
	,t1.cust_id																	--客户ID
	,sum(t1.month_sale_cnt) month_sale_cnt 									--本月销量
	,sum(t1.month_sale_amt) month_sale_amt									--本月销售金额
	,'0'
	,'0'
	,'0'
	,'0'
from $TMP_DMP_BIRD_SALE_MKT_MM_1 t1 where op_month='$OP_MONTH' 
group by 
    t1.month_id
	,t1.org_id       
	,t1.product_line                                    
	,t1.bus_type			
	,t1.cust_id				
	
	
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_5='TMP_DMP_BIRD_SALE_MKT_MM_5'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_5="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_5(
     month_id							string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,cust_id							string		--客户ID
	,month_sale_cnt						string		--本月销量 
	,month_sale_amt						string		--本月销售金额 
	,l_month_sale_cnt					string		--上月销量 
	,l_month_sale_amt					string		--上月销售金额
	,l_year_sale_cnt					string		--去年同期销量 
	,l_year_sale_amt					string		--去年同期销售金额 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC        
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_5="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_5 PARTITION(op_month='$OP_MONTH')
SELECT 
	CASE WHEN SUBSTR(t2.month_id,5,2) = '12' THEN CONCAT(floor(SUBSTR(t2.month_id,1,4) + 1),'01') 
	ELSE CONCAT(floor(t2.month_id + 1)) END		    						--月份
	,t2.org_id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.cust_id																--客户ID
	,'0'
	,'0'
	,t2.month_sale_cnt l_month_sale_cnt 									--上月销量
	,t2.month_sale_amt l_month_sale_amt										--上月销售金额
	,'0'
	,'0' 
from  $TMP_DMP_BIRD_SALE_MKT_MM_4 t2 where op_month='$OP_MONTH' 
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_6='TMP_DMP_BIRD_SALE_MKT_MM_6'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_6="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_6(
	month_id							string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,cust_id							string		--客户ID
	,month_sale_cnt						string		--本月销量 
	,month_sale_amt						string		--本月销售金额 
	,l_month_sale_cnt					string		--上月销量 
	,l_month_sale_amt					string		--上月销售金额
	,l_year_sale_cnt					string		--去年同期销量 
	,l_year_sale_amt					string		--去年同期销售金额 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_6="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_6 PARTITION(op_month='$OP_MONTH')
SELECT 
	concat(floor(substring(t2.month_id,1,4)+1),substring(t2.month_id,5,2)) month_id		--月份
	,t2.org_id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.cust_id																--客户ID
	,'0'
	,'0'
	,'0'
	,'0'
	,t2.month_sale_cnt l_year_sale_cnt 										--去年同期销量
	,t2.month_sale_amt l_year_sale_amt										--去年同期销售金额 
from  $TMP_DMP_BIRD_SALE_MKT_MM_4 t2 where op_month='$OP_MONTH' 
	
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MKT_MM_7='TMP_DMP_BIRD_SALE_MKT_MM_7'

CREATE_TMP_DMP_BIRD_SALE_MKT_MM_7="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MKT_MM_7(
	month_id							string		--月份
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,cust_id							string		--客户ID
	,month_sale_cnt						string		--本月销量 
	,month_sale_amt						string		--本月销售金额 
	,l_month_sale_cnt					string		--上月销量 
	,l_month_sale_amt					string		--上月销售金额
	,l_year_sale_cnt					string		--去年同期销量 
	,l_year_sale_amt					string		--去年同期销售金额 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MKT_MM_7="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MKT_MM_7 PARTITION(op_month='$OP_MONTH')
select 
	t1.month_id
	,t1.org_id			
	,t1.product_line     
	,t1.bus_type         
	,t1.cust_id		
	,sum(t1.month_sale_cnt)	
	,sum(t1.month_sale_amt)	
	,sum(t1.l_month_sale_cnt)
	,sum(t1.l_month_sale_amt)
	,sum(t1.l_year_sale_cnt)
	,sum(t1.l_year_sale_amt)
from  (
		SELECT 
			month_id
			,org_id		
			,product_line     
			,bus_type         
			,cust_id		
			,month_sale_cnt	
			,month_sale_amt	
			,l_month_sale_cnt
			,l_month_sale_amt
			,l_year_sale_cnt
			,l_year_sale_amt
		from $TMP_DMP_BIRD_SALE_MKT_MM_4 where op_month = '$OP_MONTH'
		union all
		SELECT 
			month_id
			,org_id			
			,product_line     
			,bus_type         
			,cust_id		
			,month_sale_cnt	
			,month_sale_amt	
			,l_month_sale_cnt
			,l_month_sale_amt
			,l_year_sale_cnt
			,l_year_sale_amt
		from $TMP_DMP_BIRD_SALE_MKT_MM_5 where op_month = '$OP_MONTH'
		union all
		SELECT 
			month_id
			,org_id			
			,product_line     
			,bus_type         
			,cust_id		
			,month_sale_cnt	
			,month_sale_amt	
			,l_month_sale_cnt
			,l_month_sale_amt
			,l_year_sale_cnt
			,l_year_sale_amt
		from $TMP_DMP_BIRD_SALE_MKT_MM_6 where op_month = '$OP_MONTH'
	) t1
group by
	t1.month_id
	,t1.org_id			
	,t1.product_line     
	,t1.bus_type         
	,t1.cust_id
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_SALE_MKT_MM='DMP_BIRD_SALE_MKT_MM'

CREATE_DMP_BIRD_SALE_MKT_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SALE_MKT_MM (
	month_id                            string          --期间(月份)
	,day_id                             string          --期间(日)
	,level1_org_id                      string          --组织1级(股份)
	,level1_org_descr                   string          --组织1级(股份)
	,level2_org_id                      string          --组织2级(片联)
	,level2_org_descr                   string          --组织2级(片联)
	,level3_org_id                      string          --组织3级(片区)
	,level3_org_descr                   string          --组织3级(片区)
	,level4_org_id                      string          --组织4级(小片)
	,level4_org_descr                   string          --组织4级(小片)
	,level5_org_id                      string          --组织5级(公司)
	,level5_org_descr                   string          --组织5级(公司)
	,level6_org_id                      string          --组织6级(OU)
	,level6_org_descr                   string          --组织6级(OU)
	,level7_org_id                      string          --组织7级(库存组织)
	,level7_org_descr                   string          --组织7级(库存组织)
	,level1_businesstype_id             string          --业态1级
	,level1_businesstype_name           string          --业态1级
	,level2_businesstype_id             string          --业态2级
	,level2_businesstype_name           string          --业态2级
	,level3_businesstype_id             string          --业态3级
	,level3_businesstype_name           string          --业态3级
	,level4_businesstype_id             string          --业态4级
	,level4_businesstype_name           string          --业态4级
	,production_line_id                 string          --产线
	,production_line_descr              string          --产线
	,level1_prod_id                     string          --产品线1级
	,level1_prod_descr                  string          --产品线1级
	,level2_prod_id                     string          --产品线2级
	,level2_prod_descr                  string          --产品线2级
	,sale_area_id                       string          --销售区域ID
	,sale_area_name                     string          --销售区域名称
	,level1_channel_id                  string          --客户渠道1级
	,level1_channel_descr               string          --客户渠道1级
	,level2_channel_id                  string          --客户渠道2级
	,level2_channel_descr               string          --客户渠道2级
	,cust_id                            string          --客户ID
	,cust_name                          string          --客户名称
	,month_sale_cnt                     string          --本月销量
	,l_month_sale_cnt                   string          --上月销量
	,l_year_sale_cnt                    string          --去年同期销量
	,month_sale_amt                     string          --本月销售金额
	,l_month_sale_amt                   string          --上月销售金额
	,l_year_sale_amt                    string          --去年同期销售金额
	,create_time						string			--数据推送时间
)PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SALE_MKT_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_SALE_MKT_MM PARTITION(op_month='$OP_MONTH')
SELECT
	t1.month_id								               --期间(月份)
	,''											   												--期间(日)
	,case when t7.level1_org_id    is null then coalesce(t13.level1_org_id,'-1') else coalesce(t7.level1_org_id,'-1')  end as level1_org_id                --一级组织编码
    ,case when t7.level1_org_descr is null then coalesce(t13.level1_org_descr,'缺失') else coalesce(t7.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
    ,case when t7.level2_org_id    is null then coalesce(t13.level2_org_id,'-1') else coalesce(t7.level2_org_id,'-1')  end as level2_org_id                --二级组织编码
    ,case when t7.level2_org_descr is null then coalesce(t13.level2_org_descr,'缺失') else coalesce(t7.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
    ,case when t7.level3_org_id    is null then coalesce(t13.level3_org_id,'-1') else coalesce(t7.level3_org_id,'-1')  end as level3_org_id                --三级组织编码
    ,case when t7.level3_org_descr is null then coalesce(t13.level3_org_descr,'缺失') else coalesce(t7.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
    ,case when t7.level4_org_id    is null then coalesce(t13.level4_org_id,'-1') else coalesce(t7.level4_org_id,'-1')  end as level4_org_id                --四级组织编码
    ,case when t7.level4_org_descr is null then coalesce(t13.level4_org_descr,'缺失') else coalesce(t7.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
    ,case when t7.level5_org_id    is null then coalesce(t13.level5_org_id,'-1') else coalesce(t7.level5_org_id,'-1')  end as level5_org_id                --五级组织编码
    ,case when t7.level5_org_descr is null then coalesce(t13.level5_org_descr,'缺失') else coalesce(t7.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
    ,case when t7.level6_org_id    is null then coalesce(t13.level6_org_id,'-1') else coalesce(t7.level6_org_id,'-1')  end as level6_org_id                --六级组织编码
    ,case when t7.level6_org_descr is null then coalesce(t13.level6_org_descr,'缺失') else coalesce(t7.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
	,''           	                       --组织7级(库存组织)
	,''        	                       --组织7级(库存组织)
	,t9.level1_businesstype_id  	                       --业态1级
	,t9.level1_businesstype_name	                       --业态1级
	,t9.level2_businesstype_id  	                       --业态2级
	,t9.level2_businesstype_name	                       --业态2级
	,t9.level3_businesstype_id  	                       --业态3级
	,t9.level3_businesstype_name	                       --业态3级
	,t9.level4_businesstype_id  	                       --业态4级
	,t9.level4_businesstype_name	                       --业态4级
	,case when t1.product_line = '10' then '1'
			   when t1.product_line = '20' then '2'
		  else '-1' end		      	                       --产线
	,case when t1.product_line = '10' then '鸡'
			   when t1.product_line = '20' then '鸭'
		  else '缺省' end 			   	                       --产线
	,''          	                       --产品线1级
	,''       	                       		   --产品线1级
	,''          	                       --产品线2级
	,''       	                   --产品线2级
	,t10.province_id	            	                   --销售区域ID
	,t10.province          	                       		   --销售区域名称
	,t10.ID_CUST_CHAN       	                       	   --客户渠道1级
	,t10.CUST_CHAN_TYPE    	                       		   --客户渠道1级
	,t10.ID_CUST_CHAN_DETAIL_TP       	                   --客户渠道2级
	,t10.CUST_CHAN_DETAIL_TP     	                       --客户渠道2级
	,t1.cust_id                 	                       --客户ID
	,t10.CUSTOMER_DESCR               	                   --客户名称
	,nvl(t1.month_sale_cnt,0)          	                       --本月销量
	,nvl(t1.l_month_sale_cnt,0)        	                       --上月销量
	,nvl(t1.l_year_sale_cnt,0)         	                       --去年同期销量
	,nvl(t1.month_sale_amt,0)         	                       --本月销售金额
	,nvl(t1.l_month_sale_amt,0)        	                       --上月销售金额
	,nvl(t1.l_year_sale_amt,0)         	                       --去年同期销售金额
	,'$CREATE_TIME' create_time							   --数据推送时间
FROM (select * from $TMP_DMP_BIRD_SALE_MKT_MM_7 where op_month = '$OP_MONTH') t1
left join mreport_global.dim_org_management t7 
on t1.org_id=t7.org_id  and t7.attribute5='1'
left join mreport_global.dim_org_management t13 
on t1.org_id=t13.org_id and t1.bus_type=t13.bus_type_id and t13.attribute5='2'
left join mreport_global.dim_org_businesstype t9
	ON t1.bus_type = t9.level4_businesstype_id
left join mreport_global.dwu_dim_crm_customer t10
	ON t1.cust_id = t10.CUSTOMER_ACCOUNT_ID
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
	$CREATE_TMP_DMP_BIRD_SALE_MKT_MM_0;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_0;
    $CREATE_TMP_DMP_BIRD_SALE_MKT_MM_1;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_1;
	$CREATE_TMP_DMP_BIRD_SALE_MKT_MM_4;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_4;
    $CREATE_TMP_DMP_BIRD_SALE_MKT_MM_5;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_5;
	$CREATE_TMP_DMP_BIRD_SALE_MKT_MM_6;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_6;
	$CREATE_TMP_DMP_BIRD_SALE_MKT_MM_7;
    $INSERT_TMP_DMP_BIRD_SALE_MKT_MM_7;
	$CREATE_DMP_BIRD_SALE_MKT_MM;
    $INSERT_DMP_BIRD_SALE_MKT_MM;
"  -v




        

                                               