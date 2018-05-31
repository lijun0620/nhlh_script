#!/bin/bash

######################################################################
#                                                                    
# 程    序: DMP_BIRD_SALE_MM.sh                               
# 创建时间: 2018年04月10日                                            
# 创 建 者: fwj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 部门月度销量任务完成情况
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}
OP_LAST_YEAR_MONTH=$(date -d "$OP_DAY -1 years" "+%Y%m" )

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_sale_mm.sh 20180101"
    exit 1
fi


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_0='TMP_DMP_BIRD_SALE_MM_0'

CREATE_TMP_DMP_BIRD_SALE_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_0(
    day_id				  string	  --日期
	,org_id               string      --公司ID
    ,product_line         string      --产线
	,bus_type             string      --业态
	,item_id			  string      --物料编码
	,fifth_org_id         string      --销售机构id
	,organization_id      string      --库存组织ID
	,day_fin_cnt		  string	  --日销量 
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_0  PARTITION(op_month='$OP_MONTH')
SELECT 
	OUT_DATE as day_id		    		--日期   									
	,'缺矢'                     		--公司id
	,product_line               		--产线
	,bus_type							--业态
	,'缺矢'							--物料id
	,fifth_org_id						--销售机构id
	,'缺矢'          			--库存组织ID
	,sum(nvl(out_qty,0)) day_fin_cnt	--日销量
from mreport_poultry.dwu_gyl_xs01_dd
where op_day='$OP_DAY' and OUT_DATE IS NOT NULL
group by 
    OUT_DATE
	,'缺矢'                
	,'缺矢'                            
	,product_line                                      
	,bus_type
	,'缺矢'
	,fifth_org_id ;
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_1='TMP_DMP_BIRD_SALE_MM_1'

CREATE_TMP_DMP_BIRD_SALE_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_1(
	month_id				  string      	--日期
	,org_id               string      	--公司ID
    ,product_line         string      	--产线
	,bus_type             string      	--业态
	,item_id			  string      	--物料编码
	,fifth_org_id         string      	--销售机构id
	,organization_id      string      	--库存组织ID
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计 
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_1  PARTITION(op_month='$OP_MONTH')
SELECT 
	/*+ MAPJOIN(t1) */
	t1.month_id																		--日期
	,t2.org_id                            												--公司id
	,t2.product_line                                      								--产线
	,t2.bus_type																        --业态
	,t2.item_id																            --物料id
	,t2.fifth_org_id															        --销售机构id
	,t2.organization_id          												    	--库存组织ID
	,sum(t2.day_fin_cnt) month_fin_cnt --月度销量
	,'0'
	,'0'
from (select distinct month_id from mreport_global.dim_day where month_id BETWEEN '201512' AND substr(regexp_replace(current_date,'-',''),1,6)) t1
left join $TMP_DMP_BIRD_SALE_MM_0 t2
	on (
		t1.month_id = substring(t2.day_id,1,6)
		and t2.op_month = '$OP_MONTH'
	)
group by 
    t1.month_id		         
	,t2.org_id                          
	,t2.product_line                                    
	,t2.bus_type			
	,t2.item_id				
	,t2.fifth_org_id		
    ,t2.organization_id
"	

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_2='TMP_DMP_BIRD_SALE_MM_2'

CREATE_TMP_DMP_BIRD_SALE_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_2(
	month_id				  string      	--日期
	,org_id               string      	--公司ID
    ,product_line         string      	--产线
	,bus_type             string      	--业态
	,item_id			  string      	--物料编码
	,fifth_org_id         string      	--销售机构id
	,organization_id      string      	--库存组织ID
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计 
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC      
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_2  PARTITION(op_month='$OP_MONTH')
SELECT 
	/*+ MAPJOIN(t1) */
	t1.month_id
	,t2.org_id                            										--公司id
	,t2.product_line                           	 								--产线
	,t2.bus_type																--业态
	,t2.item_id																	--物料id
	,t2.fifth_org_id															--销售机构id
	,t2.organization_id          												--库存组织ID
	,'0'       
	,sum(case when t1.month_id >= t2.month_id then t2.month_fin_cnt else '0' end) year_fin_cnt												--本年度销量累计
	,'0'
from (select distinct month_id,year_id from mreport_global.dim_day where month_id BETWEEN '201512' AND substr(regexp_replace(current_date,'-',''),1,6)) t1	
left join $TMP_DMP_BIRD_SALE_MM_1 t2 
	on (
		t2.op_month='$OP_MONTH'
		and t1.year_id = substring(t2.month_id,1,4)
	)
group by 
	t1.year_id
	,t1.month_id
	,t2.organization_id               
	,t2.org_id                            
	,t2.product_line                                      
	,t2.bus_type
	,t2.item_id
	,t2.fifth_org_id;            
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_3='TMP_DMP_BIRD_SALE_MM_3'

CREATE_TMP_DMP_BIRD_SALE_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_3(
	month_id				  string      	--日期
	,org_id               string      	--公司ID
    ,product_line         string      	--产线
	,bus_type             string      	--业态
	,item_id			  string      	--物料编码
	,fifth_org_id         string      	--销售机构id
	,organization_id      string      	--库存组织ID
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_3  PARTITION(op_month='$OP_MONTH')
SELECT 
	concat(floor(substring(month_id,1,4)+1),substring(month_id,5,2)) as month_id
	,org_id                            										--公司id
	,product_line                           								--产线
	,bus_type																--业态
	,item_id																--物料id
	,fifth_org_id															--销售机构id
	,organization_id          												--库存组织ID
	,'0'
	,'0'     
	,year_fin_cnt last_year_fin_cnt 										--去年销量累计
from $TMP_DMP_BIRD_SALE_MM_2 WHERE op_month='$OP_MONTH'  
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_8='TMP_DMP_BIRD_SALE_MM_8'

CREATE_TMP_DMP_BIRD_SALE_MM_8="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_8(
	month_id				  string      	--日期
	,org_id               string      	--公司ID
    ,product_line         string      	--产线
	,bus_type             string      	--业态
	,item_id			  string      	--物料编码
	,fifth_org_id         string      	--销售机构id
	,organization_id      string      	--库存组织ID
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_8="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_8  PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.month_id
	,t1.org_id                            										--公司id
	,t1.product_line                           								--产线
	,t1.bus_type																--业态
	,t1.item_id																--物料id
	,t1.fifth_org_id															--销售机构id
	,t1.organization_id          												--库存组织ID
	,sum(t1.month_fin_cnt)
	,sum(t1.year_fin_cnt)     
	,sum(t1.last_year_fin_cnt)			 										--去年销量累计
from (
	select 
		month_id			
		,org_id           
		,product_line     
		,bus_type         
		,item_id		
		,fifth_org_id     
		,organization_id  
		,month_fin_cnt	
		,year_fin_cnt	
		,last_year_fin_cnt
	from $TMP_DMP_BIRD_SALE_MM_1 where op_month = '$OP_MONTH'
	union all
	select 
		month_id			
		,org_id           
		,product_line     
		,bus_type         
		,item_id		
		,fifth_org_id     
		,organization_id  
		,month_fin_cnt	
		,year_fin_cnt	
		,last_year_fin_cnt
	from $TMP_DMP_BIRD_SALE_MM_2 where op_month = '$OP_MONTH'
	union all
	select 
		month_id			
		,org_id           
		,product_line     
		,bus_type         
		,item_id		
		,fifth_org_id     
		,organization_id  
		,month_fin_cnt	
		,year_fin_cnt	
		,last_year_fin_cnt
	from $TMP_DMP_BIRD_SALE_MM_3 where op_month = '$OP_MONTH'
) t1  
group by 
	t1.month_id
	,t1.org_id          
	,t1.product_line    
	,t1.bus_type		
	,t1.item_id			
	,t1.fifth_org_id	
	,t1.organization_id 
"



##########################################################################################
## 将数据从大表转换至目标表
## 手工补录数据加工（本月以及去年同期月度任务量）
## 变量声明
TMP_DMP_BIRD_SALE_MM_5='TMP_DMP_BIRD_SALE_MM_5'

CREATE_TMP_DMP_BIRD_SALE_MM_5="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_5(
	month_id				string		  --月
	,sale_org_code_segments    	string      --销售组织ID
  	,product_line         	string      --产线
	,bus_type             	string      --业态
	,month_obj_cnt    		string		  --月度任务量
	,year_obj_cnt    		string		  --年度任务量累计
	,last_year_obj_cnt		string 		  --去年度任务累计 
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_5="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_5  PARTITION(op_month='$OP_MONTH')
SELECT 
	concat(substr(PERIOD_NAME,1,4),substr(PERIOD_NAME,6,2))           --月
	,sale_org_code_segments      --销售组织ID
  	,PRODUCT_LINE_CODE  --产线
	,TYPE               --业态
	,sum(TASK_TOTAL)    --月度任务量
	,'0'
	,'0' 
from MREPORT_GLOBAL.DWU_DIM_OE_SALE_TASK_ALL 
group by
	concat(substr(PERIOD_NAME,1,4),substr(PERIOD_NAME,6,2))           --月
	,sale_org_code_segments      --销售组织ID
  	,PRODUCT_LINE_CODE  --产线
	,TYPE               --业态
;
"




##########################################################################################
## 将数据从大表转换至目标表
## 手工补录数据加工（本年以及去年同期月度任务量累计）
## 变量声明
TMP_DMP_BIRD_SALE_MM_6='TMP_DMP_BIRD_SALE_MM_6'

CREATE_TMP_DMP_BIRD_SALE_MM_6="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_6(
	month_id			  string	  --月
	,sale_org_code_segments    string      --销售组织ID
  	,product_line         string      --产线
	,bus_type             string      --业态
	,month_obj_cnt    		string	  --月度任务量
	,year_obj_cnt    		string	  --年度任务量累计
	,last_year_obj_cnt		string 	  --去年度任务累计
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_6="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_6  PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.month_id           --月
	,t2.sale_org_code_segments      --销售组织ID
  	,t2.PRODUCT_LINE_CODE  --产线
	,t2.TYPE               --业态
	,'0'
	,SUM(CASE WHEN T1.month_id >= CONCAT(SUBSTR(T2.PERIOD_NAME,1,4),SUBSTR(T2.PERIOD_NAME,6,2)) THEN T2.TASK_TOTAL ELSE 0 END)    --年度任务量累计
	,'0' 
FROM (select distinct month_id,year_id from mreport_global.dim_day where month_id BETWEEN '201512' AND substr(regexp_replace(current_date,'-',''),1,6)) T1
LEFT JOIN MREPORT_GLOBAL.DWU_DIM_OE_SALE_TASK_ALL T2
ON T1.YEAR_ID = SUBSTR(T2.PERIOD_NAME,1,4)
group by
	t1.month_id 
	,t1.year_id           --月
	,t2.sale_org_code_segments      --销售组织ID
  	,t2.PRODUCT_LINE_CODE  --产线
	,t2.TYPE               --业态
;
"

##########################################################################################
## 将数据从大表转换至目标表
## 手工补录数据加工（本年以及去年同期月度任务量累计）
## 变量声明
TMP_DMP_BIRD_SALE_MM_10='TMP_DMP_BIRD_SALE_MM_10'

CREATE_TMP_DMP_BIRD_SALE_MM_10="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_10(
	month_id			  string	  --月
	,sale_org_code_segments    string      --销售组织ID
  	,product_line         string      --产线
	,bus_type             string      --业态
	,month_obj_cnt    		string	  --月度任务量
	,year_obj_cnt    		string	  --年度任务量累计
	,last_year_obj_cnt		string 	  --去年度任务累计
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_10="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_10  PARTITION(op_month='$OP_MONTH')
select 
		concat(floor(substr(month_id,1,4)+1),substr(month_id,5,2)) month_id	
		,sale_org_code_segments
		,product_line     
		,bus_type         
		,'0'   
		,'0'   
		,year_obj_cnt last_year_obj_cnt
from $TMP_DMP_BIRD_SALE_MM_6 where op_month = '$OP_MONTH'
"



##########################################################################################
## 将数据从大表转换至目标表
## 手工补录数据加工（本年以及去年同期月度任务量累计）
## 变量声明
TMP_DMP_BIRD_SALE_MM_9='TMP_DMP_BIRD_SALE_MM_9'

CREATE_TMP_DMP_BIRD_SALE_MM_9="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_9(
	month_id			  string	  --月
	,sale_org_code_segments    string      --销售组织ID
  	,product_line         string      --产线
	,bus_type             string      --业态
	,month_obj_cnt    		string	  --月度任务量
	,year_obj_cnt    		string	  --年度任务量累计
	,last_year_obj_cnt		string 	  --去年度任务累计
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC    
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_9="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_9  PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.month_id           --月
	,t1.sale_org_code_segments  --销售组织ID
  	,t1.product_line  --产线
	,t1.bus_type               --业态
	,sum(t1.month_obj_cnt)
	,sum(t1.year_obj_cnt)    
	,sum(t1.last_year_obj_cnt)
	,'0'
	,'0'
	,'0' 
FROM (
	select 
		month_id		
		,sale_org_code_segments
		,product_line     
		,bus_type         
		,month_obj_cnt    
		,year_obj_cnt    
		,last_year_obj_cnt
	from $TMP_DMP_BIRD_SALE_MM_5 where op_month = '$OP_MONTH'
	union all
	select 
		month_id		
		,sale_org_code_segments
		,product_line     
		,bus_type         
		,month_obj_cnt    
		,year_obj_cnt    
		,last_year_obj_cnt
	from $TMP_DMP_BIRD_SALE_MM_6 where op_month = '$OP_MONTH'
	union all
	select 
		month_id	
		,sale_org_code_segments
		,product_line     
		,bus_type         
		,month_obj_cnt    
		,year_obj_cnt    
		,last_year_obj_cnt
	from $TMP_DMP_BIRD_SALE_MM_10 where op_month = '$OP_MONTH'
) t1
group by
	t1.month_id 
	,t1.sale_org_code_segments  --销售组织ID
  	,t1.product_line  --产线
	,t1.bus_type               --业态
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_MM_7='TMP_DMP_BIRD_SALE_MM_7'

CREATE_TMP_DMP_BIRD_SALE_MM_7="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_MM_7(
	month_id			  string 		--月份
    ,product_line         string      	--产线
	,bus_type             string      	--业态
	,sale_org_code_segments         string      	--销售机构id
	,month_fin_cnt		  string		--月度销量
	,year_fin_cnt		  string		--本年度销量累计
	,last_year_fin_cnt		string		  --去年销量累计
	,month_obj_cnt    		string	  --月度任务量
	,year_obj_cnt    		string	  --年度任务量累计
	,last_year_obj_cnt		string 	  --去年度任务累计 
)                      
PARTITIONED BY (op_month STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_MM_7="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_MM_7  PARTITION(op_month='$OP_MONTH')
select 
	t1.month_id		
	,t1.product_line     
	,t1.bus_type 
	,t1.sale_org_code_segments
	,sum(t1.month_fin_cnt)	
	,sum(t1.year_fin_cnt)	
	,sum(t1.last_year_fin_cnt)        
	,sum(t1.month_obj_cnt)    
	,sum(t1.year_obj_cnt)    
	,sum(t1.last_year_obj_cnt)
from (select 
		month_id		
		,sale_org_code_segments
		,product_line     
		,bus_type         
		,month_obj_cnt    
		,year_obj_cnt    
		,last_year_obj_cnt
		,month_fin_cnt	
		,year_fin_cnt	
		,last_year_fin_cnt
	from $TMP_DMP_BIRD_SALE_MM_9 where op_month = '$OP_MONTH'
	union all
	select 
		month_id		
		,fifth_org_id sale_org_code_segments
		,product_line     
		,bus_type         
		,'0' month_obj_cnt    
		,'0' year_obj_cnt    
		,'0' last_year_obj_cnt
		,month_fin_cnt	
		,year_fin_cnt	
		,last_year_fin_cnt
	from $TMP_DMP_BIRD_SALE_MM_8 where op_month = '$OP_MONTH'
	) t1
group by 
	t1.month_id		
	,t1.sale_org_code_segments
	,t1.product_line     
	,t1.bus_type
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_SALE_MM='DMP_BIRD_SALE_MM'

CREATE_DMP_BIRD_SALE_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SALE_MM(
   month_id							            string      --期间（月份）	
  ,day_id                           string      --期间(日)				
  ,level1_org_id                    string      --组织1级
  ,level1_org_descr                 string      --组织1级
  ,level2_org_id                    string      --组织2级
  ,level2_org_descr                 string      --组织2级
  ,level3_org_id                    string      --组织3级
  ,level3_org_descr                 string      --组织3级
  ,level4_org_id                    string      --组织4级
  ,level4_org_descr                 string      --组织4级
  ,level5_org_id                    string      --组织5级
  ,level5_org_descr                 string      --组织5级
  ,level6_org_id                    string      --组织6级
  ,level6_org_descr                 string      --组织6级
  ,level7_org_id                    string      --组织7级
  ,level7_org_descr                 string      --组织7级
  ,level1_businesstype_id           string      --业态1级
  ,level1_businesstype_name         string      --业态1级
  ,level2_businesstype_id           string      --业态2级
  ,level2_businesstype_name         string      --业态2级
  ,level3_businesstype_id           string      --业态3级
  ,level3_businesstype_name         string      --业态3级
  ,level4_businesstype_id           string      --业态4级
  ,level4_businesstype_name         string      --业态4级
  ,level1_sale_id					          string		  --销售组织1级
  ,level1_sale_descr				        string		  --销售组织1级描述
  ,level2_sale_id					          string		  --销售组织2级
  ,level2_sale_descr				        string		  --销售组织2级描述
  ,level3_sale_id					          string		  --销售组织3级
  ,level3_sale_descr				        string		  --销售组织3级描述
  ,level4_sale_id					          string		  --销售组织4级
  ,level4_sale_descr				        string		  --销售组织4级描述
  ,level5_sale_id					          string		  --销售组织5级
  ,level5_sale_descr				        string		  --销售组织5级描述
  ,production_line_id				        string		  --产线代码
  ,production_line_descr			      string		  --产线描述
  ,level1_prod_id                   string      --产品线1级
  ,level1_prod_descr                string      --产品线1级
  ,level2_prod_id                   string      --产品线2级
  ,level2_prod_descr                string      --产品线2级
  ,month_obj_cnt					          string		  --月度任务量
  ,month_fin_cnt					          string		  --月度销量
  ,year_obj_cnt						          string		  --本年度任务累计
  ,year_fin_cnt						          string 		  --本年度销量累计
  ,last_year_obj_cnt				        string 		  --去年度任务累计
  ,last_year_fin_cnt				        string		  --去年销量累计
  ,create_time                      string      --数据推送时间    
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SALE_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_SALE_MM PARTITION(op_month='$OP_MONTH')
select
  	t1.month_id			 --月份
	,''									 --日期
	,'缺矢'                  --组织1级(股份)
	,'缺矢'                     --组织1级(股份)
	,'缺矢'                  --组织2级(片联)
	,'缺矢'                     --组织2级(片联)
	,'缺矢'                  --组织3级(片区)
	,'缺矢'                     --组织3级(片区)
	,'缺矢'                  --组织4级(小片)
	,'缺矢'                     --组织4级(小片)
	,'缺矢'                  --组织5级(公司)
	,'缺矢'                     --组织5级(公司)
	,'缺矢'                  --组织6级(OU)
	,'缺矢'                     --组织6级(OU)
	,'缺矢'                	 --组织7级(库存组织)
	,'缺矢'                     --组织7级(库存组织)
	,t2.level1_businesstype_id                --业态1级
	,t2.level1_businesstype_name              --业态1级
	,t2.level2_businesstype_id               --业态2级
	,t2.level2_businesstype_name              --业态2级
	,t2.level3_businesstype_id                --业态3级
	,t2.level3_businesstype_name              --业态3级
	,t2.level4_businesstype_id                --业态4级
	,t2.level4_businesstype_name              --业态4级
  	,t3.first_sale_org_code                      --销售组织1级
		,t3.first_sale_org_name                    --销售组织1级
		,t3.second_sale_org_code                    --销售组织2级
		,t3.second_sale_org_name                    --销售组织2级
		,t3.three_sale_org_code                    --销售组织3级
		,t3.three_sale_org_name                    --销售组织3级
		,t3.four_sale_org_code                      --销售组织4级
		,t3.four_sale_org_name                      --销售组织4级
		,t3.five_sale_org_code                     --销售组织5级
		,t3.five_sale_org_name                      --销售组织5级
    ,case when t1.product_line = '10' then '1'
			   when t1.product_line = '20' then '2'
		  else '-1' end					        --产线代码
  	,case when t1.product_line = '10' then '鸡'
			   when t1.product_line = '20' then '鸭'
		  else '缺省' end  							  --产线
  ,'缺矢'                     --产品线1级
	,'缺矢'                  --产品线1级
	,'缺矢'                     --产品线2级
	,'缺矢'                    --产品线2级
  ,nvl(t1.month_obj_cnt,0)					          --月度任务量
  ,nvl(t1.month_fin_cnt,0)					          --月度销量
  ,nvl(t1.year_obj_cnt,0)						          --本年度任务累计
  ,nvl(t1.year_fin_cnt,0)						          --本年度销量累计
  ,nvl(t1.last_year_obj_cnt,0)				            --去年度任务累计
  ,nvl(t1.last_year_fin_cnt,0)				        --去年销量累计
 	,'$CREATE_TIME' create_time                      --数据推送时间    
from (select * from $TMP_DMP_BIRD_SALE_MM_7 where op_month = '$OP_MONTH') t1
left join mreport_global.dim_org_businesstype t2
	ON t1.bus_type = t2.level4_businesstype_id
left join mreport_global.dwu_dim_xs_org t3
	ON t1.sale_org_code_segments = t3.sale_org_code
"




echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
   	$CREATE_TMP_DMP_BIRD_SALE_MM_0;
	$INSERT_TMP_DMP_BIRD_SALE_MM_0;
	$CREATE_TMP_DMP_BIRD_SALE_MM_1;
    $INSERT_TMP_DMP_BIRD_SALE_MM_1;
    $CREATE_TMP_DMP_BIRD_SALE_MM_2;
    $INSERT_TMP_DMP_BIRD_SALE_MM_2;
    $CREATE_TMP_DMP_BIRD_SALE_MM_3;
    $INSERT_TMP_DMP_BIRD_SALE_MM_3;
	$CREATE_TMP_DMP_BIRD_SALE_MM_8;
    $INSERT_TMP_DMP_BIRD_SALE_MM_8;
    $CREATE_TMP_DMP_BIRD_SALE_MM_5;
    $INSERT_TMP_DMP_BIRD_SALE_MM_5;
    $CREATE_TMP_DMP_BIRD_SALE_MM_6;
    $INSERT_TMP_DMP_BIRD_SALE_MM_6;
	$CREATE_TMP_DMP_BIRD_SALE_MM_10;
    $INSERT_TMP_DMP_BIRD_SALE_MM_10;
	$CREATE_TMP_DMP_BIRD_SALE_MM_9;
    $INSERT_TMP_DMP_BIRD_SALE_MM_9;
	$CREATE_TMP_DMP_BIRD_SALE_MM_7;
    $INSERT_TMP_DMP_BIRD_SALE_MM_7;
    $CREATE_DMP_BIRD_SALE_MM;
    $INSERT_DMP_BIRD_SALE_MM;
"  -v
