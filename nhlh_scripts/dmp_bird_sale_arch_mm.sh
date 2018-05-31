#!/bin/bash

######################################################################
#                                                                    
# 程    序: DMP_BIRD_SALE_ARCH_MM.sh                               
# 创建时间: 2018年04月11日                                            
# 创 建 者: fwj                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 定制品销量结构占比
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}
# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)
# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_sale_arch_mm.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_00='TMP_DMP_BIRD_SALE_ARCH_MM_00'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_00="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_00(
	 day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,out_main_qty						string		--出库主数量
	,custom								string		--定制品
)                      
PARTITIONED BY (op_month STRING)
STORED AS ORC       
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_00="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_00 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.APPROVE_DATE day_id
	,t1.org_id                            										--公司id
	,t1.product_line                                      						--产线
	,t1.bus_type																--业态
	,t9.prd_line_cate_id					         --产品线1级
	,t9.prd_line_cate					             --产品线1级
	,t9.sub_prd_line_tp_id 				         --产品线2级
	,t9.sub_prd_line_tp	                 		--产品线2级
	,t9.prd_channel_id                           --产品渠道ID
	,t9.prd_channel                           	 --产品渠道名称
	,t8.inventory_item_id						 --物料品名ID
	,t8.inventory_item_desc                      --物料品名名称																
	,t1.fifth_org_id	               											--销售机构id
	,t1.organization_id          												--库存组织ID
	,nvl(t1.out_main_qty,0)
	,t1.custom
from (select * from mreport_poultry.dwu_gyl_xs01_dd where op_day='$OP_DAY' and APPROVE_DATE is not null) t1
left join mreport_global.dwu_dim_material_new t8
	on (
		t1.item_id = t8.inventory_item_id  
		and t1.organization_id = t8.inv_org_id
		and t8.inv_org_id is not null
		)
left join mreport_global.dim_crm_item t9
	on (
		t8.inventory_item_code = t9.item_code
	) 
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_0='TMP_DMP_BIRD_SALE_ARCH_MM_0'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_0(
	 day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量
)                      
PARTITIONED BY (op_month STRING)
STORED AS ORC       
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_0 PARTITION(op_month='$OP_MONTH')
SELECT 
	day_id
	,org_id                            										--公司id
	,product_line                                      						--产线
	,bus_type																--业态
	,prd_line_cate_id					         --产品线1级
	,prd_line_cate					             --产品线1级
	,sub_prd_line_tp_id 				         --产品线2级
	,sub_prd_line_tp	                 		--产品线2级
	,prd_channel_id                           --产品渠道ID
	,prd_channel                           	 --产品渠道名称
	,inventory_item_id						 --物料品名ID
	,inventory_item_desc                      --物料品名名称																--物料id
	,fifth_org_id	               											--销售机构id
	,organization_id          												--库存组织ID
	,sum(out_main_qty) day_order_cnt 									--日定制品销量
	,'0'
	,'0'
	,'0'
	,'0'
from $TMP_DMP_BIRD_SALE_ARCH_MM_00 where op_month ='$OP_MONTH' and custom = '是'
group by 
    day_id
	,organization_id          
	,org_id                            
	,product_line                                      
	,bus_type
	,prd_line_cate_id					         
	,prd_line_cate					            
	,sub_prd_line_tp_id 				         
	,sub_prd_line_tp	                 		
	,prd_channel_id                          
	,prd_channel                           	 
	,inventory_item_id						
	,inventory_item_desc                      
	,fifth_org_id ;
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_1='TMP_DMP_BIRD_SALE_ARCH_MM_1'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_1(
	day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量 
)                      
PARTITIONED BY (op_month STRING)
STORED AS ORC       
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.day_id
	,t2.org_id                            										--公司id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.prd_line_cate_id					         --产品线1级
	,t2.prd_line_cate					             --产品线1级
	,t2.sub_prd_line_tp_id 				         --产品线2级
	,t2.sub_prd_line_tp	                 		--产品线2级
	,t2.prd_channel_id                           --产品渠道ID
	,t2.prd_channel                           	 --产品渠道名称
	,t2.inventory_item_id						 --物料品名ID
	,t2.inventory_item_desc                      --物料品名名称
	,t2.fifth_org_id	               											--销售机构id
	,t2.organization_id          												--库存组织ID
	,'0'
	,sum(case when t1.day_id >= t2.day_id then t2.day_order_cnt else 0 end) month_order_cnt 	--月度定制品销量
	,'0'
	,'0'
	,'0'
from (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_SALE_ARCH_MM_0 t2
	on (
		t2.op_month = '$OP_MONTH'
		and t1.month_id = substring(t2.day_id,1,6)
	)
group by 
  	t1.month_id
	,t1.day_id
	,t2.organization_id          
	,t2.org_id                            
	,t2.product_line                                      
	,t2.bus_type
	,t2.prd_line_cate_id					         
	,t2.prd_line_cate					            
	,t2.sub_prd_line_tp_id 				         
	,t2.sub_prd_line_tp	                 		
	,t2.prd_channel_id                          
	,t2.prd_channel                           	 
	,t2.inventory_item_id						
	,t2.inventory_item_desc                      
	,t2.fifth_org_id ;
	
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_2='TMP_DMP_BIRD_SALE_ARCH_MM_2'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_2(
	 day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_cnt						string		--日总销量 
)                      
PARTITIONED BY (op_month STRING)
STORED AS ORC       
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_2 PARTITION(op_month='$OP_MONTH')
SELECT 
	day_id
	,org_id                            										--公司id
	,product_line                                      						--产线
	,bus_type																--业态
	,prd_line_cate_id					         --产品线1级
	,prd_line_cate					             --产品线1级
	,sub_prd_line_tp_id 				         --产品线2级
	,sub_prd_line_tp	                 		--产品线2级
	,prd_channel_id                           --产品渠道ID
	,prd_channel                           	 --产品渠道名称
	,inventory_item_id						 --物料品名ID
	,inventory_item_desc                      --物料品名名称																--物料id
	,fifth_org_id	               											--销售机构id
	,organization_id          												--库存组织ID
	,sum(out_main_qty) day_cnt 									--日总销量
from $TMP_DMP_BIRD_SALE_ARCH_MM_00 where op_month ='$OP_MONTH' 
group by 
    day_id
	,organization_id          
	,org_id                            
	,product_line                                      
	,bus_type
	,prd_line_cate_id					         
	,prd_line_cate					            
	,sub_prd_line_tp_id 				         
	,sub_prd_line_tp	                 		
	,prd_channel_id                          
	,prd_channel                           	 
	,inventory_item_id						
	,inventory_item_desc                      
	,fifth_org_id ;
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_3='TMP_DMP_BIRD_SALE_ARCH_MM_3'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_3(
	day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量
)                      
PARTITIONED BY (op_month STRING)
STORED AS ORC       
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_3 PARTITION(op_month='$OP_MONTH')
SELECT
	t1.day_id
	,t2.org_id                            										--公司id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.prd_line_cate_id					         --产品线1级
	,t2.prd_line_cate					             --产品线1级
	,t2.sub_prd_line_tp_id 				         --产品线2级
	,t2.sub_prd_line_tp	                 		--产品线2级
	,t2.prd_channel_id                           --产品渠道ID
	,t2.prd_channel                           	 --产品渠道名称
	,t2.inventory_item_id						 --物料品名ID
	,t2.inventory_item_desc                      --物料品名名称
	,t2.fifth_org_id	               											--销售机构id
	,t2.organization_id          												--库存组织ID
	,'0'
	,'0'
	,sum(case when t1.day_id >= t2.day_id then t2.day_cnt else 0 end) month_cnt 	--月度销量
	,'0'
	,'0'
from (select day_id,month_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_SALE_ARCH_MM_2 t2
	on (
		t2.op_month = '$OP_MONTH'
		and t1.month_id = substring(t2.day_id,1,6)
	)
group by 
  	t1.month_id
	,t1.day_id
	,t2.organization_id          
	,t2.org_id                            
	,t2.product_line                                      
	,t2.bus_type
	,t2.prd_line_cate_id					         
	,t2.prd_line_cate					            
	,t2.sub_prd_line_tp_id 				         
	,t2.sub_prd_line_tp	                 		
	,t2.prd_channel_id                          
	,t2.prd_channel                           	 
	,t2.inventory_item_id						
	,t2.inventory_item_desc                      
	,t2.fifth_org_id ;
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_4='TMP_DMP_BIRD_SALE_ARCH_MM_4'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_4(
	day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量 
) 
PARTITIONED BY (op_month STRING)
STORED AS ORC         
"
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_4 PARTITION(op_month='$OP_MONTH')
SELECT
	t1.day_id
	,t2.org_id                            										--公司id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.prd_line_cate_id					         --产品线1级
	,t2.prd_line_cate					             --产品线1级
	,t2.sub_prd_line_tp_id 				         --产品线2级
	,t2.sub_prd_line_tp	                 		--产品线2级
	,t2.prd_channel_id                           --产品渠道ID
	,t2.prd_channel                           	 --产品渠道名称
	,t2.inventory_item_id						 --物料品名ID
	,t2.inventory_item_desc                      --物料品名名称
	,t2.fifth_org_id	               											--销售机构id
	,t2.organization_id          												--库存组织ID
	,'0'
	,'0'
	,'0'
	,sum(case when t1.day_id >= t2.day_id then t2.day_order_cnt
		else 0 end) year_order_cnt 											--本年度定制品销量
	,'0'
from (select day_id,year_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_SALE_ARCH_MM_0 t2
	on ( 
		t2.op_month='$OP_MONTH'
		and t1.year_id = substring(t2.day_id,1,4)
	)
group by 
	t1.year_id
	,t1.day_id
	,t2.organization_id          
	,t2.org_id                            
	,t2.product_line                                      
	,t2.bus_type
	,t2.prd_line_cate_id					         
	,t2.prd_line_cate					            
	,t2.sub_prd_line_tp_id 				         
	,t2.sub_prd_line_tp	                 		
	,t2.prd_channel_id                          
	,t2.prd_channel                           	 
	,t2.inventory_item_id						
	,t2.inventory_item_desc                      
	,t2.fifth_org_id ;
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_5='TMP_DMP_BIRD_SALE_ARCH_MM_5'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_5="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_5(
	day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量 
)                       
PARTITIONED BY (op_month STRING)
STORED AS ORC      
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_5="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_5 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.day_id
	,t2.org_id                            										--公司id
	,t2.product_line                                      						--产线
	,t2.bus_type																--业态
	,t2.prd_line_cate_id					         --产品线1级
	,t2.prd_line_cate					             --产品线1级
	,t2.sub_prd_line_tp_id 				         --产品线2级
	,t2.sub_prd_line_tp	                 		--产品线2级
	,t2.prd_channel_id                           --产品渠道ID
	,t2.prd_channel                           	 --产品渠道名称
	,t2.inventory_item_id						 --物料品名ID
	,t2.inventory_item_desc                      --物料品名名称
	,t2.fifth_org_id	               											--销售机构id
	,t2.organization_id          												--库存组织ID
	,'0'
	,'0'
	,'0'
	,'0'
	,sum(case when t1.day_id >= t2.day_id then t2.day_cnt
		else 0 end) year_cnt 													--本年度总销量
from (select day_id,year_id from mreport_global.dim_day where day_id BETWEEN '20151201' AND regexp_replace(current_date,'-','')) t1
left join $TMP_DMP_BIRD_SALE_ARCH_MM_2 t2
	on ( 
		t2.op_month='$OP_MONTH'
		and t1.year_id = substring(t2.day_id,1,4)
	)
group by 
	t1.year_id
	,t1.day_id
	,t2.organization_id          
	,t2.org_id                            
	,t2.product_line                                      
	,t2.bus_type
	,t2.prd_line_cate_id					         
	,t2.prd_line_cate					            
	,t2.sub_prd_line_tp_id 				         
	,t2.sub_prd_line_tp	                 		
	,t2.prd_channel_id                          
	,t2.prd_channel                           	 
	,t2.inventory_item_id						
	,t2.inventory_item_desc                      
	,t2.fifth_org_id ;
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_SALE_ARCH_MM_6='TMP_DMP_BIRD_SALE_ARCH_MM_6'

CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_6="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_SALE_ARCH_MM_6(
	day_id								string		--日期
	,org_id                             string      --公司ID
    ,product_line                       string      --产线
	,bus_type             				string      --业态
	,prd_line_cate_id                 	string          --产品线1级
	,prd_line_cate             	 	string          --产品线1级
	,sub_prd_line_tp_id                 	string          --产品线2级
	,sub_prd_line_tp              	string          --产品线2级
	,prd_channel_id                    	string          --产品渠道ID
	,prd_channel                  	string          --产品渠道名称
	,inventory_item_id              	string          --物料品名ID
	,inventory_item_desc            	string          --物料品名名称
	,fifth_org_id                		string      --销售机构id
	,organization_id                    string      --库存组织ID
	,day_order_cnt						string		--日定制品销量 
	,month_order_cnt					string		--月度定制品销量
	,month_cnt							string		--月度销量
	,year_order_cnt						string		--本年度定制品销量
	,year_cnt							string		--本年总销量 
)                       
PARTITIONED BY (op_month STRING)
STORED AS ORC      
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_6="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_SALE_ARCH_MM_6 PARTITION(op_month='$OP_MONTH')
SELECT 
	t1.day_id
	,t1.org_id                            										--公司id
	,t1.product_line                                      						--产线
	,t1.bus_type																--业态
	,t1.prd_line_cate_id					         --产品线1级
	,t1.prd_line_cate					             --产品线1级
	,t1.sub_prd_line_tp_id 				         --产品线2级
	,t1.sub_prd_line_tp	                 		--产品线2级
	,t1.prd_channel_id                           --产品渠道ID
	,t1.prd_channel                           	 --产品渠道名称
	,t1.inventory_item_id						 --物料品名ID
	,t1.inventory_item_desc                      --物料品名名称
	,t1.fifth_org_id	               											--销售机构id
	,t1.organization_id          												--库存组织ID
	,sum(t1.day_order_cnt)
	,sum(t1.month_order_cnt)
	,sum(t1.month_cnt)
	,sum(t1.year_order_cnt)
	,sum(t1.year_cnt) 													--本年度总销量
FROM	(	SELECT 
				day_id
				,org_id                            										--公司id
				,product_line                                      						--产线
				,bus_type																--业态
				,prd_line_cate_id					         --产品线1级
				,prd_line_cate					             --产品线1级
				,sub_prd_line_tp_id 				         --产品线2级
				,sub_prd_line_tp	                 		--产品线2级
				,prd_channel_id                           --产品渠道ID
				,prd_channel                           	 --产品渠道名称
				,inventory_item_id						 --物料品名ID
				,inventory_item_desc                      --物料品名名称
				,fifth_org_id	               											--销售机构id
				,organization_id          												--库存组织ID
				,day_order_cnt
				,month_order_cnt
				,month_cnt
				,year_order_cnt
				,year_cnt 													--本年度总销量
			from TMP_DMP_BIRD_SALE_ARCH_MM_0 where op_month = '$OP_MONTH'
			union all
			SELECT 
				day_id
				,org_id                            										--公司id
				,product_line                                      						--产线
				,bus_type																--业态
				,prd_line_cate_id					         --产品线1级
				,prd_line_cate					             --产品线1级
				,sub_prd_line_tp_id 				         --产品线2级
				,sub_prd_line_tp	                 		--产品线2级
				,prd_channel_id                           --产品渠道ID
				,prd_channel                           	 --产品渠道名称
				,inventory_item_id						 --物料品名ID
				,inventory_item_desc                      --物料品名名称
				,fifth_org_id	               											--销售机构id
				,organization_id          												--库存组织ID
				,day_order_cnt
				,month_order_cnt
				,month_cnt
				,year_order_cnt
				,year_cnt 													--本年度总销量
			from TMP_DMP_BIRD_SALE_ARCH_MM_1 where op_month = '$OP_MONTH'
			union all
			SELECT 
				day_id
				,org_id                            										--公司id
				,product_line                                      						--产线
				,bus_type																--业态
				,prd_line_cate_id					         --产品线1级
				,prd_line_cate					             --产品线1级
				,sub_prd_line_tp_id 				         --产品线2级
				,sub_prd_line_tp	                 		--产品线2级
				,prd_channel_id                           --产品渠道ID
				,prd_channel                           	 --产品渠道名称
				,inventory_item_id						 --物料品名ID
				,inventory_item_desc                      --物料品名名称
				,fifth_org_id	               											--销售机构id
				,organization_id          												--库存组织ID
				,day_order_cnt
				,month_order_cnt
				,month_cnt
				,year_order_cnt
				,year_cnt 													--本年度总销量
			from TMP_DMP_BIRD_SALE_ARCH_MM_3 where op_month = '$OP_MONTH'
			union all 
			SELECT 
				day_id
				,org_id                            										--公司id
				,product_line                                      						--产线
				,bus_type																--业态
				,prd_line_cate_id					         --产品线1级
				,prd_line_cate					             --产品线1级
				,sub_prd_line_tp_id 				         --产品线2级
				,sub_prd_line_tp	                 		--产品线2级
				,prd_channel_id                           --产品渠道ID
				,prd_channel                           	 --产品渠道名称
				,inventory_item_id						 --物料品名ID
				,inventory_item_desc                      --物料品名名称
				,fifth_org_id	               											--销售机构id
				,organization_id          												--库存组织ID
				,day_order_cnt
				,month_order_cnt
				,month_cnt
				,year_order_cnt
				,year_cnt 													--本年度总销量
			from TMP_DMP_BIRD_SALE_ARCH_MM_4 where op_month = '$OP_MONTH'
			union all
			SELECT 
				day_id
				,org_id                            										--公司id
				,product_line                                      						--产线
				,bus_type																--业态
				,prd_line_cate_id					         --产品线1级
				,prd_line_cate					             --产品线1级
				,sub_prd_line_tp_id 				         --产品线2级
				,sub_prd_line_tp	                 		--产品线2级
				,prd_channel_id                           --产品渠道ID
				,prd_channel                           	 --产品渠道名称
				,inventory_item_id						 --物料品名ID
				,inventory_item_desc                      --物料品名名称
				,fifth_org_id	               											--销售机构id
				,organization_id          												--库存组织ID
				,day_order_cnt
				,month_order_cnt
				,month_cnt
				,year_order_cnt
				,year_cnt 													--本年度总销量
			from TMP_DMP_BIRD_SALE_ARCH_MM_5 where op_month = '$OP_MONTH'
		) t1
group by 
	t1.day_id
	,t1.organization_id          
	,t1.org_id                            
	,t1.product_line                                      
	,t1.bus_type
	,t1.prd_line_cate_id					         
	,t1.prd_line_cate					            
	,t1.sub_prd_line_tp_id 				         
	,t1.sub_prd_line_tp	                 		
	,t1.prd_channel_id                          
	,t1.prd_channel                           	 
	,t1.inventory_item_id						
	,t1.inventory_item_desc                      
	,t1.fifth_org_id ;
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_SALE_ARCH_MM='DMP_BIRD_SALE_ARCH_MM'

CREATE_DMP_BIRD_SALE_ARCH_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_SALE_ARCH_MM(
	month_id						string			--期间(月份)
	,day_id							string			--期间(日)
	,level1_org_id                  string          --组织1级(股份)
	,level1_org_descr               string          --组织1级(股份)
	,level2_org_id                  string          --组织2级(片联)
	,level2_org_descr               string          --组织2级(片联)
	,level3_org_id                  string          --组织3级(片区)
	,level3_org_descr               string          --组织3级(片区)
	,level4_org_id                  string          --组织4级(小片)
	,level4_org_descr               string          --组织4级(小片)
	,level5_org_id                  string          --组织5级(公司)
	,level5_org_descr               string          --组织5级(公司)
	,level6_org_id                  string          --组织6级(OU)
	,level6_org_descr               string          --组织6级(OU)
	,level7_org_id                  string          --组织7级(库存组织)
	,level7_org_descr               string          --组织7级(库存组织)
	,level1_businesstype_id         string          --业态1级
	,level1_businesstype_name       string          --业态1级
	,level2_businesstype_id         string          --业态2级
	,level2_businesstype_name       string          --业态2级
	,level3_businesstype_id         string          --业态3级
	,level3_businesstype_name       string          --业态3级
	,level4_businesstype_id         string          --业态4级
	,level4_businesstype_name       string          --业态4级
	,level1_sale_id                 string          --销售组织1级
	,level1_sale_descr              string          --销售组织1级
	,level2_sale_id                 string          --销售组织2级
	,level2_sale_descr              string          --销售组织2级
	,level3_sale_id                 string          --销售组织3级
	,level3_sale_descr              string          --销售组织3级
	,level4_sale_id                 string          --销售组织4级
	,level4_sale_descr              string          --销售组织4级
	,level5_sale_id                 string          --销售组织5级
	,level5_sale_descr              string          --销售组织5级
	,production_line_id             string          --产线
	,production_line_descr          string          --产线
	,level1_prod_id                 string          --产品线1级
	,level1_prod_descr              string          --产品线1级
	,level2_prod_id                 string          --产品线2级
	,level2_prod_descr              string          --产品线2级
	,prod_chl_id                    string          --产品渠道ID
	,prod_chl_name                  string          --产品渠道名称
	,inventory_item_id              string          --物料品名ID
	,inventory_item_desc            string          --物料品名名称
	,day_order_cnt					string			--日定制品销量
	,month_order_cnt                string          --本月定制品销量
	,month_cnt                      string          --本月总销量
	,year_order_cnt                 string          --本年定制品销量
	,year_cnt                       string          --本年总销量
	,create_time					string			--数据推送时间
)PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE;
"


## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_SALE_ARCH_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_SALE_ARCH_MM PARTITION(op_month = '$OP_MONTH')
SELECT 
	substr(t1.day_id,1,6)									 --月份
	,t1.day_id									 --日期
	,case when t4.level1_org_id    is null then coalesce(t11.level1_org_id,'-1') else coalesce(t4.level1_org_id,'-1')  end as level1_org_id                --一级组织编码
    ,case when t4.level1_org_descr is null then coalesce(t11.level1_org_descr,'缺失') else coalesce(t4.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
    ,case when t4.level2_org_id is null    then coalesce(t11.level2_org_id,'-1') else coalesce(t4.level2_org_id,'-1')  end as level2_org_id                --二级组织编码
    ,case when t4.level2_org_descr is null then coalesce(t11.level2_org_descr,'缺失') else coalesce(t4.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
    ,case when t4.level3_org_id    is null then coalesce(t11.level3_org_id,'-1') else coalesce(t4.level3_org_id,'-1')  end as level3_org_id                --三级组织编码
    ,case when t4.level3_org_descr is null then coalesce(t11.level3_org_descr,'缺失') else coalesce(t4.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
    ,case when t4.level4_org_id    is null then coalesce(t11.level4_org_id,'-1') else coalesce(t4.level4_org_id,'-1')  end as level4_org_id                --四级组织编码
    ,case when t4.level4_org_descr is null then coalesce(t11.level4_org_descr,'缺失') else coalesce(t4.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
    ,case when t4.level5_org_id    is null then coalesce(t11.level5_org_id,'-1') else coalesce(t4.level5_org_id,'-1')  end as level5_org_id                --五级组织编码
    ,case when t4.level5_org_descr is null then coalesce(t11.level5_org_descr,'缺失') else coalesce(t4.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
    ,case when t4.level6_org_id    is null then coalesce(t11.level6_org_id,'-1') else coalesce(t4.level6_org_id,'-1')  end as level6_org_id                --六级组织编码
    ,case when t4.level6_org_descr is null then coalesce(t11.level6_org_descr,'缺失') else coalesce(t4.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
	,t1.organization_id                            --组织7级(库存组织)
	,t5.level7_org_descr                         --组织7级(库存组织)
	,t6.level1_businesstype_id                   --业态1级
	,t6.level1_businesstype_name                 --业态1级
	,t6.level2_businesstype_id                   --业态2级
	,t6.level2_businesstype_name                 --业态2级
	,t6.level3_businesstype_id                   --业态3级
	,t6.level3_businesstype_name                 --业态3级
	,t6.level4_businesstype_id                   --业态4级
	,t6.level4_businesstype_name                 --业态4级
	,t7.first_sale_org_code                                 --销售组织1级
	,t7.first_sale_org_name                                 --销售组织1级
	,t7.second_sale_org_code                                --销售组织2级
	,t7.second_sale_org_name                                --销售组织2级
	,t7.three_sale_org_code                                 --销售组织3级
	,t7.three_sale_org_name                                 --销售组织3级
	,t7.four_sale_org_code                                  --销售组织4级
	,t7.four_sale_org_name                                  --销售组织4级
	,t7.five_sale_org_code                                  --销售组织5级
	,t7.five_sale_org_name                                  --销售组织5级
	,case when t1.product_line = '10' then '1'
			   when t1.product_line = '20' then '2'
		  else '-1' end	             	             --产线
	,case when t1.product_line = '10' then '鸡'
			   when t1.product_line = '20' then '鸭'
		  else '缺省' end  							 --产线
	,t1.prd_line_cate_id					         --产品线1级
	,t1.prd_line_cate					             --产品线1级
	,t1.sub_prd_line_tp_id 				         --产品线2级
	,t1.sub_prd_line_tp	                 --产品线2级
	,t1.prd_channel_id                           --产品渠道ID
	,t1.prd_channel                           	 --产品渠道名称
	,t1.inventory_item_id						 --物料品名ID
	,t1.inventory_item_desc                      --物料品名名称
	,nvl(t1.day_order_cnt,0)							 --日定制品销量	
	,nvl(t1.month_order_cnt,0)                          --本月定制品销量
	,nvl(t1.month_cnt,0)                                --本月总销量
	,nvl(t1.year_order_cnt,0)                           --本年定制品销量
	,nvl(t1.year_cnt,0)                                --本年总销量
	,'$CREATE_TIME' create_time					 --数据推送时间
from (select * from TMP_DMP_BIRD_SALE_ARCH_MM_6 where op_month = '$OP_MONTH') t1
left join mreport_global.dim_org_management t4 
on t1.org_id=t4.org_id  and t4.attribute5='1'
left join mreport_global.dim_org_management t11 
on t1.org_id=t11.org_id and t1.bus_type=t11.bus_type_id and t11.attribute5='2'
left join mreport_global.dim_org_inv_management t5
	on t1.organization_id=t5.inv_org_id
left join mreport_global.dim_org_businesstype t6
	on t1.bus_type = t6.level4_businesstype_id
left join mreport_global.dwu_dim_xs_org t7
	ON t1.fifth_org_id = t7.sale_org_code
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
	$CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_00;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_00;
	$CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_0;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_0;
    $CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_1;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_1;
    $CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_2;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_2;
    $CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_3;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_3;
    $CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_4;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_4;
	$CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_5;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_5;
	$CREATE_TMP_DMP_BIRD_SALE_ARCH_MM_6;
    $INSERT_TMP_DMP_BIRD_SALE_ARCH_MM_6;
	$CREATE_DMP_BIRD_SALE_ARCH_MM;
    $INSERT_DMP_BIRD_SALE_ARCH_MM;
"  -v   
