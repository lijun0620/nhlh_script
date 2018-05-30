#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_b_price_mm.sh                               
# 创建时间: 2018年04月18日                                            
# 创 建 者: jhl                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 综合售价b月度变动趋势
# 修改说明:                                                          
######################################################################

op_day=$1
op_month=${op_day:0:6}

# 当前时间
create_time=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_b_price_mm.sh 20180101"
    exit 1
fi


######################################tmp_dwu_bird_b_price_mm(综合售价、产量)#####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dwu_bird_b_price_mm='tmp_dwu_bird_b_price_mm'

create_tmp_dwu_bird_b_price_mm="
create table if not exists $tmp_dwu_bird_b_price_mm (
	month_id					string		--月份id
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,amount_b                   string         --当月末累计综合售价b金额
	,inner_qty                  string         --自购产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dwu_bird_b_price_mm="
insert overwrite table $tmp_dwu_bird_b_price_mm partition (op_month = '$op_month') 
select 
t.month_id,
t.org_id,
t.product_line,
--综合售价b-运费
(t.amount_b - nvl(t1.adc,0)) as amount_b,
nvl(t2.inner_qty,0) as inner_qty
from 
(select substring(creation_date,1,6) as month_id,org_id,
case when product_line = '10' then '1' when product_line = '20' then '2' else '-1' end as product_line,
--sum(nvl(amount_b,0)) as amount_b
sum(if((nvl(amount_b,0)=='0' or amount_b=='0.0'),amount_a,amount_b)) as amount_b
from mreport_poultry.dwu_cw_cw31_dd 
where op_day ='$op_day'
group by substring(creation_date,1,6),org_id,product_line ) t
left join 
( 
select org_id, org_name,period_code as month_id,
case when segm5 = '10' then '1' when segm5 = '20' then '2' else '-1' end as product_line,
sum(nvl(acc_dr_cr,0)) as adc
from mreport_feed.dwu_finance_expense_restore_before  where segm3='6601010111' and segm4= '132020' 
group by org_id,org_name,period_code,segm5
) t1
on (t.org_id=t1.org_id  and t.month_id=t1.month_id and t.product_line=t1.product_line)
left join 
(select creation_date as month_id,org_id,
case when product_line = '10' then '1' when product_line = '20' then '2' else '-1' end as product_line,
(nvl(inner_qty,0)/1000) as inner_qty
from mreport_poultry.dwu_cw_cw28_dd 
where op_day='$op_day'
group by org_id,creation_date,product_line,inner_qty) t2
on (t1.org_id=t2.org_id  and t1.month_id=t2.month_id and t1.product_line=t2.product_line)
;
"

#######################################N-1####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_1='tmp_dmp_bird_b_price_mm_1'

create_tmp_dmp_bird_b_price_mm_1="
create table if not exists $tmp_dmp_bird_b_price_mm_1 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n1_sale_amt            	string      --n-1月综合售价b（元）
	,n1_sale_cnt				string      --n-1月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_1="
insert overwrite table $tmp_dmp_bird_b_price_mm_1 partition (op_month = '$op_month')
select
case when substr(t.month_id,5,2) = '12' then concat(floor(substr(t.month_id,1,4) + 1),'01') 
else concat(floor(t.month_id + 1)) end as month_id,
t.org_id,product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-2####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_2='tmp_dmp_bird_b_price_mm_2'

create_tmp_dmp_bird_b_price_mm_2="
create table if not exists $tmp_dmp_bird_b_price_mm_2 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n2_sale_amt            	string      --n-2月综合售价b
	,n2_sale_cnt				string      --n-2月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_2="
insert overwrite table $tmp_dmp_bird_b_price_mm_2 partition (op_month = '$op_month')
select
case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'02')
		 when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'01')
		 else concat(floor(month_id + 2)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-3####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_3='tmp_dmp_bird_b_price_mm_3'

create_tmp_dmp_bird_b_price_mm_3="
create table if not exists $tmp_dmp_bird_b_price_mm_3 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n3_sale_amt            	string      --n-3月综合售价b
	,n3_sale_cnt				string      --n-3月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_3="
insert overwrite table $tmp_dmp_bird_b_price_mm_3 partition (op_month = '$op_month')
select
case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'03') 
		 when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'02')
		 when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'01')
		 else concat(floor(month_id + 3)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-4####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_4='tmp_dmp_bird_b_price_mm_4'

create_tmp_dmp_bird_b_price_mm_4="
create table if not exists $tmp_dmp_bird_b_price_mm_4 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n4_sale_amt            	string      --n-4月综合售价b
	,n4_sale_cnt				string      --n-4月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_4="
insert overwrite table $tmp_dmp_bird_b_price_mm_4 partition (op_month = '$op_month')
select
case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 4)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-5####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_5='tmp_dmp_bird_b_price_mm_5'

create_tmp_dmp_bird_b_price_mm_5="
create table if not exists $tmp_dmp_bird_b_price_mm_5 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n5_sale_amt            	string      --n-5月综合售价b
	,n5_sale_cnt				string      --n-5月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_5="
insert overwrite table $tmp_dmp_bird_b_price_mm_5 partition (op_month = '$op_month')
select
case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'05') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 5)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"
#######################################N-6####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_6='tmp_dmp_bird_b_price_mm_6'

create_tmp_dmp_bird_b_price_mm_6="
create table if not exists $tmp_dmp_bird_b_price_mm_6 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n6_sale_amt            	string      --n-6月综合售价b
	,n6_sale_cnt				string      --n-6月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_6="
insert overwrite table $tmp_dmp_bird_b_price_mm_6 partition (op_month = '$op_month')
select
	case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'06')
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'05')
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'04')
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'03')
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'02')
		when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'01')	
		else concat(floor(month_id + 6)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"
#######################################N-7####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_7='tmp_dmp_bird_b_price_mm_7'

create_tmp_dmp_bird_b_price_mm_7="
create table if not exists $tmp_dmp_bird_b_price_mm_7 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n7_sale_amt            	string      --n-7月综合售价b
	,n7_sale_cnt				string      --n-7月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_7="
insert overwrite table $tmp_dmp_bird_b_price_mm_7 partition (op_month = '$op_month')
select
		case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'07') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'06') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'05') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '06' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 7)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-8####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_8='tmp_dmp_bird_b_price_mm_8'

create_tmp_dmp_bird_b_price_mm_8="
create table if not exists $tmp_dmp_bird_b_price_mm_8 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n8_sale_amt            	string      --n-8月综合售价b
	,n8_sale_cnt				string      --n-8月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_8="
insert overwrite table $tmp_dmp_bird_b_price_mm_8 partition (op_month = '$op_month')
select
		case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'08') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'07') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'06') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'05') 
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '06' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '05' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 8)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

#######################################N-9####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_9='tmp_dmp_bird_b_price_mm_9'

create_tmp_dmp_bird_b_price_mm_9="
create table if not exists $tmp_dmp_bird_b_price_mm_9 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n9_sale_amt            	string      --n-9月综合售价b
	,n9_sale_cnt				string      --n-9月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_9="
insert overwrite table $tmp_dmp_bird_b_price_mm_9 partition (op_month = '$op_month')
select
	case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'09') 
	when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'08') 
	when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'07') 
	when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'06') 
	when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'05') 
	when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'04') 
	when substr(month_id,5,2) = '06' then concat(floor(substr(month_id,1,4) + 1),'03') 
	when substr(month_id,5,2) = '05' then concat(floor(substr(month_id,1,4) + 1),'02') 
	when substr(month_id,5,2) = '04' then concat(floor(substr(month_id,1,4) + 1),'01') 
	else concat(floor(month_id + 9)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"
#######################################N-10####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_10='tmp_dmp_bird_b_price_mm_10'

create_tmp_dmp_bird_b_price_mm_10="
create table if not exists $tmp_dmp_bird_b_price_mm_10 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n10_sale_amt            	string      --n-10月综合售价b
	,n10_sale_cnt				string      --n-10月产量（吨
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_10="
insert overwrite table $tmp_dmp_bird_b_price_mm_10 partition (op_month = '$op_month')
select
	case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'10') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'09') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'08') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'07') 
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'06') 
		when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'05') 
		when substr(month_id,5,2) = '06' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '05' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '04' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '03' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 10)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"
#######################################N-11####################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_11='tmp_dmp_bird_b_price_mm_11'

create_tmp_dmp_bird_b_price_mm_11="
create table if not exists $tmp_dmp_bird_b_price_mm_11 (
	month_id					string		--月份
	,org_id                     string      --公司id
	,product_line            	string      --产线
	,n11_sale_amt            	string      --n-11月综合售价b
	,n11_sale_cnt				string      --n-11月产量（吨）
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_11="
insert overwrite table $tmp_dmp_bird_b_price_mm_11 partition (op_month = '$op_month')
select
	case when substr(month_id,5,2) = '12' then concat(floor(substr(month_id,1,4) + 1),'11') 
		when substr(month_id,5,2) = '11' then concat(floor(substr(month_id,1,4) + 1),'10') 
		when substr(month_id,5,2) = '10' then concat(floor(substr(month_id,1,4) + 1),'09') 
		when substr(month_id,5,2) = '09' then concat(floor(substr(month_id,1,4) + 1),'08') 
		when substr(month_id,5,2) = '08' then concat(floor(substr(month_id,1,4) + 1),'07') 
		when substr(month_id,5,2) = '07' then concat(floor(substr(month_id,1,4) + 1),'06') 
		when substr(month_id,5,2) = '06' then concat(floor(substr(month_id,1,4) + 1),'05') 
		when substr(month_id,5,2) = '05' then concat(floor(substr(month_id,1,4) + 1),'04') 
		when substr(month_id,5,2) = '04' then concat(floor(substr(month_id,1,4) + 1),'03') 
		when substr(month_id,5,2) = '03' then concat(floor(substr(month_id,1,4) + 1),'02') 
		when substr(month_id,5,2) = '02' then concat(floor(substr(month_id,1,4) + 1),'01') 
		else concat(floor(month_id + 11)) end month_id
,t.org_id,t.product_line,t.amount_b,t.inner_qty
from $tmp_dwu_bird_b_price_mm t
where t.op_month = '$op_month'
;
"

####################################得到任意月份的最近1年的综合售价b#######################################################
## 将数据从大表转换至目标表
## tmp清单表
## 变量声明
tmp_dmp_bird_b_price_mm_12='tmp_dmp_bird_b_price_mm_12'

create_tmp_dmp_bird_b_price_mm_12="
create table if not exists $tmp_dmp_bird_b_price_mm_12 (
month_id		string	--月份
,org_id         string  --公司id
,product_line   string  --产线
,n_sale_amt	   string  --n月综合售价b
,n_sale_cnt	   string  --n月产量
,n1_sale_amt   string  --n-1月综合售价b
,n1_sale_cnt   string  --n-1月产量
,n2_sale_amt   string  --n-2月综合售价b
,n2_sale_cnt   string  --n-2月产量
,n3_sale_amt   string  --n-3月综合售价b
,n3_sale_cnt   string  --n-3月产量
,n4_sale_amt   string  --n-4月综合售价b
,n4_sale_cnt   string  --n-4月产量
,n5_sale_amt   string  --n-5月综合售价b
,n5_sale_cnt   string  --n-5月产量
,n6_sale_amt   string  --n-6月综合售价b
,n6_sale_cnt   string  --n-6月产量
,n7_sale_amt   string  --n-7月综合售价b
,n7_sale_cnt   string  --n-7月产量
,n8_sale_amt   string  --n-8月综合售价b
,n8_sale_cnt   string  --n-8月产量
,n9_sale_amt   string  --n-9月综合售价b
,n9_sale_cnt   string  --n-9月产量
,n10_sale_amt  string  --n-10月综合售价b
,n10_sale_cnt   string  --n-10月产量
,n11_sale_amt  string  --n-11月综合售价b
,n11_sale_cnt   string  --n-11月产量
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as orc
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_tmp_dmp_bird_b_price_mm_12="
insert overwrite table $tmp_dmp_bird_b_price_mm_12 partition (op_month = '$op_month')
select 
t.month_id
,t.org_id
,t.product_line
,t.n_sale_amt	    --n月综合售价b
,t.n_sale_cnt       --n月产量
,t1.n1_sale_amt     --n-1月综合售价b
,t1.n1_sale_cnt       --n-1月产量
,t2.n2_sale_amt     --n-2月综合售价b
,t2.n2_sale_cnt       --n-2月产量
,t3.n3_sale_amt	    --n-3月综合售价b
,t3.n3_sale_cnt       --n-3月产量
,t4.n4_sale_amt     --n-4月综合售价b
,t4.n4_sale_cnt       --n-4月产量
,t5.n5_sale_amt     --n-5月综合售价b
,t5.n5_sale_cnt       --n-5月产量
,t6.n6_sale_amt	    --n-6月综合售价b
,t6.n6_sale_cnt       --n-6月产量
,t7.n7_sale_amt     --n-7月综合售价b
,t7.n7_sale_cnt       --n-7月产量
,t8.n8_sale_amt     --n-8月综合售价b
,t8.n8_sale_cnt      --n-8月产量
,t9.n9_sale_amt	    --n-9月综合售价b
,t9.n9_sale_cnt       --n-9月产量
,t10.n10_sale_amt   --n-10月综合售价b
,t10.n10_sale_cnt       --n-10月产量
,t11.n11_sale_amt   --n-11月综合售价b
,t11.n11_sale_cnt    --n-11月产量
from 
(select 
month_id
,org_id
,product_line
,amount_b as n_sale_amt,inner_qty as n_sale_cnt
from tmp_dwu_bird_b_price_mm where op_month = '$op_month') t
left join tmp_dmp_bird_b_price_mm_1 t1
on(
t1.op_month='$op_month'
and t.month_id = t1.month_id
and t.org_id = t1.org_id
and t.product_line = t1.product_line
)
left join tmp_dmp_bird_b_price_mm_2 t2
on(
t2.op_month='$op_month'
and t.month_id = t2.month_id
and t.org_id = t2.org_id
and t.product_line = t2.product_line
)
left join tmp_dmp_bird_b_price_mm_3 t3
on(
t3.op_month='$op_month'
and t.month_id = t3.month_id
and t.org_id = t3.org_id
and t.product_line = t3.product_line
)
left join tmp_dmp_bird_b_price_mm_4 t4
on(
t4.op_month='$op_month'
and t.month_id = t4.month_id
and t.org_id = t4.org_id
and t.product_line = t4.product_line
)
left join tmp_dmp_bird_b_price_mm_5 t5
on(
t5.op_month='$op_month'
and t.month_id = t5.month_id
and t.org_id = t5.org_id
and t.product_line = t5.product_line
)
left join tmp_dmp_bird_b_price_mm_6 t6
on(
t6.op_month='$op_month'
and t.month_id = t6.month_id
and t.org_id = t6.org_id
and t.product_line = t6.product_line
)
left join tmp_dmp_bird_b_price_mm_7 t7
on(
t7.op_month='$op_month'
and t.month_id = t7.month_id
and t.org_id = t7.org_id
and t.product_line = t7.product_line
)
left join tmp_dmp_bird_b_price_mm_8 t8
on(
t8.op_month='$op_month'
and t.month_id = t8.month_id
and t.org_id = t8.org_id
and t.product_line = t8.product_line
)
left join tmp_dmp_bird_b_price_mm_9 t9
on(
t9.op_month='$op_month'
and t.month_id = t9.month_id
and t.org_id = t9.org_id
and t.product_line = t9.product_line
)
left join tmp_dmp_bird_b_price_mm_10 t10
on(
t10.op_month='$op_month'
and t.month_id = t10.month_id
and t.org_id = t10.org_id
and t.product_line = t10.product_line
)
left join tmp_dmp_bird_b_price_mm_11 t11
on(
t11.op_month='$op_month'
and t.month_id = t11.month_id
and t.org_id = t11.org_id
and t.product_line = t11.product_line
)
;
"
########################################dm层上 final table : dmp_bird_b_price_mm###################################################
## 将数据从大表转换至目标表
## 清单表
## 变量声明
dmp_bird_b_price_mm='dmp_bird_b_price_mm'

create_dmp_bird_b_price_mm="
create table if not exists $dmp_bird_b_price_mm (
month_id               string     --月份
,day_id                string     --日期
,level1_org_id		string   --一级组织编码
,level1_org_descr   string   --一级组织描述
,level2_org_id      string   --二级组织编码
,level2_org_descr   string   --二级组织描述
,level3_org_id      string   --三级组织编码
,level3_org_descr   string   --三级组织描述
,level4_org_id      string   --四级组织编码
,level4_org_descr   string   --四级组织描述
,level5_org_id      string   --五级组织编码
,level5_org_descr   string   --五级组织描述
,level6_org_id    string   --六级组织编码
,level6_org_descr   string   --六级组织描述
,level7_org_id string   --组织7级(库存组织)
,level7_org_descr  string   --组织7级(库存组织)
,level1_businesstype_id  string   --业态1级
,level1_businesstype_name  string   --业态1级
,level2_businesstype_id   string   --业态2级
,level2_businesstype_name string   --业态2级
,level3_businesstype_id   string   --业态3级
,level3_businesstype_name string   --业态3级
,level4_businesstype_id  string   --业态4级
,level4_businesstype_name string   --业态4级
,production_line_id  string   --产线
,production_line_descr   string  --产线描述
,level1_prod_id  string   --产品线1级
,level1_prod_descr   string  --产品线1级描述
,level2_prod_id      string   --产品线2级
,level2_prod_descr   string  --产品线2级描述
,month_sale_amt string   --n月综合售价b
,month_sale_cnt string   --n月产量
,n1_sale_amt string   --n-1月综合售价b
,n1_sale_cnt string   --n-1月产量
,n2_sale_amt string   --n-2月综合售价b
,n2_sale_cnt string   --n-2月产量
,n3_sale_amt string   --n-3月综合售价b
,n3_sale_cnt string   --n-3月产量
,n4_sale_amt string   --n-4月综合售价b
,n4_sale_cnt string   --n-4月产量
,n5_sale_amt string   --n-5月综合售价b
,n5_sale_cnt string   --n-5月产量
,n6_sale_amt string   --n-6月综合售价b
,n6_sale_cnt string   --n-6月产量
,n7_sale_amt string   --n-7月综合售价b
,n7_sale_cnt string   --n-7月产量
,n8_sale_amt string   --n-8月综合售价b
,n8_sale_cnt string   --n-8月产量
,n9_sale_amt string   --n-9月综合售价b
,n9_sale_cnt string   --n-9月产量
,n10_sale_amt string  --n-10月综合售价b
,n10_sale_cnt string   --n10月产量
,n11_sale_amt string  --n-11月综合售价b
,n11_sale_cnt string   --n-11月产量
,create_time  string   --创建时间
)partitioned by (op_month string)
row format delimited fields terminated by '\011'
stored as textfile;
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
insert_dmp_bird_b_price_mm="
insert overwrite table $dmp_bird_b_price_mm partition (op_month = '$op_month')
select
t1.month_id
,'' day_id
,case when t2.level1_org_id    is null then coalesce(t3.level1_org_id,'-1') else coalesce(t2.level1_org_id,'-1')  end as level1_org_id                --一级组织编码
,case when t2.level1_org_descr is null then coalesce(t3.level1_org_descr,'缺失') else coalesce(t2.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
,case when t2.level2_org_id is null    then coalesce(t3.level2_org_id,'-1') else coalesce(t2.level2_org_id,'-1')  end as level2_org_id                --二级组织编码
,case when t2.level2_org_descr is null then coalesce(t3.level2_org_descr,'缺失') else coalesce(t2.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
,case when t2.level3_org_id    is null then coalesce(t3.level3_org_id,'-1') else coalesce(t2.level3_org_id,'-1')  end as level3_org_id                --三级组织编码
,case when t2.level3_org_descr is null then coalesce(t3.level3_org_descr,'缺失') else coalesce(t2.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
,case when t2.level4_org_id    is null then coalesce(t3.level4_org_id,'-1') else coalesce(t2.level4_org_id,'-1')  end as level4_org_id                --四级组织编码
,case when t2.level4_org_descr is null then coalesce(t3.level4_org_descr,'缺失') else coalesce(t2.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
,case when t2.level5_org_id    is null then coalesce(t3.level5_org_id,'-1') else coalesce(t2.level5_org_id,'-1')  end as level5_org_id                --五级组织编码
,case when t2.level5_org_descr is null then coalesce(t3.level5_org_descr,'缺失') else coalesce(t2.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
,case when t2.level6_org_id    is null then coalesce(t3.level6_org_id,'-1') else coalesce(t2.level6_org_id,'-1')  end as level6_org_id                --六级组织编码
,case when t2.level6_org_descr is null then coalesce(t3.level6_org_descr,'缺失') else coalesce(t2.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
,'' level7_org_id --组织7级(库存组织)
,'' level7_org_descr  --组织7级(库存组织)
,'' level1_businesstype_id  --业态1级
,'' level1_businesstype_name  --业态1级
,'' level2_businesstype_id   --业态2级
,'' level2_businesstype_name --业态2级
,'' level3_businesstype_id   --业态3级
,'' level3_businesstype_name --业态3级
,'' level4_businesstype_id  --业态4级
,'' level4_businesstype_name --业态4级
,t1.product_line  --产线
,case when t1.product_line = '1' then '鸡' when t1.product_line = '2' then '鸭' else '' end 
,'' level1_prod_id
,'' level1_prod_descr
,'' level2_prod_id
,'' level2_prod_descr
,t1.n_sale_amt 
,t1.n_sale_cnt 
,t1.n1_sale_amt
,t1.n1_sale_cnt
,t1.n2_sale_amt
,t1.n2_sale_cnt
,t1.n3_sale_amt
,t1.n3_sale_cnt
,t1.n4_sale_amt
,t1.n4_sale_cnt
,t1.n5_sale_amt
,t1.n5_sale_cnt
,t1.n6_sale_amt
,t1.n6_sale_cnt
,t1.n7_sale_amt
,t1.n7_sale_cnt
,t1.n8_sale_amt
,t1.n8_sale_cnt
,t1.n9_sale_amt
,t1.n9_sale_cnt
,t1.n10_sale_amt
,t1.n10_sale_cnt
,t1.n11_sale_amt
,t1.n11_sale_cnt
,'$op_day' as create_time
from 
(select t.* from  $tmp_dmp_bird_b_price_mm_12 t where t.op_month= '$op_month') t1
left join  mreport_global.dim_org_management t2 on t1.org_id=t2.org_id  and t2.attribute5='1'
left join  mreport_global.dim_org_management t3 on t1.org_id=t3.org_id and t3.bus_type_id='132020' and t3.attribute5='2'
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
  $create_tmp_dwu_bird_b_price_mm;
  $insert_tmp_dwu_bird_b_price_mm;
  $create_tmp_dmp_bird_b_price_mm_1;
  $insert_tmp_dmp_bird_b_price_mm_1;
  $create_tmp_dmp_bird_b_price_mm_2;
  $insert_tmp_dmp_bird_b_price_mm_2;
  $create_tmp_dmp_bird_b_price_mm_3;
  $insert_tmp_dmp_bird_b_price_mm_3;
  $create_tmp_dmp_bird_b_price_mm_4;
  $insert_tmp_dmp_bird_b_price_mm_4;
  $create_tmp_dmp_bird_b_price_mm_5;
  $insert_tmp_dmp_bird_b_price_mm_5;
  $create_tmp_dmp_bird_b_price_mm_6;
  $insert_tmp_dmp_bird_b_price_mm_6;
  $create_tmp_dmp_bird_b_price_mm_7;
  $insert_tmp_dmp_bird_b_price_mm_7;
  $create_tmp_dmp_bird_b_price_mm_8;
  $insert_tmp_dmp_bird_b_price_mm_8;
  $create_tmp_dmp_bird_b_price_mm_9;
  $insert_tmp_dmp_bird_b_price_mm_9;
  $create_tmp_dmp_bird_b_price_mm_10;
  $insert_tmp_dmp_bird_b_price_mm_10;
  $create_tmp_dmp_bird_b_price_mm_11;
  $insert_tmp_dmp_bird_b_price_mm_11;
  $create_tmp_dmp_bird_b_price_mm_12;
  $insert_tmp_dmp_bird_b_price_mm_12;
  $create_dmp_bird_b_price_mm; 
  $insert_dmp_bird_b_price_mm;
"  -v 