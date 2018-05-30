#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_forecast_mm.sh                               
# 创建时间: 2018年4月19日                                          
# 创 建 者: gl                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽产业滚动预测模型
# 修改说明:                                                          
######################################################################


OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)
CURRENT_DAY=$(date -d " -0 day" +%Y%m%d)

next_month=$(date -d "${CURRENT_DAY} +1 months" +%Y%m)
next_month_first_day=${next_month}"01"
month_first_day=$(date -d $CURRENT_DAY +%Y%m01)
last_month_end_day=$(date -d "${month_first_day} -1 day" +%Y%m%d)
echo $next_month_first_day

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_forecast_mm.sh 20180101"
    exit 1
fi
###########################################################################################
## 建立临时表，用于存放每月公司每个产线的累计宰杀数、累计回收重量,累计回收金额
TMP_DWU_BIRD_FORECAST_MM_0='tmp_dwu_bird_forecast_mm_0'
CREATE_TMP_DWU_BIRD_FORECAST_MM_0="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_0(
   
    month_id                     string    --月份
   ,org_id                       string    --公司
   ,bustype                      string    --业态
   ,product_line                 string    --產綫
 
   ,m_killed_qty                 string    --宰殺数
   ,m_buy_weight                 string    --回收重量
   ,m_amount                     string    --回收金额
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_0="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_0 PARTITION (op_month='$OP_MONTH')
SELECT 
substr(t3.js_date,1,7),
t4.org_id,
'132020',
t4.meaning_desc,
sum(t3.killed_qty),
sum(t3.buy_weight),
sum(t3.amount)
FROM
 (SELECT 

   to_date(js_date) as js_date,

   pith_no,
   sum(nvl(killed_qty,0)) as killed_qty,
   sum(nvl(buy_weight,0)) as buy_weight,
   sum(nvl(amount,0)) as amount
   from mreport_poultry.dwu_qw_qw11_dd where op_day='$OP_DAY'
   GROUP BY
   to_date(js_date),
   pith_no

  
  )t3
  
  LEFT JOIN 
   (select * from mreport_poultry.dwu_qw_contract_dd where op_day='$OP_DAY')t4
   ON(t3.pith_no=t4.contractnumber)
  GROUP BY
  substr(t3.js_date,1,7),
  t4.org_id,
  '132020',
  t4.meaning_desc "


###########################################################################################
## 建立临时表，用于存放每天每個公司每个产线的投放周期及宰杀数
TMP_DWU_BIRD_FORECAST_MM_1='tmp_dwu_bird_forecast_mm_1'
CREATE_TMP_DWU_BIRD_FORECAST_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_1(
   month_id                      string   --月份

   ,org_id                       string   --公司
   ,bustype                      string   --业态
   ,product_line                 string   --產綫
   ,killed_qty                   string   --累计宰殺数
   ,buy_weight                   string   --累计回收重量
   ,amount                       string   --累计回收金额
   ,start_day                    string   --投放开始日期
   ,end_day                      string   --投放截止日期    
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"




## 转换数据--投放周期的計算
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_1="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT 
t1.month_id,
t1.org_id,
t1.bustype,
t1.product_line,
t1.m_killed_qty,
t1.m_buy_weight,
t1.m_amount,
date_sub(t1.month_first,t2.tag)as start_day,                             --投放開始日
date_sub(t1.month_last,t2.tag) as end_day                                --投放截止日

from 
 (select 
    
    month_id,

	org_id,
	bustype,
	product_line,
	m_killed_qty,
	m_buy_weight,
	m_amount,
    case product_line 
	when '鸡线' then 'CHICHEN'
	when '鸭线' then 'DUCK'
	
	end as prod_type,                                                           
 case when (substr(month_id,1,4)%4=0  and substr(month_id,1,4)%100!=0) or substr(month_id,1,4)%400=0 
 then 
   case when substr(month_id,6,2)  in ('01','03','05','07','08','10','12')  then concat(month_id,'-31')
        when substr(month_id,6,2)  in ('02') then concat(month_id,'-29')
        when substr(month_id,6,2)  in ('04','06','09','11') then concat(month_id,'-30')
   end 
 else 
   case when  substr(month_id,6,2)  in ('01','03','05','07','08','10','12')  then concat(month_id,'-31')
        when  substr(month_id,6,2)  in ('02') then concat(month_id,'-28')
        when  substr(month_id,6,2)  in ('04','06','09','11') then concat(month_id,'-30')
   end

 end as month_last,                  
concat(month_id,'-01') as month_first
from  $TMP_DWU_BIRD_FORECAST_MM_0 where op_month='$OP_MONTH') t1 
LEFT JOIN
(select  case when lookup_code='1' then 'CHICHEN'
                                 when lookup_code='2' then 'DUCK'
                            else '-999' end prod_line
                            ,int(tag) tag
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                        AND language='ZHS'
)t2 on (t1.prod_type=t2.prod_line)"


###########################################################################################
## 建立临时表，用于存放每天每个公司的月累计投放量
TMP_DWU_BIRD_FORECAST_MM_2='tmp_dwu_bird_forecast_mm_2'
CREATE_TMP_DWU_BIRD_FORECAST_MM_2="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_2(
   month_id                     string   --月份
  ,org_id                       string   --公司id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,drop_amount                  string   --投放成本
  ,drop_num                     string   --投放数量
  ,effective_drop_num           string   --有效投放数量(向上汇总用)
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_2="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_2 PARTITION(op_month='$OP_MONTH')
SELECT
tmp.month_id,
tmp.org_id,
tmp.bus_type,
tmp.meaning_desc,
sum(tmp.drop_amount),
sum(tmp.drop_num),
sum(tmp.effective_drop_num)
FROM
(SELECT 
t3.month_id as month_id,
t3.org_id as org_id,
'132020' as bus_type,
t3.meaning_desc as meaning_desc,
sum(t3.qty*t3.chicksalemoney)/sum(t3.qty) as drop_amount,

0 as drop_num,
sum(t3.qty) as effective_drop_num
FROM 
(
SELECT 
t2.month_id,

t2.org_id as org_id,
'132020' as bus_type,
t1.meaning_desc as meaning_desc, 
t1.contractnumber as contractnumber,
t1.contract_date as contract_date,
t2.start_day as start_day,
t2.end_day as end_day,
t1.qty as qty,
t1.chicksalemoney as chicksalemoney
FROM 
(SELECT 
  org_id,
  meaning_desc,
  contractnumber,
  to_date(contract_date) as contract_date,
  sum(qty) as qty,
  sum(chicksalemoney) as chicksalemoney
 FROM mreport_poultry.dwu_qw_contract_dd where op_day='$OP_DAY' and qty is not null and chicksalemoney is not null and qty!=0
 GROUP BY
  org_id,
  meaning_desc,
  contractnumber,
  to_date(contract_date)
 ) t1
LEFT JOIN 
(SELECT * from $TMP_DWU_BIRD_FORECAST_MM_1 where op_month='$OP_MONTH')t2
on (t1.org_id=t2.org_id and t1.meaning_desc=t2.product_line)
)t3
where t3.contract_date between t3.start_day and t3.end_day 
GROUP BY
t3.month_id,
t3.org_id,
'132020',
t3.meaning_desc

UNION ALL
SELECT 
t3_1.month_id as month_id,
t3_1.org_id as org_id,
'132020' as bus_type,
t3_1.meaning_desc as meaning_desc,
0 as drop_amount,
sum(t3_1.qty) as drop_num,
0 as effective_drop_num 
FROM 
(
SELECT 
t2_1.month_id as month_id,

t2_1.org_id as org_id,
'132020' as bus_type,
t1_1.meaning_desc as meaning_desc, 
t1_1.contractnumber as contractnumber,
t1_1.contract_date as contract_date,
t2_1.start_day as start_day,
t2_1.end_day as end_day,
t1_1.qty as qty
FROM 
(SELECT 
  org_id,
  meaning_desc,
  contractnumber,
  to_date(contract_date) as contract_date,
  sum(nvl(qty,0)) as qty

 FROM mreport_poultry.dwu_qw_contract_dd where op_day='$OP_DAY' 
 GROUP BY
  org_id,
  meaning_desc,
  contractnumber,
  to_date(contract_date)
 ) t1_1
LEFT JOIN 
(SELECT * from $TMP_DWU_BIRD_FORECAST_MM_1 where op_month='$OP_MONTH')t2_1
on (t1_1.org_id=t2_1.org_id and t1_1.meaning_desc=t2_1.product_line)
)t3_1
where t3_1.contract_date between t3_1.start_day and t3_1.end_day 
GROUP BY
t3_1.month_id,
t3_1.org_id,
'132020',
t3_1.meaning_desc)tmp
GROUP BY
tmp.month_id,
tmp.org_id,
tmp.bus_type,
tmp.meaning_desc

"




###########################################################################################
## 建立临时表，用于存放每個库存组织的预计回收量
TMP_DWU_BIRD_FORECAST_MM_3='tmp_dwu_bird_forecast_mm_3'
CREATE_TMP_DWU_BIRD_FORECAST_MM_3="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_3(

   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,expected_recovery            string   --预计回收量
                       
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
## 转换数据预计回收量
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"  
INSERT_TMP_DWU_BIRD_FORECAST_MM_3="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_3 PARTITION(op_month='$OP_MONTH')
SELECT
   t1.month_id,
   t1.org_id,
   t1.bustype,
   t1.product_line,
   t1.drop_num*(t3.alve_rate/100)
FROM 
(SELECT  
   month_id, 
   org_id,                      
   bustype,
   product_line,  
   case product_line  
   when '鸡线' then '鸡'
   when '鸭线' then '鸭'
  
   end  as prod_type,  
           
   drop_num from  $TMP_DWU_BIRD_FORECAST_MM_2  where op_month='$OP_MONTH')t1 
   
   LEFT JOIN 
 (SELECT * from mreport_global.ods_ebs_cux_3_gl_coop_account)t2
 ON(t1.org_id=t2.account_ou_id)

LEFT JOIN
 (SELECT org_id,nvl(alve_rate,0) as alve_rate,kpi_type from mreport_poultry.dwu_qw_qw12_dd where op_day='$OP_DAY') t3
ON
(t1.prod_type=t3.kpi_type and t1.org_id=t2.account_ou_id and t2.org_id=t3.org_id)"







###########################################################################################
## 建立临时表，用于存放每個公司的實際綜合售價B
TMP_DWU_BIRD_FORECAST_MM_4='tmp_dwu_bird_forecast_mm_4'
CREATE_TMP_DWU_BIRD_FORECAST_MM_4="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_4(
   month_id                     string   --月份 
  
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,real_selling_price_1         string   --實際綜合售價b1(cw29)
  ,real_selling_price_2         string   --实际综合售价b2(cw31)
  
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换 实际综合售价b
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_4="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_4 PARTITION(op_month='$OP_MONTH')
SELECT
t5.month_id,
t5.org_id,
t5.bustype,
t5.product_line,

nvl(t5.amount_b_1,0),
nvl(t6.amount_b_2,0)
FROM 
(SELECT 
   concat(substr(t4.day_id,1,4),'-',substr(t4.day_id,5,2)) as month_id,
   t4.org_id as org_id,
   case t4.product_line when '10' then '鸡线' 
    when '20' then '鸭线'
	end as product_line,
	'132020' as bustype,
	sum(t4.amount_b) as amount_b_1
	FROM 
(SELECT 
t1.day_id as day_id,
t1.org_id as org_id,
t1.product_line as product_line,
t1.item_id as item_id,
t1.amount_b as amount_b,
row_number() over ( partition by t1.org_id,t1.product_line,t1.item_id,substr(t1.day_id,1,6) order by  t1.day_id desc ) num   

FROM
(select 
day.day_id,
cw29.org_id as org_id,
cw29.product_line as product_line,


cw29.item_id as item_id,
cw29.sum_amount_b as amount_b

from 

  (select 
    org_id, item_id ,product_line,creation_date as c_time,nvl(sum_amount_b,0) as sum_amount_b 
   
  from  
      mreport_poultry.dwu_cw_cw29_dd where op_day='$OP_DAY')cw29 

  INNER JOIN
   (SELECT * from mreport_global.dim_day where last_day_in_month='N' and day_name<=CURRENT_DATE)day
   ON(cw29.c_time=day.day_id)
  
)t1
  )t4 
   where t4.num=1
   GROUP BY
   t4.org_id,
   case t4.product_line when '10' then '鸡线' 
    when '20' then '鸭线'
	end ,
	'132020',
   concat(substr(t4.day_id,1,4),'-',substr(t4.day_id,5,2))
  )t5
  LEFT JOIN
  (SELECT
 concat(substr(creation_date,1,4),'-',substr(creation_date,5,2)) as month_id,

  org_id,
  '132020' as bustype,
  case product_line when '10' then '鸡线'
  when '20' then '鸭线'
  end  as product_line,

  sum(nvl(amount_b,amount_a)) as amount_b_2
  
  FROM 
     mreport_poultry.DWU_CW_CW31_DD where op_day='$OP_DAY'
	
	
	
 GROUP BY
  concat(substr(creation_date,1,4),'-',substr(creation_date,5,2)),
 
  org_id,
  '132020',
  case product_line when '10' then '鸡线'
  when '20' then '鸭线'
  end
 )t6
 ON(t5.month_id=t6.month_id and t5.product_line=t6.product_line and t5.bustype=t6.bustype and t5.org_id=t6.org_id)

"

###########################################################################################
## 建立临时表，用于存放每個公司的保底綜合售價
TMP_DWU_BIRD_FORECAST_MM_5='tmp_dwu_bird_forecast_mm_5'
CREATE_TMP_DWU_BIRD_FORECAST_MM_5="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_5(
   month_id                     string   --月份
 
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,break_even_price             string   --保底成本
  ,fu_cost                      string   --副產品收入
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换保底綜合售價

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_5="   
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_5 PARTITION(op_month='$OP_MONTH')
SELECT 
   t4.month_id,
   t4.org_id,
   '132020',
   t4.product_line,
   t4.break_even_price,
   t4.fu_cost
FROM

  (SELECT 
     t4_2.month_id as month_id,
     sum(nvl(t4_1.break_even_price,0)) as break_even_price,
	 sum(nvl(t4_1.fu_cost,0)) as fu_cost,
	 t4_2.product_line as product_line,
	 t4_2.ou_id as org_id
	 FROM
   ( 
   SELECT
   substr(cost_date,1,7) as month_id,
   ou_id,
   cost_date,
   organization_id,
   '132020' as bus_type,
   item_id,
   case line_type when '10' then '鸡线' 
 	when '20' then '鸭线'
	end as product_line,
	sum(nvl(byproduct_amount,0)) as fu_cost,
    sum(nvl(byproduct_amount,0)+nvl(freight_fee_amount,0)+nvl(manual_amount,0)+nvl(fuel_amount,0)+nvl(water_elec_amount,0)+nvl(wip_chg_amount,0)        
    +nvl(wip_fix_amount,0)) as break_even_price	
	from mreport_poultry.DWU_QW_ACCOUNT_COST_DD where op_day='$OP_DAY' and account_flag='Y'
	GROUP BY
	 substr(cost_date,1,7),
	 cost_date,
	 organization_id,
	 '132020',
     ou_id,
     item_id,
   case line_type when '10' then '鸡线' 
 	when '20' then '鸭线'
	end
	
   )t4_1
   INNER JOIN
   (   SELECT
   substr(cost_date,1,7) as month_id,
   ou_id,
   max(cost_date) as cost_date,
   item_id,
   organization_id,
   '132020' as bus_type,
   case line_type when '10' then '鸡线' 
 	when '20' then '鸭线'
	end as product_line
    
	from mreport_poultry.DWU_QW_ACCOUNT_COST_DD where op_day='$OP_DAY' and account_flag='Y'
	GROUP BY
	 substr(cost_date,1,7),
	 organization_id,
	 '132020',
     ou_id,
     item_id,
   case line_type when '10' then '鸡线' 
 	when '20' then '鸭线'
	end
   )t4_2
   on(t4_1.month_id=t4_2.month_id and t4_1.cost_date=t4_2.cost_date and t4_1.product_line=t4_2.product_line and t4_1.item_id=t4_2.item_id and t4_1.ou_id=t4_2.ou_id and t4_1.bus_type=t4_2.bus_type and t4_1.organization_id=t4_2.organization_id)
   GROUP BY
     t4_2.month_id,
     
	 t4_2.product_line,
	 t4_2.ou_id
   )t4
  

"


#SG03算预估售价
###########################################################################################
## 建立临时表，用于存放每個公司的预估综合售价
TMP_DWU_BIRD_FORECAST_MM_6='tmp_dwu_bird_forecast_mm_6'
CREATE_TMP_DWU_BIRD_FORECAST_MM_6="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_6(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,estimate_price              string   --预估综合售价(月平均）
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_MM_6="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_6 PARTITION(op_month='$OP_MONTH')
SELECT 
t1.month_id,
t1.org_id,
t1.bus_type,
t1.product_line,
t5.estimate_price
FROM 
 (SELECT * FROM $TMP_DWU_BIRD_FORECAST_MM_1 where OP_MONTH='$OP_MONTH')t1
 LEFT JOIN 
(SELECT
 t2.period_name as period_name,
 t4.org_id as org_id,
 t2.product_line_name as product_line,
 '132020' as bus_type,
 avg(t2.month_price) as estimate_price
 FROM 
(SELECT product_line_name,org_code,period_name,nvl(month_price,0) as month_price,product_class_name from mreport_poultry.dwu_sg_sg03_dd where op_day='$OP_DAY')t2
 
 LEFT JOIN 
 (SELECT * from mreport_global.dim_org_management)t4
 ON(t2.org_code=t4.level6_org_id)
 
  GROUP BY
 t2.period_name,
 '132020',
 t4.org_id,
 t2.product_line_name
 
)t5

 ON (substr(date_sub(concat(t1.month_id,'-01'),1),1,7)=t5.period_name and t1.org_id=t5.org_id and t1.product_line=t5.product_line and t1.bustype=t5.bus_type)

"

###########################################################################################
## 建立临时表，用于存放每個公司的保本成本
TMP_DWU_BIRD_FORECAST_MM_7='tmp_dwu_bird_forecast_mm_7'
CREATE_TMP_DWU_BIRD_FORECAST_MM_7="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_7(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --业态
  ,product_line                 string   --產綫
  ,cost_save                    string   --保本成本
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

INSERT_TMP_DWU_BIRD_FORECAST_MM_7="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_7 PARTITION(op_month='$OP_MONTH')
SELECT 
  t5.month_id,
  t5.org_id,
  '132020',
  t5.product_line,
  t5.cost_save
  FROM
 (SELECT
	   concat(substr(t3.month_id,1,4),'-',substr(t3.month_id,5,2)) as month_id,
	   t4.org_id as org_id,
	   t3.production_line_descr as product_line,
	   t2.mon_keep_amt as cost_save
  FROM
    (SELECT day_id,month_id,level6_org_id,production_line_descr,nvl(mon_keep_amt,0) as mon_keep_amt FROM mreport_poultry.dmp_bird_keep_price_dd where op_day='$OP_DAY')t2
	JOIN
   (SELECT max(day_id) as day_id,month_id,production_line_descr,level6_org_id FROM mreport_poultry.dmp_bird_keep_price_dd where op_day='$OP_DAY' group by month_id,production_line_descr,level6_org_id)t3
   ON(t2.day_id=t3.day_id and t2.level6_org_id=t3.level6_org_id and t2.production_line_descr=t3.production_line_descr)
    LEFT JOIN 
	  (SELECT * FROM mreport_global.dim_org_management)t4
	  ON(t2.day_id=t3.day_id and t2.level6_org_id=t3.level6_org_id and t2.production_line_descr=t3.production_line_descr and t3.level6_org_id=t4.level6_org_id)
   )t5
   
   
 "
   
###########################################################################################
## 建立临时表，用于存放每個公司的产量
TMP_DWU_BIRD_FORECAST_MM_8='tmp_dwu_bird_forecast_mm_8'
CREATE_TMP_DWU_BIRD_FORECAST_MM_8="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_8(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --业态
  ,product_line                 string   --產綫
  ,inner_qty_1                  string   --产量(cw26)
  ,inner_qty_2                  string   --产量(cw28)
  
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

INSERT_TMP_DWU_BIRD_FORECAST_MM_8="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_8 PARTITION(op_month='$OP_MONTH')
SELECT 
tmp.month_id,
tmp.org_id,
tmp.bus_type,
tmp.product_line,
sum(tmp.inner_qty_1),
sum(tmp.inner_qty_2)
FROM
(SELECT 
concat(substr(creation_date,1,4),'-',substr(creation_date,5,2)) as month_id,
org_id as org_id,
'132020' as bus_type,
case product_line when '10' then '鸡线'
when '20' then '鸭线'
else null
end as product_line,
0 as inner_qty_1,
nvl(inner_qty,0) as inner_qty_2
from mreport_poultry.DWU_CW_CW28_DD where op_day='$OP_DAY'
UNION ALL
SELECT
concat(substr(period_id,1,4),'-',substr(period_id,5,2)) as month_id,
org_id as org_id,
'132020' as bus_type,
case product_line when '10' then '鸡线'
when '20' then '鸭线'
else null
end as product_line,
sum(nvl(self_buy_amount,0)) as inner_qty_1,
0 as inner_qty_2
from mreport_poultry.DWU_CW_CW26_DD where op_day='$OP_DAY'
group by
concat(substr(period_id,1,4),'-',substr(period_id,5,2)),
org_id,
'132020',
case product_line when '10' then '鸡线'
when '20' then '鸭线'
else null
end )tmp
GROUP BY
tmp.month_id,
tmp.org_id,
tmp.bus_type,
tmp.product_line

"

#创建报表的主要数据
TMP_DMP_BIRD_FORECAST_MM='tmp_dmp_bird_forecast_mm'
CREATE_TMP_DMP_BIRD_FORECAST_MM="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_FORECAST_MM(
  month_id                   string            --期间月
  ,org_id                    string            --公司
  ,bustype                   string            --业态
  ,product_line_descr        string            --产线
  ,drop_num                  string            --实际投放数量
  ,expected_recovery         string            --预计回收数量
  ,actual_recovery           string            --实际回收数量
  ,amount_investment         string            --投放汇总金额
  ,cost_save                 string            --保本成本
  ,recovery_weight           string            --回收重量       
  ,recovery_amount           string            --回收金额
  ,estimate_price_b          string            --预估综合售价B
  ,real_selling_price        string            --实际综合售价
  ,break_even_price          string            --保底综合售价
  ,inner_qty                 string            --产量
  ,effective_drop_num        string            --有效投放数量
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"

#将数据插入临时报表
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_FORECAST_MM="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_FORECAST_MM PARTITION(op_month='$OP_MONTH')
SELECT 
 regexp_replace(tmp2.month_id,'-',''),
 tmp2.org_id,
 tmp2.bus_type,
 tmp2.product_line, 
 tmp2.drop_num,
 
 tmp2.expected_recovery,
 tmp2.killed_qty,
 tmp2.drop_amount,
 tmp2.cost_save,      
 tmp2.buy_weight,
 tmp2.amount,
 tmp2.estimate_price,
 case when tmp2.inner_qty_2!=0 and tmp2.month_id <substr(CURRENT_DATE,1,7) then (tmp2.real_selling_price_2/tmp2.inner_qty_2)*1000
      when tmp2.inner_qty_1!=0 and tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)!=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  (tmp2.real_selling_price_1/tmp2.inner_qty_1)*1000
      when tmp2.inner_qty_2!=0 and tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  (tmp2.real_selling_price_2/tmp2.inner_qty_2)*1000
 else null
 end as real_selling_price,

 case when tmp2.inner_qty_2!=0 and tmp2.buy_weight!=0 and tmp2.month_id <substr(CURRENT_DATE,1,7) then ((tmp2.amount/tmp2.buy_weight)*1000)+(tmp2.break_even_price/tmp2.inner_qty_2*1000)
      when tmp2.inner_qty_1!=0 and tmp2.buy_weight!=0 and tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)!=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then ((tmp2.amount/tmp2.buy_weight)*1000)+(tmp2.break_even_price/tmp2.inner_qty_1*1000)
	  when tmp2.inner_qty_2!=0 and tmp2.buy_weight!=0 and tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then ((tmp2.amount/tmp2.buy_weight)*1000)+(tmp2.break_even_price/tmp2.inner_qty_2*1000) 
 else null	  
end as break_even_price,
 case when tmp2.month_id <substr(CURRENT_DATE,1,7) then tmp2.inner_qty_2/1000
      when tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)!=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  tmp2.inner_qty_1/1000
      when tmp2.month_id =substr(CURRENT_DATE,1,7) and date_sub(CURRENT_DATE,1)=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  tmp2.inner_qty_2/1000
	  else null
end as inner_qty,
tmp2.effective_drop_num 
 FROM  
   (SELECT 
     tmp.month_id as month_id,
	 tmp.org_id as org_id,
	 tmp.bustype as bus_type,
	 tmp.product_line as product_line,
	 sum(tmp.killed_qty) as killed_qty,
	 sum(tmp.buy_weight) as buy_weight,
	 sum(tmp.amount) as amount,
	 sum(tmp.drop_num) as drop_num,
	 sum(tmp.drop_amount) as drop_amount,
	 
	 sum(tmp.expected_recovery) as expected_recovery,
	 sum(tmp.real_selling_price_1) as real_selling_price_1,
	 sum(tmp.real_selling_price_2) as real_selling_price_2,
	 sum(tmp.break_even_price) as break_even_price,
	 sum(tmp.estimate_price) as estimate_price,
	 sum(tmp.cost_save) as cost_save,
	 sum(tmp.inner_qty_1) as inner_qty_1,
	 sum(tmp.inner_qty_2) as inner_qty_2,
	 sum(tmp.effective_drop_num) as effective_drop_num
	 FROM
     (select month_id,org_id,bustype,product_line,killed_qty,buy_weight,amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_1 where op_month='$OP_MONTH'
     UNION ALL
     select month_id,org_id,bustype,product_line,0 as killed_qty,0 as buy_weight, 0 as amount,
      drop_num,drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2,effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_2 where op_month='$OP_MONTH'
     
     UNION ALL
      select month_id,org_id,bustype,product_line,0 as killed_qty, 0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2 ,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_3 where op_month='$OP_MONTH'
      UNION ALL
      select month_id,org_id,bustype,product_line, 0 as killed_qty,0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, real_selling_price_1,real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_4 where op_month='$OP_MONTH'
      UNION ALL
    select month_id,org_id,bustype,product_line,0 as killed_qty,0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  break_even_price,0 as estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_5 where op_month='$OP_MONTH'
	 
     UNION ALL
	   select month_id,org_id,bustype,product_line,0 as killed_qty,0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,estimate_price,0 as cost_save,0 as inner_qty_1,0 as inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_6 where op_month='$OP_MONTH'
	  UNION ALL
	    select month_id,org_id,bustype,product_line,0 as killed_qty,0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,cost_save,0 as inner_qty_1,0 as inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_7 where op_month='$OP_MONTH'
	   UNION ALL
	   select month_id,org_id,bustype,product_line,0 as killed_qty,0 as buy_weight,0 as amount,
      0 as drop_num,0 as drop_amount,0 as expected_recovery, 0 as real_selling_price_1, 0 as real_selling_price_2,
	  
	  
	  0 as break_even_price,0 as estimate_price,0 as cost_save,inner_qty_1,inner_qty_2,0 as effective_drop_num


	 from $TMP_DWU_BIRD_FORECAST_MM_8 where op_month='$OP_MONTH'
	  )tmp
	  GROUP BY
	 tmp.month_id,
	 tmp.org_id,
	 tmp.bustype,
	 tmp.product_line
	 )tmp2
	 INNER JOIN
	 (SELECT * from mreport_global.ods_ebs_hr_all_organization_units)t2
	 ON(t2.attribute2='冷藏厂-生产性' AND tmp2.org_id = t2.organization_id)

    "



#创建最终报表样式
DMP_BIRD_FORECAST_MM='dmp_bird_forecast_mm'
CREATE_DMP_BIRD_FORECAST_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_FORECAST_MM(
 month_id                     string            --期间(月)   
,day_id                       string            --期间(月）
,level1_org_id                string            --组织1级
,level1_org_descr             string            --组织1级
,level2_org_id                string            --组织2级
,level2_org_descr             string            --组织2级
,level3_org_id                string            --组织3级
,level3_org_descr             string            --组织3级
,level4_org_id                string            --组织4级
,level4_org_descr             string            --组织4级
,level5_org_id                string            --组织5级
,level5_org_descr             string            --组织5级
,level6_org_id                string            --组织6级
,level6_org_descr             string            --组织6级
,level7_org_id                string            --组织7级
,level7_org_descr             string            --组织7级
,level1_businesstype_id       string            --业态1级
,level1_businesstype_name     string            --业态1级
,level2_businesstype_id       string            --业态2级
,level2_businesstype_name     string            --业态2级
,level3_businesstype_id       string            --业态3级
,level3_businesstype_name     string            --业态3级
,level4_businesstype_id       string            --业态4级
,level4_businesstype_name     string            --业态4级
,production_line_id           string            --产线id
,production_line_descr        string            --产线描述
,drop_num                     string            --投放数量
,expected_recovery            string            --预计回收数量
,actual_recovery              string            --实际回收数量
,amount_investment            string            --投放汇总金额
,cost_save                    string            --保本成本
,recovery_weight              string            --回收重量       
,recovery_amount              string            --回收金额
,estimate_price_b             string            --预估综合售价B
,real_selling_price           string            --实际综合售价
,break_even_price             string            --保底综合售价
,inner_qty                    string            --产量
,effective_drop_num           string            --有效投放数量
,create_time                  string            --数据推送时间

)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE"


##数据转换 报表样式
#

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_FORECAST_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_FORECAST_MM PARTITION(op_month='$OP_MONTH')
SELECT 
regexp_replace(t1.month_id,'-',''),
$OP_DAY,
case when t7.level1_org_id    is null then coalesce(t3.level1_org_id,'-1') else coalesce(t7.level1_org_id,'-1')  end as level1_org_id,                --一级组织编码
case when t7.level1_org_descr is null then coalesce(t3.level1_org_descr,'缺失') else coalesce(t7.level1_org_descr,'缺失')  end as level1_org_descr,   --一级组织描述
case when t7.level2_org_id is null    then coalesce(t3.level2_org_id,'-1') else coalesce(t7.level2_org_id,'-1')  end as level2_org_id,                --二级组织编码
case when t7.level2_org_descr is null then coalesce(t3.level2_org_descr,'缺失') else coalesce(t7.level2_org_descr,'缺失')  end as level2_org_descr,   --二级组织描述
case when t7.level3_org_id    is null then coalesce(t3.level3_org_id,'-1') else coalesce(t7.level3_org_id,'-1')  end as level3_org_id,                --三级组织编码
case when t7.level3_org_descr is null then coalesce(t3.level3_org_descr,'缺失') else coalesce(t7.level3_org_descr,'缺失')  end as level3_org_descr,   --三级组织描述
case when t7.level4_org_id    is null then coalesce(t3.level4_org_id,'-1') else coalesce(t7.level4_org_id,'-1')  end as level4_org_id ,               --四级组织编码
case when t7.level4_org_descr is null then coalesce(t3.level4_org_descr,'缺失') else coalesce(t7.level4_org_descr,'缺失')  end as level4_org_descr,   --四级组织描述
case when t7.level5_org_id    is null then coalesce(t3.level5_org_id,'-1') else coalesce(t7.level5_org_id,'-1')  end as level5_org_id,                --五级组织编码
case when t7.level5_org_descr is null then coalesce(t3.level5_org_descr,'缺失') else coalesce(t7.level5_org_descr,'缺失')  end as level5_org_descr,   --五级组织描述
case when t7.level6_org_id    is null then coalesce(t3.level6_org_id,'-1') else coalesce(t7.level6_org_id,'-1')  end as level6_org_id ,               --六级组织编码
case when t7.level6_org_descr is null then coalesce(t3.level6_org_descr,'缺失') else coalesce(t7.level6_org_descr,'缺失')  end as level6_org_descr,   --六级组织描述
'缺失',
'缺失',
t2.level1_businesstype_id,
t2.level1_businesstype_name,
t2.level2_businesstype_id,
t2.level2_businesstype_name,
t2.level3_businesstype_id,
t2.level3_businesstype_name,
t2.level4_businesstype_id,
t2.level4_businesstype_name,
case t1.product_line_descr 
when '鸡线' then '1'
when '鸭线' then '2' 
else '-1'
end as product_line_id,
nvl(t1.product_line_descr,'缺失'),
t1.drop_num,
t1.expected_recovery,
t1.actual_recovery,
t1.amount_investment,
t1.cost_save,      
t1.recovery_weight,
t1.recovery_amount,
t1.estimate_price_b,
NVL(t1.real_selling_price,0),
t1.break_even_price,
t1.inner_qty,
t1.effective_drop_num,
$CREATE_TIME
FROM 

  (SELECT * FROM $TMP_DMP_BIRD_FORECAST_MM WHERE op_month='$OP_MONTH' )t1
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
      ON (t1.bustype=t2.level4_businesstype_id)
 
    LEFT JOIN 
    (SELECT
       org_id,
	   bus_type_id,
       level1_org_id, 
       level1_org_descr,
       level2_org_id,
       level2_org_descr,
       level3_org_id,
       level3_org_descr,
       level4_org_id,
       level4_org_descr,
       level5_org_id,
       level5_org_descr,
       level6_org_id,
       level6_org_descr,
	   attribute5
    FROM mreport_global.dim_org_management)t3
	  ON(t1.org_id=t3.org_id and t1.bustype=t3.bus_type_id and t3.attribute5='2')
	 LEFT JOIN 
        (select * from mreport_global.dim_org_management)t7
	ON(t1.org_id=t7.org_id and t7.attribute5='1')	
         		
	 
	"
 

 echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;
	$CREATE_TMP_DWU_BIRD_FORECAST_MM_0;
	$INSERT_TMP_DWU_BIRD_FORECAST_MM_0;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_1;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_1;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_2;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_2;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_3;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_3;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_4;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_4;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_5;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_5;
	$CREATE_TMP_DWU_BIRD_FORECAST_MM_6;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_6;
    $CREATE_TMP_DWU_BIRD_FORECAST_MM_7;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_7;
	$CREATE_TMP_DWU_BIRD_FORECAST_MM_8;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_8;
	$CREATE_TMP_DMP_BIRD_FORECAST_MM;
	$INSERT_TMP_DMP_BIRD_FORECAST_MM;
    $CREATE_DMP_BIRD_FORECAST_MM;
    $INSERT_DMP_BIRD_FORECAST_MM;
    "  -v
