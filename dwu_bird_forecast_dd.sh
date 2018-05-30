OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)


# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_forecast_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 建立临时表，用于存放每個合同對應的投放周期
TMP_DWU_BIRD_FORECAST_DD_1='tmp_dwu_bird_forecast_dd_1'
CREATE_TMP_DWU_BIRD_FORECAST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_1(
   
   js_d                         string   --結算日期
  ,pith_no                      string   --合同號
 
  ,product_line                 string   --產綫
  ,killed_qty                   string   --宰殺数
  ,start_day                    string   --投放开始日期
  ,end_day                      string   --投放截止日期    
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"




## 转换数据--投放周期的計算
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_1="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_1 PARTITION(op_day='$OP_DAY')
SELECT 

t1.js_d,
t1.pith_no,
t1.work_unit,
t1.killed_qty,
date_sub(t1.month_first,t2.tag)as start_day,                               --投放開始日
date_sub(t1.month_last,t2.tag) as end_day                                --投放截止日

from 
 (select 
   
    to_date(js_date) as js_d,                                              --结算日期
    pith_no,                                                                --批次号
    work_unit,                                                   
    killed_qty,                                                             --宰杀数量
                                                                         
    case work_unit 
	when '鸡线' then 'CHICHEN'
	when '鸭线' then 'DUCK'
	else '缺省'
	end as prod_line,                                                           --产线
 case when (substr(to_date(js_date),1,4)%4=0  and substr(to_date(js_date),1,4)%100!=0) or substr(to_date(js_date),1,4)%400=0 
 then 
   case when substr(to_date(js_date),6,2)  in ('01','03','05','07','08','10','12')  then concat(substr(to_date(js_date),1,7),'-31')
        when substr(to_date(js_date),6,2)  in ('02') then concat(substr(to_date(js_date),1,7),'-29')
        when substr(to_date(js_date),6,2)  in ('04','06','09','11') then concat(substr(to_date(js_date),1,7),'-30')
   end 
 else 
   case when  substr(to_date(js_date),6,2)  in ('01','03','05','07','08','10','12')  then concat(substr(to_date(js_date),1,7),'-31')
        when  substr(to_date(js_date),6,2)  in ('02') then concat(substr(to_date(js_date),1,7),'-28')
        when  substr(to_date(js_date),6,2)  in ('04','06','09','11') then concat(substr(to_date(js_date),1,7),'-30')
   end

 end as month_last,                       
concat(substr(js_date,1,7),'-01') as month_first
from mreport_poultry.DWU_QW_QW11_DD where op_day='$OP_DAY') t1 
LEFT JOIN
(select  case when lookup_code='1' then 'CHICHEN'
                                 when lookup_code='2' then 'DUCK'
                            else '-999' end prod_line
                            ,int(tag) tag
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                        AND language='ZHS'
)t2 on (t1.prod_line=t2.prod_line)"


###########################################################################################
## 建立临时表，用于存放每個月每个公司的投放量
TMP_DWU_BIRD_FORECAST_DD_2='tmp_dwu_bird_forecast_dd_2'
CREATE_TMP_DWU_BIRD_FORECAST_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_2(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --10实际投放量
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
## 转换数据投放量
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_2="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_2 PARTITION(op_day='$OP_DAY')
SELECT 
regexp_replace(substr(t3.js_d,1,7),'-',''),
t3.org_id,
t3.bus_type,
t3.meaning_desc,
10,
sum(t3.qty) as sum_qty

FROM 
(
SELECT 
t1.org_id,
t1.bus_type,
t1.meaning_desc,
t1.contractnumber,
t1.contract_date,
t1.qty,

t2.js_d,
t2.start_day,
t2.end_day
FROM 
(SELECT 
  org_id,
  bus_type,
  meaning_desc,
  contractnumber,
  contract_date,
  qty
 FROM mreport_poultry.dwu_qw_contract_dd where op_day='$OP_DAY') t1
LEFT JOIN 
(SELECT pith_no,start_day,end_day,js_d from $TMP_DWU_BIRD_FORECAST_DD_1 where op_day='$OP_DAY')t2
on t1.contractnumber=t2.pith_no
)t3
where t3.contract_date between t3.start_day and t3.end_day
GROUP BY
regexp_replace(substr(t3.js_d,1,7),'-',''),
t3.org_id,
t3.bus_type,
t3.meaning_desc,
10"


###########################################################################################
## 建立临时表，用于存放每個公司的实际回收量
TMP_DWU_BIRD_FORECAST_DD_3='tmp_dwu_bird_forecast_dd_3'
CREATE_TMP_DWU_BIRD_FORECAST_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_3(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --12实际回收量
  ,value                        string   --回收数量
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
## 转换数据实际回收量
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"  
INSERT_TMP_DWU_BIRD_FORECAST_DD_3="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_3 PARTITION(op_day='$OP_DAY')
SELECT 
regexp_replace(substr(t1.js_d,1,7),'-',''),
t2.org_id,
t2.bus_type,
t2.meaning_desc,
12,
sum(t1.killed_qty)
FROM
(SELECT
pith_no,
killed_qty,
to_date(js_date) as js_d
FROM mreport_poultry.dwu_qw_qw11_dd
where op_day='OP_DAY')t1
LEFT JOIN
(SELECT  
 org_id,
 bus_type,
 meaning_desc,
 contractnumber
 from mreport_poultry.DWU_QW_CONTRACT_DD where op_day='$OP_DAY')t2
ON 
(t1.pith_no=t2.contractnumber)
GROUP BY
regexp_replace(substr(t1.js_d,1,7),'-',''), 
t2.org_id,
t2.bustype,
t2.meaning_desc,
12"

###########################################################################################
## 建立临时表，用于存放每個公司的预计回收量
TMP_DWU_BIRD_FORECAST_DD_4='tmp_dwu_bird_forecast_dd_4'
CREATE_TMP_DWU_BIRD_FORECAST_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_4(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --11预计回收
  ,value                        string   --回收数量
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"
## 转换数据预计回收量
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"  
INSERT_TMP_DWU_BIRD_FORECAST_DD_4="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_4 PARTITION(op_day='$OP_DAY')
SELECT
   t1.month_id,
   t1.org_id,
   t1.bustype,
   t1.product_line,
   11,
   t1.value*t2.alve_rate
FROM 
(SELECT  
   month_id,                    
   org_id,                      
   bustype,
   product_line,  
   case product_line  
   when '鸡线' then '鸡'
   when '鸭线' then '鸭'
   else '缺省'
   end  as prod_type,  
   index_type,                  
   value from  $TMP_DWU_BIRD_FORECAST_DD_2  where op_day='OP_DAY')t1 
LEFT JOIN
 (SELECT org_id,alve_rate,kpi_type from mreport_poultry.dwu_qw_qw12_dd where op_day='$OP_DAY') t2
ON
(t1.prod_type=t2.kpi_type and t1.org_id=t2.org_id)"


###########################################################################################
## 建立临时表，用于存放每個公司的投放汇总金额
TMP_DWU_BIRD_FORECAST_DD_5='tmp_dwu_bird_forecast_dd_5'
CREATE_TMP_DWU_BIRD_FORECAST_DD_5="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_5(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --13投放汇总金额
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

##数据转换 投放汇总金额
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_5="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_5 PARTITION(op_day='$OP_DAY')
SELECT 
regexp_replace(substr(t3.js_d,1,7),'-',''),
t3.org_id,
t3.bus_type,
t3.meaning_desc,
13,
sum(t3.chicksalemoney*t3.qty)

FROM 
(
SELECT 
t1.org_id,
t1.bus_type,
t1.meaning_desc,
t1.contractnumber,
t1.contract_date,
t1.qty,
t1.chicksalemoney,
t2.js_d,
t2.start_day,
t2.end_day
FROM 
(SELECT 
  org_id,
  bus_type,
  meaning_desc,
  contractnumber,
  contract_date,
  qty,
  chicksalemoney
 FROM mreport_poultry.dwu_qw_contract_dd where op_day='$OP_DAY') t1
LEFT JOIN 
(SELECT pith_no,start_day,end_day,js_d from $TMP_DWU_BIRD_FORECAST_DD_1 where op_day='$OP_DAY')t2
on t1.contractnumber=t2.pith_no
)t3
where t3.contract_date between t3.start_day and t3.end_day
GROUP BY
regexp_replace(substr(t3.js_d,1,7),'-',''),
t3.org_id,
t3.bus_type,
t3.meaning_desc,
13"

###########################################################################################
## 建立临时表，用于存放每個公司的保本成本
TMP_DWU_BIRD_FORECAST_DD_6='tmp_dwu_bird_forecast_dd_6'
CREATE_TMP_DWU_BIRD_FORECAST_DD_6="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_6(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --14保本成本
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"
    
##数据转换 保本成本
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_6="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_6 PARTITION(op_day='$OP_DAY')
SELECT 
month_id,
org_id,
bustype,
product_line,
14,
null
FROM 
$TMP_DWU_BIRD_FORECAST_DD_2 where op_day='$OP_DAY'
"


###########################################################################################
## 建立临时表，用于存放每個公司的回收重量
TMP_DWU_BIRD_FORECAST_DD_7='tmp_dwu_bird_forecast_dd_7'
CREATE_TMP_DWU_BIRD_FORECAST_DD_7="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_7(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --15回收重量
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"


##数据转换 回收重量
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_7="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_7 PARTITION(op_day='$OP_DAY')
SELECT 
regexp_replace(substr(t1.js_d,1,7),'-',''),
t2.org_id,
t2.bus_type,
t2.meaning_desc,
15,
sum(t1.buy_weight)
FROM
(SELECT
pith_no,
buy_weight,
to_date(js_date) as js_d
FROM mreport_poultry.dwu_qw_qw11_dd
where op_day='OP_DAY')t1
LEFT JOIN
(SELECT  
 org_id,
 bus_type,
 meaning_desc,
 contractnumber
 from mreport_poultry.DWU_QW_CONTRACT_DD where op_day='$OP_DAY')t2
ON 
(t1.pith_no=t2.contractnumber)
GROUP BY
regexp_replace(substr(t1.js_d,1,7),'-',''), 
t2.org_id,
t2.bustype,
t2.meaning_desc,
15"

###########################################################################################
## 建立临时表，用于存放每個公司的回收金额
TMP_DWU_BIRD_FORECAST_DD_8='tmp_dwu_bird_forecast_dd_8'
CREATE_TMP_DWU_BIRD_FORECAST_DD_8="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_8(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --16回收金额
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换 回收金額
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_8="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_8 PARTITION(op_day='$OP_DAY')
SELECT 
regexp_replace(substr(t1.js_d,1,7),'-',''),
t2.org_id,
t2.bus_type,
t2.meaning_desc,
16,
sum(t1.amount)
FROM
(SELECT
pith_no,
amount,
to_date(js_date) as js_d
FROM mreport_poultry.dwu_qw_qw11_dd
where op_day='OP_DAY')t1
LEFT JOIN
(SELECT  
 org_id,
 bus_type,
 meaning_desc,
 contractnumber
 from mreport_poultry.DWU_QW_CONTRACT_DD where op_day='$OP_DAY')t2
ON 
(t1.pith_no=t2.contractnumber)
GROUP BY
regexp_replace(substr(t1.js_d,1,7),'-',''), 
t2.org_id,
t2.bustype,
t2.meaning_desc,
16"
###########################################################################################
## 建立临时表，用于存放每個公司的预估综合售价B
TMP_DWU_BIRD_FORECAST_DD_9='tmp_dwu_bird_forecast_dd_9'
CREATE_TMP_DWU_BIRD_FORECAST_DD_9="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_9(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --预估综合售价B17
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换 预估综合售价
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_9="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_9 PARTITION(op_day='$OP_DAY')
SELECT 
month_id,
org_id,
bustype,
product_line,
17,
null
FROM 
$TMP_DWU_BIRD_FORECAST_DD_2 where op_day='$OP_DAY'
"


###########################################################################################
## 建立临时表，用于存放每個公司的實際綜合售價B
TMP_DWU_BIRD_FORECAST_DD_10='tmp_dwu_bird_forecast_dd_10'
CREATE_TMP_DWU_BIRD_FORECAST_DD_10="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_10(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --實際綜合售價18
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换 实际综合售价b
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_10="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_10 PARTITION(op_day='$OP_DAY')
SELECT 
t1.month_id,
t1.org_id,
t1.bustype,
t1.product_line,
18,
t2.b_sale_price
FROM 
(SELECT * from $TMP_DWU_BIRD_FORECAST_DD_2 where op_day='$OP_DAY')t1
LEFT JOIN 
(SELECT 
  period_id,
  org_id,
  bus_type,
 case product_line 
 when '10' then '鸡线'
 when '20' then '鸭线'
 end as product_line_descr,
 b_sale_price 
 from mreport_poultry.DWU_CW_CW26_DD where op_day='$OP_DAY')t2
ON(t1.month_id=t2.period_id and t1.org_id=t2.org_id and t1.product_line=t2.product_line_descr)
"

###########################################################################################
## 建立临时表，用于存放每個公司的保底綜合售價
TMP_DWU_BIRD_FORECAST_DD_11='tmp_dwu_bird_forecast_dd_11'
CREATE_TMP_DWU_BIRD_FORECAST_DD_11="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_DD_11(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --業態
  ,product_line                 string   --產綫
  ,index_type                   string   --保底綜合售價19
  ,value                        string   --值
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC"


##数据转换保底綜合售價

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWU_BIRD_FORECAST_DD_11="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_DD_11 PARTITION(op_day='$OP_DAY')
SELECT 
t1.month_id,
t1.org_id,
t1.bustype,
t1.product_line,
19,
t2.baodi_sale
FROM 
(SELECT * from $TMP_DWU_BIRD_FORECAST_DD_2 where op_day='$OP_DAY')t1
LEFT JOIN 

(SELECT 
 org_id,
 period_id,
 bus_type,
 case product_line 
 when '10' then '鸡线'
 when '20' then '鸭线'
 end as product_line_descr,
(resource_cost+working_cost+parking_cost+fuel_cost+walter_cost+making_cost+period_fee+income_amount) as baodi_sale
 from mreport_poultry.dwu_cw_cw26_dd)t2
ON(t1.month_id=t2.period_id and t1.org_id=t2.org_id and t1.product_line=t2.product_line_descr)"



#创建最终报表样式
DMP_BIRD_FORECAST_DD='dmp_bird_forecast_dd'
CREATE_DMP_BIRD_FORECAST_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_FORECAST_DD(
 month_id                     string            --期间(月)   
,day_id                       string            --期间(日)
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
,index_type                   string            --指标定义类型
,index_type_val               string            --指标定义对应的值
,create_time                  string            --数据推送时间

)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE"


##数据转换 报表样式
#将2-13临时表全部连起

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_FORECAST_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_FORECAST_DD PARTITION(op_day='$OP_DAY')
SELECT 
t1.month_id,
$OP_DAY,
t3.level1_org_id, 
t3.level1_org_descr,
t3.level2_org_id,
t3.level2_org_descr,
t3.level3_org_id,
t3.level3_org_descr,
t3.level4_org_id,
t3.level4_org_descr,
t3.level5_org_id,
t3.level5_org_descr,
t3.level6_org_id,
t3.level6_org_descr,
null,
null,
t2.level1_businesstype_id,
t2.level1_businesstype_name,
t2.level2_businesstype_id,
t2.level2_businesstype_name,
t2.level3_businesstype_id,
t2.level3_businesstype_name,
t2.level4_businesstype_id,
t2.level4_businesstype_name,
t1.product_line as product_line_id,

t1.product_line_descr,
t1.index_type,
t1.value,
$CREATE_TIME
FROM 
(select * from $TMP_DWU_BIRD_FORECAST_DD_2 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_3 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_4 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_5 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_6 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_7 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_8 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_9 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_10 where op_day='$OP_DAY'
union all 
select * from $TMP_DWU_BIRD_FORECAST_DD_11 where op_day='$OP_DAY'
)t1

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
       level6_org_descr
 FROM mreport_global.dim_org_management)t3
 ON(t1.org_id=t3.org_id)"
 

 echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_1;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_1;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_2;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_2;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_3;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_3;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_4;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_4;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_5;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_5;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_6;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_6;
	$CREATE_TMP_DWU_BIRD_FORECAST_DD_7;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_7;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_8;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_8;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_9;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_9;
	$CREATE_TMP_DWU_BIRD_FORECAST_DD_10;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_10;
    $CREATE_TMP_DWU_BIRD_FORECAST_DD_11;
    $INSERT_TMP_DWU_BIRD_FORECAST_DD_11;
    $CREATE_DMP_BIRD_FORECAST_DD;
    $INSERT_DMP_BIRD_FORECAST_DD;
    "  -v