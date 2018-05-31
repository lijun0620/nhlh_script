#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_profit_fin_dd.sh                               
# 创建时间: 2018年04月19日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-利润完成-股份
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_profit_fin_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)
CURRENT_MONTH=$(date -d " -1 day" +%Y%m)

###########################################################################################
## 年考核利润
TMP_DMP_BIRD_PROFIT_FIN_MM_1='TMP_DMP_BIRD_PROFIT_FIN_MM_1'

CREATE_TMP_DMP_BIRD_PROFIT_FIN_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROFIT_FIN_MM_1(
     month_id                      string         --期间(月份)
    ,level5_org_id                 string         --组织5级(公司)
    ,level6_org_id                 string         --组织6级(OU)
    ,level4_businesstype_id        string         --业态4级
    ,production_line_id            string         --产线
    ,year_profit_amt          string              --年考核利润
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS orc
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取月考核利润>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROFIT_FIN_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROFIT_FIN_MM_1 PARTITION(op_month='$OP_MONTH')
select 
	  /*+ MAPJOIN(dim_month) */
	 t1.month_id              
    ,t2.level5_org_id          
    ,t2.level6_org_id          
    ,t2.level4_businesstype_id 
    ,t2.production_line_id     
	,sum(t2.feed_income_amt+t2.breed_income_amt
+t2.breed_vet_amt+t2.fost_income_amt+t2.tech_serv_amt
+t2.other_amt+t2.no_oper_income-t2.feed_cost_amt
-t2.breed_cost_amt-t2.breed_vet_sale_amt-t2.fost_cost_amt-t2.tax_etc_amt
-t2.total_amt-t2.impair_amt) year_profit_amt
from
  (select * from mreport_global.dim_month where month_id BETWEEN '201501' AND '$CURRENT_MONTH') t1
inner join 
  (select * from DMP_BIRD_PROFIT_COMP_MM where op_month='$OP_MONTH' 
  and month_id BETWEEN '201501' AND '$CURRENT_MONTH') t2  
  where   t1.month_id>=t2.month_id
  and substr(t2.month_id,1,4)=t1.year_id
  group by
     t1.month_id              
    ,t2.level5_org_id          
    ,t2.level6_org_id          
    ,t2.level4_businesstype_id 
    ,t2.production_line_id 
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PROFIT_FIN_DD='DMP_BIRD_PROFIT_FIN_DD'
CREATE_DMP_BIRD_PROFIT_FIN_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PROFIT_FIN_DD(
   month_id                    string    --期间(月)
  ,day_id                      string    --期间(日)
  ,level1_org_id               string    --组织1级(股份)  
  ,level1_org_descr            string    --组织1级(股份)  
  ,level2_org_id               string    --组织2级(片联)  
  ,level2_org_descr            string    --组织2级(片联)  
  ,level3_org_id               string    --组织3级(片区)  
  ,level3_org_descr            string    --组织3级(片区)  
  ,level4_org_id               string    --组织4级(小片)  
  ,level4_org_descr            string    --组织4级(小片)  
  ,level5_org_id               string    --组织5级(公司)  
  ,level5_org_descr            string    --组织5级(公司)  
  ,level6_org_id               string    --组织6级(OU)  
  ,level6_org_descr            string    --组织6级(OU)  
  ,level7_org_id               string    --组织7级(库存组织)
  ,level7_org_descr            string    --组织7级(库存组织)
  ,level1_businesstype_id      string    --业态1级
  ,level1_businesstype_name    string    --业态1级
  ,level2_businesstype_id      string    --业态2级
  ,level2_businesstype_name    string    --业态2级
  ,level3_businesstype_id      string    --业态3级
  ,level3_businesstype_name    string    --业态3级
  ,level4_businesstype_id      string    --业态4级
  ,level4_businesstype_name    string    --业态4级
  ,production_line_id          string    --产线ID
  ,production_line_descr       string    --产线名称
  ,month_profit_amt            string    --本月考核利润
  ,month_obj_amt               string    --本月总部预算
  ,month_budget_amt            string    --本月总部预算
  ,year_profit_amt             string    --本年考核利润总额
  ,year_obj_amt                string    --本年挑战目标
  ,year_budget_amt             string    --本年度总部预算总部预算      
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PROFIT_FIN_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_PROFIT_FIN_DD PARTITION(op_month='$OP_MONTH')
SELECT
        t1.month_id                  
		,t1.day_id                  
		,t1.level1_org_id           
		,t1.level1_org_descr        
		,t1.level2_org_id           
		,t1.level2_org_descr        
		,t1.level3_org_id           
		,t1.level3_org_descr        
		,t1.level4_org_id           
		,t1.level4_org_descr        
		,t1.level5_org_id           
		,t1.level5_org_descr        
		,t1.level6_org_id           
		,t1.level6_org_descr        
		,t1.level7_org_id           
		,t1.level7_org_descr        
		,t1.level1_businesstype_id  
		,t1.level1_businesstype_name
		,t1.level2_businesstype_id  
		,t1.level2_businesstype_name
		,t1.level3_businesstype_id  
		,t1.level3_businesstype_name
		,t1.level4_businesstype_id  
		,t1.level4_businesstype_name
		,t1.production_line_id      
		,t1.production_line_descr   
       ,round((t1.feed_income_amt+t1.breed_income_amt
+t1.breed_vet_amt+t1.fost_income_amt+t1.tech_serv_amt
+t1.other_amt+t1.no_oper_income-t1.feed_cost_amt
-t1.breed_cost_amt-t1.breed_vet_sale_amt-t1.fost_cost_amt-t1.tax_etc_amt
-t1.total_amt-t1.impair_amt),2)          --本月考核利润
       ,0 as month_obj_amt                    --本月总部预算
       ,0 as month_budget_amt                 --本月总部预算
       ,t2.year_profit_amt as year_profit_amt                  --本年考核利润总额
       ,0 as year_obj_amt                     --本年挑战目标
       ,0 as year_budget_amt                  --本年度总部预算总部预算   
       ,'$CREATE_TIME' as create_time         --创建时间
FROM
   (select * from DMP_BIRD_PROFIT_COMP_MM where op_month='$OP_MONTH') t1
inner join
   (select * from TMP_DMP_BIRD_PROFIT_FIN_MM_1 where op_month='$OP_MONTH')	t2 
 on  t1.month_id=t2.month_id and t1.level5_org_id=t2.level5_org_id 
  and  t2.level6_org_id=t1.level6_org_id and t1.level4_businesstype_id=t2.level4_businesstype_id
and t1. production_line_id=t2.production_line_id 
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
	$CREATE_TMP_DMP_BIRD_PROFIT_FIN_MM_1;
	$INSERT_TMP_DMP_BIRD_PROFIT_FIN_MM_1;
    $CREATE_DMP_BIRD_PROFIT_FIN_DD;
    $INSERT_DMP_BIRD_PROFIT_FIN_DD;
"  -v