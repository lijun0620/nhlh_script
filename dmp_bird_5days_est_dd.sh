#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_5days_est_dd.sh                               
# 创建时间: 2018年04月19日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月中测算—五日测算
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_5days_est_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)
CURRENT_DAY=$(date -d " -0 day" +%Y%m%d)

###########################################################################################
## 将CW19数据按天和组织，业态汇总
## 变量声明
TMP_DMP_BIRD_5DAYS_EST_DD_1='TMP_DMP_BIRD_5DAYS_EST_DD_1'

CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_5DAYS_EST_DD_1(
   org_id          string,           --ou_id
   bus_type        string,           --业态
   org_code        string,           
   ordered_qty     string,           --销量
   loc_income      string,           --总收入
   sales_profits   string,           --利润
   during_cost     string,           --期间费用
   sales_income    string,           --销售收入
   cost_amount_t_loc string,         --总成本
   period_id       string,           
   product_line    string            --产线
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_5DAYS_EST_DD_1 PARTITION(op_day='$OP_DAY')
SELECT
      /*+ MAPJOIN(dim_day) */
    d2.org_id,
    d2.bus_type,
    d2.org_code,
    SUM(coalesce(d2.ordered_qty,0))                         ordered_qty, --销量
    SUM(coalesce(d2.income,0))                              loc_income, --收入
    SUM(coalesce(d2.income,0)-coalesce(d2.cost_amount_t,0)) sales_profits, --利润
    SUM(coalesce(d2.selling_expense_fixed,0)+coalesce(d2.selling_expense_change,0) +coalesce(d2.fin_expense,0)+coalesce(d2.admini_expense,0)) during_cost, --期间费用
    SUM(0.00)                                         sales_income, --销售收入(元)
    SUM(coalesce(d2.cost_amount_t,0))                 cost_amount_t_loc,
    d1.day_id,
    d2.product_line
FROM
    (
        SELECT
            *
        FROM
            mreport_global.dim_day
        WHERE
            day_id BETWEEN '20170101' AND '$CURRENT_DAY')d1
 JOIN
    (
        SELECT
            dm1.*
            ,dc2.org_code
        FROM
            dmd_fin_exps_profits dm1
        inner join mreport_global.dwu_dim_material_new dm2
        on dm1.inventory_item_id =dm2.inventory_item_id
		and dm1.inv_org_id=dm2.inv_org_id
        AND dm2.material_segment5_desc LIKE '%雏%'
        AND dm1.period_id BETWEEN '20170101' AND '$CURRENT_DAY'
        AND dm1.bus_type IN('132011','132012')
        AND dm1.currency_type='3' 
       INNER JOIN (
         select 
              org_id,
              level6_org_id org_code
         from mreport_global.dim_org_management  
         group by 
             org_id,
             level6_org_id 
 ) dc2 
       on dm1.org_id = dc2.org_id 
)d2
WHERE
    d1.day_id>=d2.period_id
AND SUBSTR(d2.period_id,1,6)=d1.month_id
GROUP BY
    d2.org_id,
    d2.org_code,
    d2.bus_type,
    d1.day_id,
    d2.product_line
;
"

###########################################################################################
## 将ZQ08销量
## 变量声明
TMP_DMP_BIRD_5DAYS_EST_DD_2='TMP_DMP_BIRD_5DAYS_EST_DD_2'

CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_5DAYS_EST_DD_2(
   org_id          string,           --ou_id
   bus_type        string,           --业态
   sales_qty       string,            
   period_id       string,
   org_code        string,
   product_line    string         --产线
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_5DAYS_EST_DD_2 PARTITION(op_day='$OP_DAY')
SELECT
    /*+ MAPJOIN(dim_day) */
    d2.org_id,
    d2.bus_type,
    SUM(coalesce(plan_cubs,0)),
    d1.day_id,
    d2.org_code,
    d2.product_line        --产线
FROM
    (
        SELECT
            *
        FROM
            mreport_global.dim_day
        WHERE
            day_id BETWEEN '20170101' AND '$CURRENT_DAY')d1
JOIN
    (
        SELECT
            regexp_replace(substr(hatching_date,1,10),'-','') day_id,
            bus_type,
            org_id,
            plan_cubs,
            org_code,
            line_type product_line
        FROM
            dwu_zq_zq08_dd dm1
        inner join mreport_global.dim_material dm2
        on dm1.product_item_code =dm2.item_id
        and dm1.bus_type IN('132011','132012')
        AND dm2.level5_material_descr LIKE '%雏%'
        and dm1.op_day='$OP_DAY'
        AND regexp_replace(substr(dm1.hatching_date,1,10),'-','') BETWEEN '20170101' AND '$CURRENT_DAY'
    )d2
WHERE
    d1.day_id>=d2.day_id
AND substr(d2.day_id,1,6)=d1.month_id
GROUP BY
    d2.org_id,
    d2.bus_type,
    d1.day_id,
    d2.org_code,
    d2.product_line
"
###########################################################################################
## 应收账款余额计算
## 变量声明
TMP_DMP_BIRD_5DAYS_EST_DD_4='TMP_DMP_BIRD_5DAYS_EST_DD_4'

CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_5DAYS_EST_DD_4(
   period_id                     string     --总账日期
  ,org_id                        string     --组织ID
  ,bus_type                      string     --业态 
  ,next_month_id                 string     --下月id
  ,inventory_amt                 string     --存货余额(母币)
  ,product_line                  string     --产线
  
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_4="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_5DAYS_EST_DD_4 PARTITION(op_day='$OP_DAY')
    select
        dw1.period_id,
        dw1.org_id,
        dw1.bus_type,
        case when substr(dw1.period_id,5,2)<12 
          then floor(substr(dw1.period_id,1,6)+1) else 
           concat(floor(substr(dw1.period_id,1,4)+1),'01') end next_month_id,
       sum(coalesce(dw1.month_qty,0) *coalesce(dw2.cost_amount_t,0)) inventory_amt,
       dw1.product_line
    from
        (
            select
                *
            from
                dwu_qtz_begin_end_inventory_dd
            where
                op_day='$OP_DAY') dw1
    inner join
      dwu_finance_cost_pric dw2
    on
        dw1.org_id=dw2.org_id
    and dw1.material_id =dw2.material_item_id
    and substr(dw1.period_id,1,6)=dw2.period_id
    and dw1.bus_type in ('132011','132012')
    inner join mreport_global.dim_material dw3
    on dw1.material_id =dw3.item_id
    and dw3.level5_material_descr LIKE '%雏%'
    group by  
        dw1.org_id,
        dw1.bus_type,
        dw1.period_id,
        dw1.product_line        
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_5DAYS_EST_DD='DMP_BIRD_5DAYS_EST_DD'

CREATE_DMP_BIRD_5DAYS_EST_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_5DAYS_EST_DD(
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
  ,trade_type_id               string    --交易关系
  ,trade_type_descr            string    --描述
  ,currency_type_id            string    --币种
  ,currency_type_descr         string    --币种
  ,sales_qty                   string   --销量(只)
  ,sales_amt                   string   --销售金额(元)
  ,sales_profits               string   --销售利润(元)
  ,sales_income                string   --销售收入(元)
  ,sales_cost                  string   --销售成本(元)
  ,during_cost                 string   --期间费用(元)
  ,ar_amt                      string   --应收账款(元)
  ,inventory_amt               string   --存货余额(元)
  ,pre_sales_qty               string   --预算_本月销量(只)
  ,pre_kpi_profits             string   --预算_本月考核利润(元)
  ,pre_sales_amt               string   --预算_销售金额(元)
  ,pre_sales_cost              string   --预算_销售成本(元)
  ,pre_sales_profits           string   --预算_销售利润(元)
  ,pre_during_cost             string   --预算_期间费用(元)
  ,pre_sales_income            string   --预算_销售收入(元)
  ,cash_amt                    string   --现金(元)
  ,platform_balance            string   --资金平台存款
  ,offline_acct_balance        string   --离线账户存款                                      
  ,other_balance               string   --其他货币资金
 ,create_time                  string   --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_5DAYS_EST_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_5DAYS_EST_DD PARTITION(op_day='$OP_DAY')
  SELECT
        substr(t1.period_id,1,6)               --month_id
       ,t1.period_id       as  day_id  
       ,case when t6.level1_org_id    is null then coalesce(t7.level1_org_id,'-1')      else coalesce(t6.level1_org_id,'-1')       end as level1_org_id      --一级组织编码
       ,case when t6.level1_org_descr is null then coalesce(t7.level1_org_descr,'缺失') else coalesce(t6.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
       ,case when t6.level2_org_id    is null then coalesce(t7.level2_org_id,'-1')      else coalesce(t6.level2_org_id,'-1')       end as level2_org_id      --二级组织编码
       ,case when t6.level2_org_descr is null then coalesce(t7.level2_org_descr,'缺失') else coalesce(t6.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
       ,case when t6.level3_org_id    is null then coalesce(t7.level3_org_id,'-1')      else coalesce(t6.level3_org_id,'-1')       end as level3_org_id      --三级组织编码
       ,case when t6.level3_org_descr is null then coalesce(t7.level3_org_descr,'缺失') else coalesce(t6.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
       ,case when t6.level4_org_id    is null then coalesce(t7.level4_org_id,'-1')      else coalesce(t6.level4_org_id,'-1')       end as level4_org_id      --四级组织编码
       ,case when t6.level4_org_descr is null then coalesce(t7.level4_org_descr,'缺失') else coalesce(t6.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
       ,case when t6.level5_org_id    is null then coalesce(t7.level5_org_id,'-1')      else coalesce(t6.level5_org_id,'-1')       end as level5_org_id      --五级组织编码
       ,case when t6.level5_org_descr is null then coalesce(t7.level5_org_descr,'缺失') else coalesce(t6.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
       ,case when t6.level6_org_id    is null then coalesce(t7.level6_org_id,'-1')      else coalesce(t6.level6_org_id,'-1')       end as level6_org_id      --六级组织编码
       ,case when t6.level6_org_descr is null then coalesce(t7.level6_org_descr,'缺失') else coalesce(t6.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
       ,'' as level7_org_id                   --组织7级
       ,'' as level7_org_descr                --组织7级
       ,t5.level1_businesstype_id             --业态1级
       ,t5.level1_businesstype_name           --业态1级
       ,t5.level2_businesstype_id             --业态2级
       ,t5.level2_businesstype_name           --业态2级
       ,t5.level3_businesstype_id             --业态3级
       ,t5.level3_businesstype_name           --业态3级
       ,t5.level4_businesstype_id             --业态4级
       ,t5.level4_businesstype_name           --业态4级
       ,case when t1.product_line='10' then '1'
             when t1.product_line='20' then '2'
             else '-1' end              as product_line                    --产线ID
       ,case when t1.product_line='10' then '鸡线'
             when t1.product_line='20' then '鸭线' 
             else '缺省' end as production_line_descr           --产线名称
       ,''                                    --交易关系
       ,'' as trade_type_descr                --描述
       ,'3' as currency_type_id               --币种
       ,'母币' as currency_type_descr         --币种
       ,t1.ordered_qty  sales_qty             --销售数量(只)
       ,t1.loc_income      sales_amt          --总收入
       ,t1.sales_profits   sales_profits      --利润 
       ,t1.cost_amount_t_loc sales_cost       --销售成本
       ,t1.during_cost                        --期间费用(元)
       ,t1.sales_income                       --销售主营收入
       ,t3.ar_amt                             --应收账余额                 
       ,t4.inventory_amt                      --存货余额（）
       ,t2.sales_qty                          -- 预算_本月销量(只)    
       ,0 as pre_kpi_profits                  --预算_本月考核利润(元)
       ,0 as pre_sales_amt                    --预算_销售金额(元)
       ,0 as pre_sales_cost                   --预算_销售成本(元)
       ,0 as pre_sales_profits                --预算_销售利润(元)
       ,0 as pre_during_cost                  --预算_期间费用(元)
       ,0 as pre_sales_income                 --预算_销售收入(元)
       ,t8.currency_bal                       --现金(元) 
       ,t8.currency_bal                       --资金平台存款
       ,t8.currency_bal                       --离线账户存款 
       ,t8.currency_bal                       --其他货币资金
       ,'$CREATE_TIME'                        --创建时间
from
(select * from TMP_DMP_BIRD_5DAYS_EST_DD_1  where op_day='$OP_DAY') t1
LEFT JOIN(
    SELECT
        *
    FROM
        TMP_DMP_BIRD_5DAYS_EST_DD_2
    WHERE op_day='$OP_DAY'
)t2 on t1.org_id=t2.org_id and t1.bus_type=t2.bus_type and t1.period_id=t2.period_id
and t1.product_line=t2.product_line
LEFT JOIN (
   SELECT
       day_id period_id,
       level6_org_id,
       level4_businesstype_id,
       SUM(ar_end_amt) ar_amt,
       case when production_line_id='1' then '10'
       when production_line_id='2' then '20'
       else production_line_id end product_line
   FROM
       dwp_bird_ar_dd
   WHERE
       op_day='$OP_DAY'
   AND currency_id='3'
   AND level4_businesstype_id in ('132011','132012')
   GROUP BY
       level6_org_id,
       level4_businesstype_id,
       day_id,
       production_line_id
)t3 on t2.bus_type=t3.level4_businesstype_id and t3.level6_org_id=t2.org_code 
and t2.period_id=t3.period_id  and t1.product_line=t3.product_line
LEFT JOIN (
  select * from TMP_DMP_BIRD_5DAYS_EST_DD_4 where op_day='$OP_DAY' 
) t4 on t4.next_month_id=substr(t1.period_id,1,6) 
     and t4.org_id=t1.org_id and t1.bus_type=t4.bus_type     
LEFT JOIN
    (
        SELECT * FROM mreport_global.dim_org_businesstype
        WHERE  level4_businesstype_name IS NOT NULL) t5
     ON  (t1.bus_type=t5.level4_businesstype_id)
left join mreport_global.dim_org_management t6 
     on t1.org_id=t6.org_id  
     and t6.attribute5='1'
left join mreport_global.dim_org_management t7 
     on t1.org_id=t7.org_id 
     and t1.bus_type=t7.bus_type_id 
     and t7.attribute5='2'
left join (
   SELECT
       CASE
           WHEN SUBSTR(regexp_replace(period_name,'-',''),5,2)<12
           THEN floor(regexp_replace(period_name,'-','')+1)
           ELSE concat(floor(SUBSTR(period_name,1,4)+1),'01')
       END next_period_id,
       period_name,
       short_code org_code,
       round(sum(coalesce(currency_bal,0)),2) currency_bal
   FROM
       DWU_CW_BANK_ACCOUNT_DD
   WHERE
      op_day='$OP_DAY' and SUBSTR(regexp_replace(period_name,'-',''),5,2)<=12
	  group by 
	  period_name,
	  short_code  	    
) t8 on substr(t1.period_id,1,6)=t8.next_period_id 
      and t8.org_code=t1.org_code
"
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_1;
    $INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_1;
    $CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_2;
    $INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_2;
    $CREATE_TMP_DMP_BIRD_5DAYS_EST_DD_4;
    $INSERT_TMP_DMP_BIRD_5DAYS_EST_DD_4;
    $CREATE_DMP_BIRD_5DAYS_EST_DD;
    $INSERT_DMP_BIRD_5DAYS_EST_DD;
"  -v
