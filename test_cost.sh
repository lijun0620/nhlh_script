OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_ar_turn_dd.sh 20180101"
    exit 1
fi


## 建立临时表，用于存放每個公司的保本成本所有字段清单
TMP_DWU_BIRD_FORECAST_MM_10='tmp_dwu_bird_forecast_mm_10'
CREATE_TMP_DWU_BIRD_FORECAST_MM_10="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_10(
   month_id                     string   --月份
  ,org_id                       string   --公司Id
  ,bustype                      string   --业态
  ,product_line                 string   --產綫
  ,buy_weight                   string   --回收重量
  ,breaking_cost                string   --保底售价

  ,during_cost                  string   --期间费用
  ,realing_prince_1             string   --实际售价1
  ,realing_prince_2            string   --实际售价2
  
  ,inner_qty_1                 string  --产量1
  ,inner_qty_2                 string  --产量2
  ,jin_cost                    string  --进项税
  ,fu_cost                     string  --副产品收入  
  ,rate                        string  --税率
)  
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"
INSERT_TMP_DWU_BIRD_FORECAST_MM_10="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_10 PARTITION(op_month='$OP_MONTH')
SELECT 
tmp_2.month_id,
tmp_2.org_id,
tmp_2.product_line,
tmp_2.bus_type,
tmp_2.buy_weight,
tmp_2.breaking_cost,
tmp_2.during_cost,
tmp_2.realing_prince_1,
tmp_2.realing_prince_2,
tmp_2.inner_qty_1,
tmp_2.inner_qty_2,
tmp_2.jin_cost,
tmp_2.fu_cost,
t2.rate
FROM
(SELECT 
  tmp.month_id as month_id,
  tmp.org_id as org_id,
  tmp.product_line as product_line,
  tmp.bus_type as bus_type,
  sum(tmp.buy_weight) as buy_weight,
  sum(tmp.breaking_cost) as breaking_cost,
  sum(tmp.during_cost) as during_cost,
  sum(tmp.realing_prince_1) as realing_prince_1,
  sum(tmp.realing_prince_2) as realing_prince_2,
  sum(tmp.inner_qty_1) as inner_qty_1,
  sum(tmp.inner_qty_2) as inner_qty_2,
  sum(tmp.jin_cost) as jin_cost,
  sum(tmp.fu_cost) as fu_cost
  FROM
(SELECT
   month_id as month_id,
   org_id as org_id,
   product_line as product_line,
   bustype as bus_type,
   buy_weight as buy_weight,
   0 as breaking_cost,
   0 as during_cost,
   0 as fu_cost,
   0 as realing_prince_1,
   0 as realing_prince_2,
   0 as inner_qty_1,
   0 as inner_qty_2,
   0 as jin_cost
FROM
 TMP_DWU_BIRD_FORECAST_MM_1 where op_month='$OP_MONTH'

UNION ALL
SELECT 
   concat(substr(period_id,1,4),'-',substr(period_id,5,2)) as month_id,
   org_id as org_id,
   case product_line when '10' then '鸡线'
      when '20' then '鸭线'
   end as product_line,	  
   '132020' as bus_type,
   0 as buy_weight,
   0 as breaking_cost,
   sum(coalesce(selling_expense_fixed,0)+coalesce(selling_expense_change,0)+coalesce(fin_expense,0)+coalesce(admini_expense,0)) as during_cost,
   0 as fu_cost,
 
   0 as realing_prince_1,
   0 as realing_prince_2,
   0 as inner_qty_1,
   0 as inner_qty_2,
   sum(nvl(cost_amount17,0)) as jin_cost
   
  
   from DMD_FIN_EXPS_PROFITS where op_month='$OP_MONTH' and currency_type='3'
   GROUP BY
    substr(period_id,1,6),
   org_id ,
   case product_line when '10' then '鸡线'
      when '20' then '鸭线'
   end,	  
   '132020'
   
 UNION ALL
 SELECT 
   month_id as month_id,
   org_id as org_id,
   '132020' as bus_type,
   product_line as product_line,
   0 as buy_weight,
   break_even_price  as breaking_cost,
   0 as during_cost,
   fu_cost,
 
   0 as realing_prince_1,
   0 as realing_prince_2,
   0 as inner_qty_1,
   0 as inner_qty_2,
   0 as jin_cost
FROM
   TMP_DWU_BIRD_FORECAST_MM_5 where op_month='$OP_MONTH'
  
   UNION ALL
   SELECT 
   month_id,
   org_id,
   bustype as bus_type,
   product_line,
   0 as buy_weight,
   0 as breaking_cost,
   0 as during_cost,
   0 as fu_cost,
   realing_prince_1,
   realing_prince_2,
   0 as inner_qty_1,
   0 as inner_qty_2,
   0 as jin_cost
   FROM TMP_DWU_BIRD_FORECAST_MM_4 where op_month='$OP_MONTH'
   UNION ALL
   SELECT 
   month_id,
   org_id,
   bustype as bus_type,
   product_line,
   0 as buy_weight,
   0 as breaking_cost,
   0 as during_cost,
   0 as fu_cost,
   0 as realing_prince_1,
   0 as realing_prince_2,
   inner_qty_1,
   inner_qty_2,
   0 as jin_cost
   FROM TMP_DWU_BIRD_FORECAST_MM_8 where op_month='$OP_MONTH'
 )tmp
 GROUP BY
  tmp.month_id,
  tmp.org_id,
  tmp.product_line,
  tmp.bus_type 
 )tmp_2
 LEFT JOIN 
 (SELECT 1-meaning rate
               FROM mreport_global.ods_ebs_fnd_lookup_values
              WHERE lookup_type='CUX_QW_FINISHED_PRODUCT_RATE'
                AND language='ZHS') t2
    ON (1=1)

"

TMP_DWU_BIRD_FORECAST_MM_11='tmp_dwu_bird_forecast_mm_11'
CREATE_TMP_DWU_BIRD_FORECAST_MM_11="
CREATE TABLE IF NOT EXISTS $TMP_DWU_BIRD_FORECAST_MM_11(
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


INSERT_TMP_DWU_BIRD_FORECAST_MM_11="
INSERT OVERWRITE TABLE $TMP_DWU_BIRD_FORECAST_MM_11 PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                             --期间(月)
       ,t1.org_id
       ,t1.bustype
       ,t1.product_line                        --产线
       ,case when t1.buy_weight!=0 and t1.month_id <substr(CURRENT_DATE,1,7) and t4.level6_org_id is not null then t1.real_selling_price_2+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_2/t1.buy_weight)/2000
      when t1.buy_weight!=0 and t1.month_id =substr(CURRENT_DATE,1,7) and t4.level6_org_id is not null and date_sub(CURRENT_DATE,1)<date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  t1.real_selling_price_1+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_1/t1.buy_weight)/2000
      when t1.buy_weight!=0 and t1.month_id =substr(CURRENT_DATE,1,7) and t4.level6_org_id is not null and date_sub(CURRENT_DATE,1)=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  t1.real_selling_price_2+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_2/t1.buy_weight)/2000
	  when t1.buy_weight!=0 and t1.month_id <substr(CURRENT_DATE,1,7) and t4.level6_org_id is null then t1.real_selling_price_2+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_2/t1.buy_weight)/t1.rate/2000
      when t1.buy_weight!=0 and t1.month_id =substr(CURRENT_DATE,1,7) and t4.level6_org_id is null and date_sub(CURRENT_DATE,1)<date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  t1.real_selling_price_1+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_1/t1.buy_weight)/t1.rate/2000
      when t1.buy_weight!=0 and t1.month_id =substr(CURRENT_DATE,1,7) and t4.level6_org_id is null and date_sub(CURRENT_DATE,1)=date_sub(concat(substr('$next_month_first_day',1,4),'-', substr('$next_month_first_day',5,2),'-',substr('$next_month_first_day',7,2)),1) then  t1.real_selling_price_2+t1.fu_cost+jin_cost-(t1.breaking_cost-t1.fu_cost+t1.during_cost)*(t1.inner_qty_2/t1.buy_weight)/t1.rate/2000
	  else null
 end as cost_save
  FROM (SELECT month_id as month_id,
               ,org_id as org_id
			   ,bustype as bustype
               ,product_line as product_line
               ,buy_weight    as buy_weight              
               ,breaking_cost as breaking_cost              --保底售价

               ,during_cost as during_cost                   --期间费用
               ,realing_prince_1  as real_selling_price_1    --实际售价1
               ,realing_prince_2  as realing_prince_2        --实际售价2
  
               ,inner_qty_1 as inner_qty_1                   --产量1
               ,inner_qty_2  as inner_qty_2                  --产量2
               ,jin_cost  as jin_cost                        --进项税
               ,fu_cost   as fu_cost                         --副产品收入  
               ,rate                           --税率
          FROM $TMP_DMP_BIRD_KEEP_PRICE_DD_1
         WHERE op_month='$OP_MONTH') t1
 
  LEFT JOIN (SELECT level1_org_id
                    ,level1_org_descr
                    ,level2_org_id
                    ,level2_org_descr
                    ,level3_org_id
                    ,level3_org_descr
                    ,level4_org_id
                    ,level4_org_descr
                    ,level5_org_id
                    ,level5_org_descr
                    ,level6_org_id
                    ,level6_org_descr
                    ,org_id
               FROM mreport_global.dim_org_management
              WHERE org_id is not null
              GROUP BY level1_org_id
                    ,level1_org_descr
                    ,level2_org_id
                    ,level2_org_descr
                    ,level3_org_id
                    ,level3_org_descr
                    ,level4_org_id
                    ,level4_org_descr
                    ,level5_org_id
                    ,level5_org_descr
                    ,level6_org_id
                    ,level6_org_descr
                    ,org_id) t3
    ON (t1.org_id=t3.org_id)
  LEFT JOIN (SELECT a2.level6_org_id
               FROM (SELECT lookup_code inv_org_code,
                            meaning org_name
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='BWP_TAX_RATE_CONVERSION'
                        AND language='ZHS') a1
              INNER JOIN (SELECT inv_org_code,
                                 level6_org_id
                            FROM mreport_global.dim_org_inv_management
                           WHERE inv_org_code is not null) a2
                 ON (a1.inv_org_code=a2.inv_org_code)
             GROUP BY a2.level6_org_id) t4
    ON (t3.level6_org_id=t4.level6_org_id)
  WHERE t3.level2_org_id NOT IN('1015')
  
  "
  hive -e "
    use mreport_poultry;
	
	$CREATE_TMP_DWU_BIRD_FORECAST_MM_10;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_10;
	$CREATE_TMP_DWU_BIRD_FORECAST_MM_11;
    $INSERT_TMP_DWU_BIRD_FORECAST_MM_11;
	
    "  -v
	
	
	