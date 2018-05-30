#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_lose_est_dd.sh                               
# 创建时间: 2018年04月13日                                            
# 创 建 者: khz                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺盈亏预测表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_lose_est_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 获取出成率
TMP_DMP_BIRD_LOSE_EST_DD_1='TMP_DMP_BIRD_LOSE_EST_DD_1'

CREATE_TMP_DMP_BIRD_LOSE_EST_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_LOSE_EST_DD_1(
   month_id              string,
   pre_month_id          string,  --下一个月
   org_id                string,
   bus_type              string,
   product_line          string,
   primary_qty           string,  --数量
   buy_weight            string   --重量
   
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取禽旺放养测算信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_LOSE_EST_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_LOSE_EST_DD_1 PARTITION(op_day='$OP_DAY')
 select  
   t1.period_id,
   case when substr(t1.period_id,5,2)<12 
   then floor(substr(t1.period_id,1,6)+1) else 
   concat(floor(substr(t1.period_id,1,4)+1),'01') end pre_month_id,
   t1.org_id,
   '132020',
   t1.product_line,
   coalesce(t1.primary_qty,0),          --数量 TZ02011
   coalesce(t2.buy_weight,0)            --重量QW11016
 from 
 (
     select
       org_id,
       product_line,
       substr(period_id,1,6) period_id,
       sum(primary_quantity) primary_qty
   from
       dwu_tz_storage_transation02_dd
   where
       op_day='$OP_DAY' and (item_source='N' or item_source is null) and bus_type='132020'
   group by
       org_id,
       product_line,
       substr(period_id,1,6)     
 ) t1 inner join 
 (
  select 
       d2.org_id,
       case when d2.meaning='CHICHEN' then '10' when  d2.meaning='DUCK' then '20' else d2.meaning end  product_line,
       regexp_replace(substr(JS_DATE,1,7),'-','')  period_id,
       sum(buy_weight) buy_weight
from dwu_qw_qw11_dd d1 inner join   dwu_qw_contract_dd d2 on 
 d1.pith_no=d2.contractnumber   
where d1.op_day='$OP_DAY' and d2.op_day='$OP_DAY'
group by  d2.org_id,
       d2.meaning, regexp_replace(substr(JS_DATE,1,7),'-','')
 ) t2 on t1.org_id=t2.org_id
   and t1.period_id=t2.period_id and t1.product_line=t2.product_line
"

###########################################################################################
## 税率,预计吨副产品收入
TMP_DMP_BIRD_LOSE_EST_DD_2='TMP_DMP_BIRD_LOSE_EST_DD_2'

CREATE_TMP_DMP_BIRD_LOSE_EST_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_LOSE_EST_DD_2(
   period_id              string,
   org_id                string,
   bus_type              string,
   product_line          string,
   put_type_id           string,   --投放类型 
   chicksalemoney        string,   --数量
   alve_rate             string,   --存活率            
   avg_weight            string,   --平均只重
   second_input          string,   --副产品单位收入
   fee_price             string,   --吨费用
   coefficient_rate      string,   --折算率
   tax_rate              string    --税率
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取禽旺放养测算信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_LOSE_EST_DD_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_LOSE_EST_DD_2 PARTITION(op_day='$OP_DAY')
 SELECT
       /*+ MAPJOIN(ods_ebs_fnd_lookup_values) */
       t1.period_id,                         
       t1.org_id,                                     
       '132020',                                    
       t1.product_line,                            
       t1.put_type_id,                                         
       t1.qty,                         
       t2.alve_rate,                            
       t2.avg_weight,                          
       t3.secnd_input ,    
       t3.fee_price fee_price,    
       t4.meaning coefficient_rate,                             
       t5.meaning tax_rate 
       
   FROM
       (
           SELECT
               org_id,
               case when meaning='CHICHEN' then '10' when  meaning='DUCK' then '20' else meaning end  product_line,
               case when meaning='CHICHEN' then 'CHICKEN'  else  meaning end                 lookup_code,
               regexp_replace(SUBSTR(contract_date,1,10),'-','') period_id,
               SUM(coalesce(qty,0))                               qty, --qw03024
               CASE
                   WHEN guarantees_market = '保值'
                   THEN '1'
                   WHEN guarantees_market = '保底'
                   THEN '2'
                   WHEN guarantees_market = '市场'
                   THEN '3'
                   ELSE ''
               END put_type_id
           FROM
               dwu_qw_contract_dd
           WHERE
               op_day='$OP_DAY'
           GROUP BY
               org_id,
               meaning,
               guarantees_market,
               regexp_replace(SUBSTR(contract_date,1,10),'-','') )t1
   INNER JOIN mreport_global.ods_ebs_cux_3_gl_coop_account t6
    on t6.account_ou_id =t1.org_id 
   INNER JOIN
       (
           SELECT
               org_id,
               coalesce(alve_rate,0) alve_rate,
               coalesce(avg_weight,0) avg_weight,
			   case when  KPI_TYPE = '鸡' THEN '10'
                  WHEN KPI_TYPE = '鸭' then '20' 
                  end product_line
           FROM
               dwu_qw_qw12_dd
           WHERE
               op_day='$OP_DAY' )t2
   ON
       t6.org_id=t2.org_id and t2.product_line=t1.product_line
   left JOIN
       (
           SELECT
               org_id,
               regexp_replace(period_id,'-','') period_id,
               product_line,
               coalesce(secnd_input,0) secnd_input,  ---预计吨副产品费用
               (coalesce(ton_packing_fee,0) +coalesce(g_wip_fix,0) +coalesce(g_wip_chg,0) 
               +coalesce(g_water_elec,0)+coalesce(g_fuel,0) +coalesce(g_manual,0)  
                +coalesce(manage_fixed_fee,0)+coalesce(manage_chg_fee,0)
                +coalesce(sales_fixed_fee,0) +coalesce(sales_chg_fee,0)+coalesce(financial_fee,0))
				*coalesce(ton_freight_rate,1)  fee_price --预计吨费用        
           FROM
               DWU_CW_CW27_DD
           WHERE
               op_day='$OP_DAY' and bus_type='132020')t3
   ON
       t1.org_id=t3.org_id
   AND t1.product_line=t3.product_line
   AND SUBSTR(t1.period_id,1,6)=t3.period_id
   left JOIN
       mreport_global.ods_ebs_fnd_lookup_values t4
   ON
       t4.lookup_type='CUX_QW_FINISHED_PRODUCT_RATE'
   AND t4.language='ZHS'
   left JOIN
       mreport_global.ods_ebs_fnd_lookup_values t5
   ON t5.lookup_type='CUX_QW_CONVERSION RATE'
  AND t5.language='ZHS'
  AND t5.lookup_code=t1.lookup_code
"
###########################################################################################
## 税率,预计吨副产品收入
TMP_DMP_BIRD_LOSE_EST_DD_3='TMP_DMP_BIRD_LOSE_EST_DD_3'

CREATE_TMP_DMP_BIRD_LOSE_EST_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_LOSE_EST_DD_3(
   period_id                 string,
   org_id                    string,
   bus_type                  string,
   product_line              string,
   put_type_id               string,    --投放类型 
   pre_put_cnt               string,    --预计产量
   pre_sell_price            string,    --预计综合销售单价
   pre_material_price        string,    --预计原料成本单价
   pre_prouct_price          string,    --预计副产品销售额单价
   pre_all_cost_price        string,    --预计总成本单价
   pre_lr_price              string,    --预计吨利润
   pre_b_cost_price          string,    --预计保本投放成本单价
   avg_weight                string,    --预计只重
   primary_qty               string,    --入库主数量
   buy_weight                string,    --收购重量,
   next_month_price          string,    --1-10日预计综合售价
   level6_org_id             string     --6级组织
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取禽旺放养测算信息>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_LOSE_EST_DD_3="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_LOSE_EST_DD_3 PARTITION(op_day='$OP_DAY')
SELECT
	     t3.period_id
		,t3.org_id
        ,t3.bus_type
        ,case when t3.product_line ='10' then '1'
		      when t3.product_line='20' then '2'
			  else '-1' end
        ,t3.put_type_id		
        ,(t3.alve_rate/100*t3.avg_weight/2000*ROUND((t2.primary_qty/t2.buy_weight),2)) --预计产量
        ,coalesce(t5.month_price,0) AS pre_sell_price --预计综合销售单价
        ,((1-ROUND(t2.primary_qty/t2.buy_weight,2))*coefficient_rate*tax_rate/(1+tax_rate)/2000) --预计原料成本单价
        ,second_input --预计副产品销售额单价
        ,fee_price --预计总成本单价
        ,(coalesce(t5.month_price,0)+second_input-fee_price-(t3.chicksalemoney/2000*(1-ROUND(t2.primary_qty/t2.buy_weight,2))*coefficient_rate*tax_rate/(1+tax_rate)))  --预计吨利润
        ,(coalesce(t5.month_price,0)+second_input-fee_price)*ROUND((t2.primary_qty/t2.buy_weight),2)/( 1-ROUND(t2.primary_qty/t2.buy_weight,2))*coefficient_rate*tax_rate/(1+tax_rate)  -- 预计保本投放成本单价
        ,t3.avg_weight            --预计只重
        ,t2.primary_qty           --入库主数量
        ,t2.buy_weight            --收购重量
		,t5.next_month_price
		,t3.level6_org_id 
FROM
    (
        SELECT
            *
        FROM
            TMP_DMP_BIRD_LOSE_EST_DD_1
        WHERE
            op_day='$OP_DAY'
        AND bus_type='132020')t2
left join 
    (
        SELECT
            d1.* ,
            CASE
                WHEN d2.level6_org_id IS NULL
                THEN COALESCE(d3.level6_org_id,'-1')
                ELSE COALESCE(d2.level6_org_id,'-1')
            END AS level6_org_id
        FROM
            (
                SELECT
                    *
                FROM
                    TMP_DMP_BIRD_LOSE_EST_DD_2
                WHERE
                    op_day='$OP_DAY'
                AND bus_type='132020') d1
        LEFT JOIN
            mreport_global.dim_org_management d2
        ON
            d1.org_id=d2.org_id
        AND d2.attribute5='1'
        LEFT JOIN
            mreport_global.dim_org_management d3
        ON
            d1.org_id=d3.org_id
        AND d1.bus_type=d3.bus_type_id
        AND d3.attribute5='2') t3
ON
    SUBSTR(t3.period_id,1,6)=t2.pre_month_id
AND t2.org_id=t3.org_id
AND t3.product_line=t2.product_line
LEFT JOIN
    (
        SELECT
           org_code,
           type,
		   product_line_code,
		   regexp_replace(period_name,'-','') period_id,
		   avg(month_price) month_price,
		   avg(next_month_price) next_month_price
        FROM
            dwu_sg_sg03_dd
        WHERE
            op_day='$OP_DAY'
			--and type='132020'
		group by 
		   org_code,
		   type,
		   product_line_code,
		   regexp_replace(period_name,'-','')
	) t5
ON
    t5.org_code=t3.level6_org_id
AND T3.bus_type = T5.TYPE
AND t5.product_line_code=t2.product_line
and t5.period_id=t2.pre_month_id
	
"
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_LOSE_EST_DD='DMP_BIRD_LOSE_EST_DD'

CREATE_DMP_BIRD_LOSE_EST_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_LOSE_EST_DD(
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
  ,put_type_id                 string    --投放类型ID
  ,put_type_name               string    --投放类型名称
  ,month_put_cnt               string    --本月实际投放量
  ,month_put_cost_amt          string    --本月投放成本
  ,pre_put_cnt                 string    --预计产量
  ,pre_sell_amt                string    --预计综合销售额
  ,pre_material_cost           string    --预计原料成本
  ,pre_prouct_amt              string    --预计副产品销售额
  ,pre_all_cost_amt            string    --预计总成本
  ,pre_lr_amt                  string    --预计利润总额
  ,pre_b_cost_amt              string    --预计保本投放成本
  ,pre_b_sale_amt              string    --预计保本综合销售额
  ,pre_all_weigth_amt          string    --预计总重量
  ,pre_main_amt                string    --入库主数量
  ,pre_buy_amt                 string    --收购重量
  ,last_m_sale_amt             string    --上月综合销售额
  ,last_m_prod_amt             string    --上月综合产量
  ,sale_1_10_amt               string    --1_10预估综合销售额
  ,prod_1_10_amt               string    --1_10预估综合产量
  ,sale_11_20_amt              string    --11_20预估综合销售额
  ,prod_11_20_amt              string    --11_20预估综合产量
  ,sale_21_31_amt              string    --21_31预估综合销售额
  ,prod_21_31_amt              string    --21_31预估综合产量
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_LOSE_EST_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_LOSE_EST_DD PARTITION(op_day='$OP_DAY')
SELECT
        t1.month_id                           --month_id          
       ,t1.day_id                             --day_id
       ,t1.level1_org_id                      --组织1级
       ,t1.level1_org_descr                   --组织1级
       ,t1.level2_org_id                      --组织2级
       ,t1.level2_org_descr                   --组织2级
       ,t1.level3_org_id                      --组织3级
       ,t1.level3_org_descr                   --组织3级
       ,t1.level4_org_id                      --组织4级
       ,t1.level4_org_descr                   --组织4级
       ,t1.level5_org_id                      --组织5级
       ,t1.level5_org_descr                   --组织5级
       ,t1.level6_org_id                      --组织6级
       ,t1.level6_org_descr                   --组织6级
       ,t1.level7_org_id                      --组织7级
       ,t1.level7_org_descr                   --组织7级
       ,t5.level1_businesstype_id             --业态1级
       ,t5.level1_businesstype_name           --业态1级
       ,t5.level2_businesstype_id             --业态2级
       ,t5.level2_businesstype_name           --业态2级
       ,t5.level3_businesstype_id             --业态3级
       ,t5.level3_businesstype_name           --业态3级
       ,t5.level4_businesstype_id             --业态4级
       ,t5.level4_businesstype_name           --业态4级
       ,t1.production_line_id                 --产线ID
	   ,t1.production_line_descr              --产线名称
	   ,t1.put_type_id                        --投放类型ID
	   ,t1.put_type_descr                     --投放类型名称
	   ,t1.put_month_qty                      --本月实际投放量
	   ,t1.put_month_cost                     --本月投放成本
	   ,t1.put_month_qty*t2.pre_put_cnt       --预计产量
	   ,t2.pre_sell_price*t1.put_month_qty*t2.pre_put_cnt --预计综合营业额
	   ,t1.put_month_qty*t2.pre_put_cnt*t2.pre_material_price*t1.put_month_cost --预计原料成本
	   ,t2.pre_put_cnt*t1.put_month_qty*t2.pre_prouct_price --副产品销售额
	   ,t2.pre_put_cnt*t1.put_month_qty*t2.pre_all_cost_price    --预计总成本
	   ,t2.pre_lr_price*t2.pre_put_cnt*t1.put_month_qty  --预计利润总额
       ,t2.pre_put_cnt*t1.put_month_qty*t2.pre_b_cost_price  --预计保本投放成本
	   ,t2.pre_b_cost_price*t2.pre_put_cnt*t1.put_month_qty  --预计保本综合销售额
	   ,t2.avg_weight*put_month_qty                         --预计总重量
	   ,t2.primary_qty                                      --入库主数量
	   ,t2.buy_weight                                       --收购重量
	   ,t3.last_m_sale_amt*t1.put_month_qty*t2.pre_put_cnt  --上月综合销售额
	   ,t1.put_month_qty*t2.pre_put_cnt as last_m_prod_amt  --上月综合产量
	   ,t2.next_month_price*t2.pre_put_cnt*t1.put_month_qty   --1_10预估综合销售额
	   ,t2.pre_put_cnt*t1.put_month_qty                  --1_10预估综合销售量
	   ,t2.next_month_price*t2.pre_put_cnt*t1.put_month_qty   --11_20预估综合销售额
	   ,t2.pre_put_cnt*t1.put_month_qty                  --11_20预估综合销售量
	   ,t2.next_month_price*t2.pre_put_cnt*t1.put_month_qty   --21_31预估综合销售额
	   ,t2.pre_put_cnt*t1.put_month_qty                  --21_31预估综合销售量
	  ,'$CREATE_TIME' as create_time        --创建时间 
FROM
  (
        SELECT
         month_id ,
         day_id ,
         level1_org_id ,
         level1_org_descr ,
         level2_org_id ,
         level2_org_descr ,
         level3_org_id ,
         level3_org_descr ,
         level4_org_id ,
         level4_org_descr ,
         level5_org_id ,
         level5_org_descr ,
         level6_org_id ,
         level6_org_descr ,
         level7_org_id ,
         level7_org_descr ,
         production_line_id ,
         production_line_descr ,
         put_type_id ,
         put_type_descr ,
         SUM(put_month_qty) put_month_qty,
         SUM(put_month_cost) put_month_cost
      FROM
         dmp_bird_put_dd 
      WHERE
         op_day='$OP_DAY'
      GROUP BY
         month_id ,
         day_id ,
         level1_org_id ,
         level1_org_descr ,
         level2_org_id ,
         level2_org_descr ,
         level3_org_id ,
         level3_org_descr ,
         level4_org_id ,
         level4_org_descr ,
         level5_org_id ,
         level5_org_descr ,
         level6_org_id ,
         level6_org_descr ,
         level7_org_id ,
         level7_org_descr ,
         production_line_id ,
         production_line_descr ,
         put_type_id ,
         put_type_descr
  ) t1
 left  join (
	select * from TMP_DMP_BIRD_LOSE_EST_DD_3 where op_day='$OP_DAY') t2
 on t1.production_line_id=t2.product_line and t1.put_type_id=t2.put_type_id
 and t1.day_id=t2.period_id and t1.level6_org_id=t2.level6_org_id 
LEFT JOIN (
   SELECT
       d1.org_id,
       case when d1.product_line='10' then '1'
	        when d1.product_line='20' then '2'
			else '-1' end product_line,
       d1.creation_date day_id,
       sum(coalesce(amount_b,0)) last_m_sale_amt
   FROM
       DWU_CW_CW31_DD d1
   INNER JOIN
       mreport_global.dwu_dim_material_new d2
   ON
       d1.inv_org_id=d2.inv_org_id
   AND d2.inventory_item_code=d1.item_id
   AND d2.bus_type='132020'
   AND d1.op_day='$OP_DAY'
   group by
       d1.org_id,
       d1.product_line,
       d1.creation_date
) t3 on t2.org_id=t3.org_id 
    and t3.product_line=t2.product_line
	and t1.day_id=t3.day_id
 LEFT JOIN
    (SELECT * FROM mreport_global.dim_org_businesstype
        WHERE level4_businesstype_id=132020) t5
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_LOSE_EST_DD_1;
    $INSERT_TMP_DMP_BIRD_LOSE_EST_DD_1;
	$CREATE_TMP_DMP_BIRD_LOSE_EST_DD_2;
    $INSERT_TMP_DMP_BIRD_LOSE_EST_DD_2;
	$CREATE_TMP_DMP_BIRD_LOSE_EST_DD_3;
    $INSERT_TMP_DMP_BIRD_LOSE_EST_DD_3;
    $CREATE_DMP_BIRD_LOSE_EST_DD;
    $INSERT_DMP_BIRD_LOSE_EST_DD;
"  -v