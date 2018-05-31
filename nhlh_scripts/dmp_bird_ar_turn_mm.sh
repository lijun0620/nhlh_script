
######################################################################
#                                                                    
# 程    序: dmp_bird_ar_mm.sh                               
# 创建时间: 2018年04月28日                                            
# 创 建 者: gl                                                  
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 每日跑传入截止账期日，op_day分区日期请于代码中写死
# 功    能: 应收账款余额
# 修改说明:                                                          
######################################################################


GL_DAY=$1
GL_MONTH=${GL_DAY:0:6}


OP_DAY="20180520"





# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d)
FORMAT_DAY=$(date -d $GL_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $GL_DAY +%Y%m01)
echo $FORMAT_DAY

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_ar_turn_dd.sh 20180101"
    exit 1
fi





#################################################xs01每月销量，销售额计算
TMP_DMP_BIRD_AR_TURN_MM_1='TMP_DMP_BIRD_AR_TURN_MM_1'
CREATE_TMP_DMP_BIRD_AR_TURN_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_TURN_MM_1(
    month_id                     string   --月份
	
   ,day_id                       string   --日期
   ,org_id                       string   --公司id
   ,cust_id                      string   --客户id
   ,cust_name                    string   --客户名字
   ,business_id                  string   --业务员id
   ,business_name                string   --业务员名称
   ,agent_name                   string   --代理商名称
   ,sale_cnt                     string   --销量
   ,sale_amt                     string   --销售额
   
   
)  
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC    
"

##############################################插入数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_AR_TURN_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_TURN_MM_1 PARTITION(op_day='$GL_DAY')
SELECT 
substr('$GL_DAY',1,6),
'$GL_DAY',
t1.org_id,
t1.account_number,
t1.party_name,
'缺失',
nvl(t1.resource_name,'缺失'),
t1.agent_name,
sum(case when t1.day_id<='$GL_DAY' then t1.sale_cnt else 0 end ),
sum(case when t1.day_id<='$GL_DAY' then t1.sale_amt else 0 end )
FROM 
  
  (SELECT 
   org_id,
   substr(ordered_date,1,6) as month_id,           --订单日期对应的月份
   ordered_date as day_id,                          --订单日期
   sum(nvl(out_main_qty,0)) as sale_cnt,                       --销量
   sum(nvl(out_main_qty,0)*nvl(execute_price,0)) as sale_amt,
   resource_name,                                  --销售员
   account_number,                                 --客户编号
   party_name,
   nvl(agent_name,party_name) as agent_name        --代理商名称
    FROM  mreport_poultry.dwu_gyl_xs01_dd where op_day='$OP_DAY' and ordered_date is not null and ordered_date<='$GL_DAY' and ordered_date>='$FIRST_DAY_MONTH' and bus_type in ('132020','135030')
	GROUP BY
	org_id,
	substr(ordered_date,1,6), 
	ordered_date,
    resource_name,	
	account_number,
	party_name,
	nvl(agent_name,party_name)
  )t1 

  LEFT JOIN
  (SELECT 
   salesperson_id,
   salesperson_name  
    FROM mreport_global.dim_salesperson
	GROUP BY
	salesperson_id,
    salesperson_name  
  )t2
  ON(regexp_replace(t1.resource_name,'(){1,}','')=regexp_replace(t2.salesperson_name,'(){1,}',''))

   
	GROUP BY
	t1.org_id,
	substr('$GL_DAY',1,6),
    '$GL_DAY',
    t1.account_number,
    t1.party_name,
    '缺失',
    t1.resource_name,
	t1.agent_name
  
  "
 
 

 #创建临时表ar1和ar2的关联并计算出清单表
 TMP_DMP_BIRD_AR_TURN_MM_2_2='TMP_DMP_BIRD_AR_TURN_MM_2_2'
CREATE_TMP_DMP_BIRD_AR_TURN_MM_2_2="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_TURN_MM_2_2(
   
    org_id                       string   --公司Id
    ,rec_date                     string   --应收日期
	,canc_date                   string   --核销日期 
   ,cust_id                      string   --客户id
   ,cust_name                    string   --客户名字
   ,business_id                  string   --业务员id
   ,business_name                string   --业务员名称
   ,agent_name                   string   --代理商名称
   ,customer_trx_id              string   --订单编号
   ,rec_amount                   string   --原始金额
   ,canc_amount                  string   --核销金额
   ,mature_date                  string   --到期日
   
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC "
   
##############################################插入数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_AR_TURN_MM_2_2="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_TURN_MM_2_2 PARTITION(op_day='$GL_DAY')
SELECT 
     t1.org_id,
     t1.gl_date as rec_date,
	 nvl(t2.gl_date,'1937-01-01') as canc_date,
     t1.account_number,
     t1.party_name,
     '缺失',
	 nvl(t1.salesrep_desc,'缺失'),
	 t1.agent_name,
     t1.customer_trx_id,	 
	 nvl(t1.loc_amount_due_original,0),
	 nvl(t2.loc_amount,0),
     t1.mature_date
  
	 FROM
       (SELECT   
        org_id,                 
        substr(gl_date,1,10) as gl_date,
		customer_trx_id,
		account_number,
		party_name,
		salesrep_desc,
		nvl(sold_customer,party_name) as agent_name,
		sum(nvl(loc_amount_due_original,0)) as loc_amount_due_original,
        to_date(mature_date) as mature_date
		
        FROM mreport_poultry.dwu_cw_receipt_standard_invoice_dd where op_day='$OP_DAY' and bus_type in ('132020','135030')
		AND invoice_type in('INV','CM') and invoice_type_name in ('Invoice','副产品发票','联产品发票','Invoice_TLP','1122010101')
	    GROUP BY
	     org_id,                 
        substr(gl_date,1,10),
		customer_trx_id,
		account_number,
		party_name,
	
		salesrep_desc,
		nvl(sold_customer,party_name),
        to_date(mature_date)
		

	   )t1
	    LEFT JOIN
	   (SELECT org_id,applied_customer_trx_id,sum(case when invoice_type_desc='坏账_应收账款转销' then nvl(loc_amount,0)* -1 else nvl(loc_amount,0) end) as loc_amount,account_number,substr(gl_date,1,10) as gl_date FROM mreport_poultry.dwu_cw_receipt_write_off_dd where op_day='$OP_DAY' 
	   GROUP BY
	   org_id,
	 
	   applied_customer_trx_id,
	   account_number,
	   substr(gl_date,1,10)
	   )t2
	     ON (t1.customer_trx_id=t2.applied_customer_trx_id and t1.account_number=t2.account_number and t1.org_id=t2.org_id )
  "
   #月初应收账款
   TMP_DMP_BIRD_AR_TURN_MM_3='TMP_DMP_BIRD_AR_TURN_MM_3'
   CREATE_TMP_DMP_BIRD_AR_TURN_MM_3="
   CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_TURN_MM_3(
    org_id                       string   --公司id
   ,day_id                       string   --应收总账日期的时间
   ,cust_id                      string   --客户id
   ,cust_name                    string   --客户名字
   ,business_id                  string   --业务员id
   ,business_name                string   --业务员名称
   ,agent_name                   string   --代理商名称
   ,month_begin_amt              string   --月初应收账款   
   )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS ORC "
   
   ##############################################插入数据
   echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   INSERT_TMP_DMP_BIRD_AR_TURN_MM_3="
   INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_TURN_MM_3 PARTITION(op_day='$GL_DAY')   
   SELECT
   t1.org_id,
   '$GL_DAY',
   t1.cust_id,
   t1.cust_name,
   t1.business_id,
   t1.business_name,
   t1.agent_name,
   sum(t1.rec_amount-t1.canc_amount)
   
   FROM
    (SELECT 
	  org_id,
	  agent_name,
	  customer_trx_id,
	  cust_id,
	  cust_name,
	  business_id,
	  business_name,
	  max(rec_amount) as rec_amount,
	  sum(case when nvl(canc_date,'1937-01-01')<concat(substr(from_unixtime(unix_timestamp('$GL_DAY','yyyymmdd'),'yyyy-mm-dd'),1,7),'-01') then canc_amount else 0 end) as canc_amount
	  FROM
    $TMP_DMP_BIRD_AR_TURN_MM_2_2 WHERE rec_date <concat(substr(from_unixtime(unix_timestamp('$GL_DAY','yyyymmdd'),'yyyy-mm-dd'),1,7),'-01')
	AND op_day='$GL_DAY'
	GROUP BY
	  agent_name,
      org_id,
	  customer_trx_id,
	  cust_id,
	  cust_name,
	  business_id,
	  business_name

	)t1

	GROUP BY
   t1.org_id,
   t1.agent_name,
   '$GL_DAY',
   t1.cust_id,
   t1.cust_name,
   t1.business_id,
   t1.business_name
   
   "
   
   
   
   TMP_DMP_BIRD_AR_TURN_MM_4='TMP_DMP_BIRD_AR_TURN_MM_4'
   CREATE_TMP_DMP_BIRD_AR_TURN_MM_4="
   CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_TURN_MM_4(
    org_id                       string   --公司id
   ,day_id                       string   --查询日期的时间
   ,cust_id                      string   --客户id
   ,cust_name                    string   --客户名字
   ,business_id                  string   --业务员id
   ,business_name                string   --业务员名称
   ,agent_name                   string   --代理商名称
   ,month_amt                    string   --账期内应收账款
   ,month_alert_amt              string   --账期外应收账款
   ,month_0_30_amt               string  --超期0-30天
   ,month_30_90_amt              string  --超期30-90天
   ,month_90_amt                 string  --超期90-365天
   ,month_year_amt               string  --超期一年以上
   
   )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS ORC "
   
   ##############################################插入数据
   echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   INSERT_TMP_DMP_BIRD_AR_TURN_MM_4="
  INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_TURN_MM_4 PARTITION(op_day='$GL_DAY')
  SELECT 
   t1.org_id,
   '$GL_DAY',
   t1.cust_id,
   t1.cust_name,
   t1.business_id,
   t1.business_name, 
   t1.agent_name,   
   sum(case when t1.mature_date>='$FORMAT_DAY'  then t1.rec_amount-t1.canc_amount else 0 end) as month_amt,
   sum(case when t1.mature_date<'$FORMAT_DAY' then t1.rec_amount-t1.canc_amount else 0 end ) as month_alert_amt,
   sum(case when datediff('$FORMAT_DAY',t1.mature_date)>0 and  datediff('$FORMAT_DAY',mature_date)<=30 then t1.rec_amount-t1.canc_amount else 0 end) as month_0_30_amt,
   sum(case when datediff('$FORMAT_DAY',t1.mature_date)>30 and  datediff('$FORMAT_DAY',mature_date)<=90 then t1.rec_amount-t1.canc_amount else 0 end) as month_30_90_amt,
   sum(case when datediff('$FORMAT_DAY',t1.mature_date)>90 and  datediff('$FORMAT_DAY',mature_date)<=365 then t1.rec_amount-t1.canc_amount else 0 end) as month_90_amt,
   sum(case when datediff('$FORMAT_DAY',t1.mature_date)>365 then t1.rec_amount-t1.canc_amount else 0 end) as month_year_amt
  FROM
  (SELECT 
	  org_id,
	  customer_trx_id,
	  cust_id,
	  cust_name,
	  business_id,
	  business_name,
	  agent_name,
	  max(rec_amount) as rec_amount,
	  sum(case when nvl(canc_date,'1937-01-01')<='$FORMAT_DAY' then canc_amount else 0 end) as canc_amount,
	  max(mature_date) as mature_date
	  FROM
    $TMP_DMP_BIRD_AR_TURN_MM_2_2
	WHERE op_day='$GL_DAY' and rec_date<='$FORMAT_DAY'
	GROUP BY
	  org_id,
	  customer_trx_id,
	  cust_id,
	  cust_name,
	  business_id,
	  business_name,
	  agent_name
	
	)t1
	GROUP BY
	t1.org_id,
    '$GL_DAY',
    t1.cust_id,
    t1.cust_name,
    t1.business_id,
    t1.business_name,
	t1.agent_name
   "
   
 
   
   
   
  
   
   
   ################插入数据
   
   
   #########################################################连接应收账款的临时表
   
   TMP_DMP_BIRD_AR_TURN_MM_5='TMP_DMP_BIRD_AR_TURN_MM_5'
   CREATE_TMP_DMP_BIRD_AR_TURN_MM_5="
   CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_AR_TURN_MM_5(
    org_id                       string   --公司id
   ,month_id                     string   --月份
   ,day_id                       string   --日期
   ,cust_id                      string   --客户id
   ,cust_name                    string   --客户名字
   ,business_id                  string   --业务员id
   ,business_name                string   --业务员名称
   ,agent_name                   string   --代理商名称
   ,month_begin_amt              string   --月初应收账款 
   ,month_amt                    string   --账期内应收账款 
   ,month_alert_amt              string   --超期应收账款
   ,month_0_30_amt               string   --超期0-30天
   ,month_30_90_amt              string   --超期30-90天
   ,month_90_amt                 string   --超期90-365天
   ,month_year_amt               string   --超期365天
   ,end_rec                      string   --期末应收账款
   ,month_changes                string   --变动情况
   )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS ORC 

   "
   
    ##############################################插入数据
   echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   INSERT_TMP_DMP_BIRD_AR_TURN_MM_5="
  INSERT OVERWRITE TABLE $TMP_DMP_BIRD_AR_TURN_MM_5 PARTITION(op_day='$GL_DAY')
  SELECT
    t1.org_id,
    substr(t1.day_id,1,6),
    t1.day_id,
	t1.cust_id,
	t1.cust_name,
	t1.business_id,
	t1.business_name,
	t1.agent_name,
	round(t2.month_begin_amt,6),
	round(t1.month_amt,6),
	round(t1.month_alert_amt,6),
	round(t1.month_0_30_amt,6),
	round(t1.month_30_90_amt,6),
	round(t1.month_90_amt,6),
	round(t1.month_year_amt,6),
   round(t1.month_amt+t1.month_alert_amt,6) as end_rec,
   round(t1.month_amt+t1.month_alert_amt-nvl(t2.month_begin_amt,0),6)
  FROM 
	 (SELECT * FROM $TMP_DMP_BIRD_AR_TURN_MM_4 where op_day='$GL_DAY')t1
	 LEFT JOIN
	 (SELECT * FROM $TMP_DMP_BIRD_AR_TURN_MM_3 where op_day='$GL_DAY')t2
	 ON(t1.day_id=t2.day_id and t1.cust_id=t2.cust_id and nvl(t1.business_name,'缺失')=nvl(t2.business_name,'缺失') and t1.org_id=t2.org_id and t1.agent_name=t2.agent_name)

   "
   
   ##########################################################################将销量和应收关联
   
   DMP_BIRD_AR_TURN_MM='dmp_bird_ar_turn_mm'
   CREATE_DMP_BIRD_AR_TURN_MM="
   CREATE TABLE IF NOT EXISTS $DMP_BIRD_AR_TURN_MM(
       org_id                       string                                            --公司Id
      ,org_name                     string                                            --公司6级名称
	  ,level6_org_id                string                                            --公司6级id
	  ,agent_name                   string                                            --代理商名称
      ,month_id	                    string                                            --期间(月份)
      ,day_id	                    string                                            --期间(日)
      ,business_id	                string                                            --业务员ID
	  ,business_name	            string                                            --业务员名称
	  ,level1_channel_id	        string                                            --客户渠道1级
	  ,level1_channel_descr	        string                                            --客户渠道1级
	  ,level2_channel_id	        string                                            --客户渠道2级
	  ,level2_channel_descr	        string                                            --客户渠道2级
	  ,cust_id	                    string                                            --客户ID
	  ,cust_name	                string                                            --客户名称
	  ,sale_cnt	                    string                                            --销量
	  ,sale_amt	                    string                                            --销售额
	  ,month_begin_amt	            string                                            --月初应收账款
	  ,month_amt	                string                                            --账期内应收账款
	  ,month_alarm_amt	            string                                            --超期应收账款
	  ,month_0_30_amt	            string                                            --超期账龄0-30天余额
	  ,month_30_90_amt	            string                                            --超期账龄30-90天余额
	  ,month_90_amt	                string                                            --超期账龄90-一年余额
	  ,month_year_amt	            string                                            --超期账龄一年以上余额
	  ,month_end_amt	            string                                            --期末应收账款
	  ,month_changes	            string                                            --变动情况
	  ,turnover_rate                string                                            --周转率
	  ,create_time	                string                                            --数据推送时间
	 
    )
   PARTITIONED BY (op_day string)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
   STORED AS TEXTFILE
   "	
	
   ##############################################插入数据
   echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
   INSERT_DMP_BIRD_AR_TURN_MM="
   INSERT OVERWRITE TABLE $DMP_BIRD_AR_TURN_MM PARTITION(op_day='$GL_DAY')
   SELECT 
   t4.org_id,
   t4.level6_org_descr,
   t4.level6_org_id,
   tmp_2.agent_name,
   tmp_2.month_id,
   tmp_2.day_id,
   tmp_2.business_id,
   tmp_2.business_name,
   tmp_2.level1_channel_id,
   tmp_2.level1_channel_descr,
   tmp_2.level2_channel_id,
   tmp_2.level2_channel_descr,
   t3.account_number,
   tmp_2.cust_name,
   tmp_2.sale_cnt,
   tmp_2.sale_amt,
   tmp_2.month_begin_amt,
   tmp_2.month_amt,
   tmp_2.month_alert_amt,
   tmp_2.month_0_30_amt,
   tmp_2.month_30_90_amt,
   tmp_2.month_90_amt,
   tmp_2.month_year_amt,
   tmp_2.end_rec,
   tmp_2.month_changes,
   tmp_2.turnover_rate,
   tmp_2.create_time

   FROM
   (SELECT
    tmp.org_id,
    tmp.month_id as month_id,
	tmp.day_id as day_id,
	tmp.business_id as business_id,
	tmp.business_name as business_name,
	tmp.agent_name as agent_name,
	t2.id_cust_chan as level1_channel_id,
	t2.cust_chan_type as level1_channel_descr,
	t2.id_cust_chan_detail_tp as level2_channel_id,  
	t2.cust_chan_detail_tp as level2_channel_descr,
	tmp.cust_id as cust_id,
	tmp.cust_name as cust_name,
	sum(tmp.sale_cnt) as sale_cnt,
	sum(tmp.sale_amt) as sale_amt,
	sum(tmp.month_begin_amt) as month_begin_amt,
	sum(tmp.month_amt) as month_amt,
	sum(tmp.month_alert_amt) as month_alert_amt,
	sum(tmp.month_0_30_amt) as month_0_30_amt,
	sum(tmp.month_30_90_amt) as month_30_90_amt,
	sum(tmp.month_90_amt) as month_90_amt,
	sum(tmp.month_year_amt) as month_year_amt,
	sum(tmp.end_rec) as end_rec,
	sum(tmp.month_changes) as month_changes,
	'' as turnover_rate,
	'$CREATE_TIME' as create_time
   FROM 
       (SELECT  
	    org_id,
        month_id,
	    day_id,
	    cust_id,      
	    cust_name,    
	    business_id,  
        business_name,
		agent_name,
	    sale_cnt,       
	    sale_amt, 
	    0 as month_begin_amt,
	    0 as month_amt,
	    0 as month_alert_amt,
	    0 as month_0_30_amt,
	    0 as month_30_90_amt,
	    0 as month_90_amt,
	    0 as month_year_amt,
	    0 as end_rec,
	    0 as month_changes
		FROM TMP_DMP_BIRD_AR_TURN_MM_1 where op_day='$GL_DAY' and day_id='$GL_DAY'
		UNION ALL
		SELECT  
		org_id,
	    month_id,
	    day_id,
		cust_id,      
		cust_name,    
		business_id,  
        business_name,
		agent_name,
	    0 as sale_cnt,       
	    0 as sale_amt, 
		month_begin_amt,
	    month_amt,
		month_alert_amt,
	    month_0_30_amt,
		month_30_90_amt,
		month_90_amt,
		month_year_amt,
		end_rec,
		month_changes
		FROM TMP_DMP_BIRD_AR_TURN_MM_5 where op_day='$GL_DAY'
		)tmp
		
	    LEFT JOIN
	   (SELECT * FROM mreport_global.dwu_dim_crm_customer)t2
	    ON(tmp.cust_id=t2.customer_account_id)
		GROUP BY
		tmp.org_id,
	    tmp.month_id,
	    tmp.day_id,
	    tmp.business_id,
	    tmp.business_name,
		tmp.agent_name,
	    t2.id_cust_chan,
	    t2.cust_chan_type,
	    t2.id_cust_chan_detail_tp,  
	    t2.cust_chan_detail_tp,
	    tmp.cust_id,
	    tmp.cust_name,
	    '',
        '$CREATE_TIME'
	 )tmp_2	
		INNER JOIN 
		(SELECT * from mreport_global.dwu_dim_customer where customer_type!='内部')t3
		ON(tmp_2.cust_id=t3.account_number)
		LEFT JOIN 
		(SELECT org_id,level6_org_id,level6_org_descr from mreport_global.dim_org_management group by org_id,level6_org_descr,level6_org_id) t4 
	    ON(tmp_2.cust_id=t3.account_number and tmp_2.org_id=t4.org_id)
      "
	 
	 
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;

    $CREATE_TMP_DMP_BIRD_AR_TURN_MM_1;
    $INSERT_TMP_DMP_BIRD_AR_TURN_MM_1;
    $CREATE_TMP_DMP_BIRD_AR_TURN_MM_2_2;
    $INSERT_TMP_DMP_BIRD_AR_TURN_MM_2_2;
    $CREATE_TMP_DMP_BIRD_AR_TURN_MM_3;
    $INSERT_TMP_DMP_BIRD_AR_TURN_MM_3;
    $CREATE_TMP_DMP_BIRD_AR_TURN_MM_4;
    $INSERT_TMP_DMP_BIRD_AR_TURN_MM_4;
	$CREATE_TMP_DMP_BIRD_AR_TURN_MM_5;
    $INSERT_TMP_DMP_BIRD_AR_TURN_MM_5;


    $CREATE_DMP_BIRD_AR_TURN_MM;
    $INSERT_DMP_BIRD_AR_TURN_MM;
    "  -v




