#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_inv_warning_dd.sh                               
# 创建时间: 2018年4月10日                                           
# 创 建 者: gl                                                     
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 品类库存与预警
# 修改说明:                                                          
######################################################################
OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)
# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_inv_warning_dd.sh 20180101"
    exit 1
fi
echo "数据导入时间$CREATE_TIME"
DMP_BIRD_INV_WARNING_DD='dmp_bird_inv_warning_dd'

CREATE_DMP_BIRD_INV_WARNING_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_INV_WARNING_DD(
       month_id                         string     --期间(月份)
      ,day_id                          string      --期间日期
      ,level1_org_id                   string      --组织1级id(股份)
      ,level1_org_descr                string      --组织1级(股份)
      ,level2_org_id                   string      --组织2级id(片联)
      ,level2_org_descr                string      --组织2级(片联)
      ,level3_org_id                   string      --组织3级id(片区)
      ,level3_org_descr                string      --组织3级(片区)
      ,level4_org_id                   string      --组织4级id(小片)
      ,level4_org_descr                string      --组织4级(小片)
      ,level5_org_id                   string      --组织5级id(公司)
      ,level5_org_descr                string      --组织5级(公司)
      ,level6_org_id                   string      --组织6级id(OU)
      ,level6_org_descr                string      --组织6级(OU)
      ,level7_org_id                   string      --组织7级id(库存组织)
      ,level7_org_descr                string      --组织7级(库存组织)
      ,level1_businesstype_id          string      --业态1级id
      ,level1_businesstype_name        string      --业态1级
      ,level2_businesstype_id          string      --业态2级id
      ,level2_businesstype_name        string      --业态2级
      ,level3_businesstype_id          string      --业态3级id
      ,level3_businesstype_name        string      --业态3级
      ,level4_businesstype_id          string      --业态4级id
      ,level4_businesstype_name        string      --业态4级
      ,production_line_id              string      --产线id
      ,production_line_descr           string      --产线
      ,level1_prod_id                  string      --产品线一级id
      ,level1_prod_descr               string      --产品线一级
      ,level2_prod_id                  string      --产品线二级id
      ,level2_prod_descr               string      --产品线二级
      ,level1_prodtype_id              string      --产品分类一级id
      ,level1_prodtype_descr           string      --产品分类一级
      ,level2_prodtype_id              string      --产品分类二级id
      ,level2_prodtype_descr           string      --产品分类二级
      ,level3_prodtype_id              string      --产品分类三级id
      ,level3_prodtype_descr           string      --产品分类三级
      ,inventory_item_id               string      --物料品名id
      ,inventory_item_desc             string      --物料品名
      ,store_busi_cnt                  string      --可用业务库存
      ,store_busi_sale_cnt             string      --待出库业务库存
      ,day_store_busi_cnt              string      --当日业务库存     
      
      ,store_alarm_cnt                 string      --库存预警线(手工导入)
      ,create_time                     string      --数据推送时间
	) 
    PARTITIONED BY (op_day string)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
    STORED AS TEXTFILE"
	  
	## 转换数据
    echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
	INSERT_DMP_BIRD_INV_WARNING_DD="
	INSERT OVERWRITE TABLE $DMP_BIRD_INV_WARNING_DD PARTITION(op_day='$OP_DAY')
	SELECT 
    substr(regexp_replace(t1.day_id,'-',''),1,6) as month_id,                                              --期间(月份)
	regexp_replace(t1.day_id,'-','') as day_id,                                                                    --期间(日)
    case when dp1.level1_org_id    is null then coalesce(dp2.level1_org_id,'-1') else coalesce(dp1.level1_org_id,'-1')  end as level1_org_id,                --一级组织编码
    case when dp1.level1_org_descr is null then coalesce(dp2.level1_org_descr,'缺失') else coalesce(dp1.level1_org_descr,'缺失')  end as level1_org_descr,   --一级组织描述
    case when dp1.level2_org_id is null    then coalesce(dp2.level2_org_id,'-1') else coalesce(dp1.level2_org_id,'-1')  end as level2_org_id,                --二级组织编码
    case when dp1.level2_org_descr is null then coalesce(dp2.level2_org_descr,'缺失') else coalesce(dp1.level2_org_descr,'缺失')  end as level2_org_descr,   --二级组织描述
    case when dp1.level3_org_id    is null then coalesce(dp2.level3_org_id,'-1') else coalesce(dp1.level3_org_id,'-1')  end as level3_org_id,                --三级组织编码
    case when dp1.level3_org_descr is null then coalesce(dp2.level3_org_descr,'缺失') else coalesce(dp1.level3_org_descr,'缺失')  end as level3_org_descr,   --三级组织描述
    case when dp1.level4_org_id    is null then coalesce(dp2.level4_org_id,'-1') else coalesce(dp1.level4_org_id,'-1')  end as level4_org_id,                --四级组织编码
    case when dp1.level4_org_descr is null then coalesce(dp2.level4_org_descr,'缺失') else coalesce(dp1.level4_org_descr,'缺失')  end as level4_org_descr,   --四级组织描述
    case when dp1.level5_org_id    is null then coalesce(dp2.level5_org_id,'-1') else coalesce(dp1.level5_org_id,'-1')  end as level5_org_id,                --五级组织编码
    case when dp1.level5_org_descr is null then coalesce(dp2.level5_org_descr,'缺失') else coalesce(dp1.level5_org_descr,'缺失')  end as level5_org_descr,   --五级组织描述
    case when dp1.level6_org_id    is null then coalesce(dp2.level6_org_id,'-1') else coalesce(dp1.level6_org_id,'-1')  end as level6_org_id,                --六级组织编码
    case when dp1.level6_org_descr is null then coalesce(dp2.level6_org_descr,'缺失') else coalesce(dp1.level6_org_descr,'缺失')  end as level6_org_descr,   --六级组织描述
	dp3.level7_org_id,                                                               --组织7级id(库存组织)
	dp3.level7_org_descr,                                                            --组织7级(库存组织)
	bs.level1_businesstype_id,                                                      --业态1级id
	bs.level1_businesstype_name,                                                    --业态1级
	bs.level2_businesstype_id,                                                      --业态2级id
	bs.level2_businesstype_name,                                                    --业态2级
	bs.level3_businesstype_id,                                                      --业态3级id
	bs.level3_businesstype_name,                                                    --业态3级
	bs.level4_businesstype_id,                                                      --业态4级id
	bs.level4_businesstype_name,                                                    --业态4级
	case t1.product_line  
	when '10' then '1'
	when '20' then '2'
	else '-1'
	end 
	as production_line_id,                                          --产线id
	case  t1.product_line 
    when '10' then '鸡线'
    when '20' then '鸭线'
    else '缺失'                                                      
    end  as production_line_descr,                                                  --产线描述
	crm.prd_line_cate_id as level1_prod_id,                                         --产品线一级id
	crm.prd_line_cate as level1_prod_descr,                                         --产品线一级
	crm.sub_prd_line_tp_id as level2_prod_id,                                       --产品线二级id
	crm.sub_prd_line_tp as level2_prod_descr,                                       --产品线二级
	crm.first_lv_tp_id as level1_prodtype_id,                                       --产品分类一级id
	crm.first_lv_tp as level1_prodtype_descr,                                       --产品分类一级
	crm.scnd_lv_tp_id as level2_prodtype_id,                                        --产品分类二级id
	crm.scnd_lv_tp as level2_prodtype_descr,                                        --产品分类二级
	crm.thrd_lv_tp_id as level3_prodtype_id,                                        --产品分类三级id
	crm.thrd_lv_tp as level3_prodtype_descr,                                        --产品分类三级
	t3.inventory_item_id,                                                           --物料品名id
	t3.inventory_item_desc,                                                         --物料品名名称
	t1.avi_amount as store_busi_cnt,                                                --可用业务库存
	t1.cur_aount as store_busi_sale_cnt,                                            --待出库业务库存
	(nvl(t1.finish_product_stock,0)+nvl(t1.fresh_normal_sto_count,0)) as day_store_busi_cnt,      --当日业务库存
                                                                           
	al.stock_alert,                                                                 --库存警戒线(手工导入)
	'$CREATE_TIME' as create_time                                                   --数据推送时间	
	FROM
    (SELECT
	   
       item_id, 	 
	   organization_id, 
	  
	  
	   bus_type,
	   sum(nvl(finish_product_stock,0)) as finish_product_stock,
	   sum(nvl(fresh_normal_sto_count,0)) as fresh_normal_sto_count,
	   sum(nvl(avi_amount,0)) as avi_amount,
	   sum(nvl(cur_aount,0)) as cur_aount,
	   to_date(available_stock_time) as day_id,	        
	   product_line
	FROM mreport_poultry.dwu_xs_xs03_dd where op_day='$OP_DAY' group by 
	  
	   item_id,
	   organization_id,
	   bus_type,
	   to_date(available_stock_time),
	   product_line     
	)t1 
	
	LEFT JOIN
	    (SELECT 
	      inventory_item_id,
	      inventory_item_code,
	      inventory_item_desc,
	      inv_org_id
	FROM  mreport_global.dwu_dim_material_new) t3
	  ON (t1.item_id=t3.inventory_item_id and t1.organization_id=t3.inv_org_id)
	LEFT JOIN 
	   (SELECT 
         item_code,
		 item_id,
	     prd_line_cate_id,
	     prd_line_cate,
	     sub_prd_line_tp_id,
	     sub_prd_line_tp,
		 sub_prd_line_tp_code,
	     first_lv_tp_id,
	     first_lv_tp,
         scnd_lv_tp_id,
	     scnd_lv_tp,
	     thrd_lv_tp_id,  
	     thrd_lv_tp,
		 thrd_lv_tp_code  
	FROM mreport_global.dim_crm_item) crm 
	  ON(t1.item_id=t3.inventory_item_id and t1.organization_id=t3.inv_org_id and t3.inventory_item_code=crm.item_code)
	  
	  LEFT JOIN 
	       (SELECT inv_org_id,level7_org_descr,level7_org_id,ou_org_id FROM mreport_global.dim_org_inv_management)dp3
	  ON(t1.organization_id=dp3.inv_org_id)
	LEFT JOIN 
           (SELECT 
            * 
			FROM mreport_global.dim_org_management
			)dp1 
	  ON(t1.organization_id=dp3.inv_org_id and dp3.ou_org_id=dp1.org_id and dp1.attribute5='1')	
    LEFT JOIN 
           (SELECT 
            * FROM mreport_global.dim_org_management
			)dp2
      ON(t1.organization_id=dp3.inv_org_id and dp3.ou_org_id=dp2.org_id and dp2.attribute5='2' and t1.bus_type=dp2.bus_type_id)
	 

	LEFT JOIN 
	(select level1_businesstype_id,level1_businesstype_name,level2_businesstype_id,level2_businesstype_name,
	 level3_businesstype_id,level3_businesstype_name,
	 level4_businesstype_id,level4_businesstype_name from mreport_global.dim_org_businesstype)bs 
	  ON(t1.bus_type=bs.level4_businesstype_id)
	 LEFT JOIN 
     (select * from mreport_global.dwu_dim_inv_stock_alert_all) al 
	  ON (substr(regexp_replace(t1.day_id,'-',''),1,6)=regexp_replace(al.period_name,'-','') and crm.thrd_lv_tp_code=al.product_second_category_code and t1.product_line=
	   al.product_line_code)
	  
	  
	                   
	  "
	 
	echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
    hive -e "
    use mreport_poultry;
    $CREATE_DMP_BIRD_INV_WARNING_DD;
    $INSERT_DMP_BIRD_INV_WARNING_DD;
    "  -v

