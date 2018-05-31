#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_channel_ay_dd.sh                               
# 创建时间: 2018年04月09日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 渠道指标分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_channel_ay_dd.sh 20180101"
    exit 1
fi

# 当前时间减去30天时间
FORMAT_DAY=$(date -d $OP_DAY +%Y-%m-%d)
FIRST_DAY_MONTH=$(date -d $OP_DAY +%Y-%m-01)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_CHANNEL_AY_DD='DMP_BIRD_CHANNEL_AY_DD'

CREATE_DMP_BIRD_CHANNEL_AY_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_CHANNEL_AY_DD(
    MONTH_ID                STRING        --期间(月份)
    ,DAY_ID                 STRING        --期间(日)
    ,LEVEL1_ORG_ID          STRING        --组织1级(股份)
    ,LEVEL1_ORG_DESCR       STRING        --组织1级(股份)
    ,LEVEL2_ORG_ID          STRING        --组织2级(片联)
    ,LEVEL2_ORG_DESCR       STRING        --组织2级(片联)
    ,LEVEL3_ORG_ID          STRING        --组织3级(片区)
    ,LEVEL3_ORG_DESCR       STRING        --组织3级(片区)
    ,LEVEL4_ORG_ID          STRING        --组织4级(小片)
    ,LEVEL4_ORG_DESCR       STRING        --组织4级(小片)
    ,LEVEL5_ORG_ID          STRING        --组织5级(公司)
    ,LEVEL5_ORG_DESCR       STRING        --组织5级(公司)
    ,LEVEL6_ORG_ID          STRING        --组织6级(OU)
    ,LEVEL6_ORG_DESCR       STRING        --组织6级(OU)
    ,LEVEL7_ORG_ID          STRING        --组织7级(库存组织)
    ,LEVEL7_ORG_DESCR       STRING        --组织7级(库存组织)
    ,LEVEL1_BUSINESSTYPE_ID STRING        --业态1级
    ,LEVEL1_BUSINESSTYPE_NAME STRING      --业态1级
    ,LEVEL2_BUSINESSTYPE_ID   STRING      --业态2级
    ,LEVEL2_BUSINESSTYPE_NAME STRING      --业态2级
    ,LEVEL3_BUSINESSTYPE_ID   STRING      --业态3级
    ,LEVEL3_BUSINESSTYPE_NAME STRING      --业态3级
    ,LEVEL4_BUSINESSTYPE_ID   STRING      --业态4级
    ,LEVEL4_BUSINESSTYPE_NAME STRING      --业态4级
    ,LEVEL1_SALE_ID           STRING      --销售组织1级
    ,LEVEL1_SALE_DESCR        STRING      --销售组织1级
    ,LEVEL2_SALE_ID           STRING      --销售组织2级
    ,LEVEL2_SALE_DESCR        STRING      --销售组织2级
    ,LEVEL3_SALE_ID           STRING      --销售组织3级
    ,LEVEL3_SALE_DESCR        STRING      --销售组织3级
    ,LEVEL4_SALE_ID           STRING      --销售组织4级
    ,LEVEL4_SALE_DESCR        STRING      --销售组织4级
    ,LEVEL5_SALE_ID           STRING      --销售组织5级
    ,LEVEL5_SALE_DESCR        STRING      --销售组织5级
    ,PRODUCTION_LINE_ID       STRING      --产线
    ,PRODUCTION_LINE_DESCR    STRING      --产线
    ,LEVEL1_PROD_ID           STRING      --产品线1级
    ,LEVEL1_PROD_DESCR        STRING      --产品线1级
    ,LEVEL2_PROD_ID           STRING      --产品线2级
    ,LEVEL2_PROD_DESCR        STRING      --产品线2级
    ,LEVEL1_CHANNEL_ID        STRING      --客户渠道1级
    ,LEVEL1_CHANNEL_DESCR     STRING      --客户渠道1级
    ,LEVEL2_CHANNEL_ID        STRING      --客户渠道2级
    ,LEVEL2_CHANNEL_DESCR     STRING      --客户渠道2级
    ,DAY_CHL_CNT              STRING      --渠道部门日销量
    ,MONTH_CHL_CNT            STRING      --本月渠道部门销量累计
    ,QUARTER_CHL_CNT          STRING      --季度渠道部门销量累计
    ,YEAR_CHL_CNT             STRING      --本年渠道部门销量累计
    ,LAST_MONTH_CHL_CNT       STRING      --上月渠道部门销量
    ,CREATE_TIME              STRING      --数据推送时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从DWP_BIRD_CHANNEL_AY_DD转换至目标表DMP_BIRD_CHANNEL_AY_DD>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_CHANNEL_AY_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_CHANNEL_AY_DD PARTITION(OP_DAY='$OP_DAY')
SELECT
    t1.MONTH_ID                 --期间(月份)
    ,t1.DAY_ID                  --期间(日)
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
    ,t10.LEVEL7_ORG_ID --组织7级(库存组织)
    ,t10.LEVEL7_ORG_DESCR --组织7级(库存组织)
    ,T11.LEVEL1_BUSINESSTYPE_ID --业态1级
    ,T11.LEVEL1_BUSINESSTYPE_NAME --业态1级
    ,T11.LEVEL2_BUSINESSTYPE_ID --业态2级
    ,T11.LEVEL2_BUSINESSTYPE_NAME --业态2级
    ,T11.LEVEL3_BUSINESSTYPE_ID --业态3级
    ,T11.LEVEL3_BUSINESSTYPE_NAME --业态3级
    ,T11.LEVEL4_BUSINESSTYPE_ID --业态4级
    ,T11.LEVEL4_BUSINESSTYPE_NAME --业态4级
    ,T7.FIRST_SALE_ORG_CODE--销售组织1级
    ,T7.FIRST_SALE_ORG_NAME--销售组织1级
    ,T7.SECOND_SALE_ORG_CODE--销售组织2级
    ,T7.SECOND_SALE_ORG_NAME--销售组织2级
    ,T7.THREE_SALE_ORG_CODE--销售组织3级
    ,T7.THREE_SALE_ORG_NAME--销售组织3级
    ,T7.FOUR_SALE_ORG_CODE--销售组织4级
    ,T7.FOUR_SALE_ORG_NAME--销售组织4级
    ,T7.FIVE_SALE_ORG_CODE--销售组织5级
    ,T7.FIVE_SALE_ORG_NAME--销售组织5级
    ,coalesce(substr(T1.PRODUCTION_LINE_ID,1,1),'-1')       --产线
    ,coalesce(T1.PRODUCTION_LINE_DESCR,'缺省')    --产线
    ,t1.LEVEL1_PROD_ID       
    ,t1.LEVEL1_PROD_DESCR    
    ,t1.LEVEL2_PROD_ID       
    ,t1.LEVEL2_PROD_DESCR    
    ,t1.LEVEL1_CHANNEL_ID    
    ,t1.LEVEL1_CHANNEL_DESCR 
    ,t1.LEVEL2_CHANNEL_ID    
    ,t1.LEVEL2_CHANNEL_DESCR 
    ,coalesce(T1.DAY_CHL_CNT,0)       --渠道部门日销量
    ,coalesce(T1.MONTH_CHL_CNT,0)     --本月渠道部门销量累计
    ,coalesce(T1.QUARTER_CHL_CNT,0)   --季度渠道部门销量累计
    ,coalesce(T1.YEAR_CHL_CNT,0)      --本年渠道部门销量累计
    ,coalesce(T1.LAST_MONTH_CHL_CNT,0) --上月渠道部门销量
    ,${CREATE_TIME}               --数据推送时间
 FROM (
 select * from MREPORT_POULTRY.DWP_BIRD_CHANNEL_AY_DD
 WHERE OP_DAY = '${OP_DAY}'
 ) T1
 inner join (
 select day_id from mreport_global.dim_day a where day_id BETWEEN '20151201' AND from_unixtime(unix_timestamp(),'yyyyMMdd')
 ) T13
 ON T1.DAY_ID = T13.DAY_ID
  --关联取出对应属性
 LEFT JOIN MREPORT_GLOBAL.DWU_DIM_XS_ORG T7 ON T1.FIFTH_ORG_ID = T7.SALE_ORG_CODE --销售组织
 --LEFT JOIN MREPORT_GLOBAL.DIM_CRM_ITEM T8 ON T1.ITEM_CODE = T8.ITEM_CODE --CRM物料表-产品线
 --LEFT JOIN MREPORT_GLOBAL.DIM_ORG_MANAGEMENT T9 ON T1.ORG_ID = T9.ORG_ID AND T1.BUS_TYPE = T9.BUS_TYPE_ID  --6级组织
 LEFT JOIN MREPORT_GLOBAL.DIM_ORG_INV_MANAGEMENT T10 ON T1.ORGANIZATION_ID = T10.INV_ORG_ID  --7级库存组织
 LEFT JOIN MREPORT_GLOBAL.DIM_ORG_BUSINESSTYPE T11 ON T1.BUS_TYPE = T11.LEVEL4_BUSINESSTYPE_ID --业态
 --LEFT JOIN MREPORT_GLOBAL.DWU_DIM_CRM_CUSTOMER T12 ON T1.ACCOUNT_NUMBER = T12.CUSTOMER_ACCOUNT_ID  --客户渠道
 left join mreport_global.dim_org_management t2 on t1.org_id=t2.org_id  and t2.attribute5='1'
 left join mreport_global.dim_org_management t3 on t1.org_id=t3.org_id and t1.bus_type=t3.bus_type_id and t3.attribute5='2'
 "


echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DMP_BIRD_CHANNEL_AY_DD;
    $INSERT_DMP_BIRD_CHANNEL_AY_DD;
"  -v
