#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_key_aly_dd.sh                               
# 创建时间: 2018年04月09日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyyMMdd]                                             
# 补充说明: 
# 功    能: 禽旺关键指标分析
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_key_aly_dd.sh 20180101"
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
DMP_BIRD_KEY_ALY_DD='DMP_BIRD_KEY_ALY_DD'

CREATE_DMP_BIRD_KEY_ALY_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_KEY_ALY_DD(
    MONTH_ID                                 STRING  --期间(月份)
    ,DAY_ID                                  STRING  --期间(日)
    ,LEVEL1_ORG_ID                           STRING  --组织1级(股份)
    ,LEVEL1_ORG_DESCR                        STRING  --组织1级(股份)
    ,LEVEL2_ORG_ID                           STRING  --组织2级(片联)
    ,LEVEL2_ORG_DESCR                        STRING  --组织2级(片联)
    ,LEVEL3_ORG_ID                           STRING  --组织3级(片区)
    ,LEVEL3_ORG_DESCR                        STRING  --组织3级(片区)
    ,LEVEL4_ORG_ID                           STRING  --组织4级(小片)
    ,LEVEL4_ORG_DESCR                        STRING  --组织4级(小片)
    ,LEVEL5_ORG_ID                           STRING  --组织5级(公司)
    ,LEVEL5_ORG_DESCR                        STRING  --组织5级(公司)
    ,LEVEL6_ORG_ID                           STRING  --组织6级(OU)
    ,LEVEL6_ORG_DESCR                        STRING  --组织6级(OU)
    ,LEVEL7_ORG_ID                           STRING  --组织7级(库存组织)
    ,LEVEL7_ORG_DESCR                        STRING  --组织7级(库存组织)
    ,LEVEL1_BUSINESSTYPE_ID                  STRING  --业态1级
    ,LEVEL1_BUSINESSTYPE_NAME                STRING  --业态1级
    ,LEVEL2_BUSINESSTYPE_ID                  STRING  --业态2级
    ,LEVEL2_BUSINESSTYPE_NAME                STRING  --业态2级
    ,LEVEL3_BUSINESSTYPE_ID                  STRING  --业态3级
    ,LEVEL3_BUSINESSTYPE_NAME                STRING  --业态3级
    ,LEVEL4_BUSINESSTYPE_ID                  STRING  --业态4级
    ,LEVEL4_BUSINESSTYPE_NAME                STRING  --业态4级
    ,PRODUCTION_LINE_ID                      STRING  --产线
    ,PRODUCTION_LINE_DESCR                   STRING  --产线
    ,RECYCLE_DEATH_CNT                       STRING  --回收路途死亡数
    ,RECYCLE_CNT                             STRING  --回收只数
    ,BEST_WEIGTH_NUM                         STRING  --最佳只重数量
    ,RECYCLE_WEIGHT_AMT                      STRING  --总回收量
    ,CLOSE_RANGE_CNT                         STRING  --近距离数
    ,EMPTY_TIMES                             STRING  --空鸡/空鸭时间
    ,KILL_LV_CNT                             STRING  --宰杀均衡数量
    ,KILL_LV_IN_CNT                          STRING  --宰杀均衡范围内数量
    ,D_PRODUCT_CNT                           STRING  --D产量
    ,PRODUCT_CNT                             STRING  --总产量
    ,PRODUCT_NUM                             STRING  --总只数
    ,PRODUCT_AMT                             STRING  --总重量
    ,KILL_CNT                                STRING  --屠宰只数
    ,PUT_CNT                                 STRING  --投放只数
    ,PUT_BREED_CNT                           STRING  --投入饲料总重量
    ,FEED_DAYS                               STRING  --饲养天数
    ,FEED_PRICE                              STRING  --料价
    ,CREATE_TIME                             STRING  --数据推送时间
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_KEY_ALY_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_KEY_ALY_DD PARTITION(op_day='${OP_DAY}')
select 
    a.MONTH_ID                                  --期间(月份)                   
    ,a.DAY_ID                                   --期间(日)                     
    ,a.LEVEL1_ORG_ID                            --组织1级(股份)                
    ,a.LEVEL1_ORG_DESCR                         --组织1级(股份)                
    ,a.LEVEL2_ORG_ID                            --组织2级(片联)                
    ,a.LEVEL2_ORG_DESCR                         --组织2级(片联)                
    ,a.LEVEL3_ORG_ID                            --组织3级(片区)                
    ,a.LEVEL3_ORG_DESCR                         --组织3级(片区)                
    ,a.LEVEL4_ORG_ID                            --组织4级(小片)                
    ,a.LEVEL4_ORG_DESCR                         --组织4级(小片)                
    ,a.LEVEL5_ORG_ID                            --组织5级(公司)                
    ,a.LEVEL5_ORG_DESCR                         --组织5级(公司)                
    ,a.LEVEL6_ORG_ID                            --组织6级(OU)                  
    ,a.LEVEL6_ORG_DESCR                         --组织6级(OU)                  
    ,a.LEVEL7_ORG_ID                            --组织7级(库存组织)            
    ,a.LEVEL7_ORG_DESCR                         --组织7级(库存组织)            
    ,a.LEVEL1_BUSINESSTYPE_ID                   --业态1级                      
    ,a.LEVEL1_BUSINESSTYPE_NAME                 --业态1级                      
    ,a.LEVEL2_BUSINESSTYPE_ID                   --业态2级                      
    ,a.LEVEL2_BUSINESSTYPE_NAME                 --业态2级                      
    ,a.LEVEL3_BUSINESSTYPE_ID                   --业态3级                      
    ,a.LEVEL3_BUSINESSTYPE_NAME                 --业态3级                      
    ,a.LEVEL4_BUSINESSTYPE_ID                   --业态4级                      
    ,a.LEVEL4_BUSINESSTYPE_NAME                 --业态4级                      
    ,a.PRODUCTION_LINE_ID                       --产线                         
    ,a.PRODUCTION_LINE_DESCR                    --产线                         
    ,a.RECYCLE_DEATH_CNT                        --回收路途死亡数               
    ,a.RECYCLE_CNT                              --回收只数                     
    ,b.BEST_WEIGTH_NUM                          --最佳只重数量                 
    ,a.RECYCLE_WEIGHT_AMT                       --总回收量                     
    ,b.CLOSE_RANGE_CNT                          --近距离数                     
    ,a.EMPTY_TIMES                              --空鸡/空鸭时间                
    ,a.KILL_LV_CNT                              --宰杀均衡数量                 
    ,a.KILL_LV_IN_CNT                           --宰杀均衡范围内数量           
    ,a.D_PRODUCT_CNT                            --D产量                        
    ,a.PRODUCT_CNT                              --总产量                       
    ,a.PRODUCT_NUM                              --总只数                       
    ,a.PRODUCT_AMT                              --总重量                       
    ,a.KILL_CNT                                 --屠宰只数                     
    ,a.PUT_CNT                                  --投放只数                     
    ,a.PUT_BREED_CNT                            --投入饲料总重量               
    ,a.FEED_DAYS                                --饲养天数                     
    ,a.FEED_PRICE                               --料价                         
    ,${CREATE_TIME}                           --数据推送时间                 
from (SELECT 
    substr(T1.DAY_ID,1,6)      month_id                        --期间(月份)
    ,T1.DAY_ID                               --期间(日)
    ,case when t13.level1_org_id    is null then coalesce(t14.level1_org_id,'-1') else coalesce(t13.level1_org_id,'-1')  end as level1_org_id                --一级组织编码
    ,case when t13.level1_org_descr is null then coalesce(t14.level1_org_descr,'缺失') else coalesce(t13.level1_org_descr,'缺失')  end as level1_org_descr   --一级组织描述
    ,case when t13.level2_org_id is null    then coalesce(t14.level2_org_id,'-1') else coalesce(t13.level2_org_id,'-1')  end as level2_org_id                --二级组织编码
    ,case when t13.level2_org_descr is null then coalesce(t14.level2_org_descr,'缺失') else coalesce(t13.level2_org_descr,'缺失')  end as level2_org_descr   --二级组织描述
    ,case when t13.level3_org_id    is null then coalesce(t14.level3_org_id,'-1') else coalesce(t13.level3_org_id,'-1')  end as level3_org_id                --三级组织编码
    ,case when t13.level3_org_descr is null then coalesce(t14.level3_org_descr,'缺失') else coalesce(t13.level3_org_descr,'缺失')  end as level3_org_descr   --三级组织描述
    ,case when t13.level4_org_id    is null then coalesce(t14.level4_org_id,'-1') else coalesce(t13.level4_org_id,'-1')  end as level4_org_id                --四级组织编码
    ,case when t13.level4_org_descr is null then coalesce(t14.level4_org_descr,'缺失') else coalesce(t13.level4_org_descr,'缺失')  end as level4_org_descr   --四级组织描述
    ,case when t13.level5_org_id    is null then coalesce(t14.level5_org_id,'-1') else coalesce(t13.level5_org_id,'-1')  end as level5_org_id                --五级组织编码
    ,case when t13.level5_org_descr is null then coalesce(t14.level5_org_descr,'缺失') else coalesce(t13.level5_org_descr,'缺失')  end as level5_org_descr   --五级组织描述
    ,case when t13.level6_org_id    is null then coalesce(t14.level6_org_id,'-1') else coalesce(t13.level6_org_id,'-1')  end as level6_org_id                --六级组织编码
    ,case when t13.level6_org_descr is null then coalesce(t14.level6_org_descr,'缺失') else coalesce(t13.level6_org_descr,'缺失')  end as level6_org_descr   --六级组织描述
    ,''  as  level7_org_id                       --组织7级
    ,''  as  level7_org_descr                    --组织7级              
    ,''  as  level1_businesstype_id              --业态1级
    ,''  as  level1_businesstype_name            --业态1级
    ,''  as  level2_businesstype_id              --业态2级
    ,''  as  level2_businesstype_name            --业态2级
    ,''  as  level3_businesstype_id              --业态3级
    ,''  as  level3_businesstype_name            --业态3级
    ,''  as  level4_businesstype_id              --业态4级
    ,''  as  level4_businesstype_name            --业态4级
    ,coalesce(case when T1.PRODUCT_LINE = '鸡' then '1' when T1.PRODUCT_LINE = '鸭' then '2' end,'-1')      as PRODUCTION_LINE_ID              --产线
    ,coalesce(case when T1.PRODUCT_LINE = '鸡' then '鸡线' when T1.PRODUCT_LINE = '鸭' then '鸭线' end,'缺省')    as PRODUCTION_LINE_DESCR         --产线
    ,coalesce(T5.recycle_death_cnt,'0')           as RECYCLE_DEATH_CNT      --回收路途死亡数
    ,coalesce(t2.recycle_cnt,'0')                 as RECYCLE_CNT            --回收只数
    --,coalesce(T3.BEST_WEIGTH_NUM,'0')           as BEST_WEIGTH_NUM        --最佳只重数量
    ,coalesce(t2.recycle_weight_amt,'0')          as RECYCLE_WEIGHT_AMT     --总回收量
    --,coalesce(T3.CLOSE_RANGE_CNT,'0')           as CLOSE_RANGE_CNT        --近距离数
    ,round(coalesce(T5.empty_times,'0'),2)        as EMPTY_TIMES            --空鸡/空鸭时间
    ,coalesce(T5.KILL_LV_CNT,'0')                 as KILL_LV_CNT            --宰杀均衡数量
    ,coalesce(T5.KILL_LV_IN_CNT,'0')              as KILL_LV_IN_CNT         --宰杀均衡范围内数量
    ,coalesce(T4.d_product_cnt,'0')               as D_PRODUCT_CNT          --D产量
    ,coalesce(T4.product_cnt,'0')                 as PRODUCT_CNT            --总产量
    ,coalesce(t1.product_num,'0')                 as PRODUCT_NUM            --总只数
    ,coalesce(t1.product_amt,'0')                 as PRODUCT_AMT            --总重量
    ,coalesce(t1.kill_cnt,'0')                    as KILL_CNT               --屠宰只数
    ,coalesce(t1.put_cnt,'0')                     as PUT_CNT                --投放只数
    ,coalesce(t1.put_breed_cnt,'0')               as PUT_BREED_CNT          --投入饲料总重量
    ,coalesce(T1.feed_days,'0')                   as FEED_DAYS              --饲养天数
    ,coalesce(t2.FEED_PRICE,'0')                  as FEED_PRICE             --料价
    ,${CREATE_TIME}                                              --数据推送时间
FROM TMP_DWP_BIRD_KEY_ALY_DD_4 T1
LEFT JOIN TMP_DWP_BIRD_KEY_ALY_DD_0 T2
ON T1.ORG_ID = T2.ORG_ID AND T1.BUS_TYPE = T2.BUS_TYPE AND T1.product_line = T2.product_line AND T1.DAY_ID = T2.DAY_ID
--LEFT JOIN TMP_DWP_BIRD_KEY_ALY_DD_1 T3
--ON T1.ORG_ID = T3.ORG_ID AND T1.BUS_TYPE = T3.BUS_TYPE AND T1.product_line = T3.product_line AND T1.DAY_ID = T3.DAY_ID
LEFT JOIN TMP_DWP_BIRD_KEY_ALY_DD_2 T4
ON T1.ORG_ID = T4.ORG_ID AND T1.BUS_TYPE = T4.BUS_TYPE AND T1.product_line = T4.product_line AND T1.DAY_ID = T4.DAY_ID
LEFT JOIN TMP_DWP_BIRD_KEY_ALY_DD_3 T5
ON T1.ORG_ID = T5.ORG_ID AND T1.BUS_TYPE = T5.BUS_TYPE AND T1.product_line = T5.product_line AND T1.DAY_ID = T5.DAY_ID
--LEFT JOIN MREPORT_GLOBAL.DIM_ORG_INV_MANAGEMENT T11 ON T1.INV_ORG_ID = T11.INV_ORG_ID  --7级库存组织
LEFT JOIN MREPORT_GLOBAL.DIM_ORG_BUSINESSTYPE T12 ON T1.BUS_TYPE = T12.LEVEL4_BUSINESSTYPE_ID --业态
left join mreport_global.dim_org_management t13 on t1.org_id=t13.org_id  and t13.attribute5='1'
left join mreport_global.dim_org_management t14 on t1.org_id=t14.org_id and t1.bus_type=t14.bus_type_id and t14.attribute5='2'
) a
left join (select * from TMP_DWP_BIRD_KEY_ALY_DD_5 where op_day = '${OP_DAY}' ) b
on a.DAY_ID               =     b.DAY_ID                    
and a.LEVEL1_ORG_ID       =     b.LEVEL1_ORG_ID             
and a.LEVEL2_ORG_ID       =     b.LEVEL2_ORG_ID             
and a.LEVEL3_ORG_ID       =     b.LEVEL3_ORG_ID             
and a.LEVEL4_ORG_ID       =     b.LEVEL4_ORG_ID             
and a.LEVEL5_ORG_ID       =     b.LEVEL5_ORG_ID             
and a.LEVEL6_ORG_ID       =     b.LEVEL6_ORG_ID
and a.PRODUCTION_LINE_ID  =     b.PRODUCTION_LINE_ID        
"




echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_DMP_BIRD_KEY_ALY_DD;
    $INSERT_DMP_BIRD_KEY_ALY_DD;
"  -v
