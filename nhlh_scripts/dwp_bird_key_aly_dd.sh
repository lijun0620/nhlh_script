#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_key_aly_dd.sh                               
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
    echo "输入参数错误，调用示例: dwp_bird_key_aly_dd.sh 20180101"
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
TMP_DWP_BIRD_KEY_ALY_DD_0='TMP_DWP_BIRD_KEY_ALY_DD_0'

CREATE_TMP_DWP_BIRD_KEY_ALY_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KEY_ALY_DD_0(
    DAY_ID                                STRING  --期间(日)
    ,ORG_ID                               STRING  --OU组织  
    ,BUS_TYPE                             STRING  --业态
    ,PRODUCT_LINE                         STRING  --产线
    ,FEED_PRICE                           STRING  --料价
    ,recycle_cnt	                        STRING  --回收只数
    ,recycle_weight_amt                   STRING  --总回收量
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KEY_ALY_DD_0="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KEY_ALY_DD_0 PARTITION(op_day='${OP_DAY}')
select t2.js_date
,t1.ORG_ID
,'132020'   --业态
,CASE WHEN t1.MEANING = 'CHICKEN' THEN '鸡' 
      WHEN t1.MEANING = 'DUCK' THEN '鸭'
      end 
,MAX(coalesce(T1.FEEDOUTPRICE,0))
,sum(t2.KILLED_QTY) KILLED_QTY
,sum(t2.BUY_WEIGHT) BUY_WEIGHT
 FROM (
       select PITH_NO
       ,regexp_replace(substr(JS_DATE,1,10),'-','') js_date
       ,sum(KILLED_QTY) KILLED_QTY  --回收只数
       ,sum(BUY_WEIGHT) BUY_WEIGHT  --回收重量
       from MREPORT_POULTRY.DWU_QW_QW11_DD --qw11
       WHERE OP_DAY = '${OP_DAY}'
       group by PITH_NO,regexp_replace(substr(JS_DATE,1,10),'-','')
      ) T2  --qw11
 left JOIN MREPORT_POULTRY.DWU_QW_CONTRACT_DD T1  --QW03合同信息
  ON T1.CONTRACTNUMBER = T2.PITH_NO AND T1.OP_DAY= '${OP_DAY}'
group by t1.ORG_ID
,t2.js_date
,CASE WHEN t1.MEANING = 'CHICKEN' THEN '鸡' 
      WHEN t1.MEANING = 'DUCK' THEN '鸭'
      end 
"

  
             
             
###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KEY_ALY_DD_1='TMP_DWP_BIRD_KEY_ALY_DD_1'

CREATE_TMP_DWP_BIRD_KEY_ALY_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KEY_ALY_DD_1(
    DAY_ID                                STRING  --期间(日)
    ,ORG_ID                               STRING  --OU组织  
    ,BUS_TYPE                             STRING  --业态
    ,product_line                         STRING  --产线
    ,BEST_WEIGTH_NUM                      STRING  --最佳只重数量
    ,CLOSE_RANGE_CNT                      STRING  --近距离数
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KEY_ALY_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KEY_ALY_DD_1 PARTITION(op_day='${OP_DAY}')
  SELECT 
     T1.WeighTime
     ,T1.ORG_ID
     ,'132020'
     ,T1.product_line
     ,SUM(CASE WHEN T1.AVGWEIGHT * 2 BETWEEN T2.BEST_WEIGHT_FROM AND T2.BEST_WEIGHT_TO THEN T1.FACTNUMBER ELSE 0 END )  --区间重量为斤，过磅中重量为KG
     ,SUM(CASE WHEN T1.MILEAGE <= 50 THEN T1.FACTNUMBER ELSE 0 END)
   FROM (
            SELECT 
                   A.WeighTime
                  ,B.OU_ORG_ID ORG_ID        --机构号
                  ,'132020'
                  ,coalesce(D.product_line,E.product_line,SUBSTR(A.WORKSHOP,1,1)) product_line
                  ,A.AVGWEIGHT     --平均重量
                  ,A.FACTNUMBER    --数量
                  ,A.MILEAGE       --里程
              FROM  
                (SELECT ORG_ID,CONTRACT_ID,CALLBACK_ID,AVGWEIGHT,FACTNUMBER,MILEAGE,WORKSHOP,regexp_replace(substr(WeighTime,1,10),'-','') WeighTime FROM MREPORT_POULTRY.DWU_QW_WEIGHFREIGHT_DD  
                 WHERE OP_DAY = '${OP_DAY}' and BillState = '1' --筛选有效数据
                 ) A  --过磅运费
             INNER JOIN MREPORT_GLOBAL.DIM_ORG_INV_MANAGEMENT B  --库存组织转换(根据机构过滤掉垃圾数据)
                 ON A.ORG_ID = B.LEVEL7_ORG_ID
                
             left join (
                 select 
                     DISTINCT
                     t1.PITH_NO
                     ,t1.ORG_ID
                     ,CASE WHEN t2.MEANING = 'CHICKEN' THEN '鸡' WHEN t2.MEANING = 'DUCK' THEN '鸭' end product_line
                from 
                  (select * from MREPORT_POULTRY.DWU_QW_QW11_DD
                   WHERE OP_DAY = '${OP_DAY}' ) t1
                 left join MREPORT_POULTRY.DWU_QW_CONTRACT_DD T2 --QW03合同信息
                 ON T2.CONTRACTNUMBER = T1.PITH_NO AND T2.OP_DAY= '${OP_DAY}'
             ) D
             on A.CONTRACT_ID = D.PITH_NO AND B.OU_ORG_ID = D.ORG_ID
             
             LEFT JOIN (
                        select 
                        DISTINCT
                     t1.CACU_DOC_NO
                     ,t1.ORG_ID
                     ,CASE WHEN t2.MEANING = 'CHICKEN' THEN '鸡' WHEN t2.MEANING = 'DUCK' THEN '鸭' end product_line
                 from (select * from MREPORT_POULTRY.DWU_QW_QW11_DD
                   WHERE OP_DAY = '${OP_DAY}' ) t1
                 left join MREPORT_POULTRY.DWU_QW_CONTRACT_DD T2 --QW03合同信息
                 ON T2.CONTRACTNUMBER = T1.PITH_NO AND T2.OP_DAY= '${OP_DAY}'
             ) E
          ON A.CALLBACK_ID = E.CACU_DOC_NO AND B.OU_ORG_ID = E.ORG_ID
     ) T1
     
  left join (
     select 
         b.account_ou_id org_id
         ,a.BEST_WEIGHT_FROM
         ,a.BEST_WEIGHT_TO
         ,a.KPI_TYPE
     from (select * from DWU_QW_QW12_DD where op_day='${OP_DAY}') a
     left JOIN MREPORT_GLOBAL.ODS_EBS_CUX_3_GL_COOP_ACCOUNT b   --中间表
      ON a.ORG_ID = b.org_id
    ) t2
   ON T1.ORG_ID = T2.ORG_ID AND T1.PRODUCT_LINE = T2.KPI_TYPE
  group by      
      T1.WeighTime
     ,T1.ORG_ID
     ,T1.product_line
"




###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KEY_ALY_DD_2='TMP_DWP_BIRD_KEY_ALY_DD_2'

CREATE_TMP_DWP_BIRD_KEY_ALY_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KEY_ALY_DD_2(
    DAY_ID                                STRING  --期间(日)
    ,ORG_ID                               STRING  --OU组织  
    ,BUS_TYPE                             STRING  --业态
    ,product_line                         STRING  --产线
    ,d_product_cnt                        STRING  --D产量
    ,product_cnt                          STRING  --总产量
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KEY_ALY_DD_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KEY_ALY_DD_2 PARTITION(op_day='${OP_DAY}')
           SELECT A.PERIOD_ID
           ,A.ORG_ID
           ,'132020'
           ,CASE WHEN A.PRODUCT_LINE = '10' THEN '鸡' WHEN A.PRODUCT_LINE = '20' THEN '鸭' END
           ,SUM(CASE WHEN B.IS_D_PRODUCT = 'Y' THEN A.PRIMARY_QUANTITY ELSE 0 END) AS D_PRODUCT_CNT
           ,SUM(A.PRIMARY_QUANTITY) AS PRODUCT_CNT
           FROM (
           select * from MREPORT_POULTRY.DWU_TZ_STORAGE_TRANSATION02_DD
           where OP_DAY = '${OP_DAY}'
           ) A --TZ02成品
           LEFT JOIN MREPORT_GLOBAL.DWU_DIM_MATERIAL_NEW B   --EBS物料表
            ON A.ITEM_ID = B.INVENTORY_ITEM_ID AND A.ORGANIZATION_ID = B.INV_ORG_ID
           GROUP BY A.PERIOD_ID
           ,A.ORG_ID
           ,CASE WHEN A.PRODUCT_LINE = '10' THEN '鸡' WHEN A.PRODUCT_LINE = '20' THEN '鸭' END
"


###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KEY_ALY_DD_3='TMP_DWP_BIRD_KEY_ALY_DD_3'

CREATE_TMP_DWP_BIRD_KEY_ALY_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KEY_ALY_DD_3(
    DAY_ID                                STRING  --期间(日)
    ,ORG_ID                               STRING  --OU组织  
    ,BUS_TYPE                             STRING  --业态
    ,product_line                         STRING  --产线
    ,empty_times                          STRING  --空鸡/空鸭时间
    ,KILL_LV_CNT                          STRING  --宰杀总量
    ,KILL_LV_IN_CNT                       STRING  --宰杀均衡内数量
    ,recycle_death_cnt                    STRING  --回收路途死亡数

)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KEY_ALY_DD_3="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KEY_ALY_DD_3 PARTITION(op_day='${OP_DAY}')

select t1.day_id
,ORG_ID
,bus_type
,PRODUCT_LINE
,sum(case when t1.day_id = t2.day_id then t2.EMPTY_TIMES else 0 end)
,sum(case when t1.day_id >= t2.day_id then t2.KILL_LV_CNT else 0 end)
,sum(case when t1.day_id >= t2.day_id then t2.KILL_LV_IN_CNT else 0 end)
,sum(case when t1.day_id = t2.day_id then t2.RECYCLE_DEATH_CNT else 0 end)
 from 
(select day_id,month_id from mreport_global.dim_day a where day_id BETWEEN '20151201' AND from_unixtime(unix_timestamp(),'yyyyMMdd') )t1
      inner join 
      (
           SELECT PERIOD_ID day_id
           ,ORG_ID
           ,'132020' bus_type
           ,CASE WHEN A.PRODUCT_LINE = '10' THEN '鸡' WHEN A.PRODUCT_LINE = '20' THEN '鸭' END PRODUCT_LINE
           ,SUM(A.RAW_CHICKEN_DUCK_TIME) AS EMPTY_TIMES
           ,1 AS KILL_LV_CNT
           ,SUM(CASE WHEN A.SLAUGHTER_FLAG = 'Y' THEN 1 ELSE 0 END) AS KILL_LV_IN_CNT
           ,SUM(DEATH_NUMBER) AS RECYCLE_DEATH_CNT    --回收路途死亡数
           FROM MREPORT_POULTRY.DWU_QTZ_PRODUCT_MANAGE_DD A --TZ08生产过程指标
           WHERE A.OP_DAY = '${OP_DAY}'
            GROUP BY PERIOD_ID,ORG_ID,BUS_TYPE,PRODUCT_LINE
       ) t2
       on t1.month_id = substr(t2.day_id,1,6)
       where t1.day_id >= t2.day_id
group by t1.day_id
,ORG_ID
,bus_type
,PRODUCT_LINE
"



###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_KEY_ALY_DD_4='TMP_DWP_BIRD_KEY_ALY_DD_4'

CREATE_TMP_DWP_BIRD_KEY_ALY_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_KEY_ALY_DD_4(
    DAY_ID                                STRING  --期间(日)
    ,ORG_ID                               STRING  --OU组织  
    ,BUS_TYPE                             STRING  --业态
    ,product_line                         STRING  --产线
    ,product_num                          STRING  --总只数
    ,product_amt                          STRING  --总重量
    ,kill_cnt                             STRING  --屠宰只数
    ,put_cnt                              STRING  --投放只数
    ,put_breed_cnt                        STRING  --投入饲料总重量
    ,feed_days                            STRING  --饲养天数
)
 PARTITIONED BY (OP_DAY STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS ORC
"
## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据从转换至目标表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_KEY_ALY_DD_4="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_KEY_ALY_DD_4 PARTITION(op_day='${OP_DAY}')
select recycle_date
,org_id
,'132020'
,product_line
,sum(recycle_qty)        --总只数
,sum(recycle_weight)        --总重量
,sum(recycle_qty)           --屠宰只数
,sum(contract_qty)            --投放只数
,sum(material_weight_qty)      --投入饲料总重量
,AVG(FEED_DAYS)          --饲养天数
from (
      SELECT   regexp_replace(T1.recycle_date,'-','')  recycle_date    --回收日期
             ,T2.org_id
             ,case when T1.production_line_id = '1' then '鸡' when T1.production_line_id = '2' then '鸭' end  product_line   --产线代码
             ,T1.contract_no                      --合同号
             ,avg(datediff ( concat(substr(regexp_replace(recycle_date,'-',''),1,4),'-',substr(regexp_replace(recycle_date,'-',''),5,2),'-',substr(regexp_replace(recycle_date,'-',''),7,2))
                        ,concat(substr(regexp_replace(contract_date,'-',''),1,4),'-',substr(regexp_replace(contract_date,'-',''),5,2),'-',substr(regexp_replace(contract_date,'-',''),7,2))
                        )) FEED_DAYS   --平均饲养天数
             ,SUM(T1.contract_qty)  contract_qty                    --投放数量
             ,SUM(T1.material_weight_qty)    material_weight_qty           --物料重量(kg)
             ,SUM(T1.recycle_qty)    recycle_qty                   --回收数量(支)
             ,SUM(T1.recycle_weight)    recycle_weight                --回收重量(kg)
       FROM (
       select * from DWP_BIRD_FINISHED_DD where op_day = '${OP_DAY}' 
       and recycle_type_descr <> '市场'
      -- AND coalesce(recycle_weight,0) <> 0 
       --and coalesce(recycle_qty,0) <> 0 
       --and regexp_replace(recycle_date,'-','') > regexp_replace(contract_date,'-','')
       ) t1
       LEFT JOIN (SELECT *
                    FROM mreport_global.dim_org_management
                   WHERE org_id is not null) t2
         ON t1.level1_org_id  =  t2.level1_org_id               --组织1级
        and t1.level2_org_id  =  t2.level2_org_id               --组织2级
        and t1.level3_org_id  =  t2.level3_org_id               --组织3级
        and t1.level4_org_id  =  t2.level4_org_id               --组织4级
        and t1.level5_org_id  =  t2.level5_org_id               --组织5级
        and t1.level6_org_id  =  t2.level6_org_id               --组织6级
        group by regexp_replace(T1.recycle_date,'-','')      --回收日期
             ,T2.org_id
             ,case when T1.production_line_id = '1' then '鸡' when T1.production_line_id = '2' then '鸭' end     --产线代码
             ,T1.contract_no 
             
     ) a
group by recycle_date
,org_id
,product_line
"






            

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWP_BIRD_KEY_ALY_DD_0;
    $INSERT_TMP_DWP_BIRD_KEY_ALY_DD_0;
    $CREATE_TMP_DWP_BIRD_KEY_ALY_DD_1;
    $INSERT_TMP_DWP_BIRD_KEY_ALY_DD_1;
    $CREATE_TMP_DWP_BIRD_KEY_ALY_DD_2;
    $INSERT_TMP_DWP_BIRD_KEY_ALY_DD_2;
    $CREATE_TMP_DWP_BIRD_KEY_ALY_DD_3;
    $INSERT_TMP_DWP_BIRD_KEY_ALY_DD_3;
    $CREATE_TMP_DWP_BIRD_KEY_ALY_DD_4;
    $INSERT_TMP_DWP_BIRD_KEY_ALY_DD_4;
"  -v
