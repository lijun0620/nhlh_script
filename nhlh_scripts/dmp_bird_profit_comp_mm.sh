#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_profit_comp_mm.sh                               
# 创建时间: 2018年04月10日                                            
# 创 建 者: lh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-利润构成-股份
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}
OP_YEAR=${OP_DAY:0:4}
OP_LAST_YEAR_MONTH=$(date -d "$OP_DAY -1 years" "+%Y%m" )

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_profit_comp_mm.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DMP_BIRD_PROFIT_COMP_MM_1='TMP_DMP_BIRD_PROFIT_COMP_MM_1'

CREATE_TMP_DMP_BIRD_PROFIT_COMP_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_PROFIT_COMP_MM_1(
       month_id                      string         --期间(月份)
      ,day_id                        string         --期间(日)
      ,level1_org_id                 string         --组织1级(股份)
      ,level1_org_descr              string         --组织1级(股份)
      ,level2_org_id                 string         --组织2级(片联)
      ,level2_org_descr              string         --组织2级(片联)
      ,level3_org_id                 string         --组织3级(片区)
      ,level3_org_descr              string         --组织3级(片区)
      ,level4_org_id                 string         --组织4级(小片)
      ,level4_org_descr              string         --组织4级(小片)
      ,level5_org_id                 string         --组织5级(公司)
      ,level5_org_descr              string         --组织5级(公司)
      ,level6_org_id                 string         --组织6级(OU)
      ,level6_org_descr              string         --组织6级(OU)
      ,level7_org_id                 string         --组织7级(库存组织)
      ,level7_org_descr              string         --组织7级(库存组织)
      ,level1_businesstype_id        string         --业态1级
      ,level1_businesstype_name      string         --业态1级
      ,level2_businesstype_id        string         --业态2级
      ,level2_businesstype_name      string         --业态2级
      ,level3_businesstype_id        string         --业态3级
      ,level3_businesstype_name      string         --业态3级
      ,level4_businesstype_id        string         --业态4级
      ,level4_businesstype_name      string         --业态4级
      ,production_line_id            string         --产线
      ,production_line_descr         string         --产线
      ,feed_income_amt               string         --饲料收入
      ,feed_cost_amt                 string         --饲料销售成本
      ,breed_income_amt              string         --放养种苗收入
      ,breed_cost_amt                string         --放养种苗销售成本
      ,breed_vet_amt                 string         --放养兽药收入
      ,breed_vet_sale_amt            string         --放养兽药销售成本
      ,sale_income_amt               string         --外销收入
      ,sale_cost_amt                 string         --外销成本
      ,fost_income_amt               string         --代养收入
      ,fost_cost_amt                 string         --代养销售成本
      ,tech_serv_amt                 string         --技术服务费
      ,other_amt                     string         --其他业务利润
      ,tax_etc_amt                   string         --税金及附加
      ,total_amt                     string         --费用总额
      ,impair_amt                    string         --资产减值损失
      ,no_oper_income                string         --营业外收支
      ,recycle_cnt                   string         --回收只数
      ,create_time                   string         --数据推送时间
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS TEXTFILE    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_PROFIT_COMP_MM_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_PROFIT_COMP_MM_1  PARTITION(op_month='$OP_MONTH')
SELECT 
    t1.PERIOD_ID --期间(月份)
    ,$OP_DAY --期间(日)
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
    ,t5.LEVEL7_ORG_ID --组织7级(库存组织)
    ,t5.LEVEL7_ORG_DESCR --组织7级(库存组织)
    ,T4.LEVEL1_BUSINESSTYPE_ID --业态1级
    ,T4.LEVEL1_BUSINESSTYPE_NAME --业态1级
    ,T4.LEVEL2_BUSINESSTYPE_ID --业态2级
    ,T4.LEVEL2_BUSINESSTYPE_NAME --业态2级
    ,T4.LEVEL3_BUSINESSTYPE_ID --业态3级
    ,T4.LEVEL3_BUSINESSTYPE_NAME --业态3级
    ,T4.LEVEL4_BUSINESSTYPE_ID --业态4级
    ,T4.LEVEL4_BUSINESSTYPE_NAME --业态4级
    ,coalesce(substr(t1.product_line,1,1),'-1') --产线
    ,coalesce(CASE WHEN T1.product_line = '10' THEN '鸡线' WHEN T1.product_line = '20' THEN '鸭线' ELSE T1.product_line END,'缺省') --产线
    ,sum(COALESCE(feed_income_amt,0))     --饲料收入
    ,sum(COALESCE(feed_cost_amt,0))       --饲料销售成本
    ,sum(COALESCE(breed_income_amt,0))    --放养种苗收入
    ,sum(COALESCE(breed_cost_amt,0))      --放养种苗销售成本
    ,sum(COALESCE(breed_vet_amt,0))       --放养兽药收入
    ,sum(COALESCE(breed_vet_sale_amt,0))  --放养兽药销售成本
    ,sum(COALESCE(sale_income_amt,0))     --外销收入
    ,sum(COALESCE(sale_cost_amt,0))       --外销成本
    ,sum(COALESCE(fost_income_amt,0))     --代养收入
    ,sum(COALESCE(fost_cost_amt,0))       --代养销售成本
    ,sum(COALESCE(tech_serv_amt,0))       --技术服务费
    ,sum(COALESCE(other_amt,0))           --其他业务利润
    ,sum(COALESCE(tax_etc_amt,0))         --税金及附加
    ,sum(COALESCE(total_amt,0))           --费用总额
    ,sum(COALESCE(impair_amt,0))          --资产减值损失
    ,sum(COALESCE(no_oper_income,0))      --营业外收支
    ,sum(COALESCE(recycle_cnt,0))         --回收只数
    ,$CREATE_TIME                    --数据推送时间
FROM (
   select * from MREPORT_POULTRY.DWP_BIRD_PROFIT_COMP_MM
   WHERE OP_MONTH = '$OP_MONTH') T1
 --LEFT JOIN MREPORT_GLOBAL.DIM_ORG_MANAGEMENT T2 ON T1.ORG_ID = T2.ORG_ID AND T1.BUS_TYPE = T2.BUS_TYPE_ID  --6级组织
 left join mreport_global.dim_org_management t2 on t1.org_id=t2.org_id  and t2.attribute5='1'
 left join mreport_global.dim_org_management t3 on t1.org_id=t3.org_id and t1.bus_type=t3.bus_type_id and t3.attribute5='2'
 LEFT JOIN MREPORT_GLOBAL.DIM_ORG_INV_MANAGEMENT T5 ON T1.ORGANIZATION_ID = T5.INV_ORG_ID  --7级库存组织
 LEFT JOIN MREPORT_GLOBAL.DIM_ORG_BUSINESSTYPE T4 ON T1.BUS_TYPE = T4.LEVEL4_BUSINESSTYPE_ID --业态
group by
    t1.PERIOD_ID --期间(月份)
    ,case when t2.level1_org_id    is null then coalesce(t3.level1_org_id,'-1') else coalesce(t2.level1_org_id,'-1')  end
    ,case when t2.level1_org_descr is null then coalesce(t3.level1_org_descr,'缺失') else coalesce(t2.level1_org_descr,'缺失')  end
    ,case when t2.level2_org_id is null    then coalesce(t3.level2_org_id,'-1') else coalesce(t2.level2_org_id,'-1')  end
    ,case when t2.level2_org_descr is null then coalesce(t3.level2_org_descr,'缺失') else coalesce(t2.level2_org_descr,'缺失')  end
    ,case when t2.level3_org_id    is null then coalesce(t3.level3_org_id,'-1') else coalesce(t2.level3_org_id,'-1')  end
    ,case when t2.level3_org_descr is null then coalesce(t3.level3_org_descr,'缺失') else coalesce(t2.level3_org_descr,'缺失')  end
    ,case when t2.level4_org_id    is null then coalesce(t3.level4_org_id,'-1') else coalesce(t2.level4_org_id,'-1')  end
    ,case when t2.level4_org_descr is null then coalesce(t3.level4_org_descr,'缺失') else coalesce(t2.level4_org_descr,'缺失')  end
    ,case when t2.level5_org_id    is null then coalesce(t3.level5_org_id,'-1') else coalesce(t2.level5_org_id,'-1')  end
    ,case when t2.level5_org_descr is null then coalesce(t3.level5_org_descr,'缺失') else coalesce(t2.level5_org_descr,'缺失')  end
    ,case when t2.level6_org_id    is null then coalesce(t3.level6_org_id,'-1') else coalesce(t2.level6_org_id,'-1')  end
    ,case when t2.level6_org_descr is null then coalesce(t3.level6_org_descr,'缺失') else coalesce(t2.level6_org_descr,'缺失')  end
    ,t5.LEVEL7_ORG_ID --组织7级(库存组织)
    ,t5.LEVEL7_ORG_DESCR --组织7级(库存组织)
    ,T4.LEVEL1_BUSINESSTYPE_ID --业态1级
    ,T4.LEVEL1_BUSINESSTYPE_NAME --业态1级
    ,T4.LEVEL2_BUSINESSTYPE_ID --业态2级
    ,T4.LEVEL2_BUSINESSTYPE_NAME --业态2级
    ,T4.LEVEL3_BUSINESSTYPE_ID --业态3级
    ,T4.LEVEL3_BUSINESSTYPE_NAME --业态3级
    ,T4.LEVEL4_BUSINESSTYPE_ID --业态4级
    ,T4.LEVEL4_BUSINESSTYPE_NAME --业态4级
    ,t1.product_line --产线
    ,CASE WHEN T1.product_line = '10' THEN '鸡线' WHEN T1.product_line = '20' THEN '鸭线' ELSE T1.product_line END --产线
"






###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMP_BIRD_PROFIT_COMP_MM='DMP_BIRD_PROFIT_COMP_MM'

CREATE_DMP_BIRD_PROFIT_COMP_MM="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_PROFIT_COMP_MM(
       month_id                      string         --期间(月份)
      ,day_id                        string         --期间(日)
      ,level1_org_id                 string         --组织1级(股份)
      ,level1_org_descr              string         --组织1级(股份)
      ,level2_org_id                 string         --组织2级(片联)
      ,level2_org_descr              string         --组织2级(片联)
      ,level3_org_id                 string         --组织3级(片区)
      ,level3_org_descr              string         --组织3级(片区)
      ,level4_org_id                 string         --组织4级(小片)
      ,level4_org_descr              string         --组织4级(小片)
      ,level5_org_id                 string         --组织5级(公司)
      ,level5_org_descr              string         --组织5级(公司)
      ,level6_org_id                 string         --组织6级(OU)
      ,level6_org_descr              string         --组织6级(OU)
      ,level7_org_id                 string         --组织7级(库存组织)
      ,level7_org_descr              string         --组织7级(库存组织)
      ,level1_businesstype_id        string         --业态1级
      ,level1_businesstype_name      string         --业态1级
      ,level2_businesstype_id        string         --业态2级
      ,level2_businesstype_name      string         --业态2级
      ,level3_businesstype_id        string         --业态3级
      ,level3_businesstype_name      string         --业态3级
      ,level4_businesstype_id        string         --业态4级
      ,level4_businesstype_name      string         --业态4级
      ,production_line_id            string         --产线
      ,production_line_descr         string         --产线
      ,feed_income_amt               string         --饲料收入
      ,feed_cost_amt                 string         --饲料销售成本
      ,breed_income_amt              string         --放养种苗收入
      ,breed_cost_amt                string         --放养种苗销售成本
      ,breed_vet_amt                 string         --放养兽药收入
      ,breed_vet_sale_amt            string         --放养兽药销售成本
      ,sale_income_amt               string         --外销收入
      ,sale_cost_amt                 string         --外销成本
      ,fost_income_amt               string         --代养收入
      ,fost_cost_amt                 string         --代养销售成本
      ,tech_serv_amt                 string         --技术服务费
      ,other_amt                     string         --其他业务利润
      ,tax_etc_amt                   string         --税金及附加
      ,total_amt                     string         --费用总额
      ,impair_amt                    string         --资产减值损失
      ,no_oper_income                string         --营业外收支
      ,recycle_cnt                   string         --回收只数
      ,create_time                   string         --数据推送时间
)                      
 PARTITIONED BY (op_month STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
  STORED AS TEXTFILE    
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_PROFIT_COMP_MM="
INSERT OVERWRITE TABLE $DMP_BIRD_PROFIT_COMP_MM  PARTITION(op_month='$OP_MONTH')
SELECT 
 T1.month_id                   --期间(月份)             
,T1.day_id                     --期间(日)              
,T1.level1_org_id              --组织1级(股份)           
,T1.level1_org_descr           --组织1级(股份)           
,T1.level2_org_id              --组织2级(片联)           
,T1.level2_org_descr           --组织2级(片联)           
,T1.level3_org_id              --组织3级(片区)           
,T1.level3_org_descr           --组织3级(片区)           
,T1.level4_org_id              --组织4级(小片)           
,T1.level4_org_descr           --组织4级(小片)           
,T1.level5_org_id              --组织5级(公司)           
,T1.level5_org_descr           --组织5级(公司)           
,T1.level6_org_id              --组织6级(OU)           
,T1.level6_org_descr           --组织6级(OU)           
,T1.level7_org_id              --组织7级(库存组织)         
,T1.level7_org_descr           --组织7级(库存组织)         
,T1.level1_businesstype_id     --业态1级               
,T1.level1_businesstype_name   --业态1级               
,T1.level2_businesstype_id     --业态2级               
,T1.level2_businesstype_name   --业态2级               
,T1.level3_businesstype_id     --业态3级               
,T1.level3_businesstype_name   --业态3级               
,T1.level4_businesstype_id     --业态4级               
,T1.level4_businesstype_name   --业态4级               
,T1.production_line_id         --产线                 
,T1.production_line_descr      --产线                 
,T1.feed_income_amt            --饲料收入               
,T1.feed_cost_amt              --饲料销售成本             
,T1.breed_income_amt           --放养种苗收入             
,T1.breed_cost_amt             --放养种苗销售成本           
,T1.breed_vet_amt              --放养兽药收入             
,T1.breed_vet_sale_amt         --放养兽药销售成本           
,T1.sale_income_amt            --外销收入               
,T1.sale_cost_amt              --外销成本               
,T1.fost_income_amt            --代养收入               
,T1.fost_cost_amt              --代养销售成本             
,T1.tech_serv_amt              --技术服务费              
,T1.other_amt                  --其他业务利润             
,T1.tax_etc_amt                --税金及附加              
,T1.total_amt                  --费用总额               
,T1.impair_amt                 --资产减值损失             
,T1.no_oper_income             --营业外收支              
,T2.KILLED_QTY                 --回收只数               
,T1.create_time                --数据推送时间             

FROM (
   SELECT * FROM TMP_DMP_BIRD_PROFIT_COMP_MM_1 WHERE OP_MONTH = '${OP_MONTH}'
   ) T1
  
   LEFT JOIN (
             select t3.short_code
             ,CASE WHEN T2.MEANING = 'CHICHEN' THEN '1' WHEN T2.MEANING = 'DUCK' THEN '2' END PRODUCT_LINE
             ,JS_DATE
             ,sum(t1.KILLED_QTY) KILLED_QTY from 
             (
                 select a.pith_no,b.level6_org_id
                 ,regexp_replace(substr(JS_DATE,1,7),'-','') JS_DATE,sum(a.KILLED_QTY) KILLED_QTY 
                 from (
                     SELECT * FROM MREPORT_POULTRY.DWU_QW_QW11_DD 
                     where op_day = '${OP_DAY}'
                     and doc_status in ('已完毕','已审核')
                     ) A
                 inner join MREPORT_GLOBAL.DIM_ORG_INV_MANAGEMENT b
                 on a.ORG_CODE = b.level7_org_id 
                 group by a.pith_no,b.level6_org_id,regexp_replace(substr(JS_DATE,1,7),'-','')
             ) t1
             inner join (
             select * from MREPORT_POULTRY.DWU_QW_CONTRACT_DD where op_day = '${OP_DAY}' )t2 
             on t1.pith_no = t2.contractnumber
             and t2.GUARANTEES_MARKET <> '市场'
             
             INNER JOIN MREPORT_GLOBAL.ODS_EBS_CUX_3_GL_COOP_ACCOUNT T3 --中间表
             ON t1.level6_org_id = T3.account_short_code
             group by t3.short_code,CASE WHEN T2.MEANING = 'CHICHEN' THEN '1' WHEN T2.MEANING = 'DUCK' THEN '2' END,JS_DATE
          ) t2 --QW11 回收信息
  ON t1.level6_org_id = t2.short_code and t1.production_line_id = t2.PRODUCT_LINE AND T1.month_id = T2.JS_DATE
   
"





echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_PROFIT_COMP_MM_1;
    $INSERT_TMP_DMP_BIRD_PROFIT_COMP_MM_1;
    $CREATE_DMP_BIRD_PROFIT_COMP_MM;
    $INSERT_DMP_BIRD_PROFIT_COMP_MM;
"  -v