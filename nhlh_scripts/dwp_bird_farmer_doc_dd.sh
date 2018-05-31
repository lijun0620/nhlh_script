#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_farmer_doc_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 养户档案信息
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

FORMAT_DAY=$(date -d $OP_DAY"-30 day" +%Y-%m-%d)

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

#
CREATE_TIME_FORMAT=$(date -d " -0 day" +%Y-%m-%d)

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_farmer_doc_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 建立临时表，用于存放养殖户的棚舍地址数量
TMP_DWP_BIRD_FARMER_DOC_DD_1='TMP_DWP_BIRD_FARMER_DOC_DD_1'

CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FARMER_DOC_DD_1(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级
  ,production_line_id           string     --产线ID
  ,production_line_descr        string     --产线名称
  ,farmer_id                    string     --养殖户ID     
  ,farmer_name                  string     --养殖户名称  
  ,farmer_addr                  string     --养殖户地址   
  ,tax_no                       string     --纳税登记号
  ,farmer_level                 string     --等级        
  ,breed_age                    string     --饲养年龄      
  --,is_loss_in                   string     --是否内部流失    
  --,is_loss_out                  string     --是否外部流失    
  --,booth_addr_cnt               string     --棚舍地址数量    
  ,booth_addr                   string     --棚舍地址      
  ,booth_cnt                    string     --棚舍数量 
  ,booth_code                   string     --棚舍编码     
  ,longitude                    string     --经度       
  ,latitude                     string     --纬度
  ,breed_scale                  string     --养殖规模      
  ,booth_type                   string     --棚舍类型ID    
  ,booth_type_name              string     --棚舍类型名称    
  ,breed_type                   string     --养殖类型ID    
  ,breed_type_name              string     --养殖类型名称    
  ,hardware_con                 string     --硬件条件
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FARMER_DOC_DD_1 PARTITION(op_day='$OP_DAY')
SELECT '$OP_MONTH' month_id             --期间(月份)    
       ,'$OP_DAY' day_id                --期间(日)     
       ,t5.level1_org_id                --组织1级(股份)  
       ,t5.level1_org_descr             --组织1级(股份)  
       ,t5.level2_org_id                --组织2级(片联)  
       ,t5.level2_org_descr             --组织2级(片联)  
       ,t5.level3_org_id                --组织3级(片区)  
       ,t5.level3_org_descr             --组织3级(片区)  
       ,t5.level4_org_id                --组织4级(小片)  
       ,t5.level4_org_descr             --组织4级(小片)  
       ,t5.level5_org_id                --组织5级(公司)  
       ,t5.level5_org_descr             --组织5级(公司)  
       ,t5.level6_org_id                --组织6级(OU)  
       ,t5.level6_org_descr             --组织6级(OU)  
       ,'' level7_org_id                --组织7级(库存组织)
       ,'' level7_org_descr             --组织7级(库存组织)
       ,'' level1_businesstype_id       --业态1级      
       ,'' level1_businesstype_name     --业态1级      
       ,'' level2_businesstype_id       --业态2级      
       ,'' level2_businesstype_name     --业态2级      
       ,'' level3_businesstype_id       --业态3级      
       ,'' level3_businesstype_name     --业态3级      
       ,'' level4_businesstype_id       --业态4级      
       ,'' level4_businesstype_name     --业态4级
       ,case when t4.BREED_Mode='鸡' then '10'
             when t4.BREED_Mode='鸭' then '20' end production_line_id
       ,case when t4.BREED_Mode='鸡' then '鸡线'
             when t4.BREED_Mode='鸭' then '鸭线' end production_line_descr
       ,t4.vendor_code        farmer_id                    --养殖户ID     
       ,regexp_replace(t4.vendor_name,'\011','')        farmer_name                  --养殖户名称     
       ,regexp_replace(t1.detailed_address,'\011','')   farmer_addr                  --养殖户地址
       ,t4.vendor_registration_num   tax_no                       --纳税登记号     
       ,case when t4.BREED_Mode='鸡' and t4.breed_scale>=50000 and t4.BREED_CLASS_LEVEL1='封闭' and t4.BREED_CLASS_LEVEL3 in ('网养','笼养') and t4.hardware_cond='A' then 'A级'
             when t4.BREED_Mode='鸡' and t4.breed_scale<=10000 and t4.BREED_CLASS_LEVEL1='开放' and t4.BREED_CLASS_LEVEL3 in ('地养') and t4.hardware_cond='C' then 'C级'
             when t4.BREED_Mode='鸭' and t4.breed_scale>=10000 and t4.BREED_CLASS_LEVEL1='封闭' and t4.BREED_CLASS_LEVEL3 in ('网养','笼养') and t4.hardware_cond='A' then 'A级'
             when t4.BREED_Mode='鸭' and t4.breed_scale<=5000 and t4.BREED_CLASS_LEVEL1='开放' and t4.BREED_CLASS_LEVEL3 in ('地养') and t4.hardware_cond='C' then 'C级'
         else 'B级' end farmer_level    --等级        
       ,case when t6.contract_date is not null 
	    then datediff('$CREATE_TIME_FORMAT',t6.contract_date) else 0 end breed_age                    --饲养年龄    
	   --,'' is_loss_in                 --是否内部流失    
       --,'' is_loss_out                --是否外部流失    
       ,t4.Address  booth_addr                --棚舍地址      
       ,t4.SHED_QTY booth_cnt                 --棚舍数量  
       ,t4.invoice_ID booth_code                --棚舍编码    
       ,t4.longitude   longitude        --经度       
       ,t4.latitude    latitude         --维度
       ,t4.breed_scale breed_scale      --养殖规模      
       ,t4.BREED_CLASS_LEVEL1   booth_type       --棚舍类型ID    
       ,t4.BREED_CLASS_LEVEL1   booth_type_name  --棚舍类型名称    
       ,t4.BREED_CLASS_LEVEL3  breed_type        --养殖类型ID    
       ,t4.BREED_CLASS_LEVEL3 breed_type_name    --养殖类型名称    
       ,t4.hardware_cond hardware_con   --硬件条件
  FROM (SELECT org_id             --公司编码   
               ,org_name           --公司     
               ,vendor_id          --供应商ID  
               ,vendor_code        --供应商编码  
               ,vendor_name        --供应商名称  
               ,vendor_site_id     --供应商地点ID
               ,admin_area         --行政区域   
               ,country_id         --国家ID   
               ,country            --国家     
               ,province_id        --省ID    
               ,province           --省      
               ,city_id            --城市ID   
               ,city               --城市     
               ,county_id          --区县ID   
               ,county             --区县     
               ,detailed_address   --详细地址   
          FROM mreport_global.dwu_dim_vendor_site where end_date_active is null) t1
  inner JOIN (SELECT t2.vendor_id                 --供应商id
                    ,t2.vendor_code               --供应商编码
                    ,t2.vendor_name               --供应商名称
                    ,t2.vendor_type_id            --供应商类型id
                    ,t2.vendor_type               --供应商类型
                    ,t2.vendor_importance_id      --供应商重要性id
                    ,t2.vendor_importance         --供应商重要性
                    ,t2.vendor_trans_relation_id  --供应商交易关系id
                    ,t2.vendor_trans_relation     --供应商交易关系
                    ,t2.vendor_registration_num   --供应商税号
                    ,t3.invoice_id                --棚舍编码
                    ,t3.breed_farm_id             --纳税登记号
                    ,t3.breed_scale               --养殖规模
                    ,t3.longitude                 --经度
                    ,t3.latitude                  --维度
                    ,t3.Address                   --棚舍地址
                    ,t3.SHED_QTY                  --棚舍数量
                    ,t3.BREED_CLASS_LEVEL1        --棚舍类型名称
                    ,t3.BREED_CLASS_LEVEL3        --养殖类型
                    ,t3.BREED_Mode                 --养殖模式
                    ,t3.hardware_cond             --硬件条件
               FROM mreport_global.dwu_dim_vendor t2
           left join mreport_global.dwu_dim_b_w_porcelain t3 on t2.vendor_registration_num=t3.breed_farm_id	
           where t2.end_date_active is null
             and t2.vendor_type_id='06'    --只抓取养殖户的数据
              )  t4 on t1.vendor_code=t4.vendor_code
  inner JOIN (SELECT org_id,
                 level1_org_id,level1_org_descr,
                 level2_org_id,level2_org_descr,
                 level3_org_id,level3_org_descr,
                 level4_org_id,level4_org_descr,
                 level5_org_id,level5_org_descr,
                 level6_org_id,level6_org_descr
               FROM mreport_global.dim_org_management 
              WHERE org_id is not null and attribute2='养殖服务' and (level6_org_id not like '%A%' and   level6_org_id not like '%B%' )
             group by org_id,
                 level1_org_id,level1_org_descr,
                 level2_org_id,level2_org_descr,
                 level3_org_id,level3_org_descr,
                 level4_org_id,level4_org_descr,
                 level5_org_id,level5_org_descr,
                 level6_org_id,level6_org_descr ) t5  ---剔除重复组织和虚拟出来的组织
    ON (t1.org_id=t5.org_id)
  left join(
	SELECT
		vendor_code,
		MIN(contract_date) contract_date
	FROM
		dwu_qw_contract_dd where vendor_code is not null
	GROUP BY
		vendor_code
  )t6 on t6.vendor_code=t1.vendor_code
"


#############################################################
##根据QW03合同表处理：是否内部流失/是否外部流失 两个字段打标
#############################################################


TMP_DWP_BIRD_FARMER_DOC_DD_2='TMP_DWP_BIRD_FARMER_DOC_DD_2'

CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_2="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FARMER_DOC_DD_2(
   org_id                string     --组织6级
  ,vendor_code           string     --养殖户ID
  ,CONTRACT_DATE         string     --合同签订时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

##提取period_id日期在最近一个月之内签订的养殖户清单
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_2="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FARMER_DOC_DD_2 PARTITION(op_day='$OP_DAY')
SELECT org_id,vendor_code,CONTRACT_DATE 
  FROM mreport_poultry.DWU_QW_CONTRACT_DD
where op_day='$OP_DAY'
  and datediff('$FORMAT_DAY',CONTRACT_DATE)<0
"



TMP_DWP_BIRD_FARMER_DOC_DD_3='TMP_DWP_BIRD_FARMER_DOC_DD_3'

CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_3="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FARMER_DOC_DD_3(
   org_id                string     --组织6级
  ,vendor_code           string     --养殖户ID
  ,CONTRACT_DATE         string     --合同签订时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

##提取period_id日期在最近一个月之外签订的养殖户清单
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_3="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FARMER_DOC_DD_3 PARTITION(op_day='$OP_DAY')
SELECT org_id,vendor_code,CONTRACT_DATE 
  FROM mreport_poultry.DWU_QW_CONTRACT_DD
where op_day='$OP_DAY'
  and datediff('$FORMAT_DAY',CONTRACT_DATE)>=0
"


######抓取两段时间内都签订的养殖户
TMP_DWP_BIRD_FARMER_DOC_DD_4='TMP_DWP_BIRD_FARMER_DOC_DD_4'

CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_4="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_FARMER_DOC_DD_4(
   a1_org_id                string  
  ,a1_vendor_code           string  
  ,a2_org_id                string   
  ,a2_vendor_code           string  
  ,mark                     string 
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

##内关联，都存在，但组织ID不一样为内部流失 ,mark为1时即为内部流失用户
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_4="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_FARMER_DOC_DD_4 PARTITION(op_day='$OP_DAY')

select a.a1_org_id,
       a.a1_vendor_code,
       a.a2_org_id,
       a.a2_vendor_code,
       case when a.a1_org_id=a.a2_org_id then 0
       else 1 end mark
   from
   (
SELECT a1.org_id       a1_org_id,
       a1.vendor_code  a1_vendor_code,
       a2.org_id       a2_org_id,
       a2.vendor_code  a2_vendor_code
  FROM $TMP_DWP_BIRD_FARMER_DOC_DD_2 a1 
  inner join $TMP_DWP_BIRD_FARMER_DOC_DD_3 a2 on a1.vendor_code=a2.vendor_code
where a1.op_day='$OP_DAY'
  and a2.op_day='$OP_DAY'
  ) a
"


###########################################################################################
## 将数据从大表转换至目标表
DWP_BIRD_FARMER_DOC_DD='DMP_BIRD_FARMER_DOC_DD'

CREATE_DWP_BIRD_FARMER_DOC_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_FARMER_DOC_DD(
  month_id                      string     --期间(月份)    
  ,day_id                       string     --期间(日)     
  ,level1_org_id                string     --组织1级(股份)  
  ,level1_org_descr             string     --组织1级(股份)  
  ,level2_org_id                string     --组织2级(片联)  
  ,level2_org_descr             string     --组织2级(片联)  
  ,level3_org_id                string     --组织3级(片区)  
  ,level3_org_descr             string     --组织3级(片区)  
  ,level4_org_id                string     --组织4级(小片)  
  ,level4_org_descr             string     --组织4级(小片)  
  ,level5_org_id                string     --组织5级(公司)  
  ,level5_org_descr             string     --组织5级(公司)  
  ,level6_org_id                string     --组织6级(OU)  
  ,level6_org_descr             string     --组织6级(OU)  
  ,level7_org_id                string     --组织7级(库存组织)
  ,level7_org_descr             string     --组织7级(库存组织)
  ,level1_businesstype_id       string     --业态1级      
  ,level1_businesstype_name     string     --业态1级      
  ,level2_businesstype_id       string     --业态2级      
  ,level2_businesstype_name     string     --业态2级      
  ,level3_businesstype_id       string     --业态3级      
  ,level3_businesstype_name     string     --业态3级      
  ,level4_businesstype_id       string     --业态4级      
  ,level4_businesstype_name     string     --业态4级      
  ,production_line_id           string     --产线
  ,production_line_descr        string     --
  ,farmer_id                    string     --养殖户ID     
  ,farmer_name                  string     --养殖户名称   
  ,farmer_addr                  string     --养殖户地址
  ,tax_no                       string     --纳税登记号     
  ,farmer_level                 string     --等级        
  ,breed_age                    string     --饲养年龄      
  ,is_loss_in                   string     --是否内部流失    
  ,is_loss_out                  string     --是否外部流失
  ,booth_addr                   string     --棚舍地址      
  ,booth_cnt                    string     --棚舍数量   
  ,booth_code                   string     --棚舍编码   
  ,longitude_dimension          string     --经纬度       
  ,breed_scale                  string     --养殖规模      
  ,booth_type                   string     --棚舍类型ID    
  ,booth_type_name              string     --棚舍类型名称    
  ,breed_type                   string     --养殖类型ID    
  ,breed_type_name              string     --养殖类型名称    
  ,hardware_con                 string     --硬件条件
  ,create_time                  string      --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_FARMER_DOC_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_FARMER_DOC_DD PARTITION(op_day='$OP_DAY')
SELECT   
        t1.month_id                       --期间(月份)    
       ,t1.day_id                        --期间(日)     
       ,t1.level1_org_id                 --组织1级(股份)  
       ,t1.level1_org_descr              --组织1级(股份)  
       ,t1.level2_org_id                 --组织2级(片联)  
       ,t1.level2_org_descr              --组织2级(片联)  
       ,t1.level3_org_id                 --组织3级(片区)  
       ,t1.level3_org_descr              --组织3级(片区)  
       ,t1.level4_org_id                 --组织4级(小片)  
       ,t1.level4_org_descr              --组织4级(小片)  
       ,t1.level5_org_id                 --组织5级(公司)  
       ,t1.level5_org_descr              --组织5级(公司)  
       ,t1.level6_org_id                 --组织6级(OU)  
       ,t1.level6_org_descr              --组织6级(OU)  
       ,t1.level7_org_id                 --组织7级(库存组织)
       ,t1.level7_org_descr              --组织7级(库存组织)
       ,t1.level1_businesstype_id        --业态1级      
       ,t1.level1_businesstype_name      --业态1级      
       ,t1.level2_businesstype_id        --业态2级      
       ,t1.level2_businesstype_name      --业态2级      
       ,t1.level3_businesstype_id        --业态3级      
       ,t1.level3_businesstype_name      --业态3级      
       ,t1.level4_businesstype_id        --业态4级      
       ,t1.level4_businesstype_name      --业态4级      
       ,t1.production_line_id
       ,t1.production_line_descr
       ,COALESCE(t1.farmer_id,'缺失')                     --养殖户ID     
       ,COALESCE(t1.farmer_name,'缺失')                   --养殖户名称     
       ,COALESCE(t1.farmer_addr,'缺失')                   --养殖户地址
       ,COALESCE(t1.tax_no,'缺失')                        --纳税登记号     
       ,COALESCE(t1.farmer_level,'缺失')                  --等级        
       ,COALESCE(t1.breed_age,'缺失')                     --饲养年龄      
       ,case when t2.a1_vendor_code is not null then 'Y' else 'N' end is_loss_in             --是否内部流失    
       ,case when t3.vendor_code is null then 'Y' else 'N' end is_loss_out                   --是否外部流失
       ,COALESCE(t1.booth_addr,'缺失')                    --棚舍地址      
       ,COALESCE(t1.booth_cnt,0)                     --棚舍数量
       ,COALESCE(t1.booth_code,'缺失')               --棚舍编码
       ,COALESCE(concat(t1.longitude,',',t1.latitude),'缺失')           --经纬度       
       ,COALESCE(t1.breed_scale,0)                   --养殖规模      
       ,COALESCE(t1.booth_type,'缺失')                    --棚舍类型ID    
       ,COALESCE(t1.booth_type_name,'缺失')               --棚舍类型名称    
       ,COALESCE(t1.breed_type,'缺失')                    --养殖类型ID    
       ,COALESCE(t1.breed_type_name,'缺失')               --养殖类型名称    
       ,COALESCE(t1.hardware_con,'缺失')                  --硬件条件
       ,'$CREATE_TIME' create_time
  FROM  $TMP_DWP_BIRD_FARMER_DOC_DD_1 t1
  left outer join (select a1_vendor_code from $TMP_DWP_BIRD_FARMER_DOC_DD_4 where mark=1 and OP_DAY='$OP_DAY' group by a1_vendor_code) t2 on t1.farmer_id=t2.a1_vendor_code
  left outer join (select vendor_code from $TMP_DWP_BIRD_FARMER_DOC_DD_2 where OP_DAY='$OP_DAY' group by vendor_code) t3 on t1.farmer_id=t3.vendor_code
  WHERE t1.OP_DAY='$OP_DAY'
  
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_1;
    $INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_1;
    $CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_2;
    $INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_2;
    $CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_3;
    $INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_3;
    $CREATE_TMP_DWP_BIRD_FARMER_DOC_DD_4;
    $INSERT_TMP_DWP_BIRD_FARMER_DOC_DD_4;
    $CREATE_DWP_BIRD_FARMER_DOC_DD;
    $INSERT_DWP_BIRD_FARMER_DOC_DD;

"  -v
