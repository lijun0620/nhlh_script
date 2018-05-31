#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwp_bird_put_contract_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 投放合同信息表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dwp_bird_put_contract_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 处理投放周期
## 变量声明
TMP_DWP_BIRD_PUT_CONTRACT_DD_00='TMP_DWP_BIRD_PUT_CONTRACT_DD_00'

CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_00="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PUT_CONTRACT_DD_00(
  production_line_id             string    --产线
  ,put_start_date                string    --投放开始日期(当月)
  ,put_end_date                  string    --投放结束日期(当月)
  ,put_start_date_last           string    --投放开始日期(上月)
  ,put_end_date_last             string    --投放结束日期(上月)
  ,put_start_date_next           string    --投放开始日期(下月)
  ,put_end_date_next             string    --投放结束日期(下月)
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_00="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PUT_CONTRACT_DD_00 PARTITION(op_day='$OP_DAY')
SELECT production_line_id
       ,regexp_replace(put_start_date,'-','') put_start_date
       ,regexp_replace(put_end_date,'-','') put_end_date

       ,regexp_replace(put_start_date_last,'-','') put_start_date_last
       ,regexp_replace(put_end_date_last,'-','') put_end_date_last

       ,regexp_replace(put_start_date_next,'-','') put_start_date_next
       ,regexp_replace(put_end_date_next,'-','') put_end_date_next
  FROM (SELECT a1.production_line_id
               ,a1.contract_date
               ,date_add(concat(substr(contract_date,1,8),'01'),-a2.tag) put_start_date
               ,date_add(date_add(concat(substr(date_add(concat(substr(contract_date,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date

               ,date_add(concat(substr(contract_date_last,1,8),'01'),-a2.tag) put_start_date_last
               ,date_add(date_add(concat(substr(date_add(concat(substr(contract_date_last,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date_last

               ,date_add(concat(substr(contract_date_next,1,8),'01'),-a2.tag) put_start_date_next
               ,date_add(date_add(concat(substr(date_add(concat(substr(contract_date_next,1,8),'28'),5),1,8),'01'),-1),-a2.tag) put_end_date_next
          FROM (SELECT substr(contract_date,1,10) contract_date
                       ,date_add(concat(substr(contract_date,1,8),'01'),-1) contract_date_last  --上一个月
                       ,concat(substr(date_add(concat(substr(contract_date,1,8),'28'),5),1,8),'01') contract_date_next --下一个月
                       ,meaning production_line_id
                  FROM dwu_qw_contract_dd
                 WHERE op_day='$OP_DAY'
                 GROUP BY substr(contract_date,1,10)
                       ,date_add(concat(substr(contract_date,1,8),'01'),-1)
                       ,concat(substr(date_add(concat(substr(contract_date,1,8),'28'),5),1,8),'01')
                       ,meaning) a1
          LEFT JOIN (SELECT case when lookup_code='1' then 'CHICHEN'
                                 when lookup_code='2' then 'DUCK'
                            else '-999' end prod_line
                            ,int(tag) tag
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='CUX_ITEM_TYPE_BREED_CYCLE'
                        AND language='ZHS') a2
            ON (a1.production_line_id=a2.prod_line)) t
 GROUP BY production_line_id
       ,put_start_date
       ,put_end_date
       ,put_start_date_last
       ,put_end_date_last
       ,put_start_date_next
       ,put_end_date_next
"

###########################################################################################
## 处理投放周期
## 变量声明
TMP_DWP_BIRD_PUT_CONTRACT_DD_01='TMP_DWP_BIRD_PUT_CONTRACT_DD_01'

CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_01="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PUT_CONTRACT_DD_01(
  production_line_id             string    --产线
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_01="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PUT_CONTRACT_DD_01 PARTITION(op_day='$OP_DAY')
SELECT production_line_id
       ,put_start_date
       ,put_end_date
  FROM (SELECT production_line_id
               ,put_start_date
               ,put_end_date
          FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_00
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT production_line_id
               ,put_start_date_last put_start_date
               ,put_end_date_last put_end_date
          FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_00
         WHERE op_day='$OP_DAY'
        UNION ALL
        SELECT production_line_id
               ,put_start_date_next put_start_date
               ,put_end_date_next put_end_date
          FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_00
         WHERE op_day='$OP_DAY') t
 WHERE put_start_date is not null
 GROUP BY production_line_id
       ,put_start_date
       ,put_end_date
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_PUT_CONTRACT_DD_0='TMP_DWP_BIRD_PUT_CONTRACT_DD_0'

CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PUT_CONTRACT_DD_0(
  contract_no                    string    --合同号
  ,contract_date                 string    --合同日期
  ,production_line_id            string    --产线
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,dmt_plan_qty                  string    --计划投放数量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_0="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PUT_CONTRACT_DD_0 PARTITION(op_day='$OP_DAY')
SELECT contract_no
       ,contract_date
       ,production_line_id
       ,put_start_date
       ,put_end_date
       ,sum(dmt_plan_qty) dmt_plan_qty
  FROM (SELECT contract_no
               ,contract_date
               ,production_line_id
               ,put_start_date
               ,put_end_date
               ,dmt_plan_qty
          FROM (SELECT t1.contract_no                   --合同号
                       ,t1.contract_date                --投放日期
                       ,t1.production_line_id           --产线
                       ,case when t1.contract_date between t2.put_start_date and t2.put_end_date
                             then t2.put_start_date else null end put_start_date --投放开始日期
                       ,case when t1.contract_date between t2.put_start_date and t2.put_end_date
                             then t2.put_end_date else null end put_end_date     --投放结束日期
                       ,'0' dmt_plan_qty                --计划投放数量
                  FROM (SELECT org_id                                            --OU组织
                               ,contractnumber contract_no                       --合同号
                               ,meaning production_line_id                       --产线代码
                               ,regexp_replace(substr(contract_date,1,10),'-','') contract_date      --投放日期
                          FROM dwu_qw_contract_dd
                         WHERE op_day='$OP_DAY') t1
                  LEFT JOIN (SELECT production_line_id       --产线
                                    ,put_start_date           --投放开始日期
                                    ,put_end_date             --投放结束日期
                               FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_01
                              WHERE op_day='$OP_DAY') t2
                    ON (t1.production_line_id=t2.production_line_id)) t
          WHERE put_start_date is not null
          GROUP BY contract_no
               ,contract_date
               ,production_line_id
               ,put_start_date
               ,put_end_date
               ,dmt_plan_qty) a
 GROUP BY contract_no
       ,contract_date
       ,production_line_id
       ,put_start_date
       ,put_end_date
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWP_BIRD_PUT_CONTRACT_DD_1='TMP_DWP_BIRD_PUT_CONTRACT_DD_1'

CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWP_BIRD_PUT_CONTRACT_DD_1(
  month_id                       string    --期间(月)
  ,day_id                        string    --期间(日)
  ,level1_org_id                 string    --组织1级(股份)  
  ,level1_org_descr              string    --组织1级(股份)  
  ,level2_org_id                 string    --组织2级(片联)  
  ,level2_org_descr              string    --组织2级(片联)  
  ,level3_org_id                 string    --组织3级(片区)  
  ,level3_org_descr              string    --组织3级(片区)  
  ,level4_org_id                 string    --组织4级(小片)  
  ,level4_org_descr              string    --组织4级(小片)  
  ,level5_org_id                 string    --组织5级(公司)  
  ,level5_org_descr              string    --组织5级(公司)  
  ,level6_org_id                 string    --组织6级(OU)  
  ,level6_org_descr              string    --组织6级(OU)  
  ,level7_org_id                 string    --组织7级(库存组织)
  ,level7_org_descr              string    --组织7级(库存组织)
  ,level1_businesstype_id        string    --业态1级
  ,level1_businesstype_name      string    --业态1级
  ,level2_businesstype_id        string    --业态2级
  ,level2_businesstype_name      string    --业态2级
  ,level3_businesstype_id        string    --业态3级
  ,level3_businesstype_name      string    --业态3级
  ,level4_businesstype_id        string    --业态4级
  ,level4_businesstype_name      string    --业态4级
  ,currency_id                   string    --币种
  ,contract_no                   string    --合同号
  ,production_line_id            string    --产线代码
  ,production_line_descr         string    --产线描述
  ,farmer_id                     string    --养户号
  ,farmer_name                   string    --养殖单位
  ,farm_addr                     string    --饲养地址
  ,phone_no                      string    --联系方式
  ,salesman_name                 string    --销售员
  ,breed_type_id                 string    --养殖类型
  ,breed_type_descr              string    --养殖类型
  ,if_dsp                        string    --是否直供
  ,contract_date                 string    --合同日期
  ,contract_kill_date            string    --合同宰杀日期
  ,contract_type_descr           string    --合同类型
  ,put_type_id                   string    --投放类型
  ,put_type_descr                string    --投放类型
  ,distance                      string    --养殖距离
  ,recyle_price                  string    --基础回收单价 
  ,factory_price                 string    --饲料综合出厂价
  ,put_cost                      string    --投放成本(公司投放成本)
  ,longitude                     string    --经度
  ,latitude                      string    --纬度
  ,material_id                   string    --物料编码
  ,material_descr                string    --物料名称
  ,m_factory_descr               string    --苗厂
  ,contract_price                string    --苗单价
  ,contract_qty                  string    --投放数量
  ,dmt_plan_qty                  string    --计划投放数量(投放目标)
  ,put_date                      string    --投放日期
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,buy_amount                    string    --金额(苗厂供应金额)     
  ,unit_deposit_amt              string    --只押金    
  ,deposit_balance_amt           string    --押金总额   
  ,drugs_cost                    string    --只药费    
  ,total_drugs_cost              string    --药费总额   
  ,carriage_cost                 string    --苗运费    
  ,seeding_selling_price         string    --苗销售价格  
  ,seeding_buying_price          string    --苗采购价格
  ,drugs_std                     string    --兽药标准
  ,avg_weight                    string    --平均只重
  ,feed_profit                   string    --饲料利润
  ,single_feed_cnt               string    --单只理论用料量
  ,cub_profit                    string    --苗利润
  ,drugs_profit                  string    --兽药利润
  ,trans_profit                  string    --运费利润
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_1="
INSERT OVERWRITE TABLE $TMP_DWP_BIRD_PUT_CONTRACT_DD_1 PARTITION(op_day='$OP_DAY')
SELECT substr(regexp_replace(t1.contract_date,'-',''),1,6) month_id   --投放日期
       ,substr(regexp_replace(t1.contract_date,'-',''),1,8) day_id    --投放日期
       ,t3.level1_org_id
       ,t3.level1_org_descr
       ,t3.level2_org_id
       ,t3.level2_org_descr
       ,t3.level3_org_id
       ,t3.level3_org_descr
       ,t3.level4_org_id
       ,t3.level4_org_descr
       ,t3.level5_org_id
       ,t3.level5_org_descr
       ,t3.level6_org_id
       ,t3.level6_org_descr
       ,t4.level7_org_id
       ,t4.level7_org_descr              
       ,t5.level1_businesstype_id        --业态1级
       ,t5.level1_businesstype_name      --业态1级
       ,t5.level2_businesstype_id        --业态2级
       ,t5.level2_businesstype_name      --业态2级
       ,t5.level3_businesstype_id        --业态3级
       ,t5.level3_businesstype_name      --业态3级
       ,t5.level4_businesstype_id        --业态4级
       ,t5.level4_businesstype_name      --业态4级
       ,t1.currency_id                   --币种
       ,t1.contract_no                   --合同号
       ,case when t1.production_line_id='CHICHEN' then '1'
             when t1.production_line_id='DUCK' then '2'
        else null end production_line_id           --产线代码
       ,case when t1.production_line_id='CHICHEN' then '鸡线'
             when t1.production_line_id='DUCK' then '鸭线'
        else null end production_line_descr  --产线描述
       ,t1.farmer_id                     --养户号
       ,t1.farmer_name                   --养殖单位
       ,t1.farm_addr                     --饲养地址
       ,t1.phone_no                      --联系方式
       ,t1.salesman_name                 --销售员
       ,case when t1.breed_type_descr='代养' then '1'
             when t1.breed_type_descr='放养' then '2'
        else null end breed_type_id
       ,t1.breed_type_descr              --养殖类型
       ,t1.if_dsp                        --是否直供
       ,substr(t1.contract_date,1,10) contract_date   --合同日期
       ,substr(t1.killchickdate,1,10) contract_kill_date  --合同宰杀日期
       ,t1.contract_type_descr           --合同类型
       ,case when t1.put_type_descr='保值' then '1'
             when t1.put_type_descr='保底' then '2'
             when t1.put_type_descr='市场' then '3'
        else null end put_type_id
       ,t1.put_type_descr                --投放类型
       ,t1.distance                      --养殖距离
       ,t1.recyle_price                  --基础回收单价 
       ,t1.factory_price                 --饲料综合出厂价
       ,t1.put_cost                      --投放成本(公司投放成本)
       ,t1.longitude                     --经度
       ,t1.latitude                      --纬度
       ,t1.material_id                   --物料编码
       ,t1.material_descr                --物料名称
       ,t1.m_factory_descr               --苗厂
       ,t1.contract_price                --苗单价
       ,t1.contract_qty                  --投放数量
       ,t2.dmt_plan_qty                  --计划投放数量(投放目标)
       ,substr(t1.contract_date,1,10) put_date  --投放日期
       ,t2.put_start_date                --投放开始日期
       ,t2.put_end_date                  --投放结束日期
       ,t1.buy_amount                    --金额(苗厂供应金额)     
       ,t1.unit_deposit_amt              --只押金    
       ,t7.deposit_balance_amt           --押金总额   
       ,t1.drugs_cost                    --只药费    
       ,t1.total_drugs_cost              --药费总额   
       ,t1.carriage_cost                 --苗运费    
       ,t1.seeding_selling_price         --苗销售价格  
       ,t1.seeding_buying_price          --苗采购价格
       ,t6.drugs_std                     --兽药标准
       ,t6.avg_weight                    --平均只重
       ,t6.feed_profit                   --饲料利润
       ,t6.single_feed_cnt               --单只理论用料量
       ,t6.cub_profit                    --苗利润
       ,t6.drugs_profit                  --兽药利润
       ,t6.trans_profit                  --运费利润
  FROM (SELECT org_id                              --OU组织  
               ,inv_org_id                          --库存组织ID
               ,'' currency_id                      --币种
               ,bus_type                            --业态
               ,contractnumber contract_no          --合同号
               ,meaning production_line_id          --产线代码
               ,meaning_desc production_line_descr  --产线描述
               ,vendor_code farmer_id               --养户号
               ,vendor_name farmer_name             --养殖单位   
               ,breedaddress farm_addr              --饲养地址   
               ,breedcontactway phone_no            --联系方式   
               ,breedsaleman salesman_name          --销售员    
               ,contractprop                        --合同属性 
               ,getchickdate                        --接雏日期
               ,contract_date                       --合同日期   
               ,killchickdate                       --宰杀日期   
               ,contracttype contract_type_descr    --合同类型   
               ,contracttype_grp breed_type_descr   --合同类型分组 
               ,breedcontract_no                    --连养合同   
               ,directly_supply_flag if_dsp         --是否直供   
               ,guarantees_market put_type_descr    --保值保底市场 
               ,distance                            --养殖距离   
               ,basecallbackprice recyle_price      --基础回收单价 
               ,feedoutprice factory_price          --饲料综合出厂价
               ,chicksalemoney put_cost             --投放成本   
               ,breedfactorylongitude longitude     --经度     
               ,breedfactorylatitude latitude       --纬度     
               ,material_code material_id           --物料编码   
               ,material_name material_descr        --物料名称   
               ,hatchery_code                       --孵化场编码  
               ,hatchery_name m_factory_descr       --孵化场    
               ,price contract_price                --苗单价
               ,qty contract_qty                    --投放数量   
               ,amount buy_amount                   --金额     
               ,oneamount unit_deposit_amt          --只押金    
               ,onetotalamount deposit_balance_amt  --押金总额   
               ,mediamount drugs_cost               --只药费    
               ,totoalmediamount total_drugs_cost   --药费总额   
               ,seeding_transport_fee carriage_cost --苗运费    
               ,seeding_selling_price               --苗销售价格  
               ,seeding_buying_price                --苗采购价格
          FROM dwu_qw_contract_dd
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT *
               FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_0
              WHERE op_day='$OP_DAY') t2
    ON (t1.contract_no=t2.contract_no)
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
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t4
    ON (t1.inv_org_id=t4.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t5
    ON (t1.bus_type=t5.level4_businesstype_id)
  LEFT JOIN (SELECT a2.org_id
                    ,case when a1.kpi_type='鸡' then 'CHICHEN'
                          when a1.kpi_type='鸭' then 'DUCK'
                     else null end production_line_id
                    ,a1.drugs_std        --兽药标准
                    ,a1.avg_weight       --平均只重
                    ,a1.feed_profit/2000 feed_profit --饲料利润
                    ,a1.single_feed_cnt  --单只理论用料量
                    ,a1.cub_profit       --苗利润
                    ,a1.drugs_profit     --兽药利润
                    ,a1.trans_profit/2000 trans_profit    --运费利润
               FROM (SELECT *
                       FROM dwu_qw_qw12_dd
                      WHERE op_day='$OP_DAY') a1
              INNER JOIN (SELECT b1.account_ou_id org_id
                                 ,b1.org_id co_org_id       --合作社组织
                            FROM (SELECT org_id
                                         ,account_ou_id
                                         ,last_update_date
                                    FROM mreport_global.ods_ebs_cux_3_gl_coop_account) b1
                           INNER JOIN (SELECT account_ou_id
                                              ,max(last_update_date) last_update_date
                                         FROM mreport_global.ods_ebs_cux_3_gl_coop_account
                                        GROUP BY account_ou_id) b2
                              ON (b1.account_ou_id=b2.account_ou_id
                              AND b1.last_update_date=b2.last_update_date)) a2
                 ON (a1.org_id=a2.co_org_id)) t6
    ON (t1.org_id=t6.org_id
    AND t1.production_line_id=t6.production_line_id)
  LEFT JOIN (SELECT breed_batch_num contract_no
                    ,sum(coalesce(receipt_amount,0)-coalesce(wirite_off_amount,0)) deposit_balance_amt
               FROM dwu_qw_receipt_dd
              WHERE op_day='$OP_DAY'
                AND currency_id='CNY'
              GROUP BY breed_batch_num) t7
    ON (t1.contract_no=t7.contract_no)
 WHERE t3.level2_org_id not in('1015')
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWP_BIRD_PUT_CONTRACT_DD='DWP_BIRD_PUT_CONTRACT_DD'

CREATE_DWP_BIRD_PUT_CONTRACT_DD="
CREATE TABLE IF NOT EXISTS $DWP_BIRD_PUT_CONTRACT_DD(
  month_id                       string    --期间(月)
  ,day_id                        string    --期间(日)
  ,level1_org_id                 string    --组织1级(股份)  
  ,level1_org_descr              string    --组织1级(股份)  
  ,level2_org_id                 string    --组织2级(片联)  
  ,level2_org_descr              string    --组织2级(片联)  
  ,level3_org_id                 string    --组织3级(片区)  
  ,level3_org_descr              string    --组织3级(片区)  
  ,level4_org_id                 string    --组织4级(小片)  
  ,level4_org_descr              string    --组织4级(小片)  
  ,level5_org_id                 string    --组织5级(公司)  
  ,level5_org_descr              string    --组织5级(公司)  
  ,level6_org_id                 string    --组织6级(OU)  
  ,level6_org_descr              string    --组织6级(OU)  
  ,level7_org_id                 string    --组织7级(库存组织)
  ,level7_org_descr              string    --组织7级(库存组织)
  ,level1_businesstype_id        string    --业态1级
  ,level1_businesstype_name      string    --业态1级
  ,level2_businesstype_id        string    --业态2级
  ,level2_businesstype_name      string    --业态2级
  ,level3_businesstype_id        string    --业态3级
  ,level3_businesstype_name      string    --业态3级
  ,level4_businesstype_id        string    --业态4级
  ,level4_businesstype_name      string    --业态4级
  ,currency_id                   string    --币种
  ,contract_no                   string    --合同号
  ,production_line_id            string    --产线代码
  ,production_line_descr         string    --产线描述
  ,farmer_id                     string    --养户号
  ,farmer_name                   string    --养殖单位
  ,farm_addr                     string    --饲养地址
  ,phone_no                      string    --联系方式
  ,salesman_name                 string    --销售员
  ,breed_type_descr              string    --养殖类型
  ,if_dsp                        string    --是否直供
  ,contract_date                 string    --合同日期
  ,contract_kill_date            string    --合同宰杀日期
  ,contract_type_descr           string    --合同类型
  ,put_type_id                   string    --投放类型
  ,put_type_descr                string    --投放类型
  ,distance                      string    --养殖距离
  ,recyle_price                  string    --基础回收单价 
  ,factory_price                 string    --饲料综合出厂价
  ,put_cost                      string    --投放成本(公司投放成本)
  ,longitude                     string    --经度
  ,latitude                      string    --纬度
  ,material_id                   string    --物料编码
  ,material_descr                string    --物料名称
  ,m_factory_descr               string    --苗厂
  ,contract_price                string    --苗单价
  ,contract_qty                  string    --投放数量
  ,dmt_plan_qty                  string    --计划投放数量(投放目标)
  ,put_date                      string    --投放日期
  ,put_start_date                string    --投放开始日期
  ,put_end_date                  string    --投放结束日期
  ,buy_amount                    string    --金额(苗厂供应金额)     
  ,unit_deposit_amt              string    --只押金    
  ,deposit_balance_amt           string    --押金总额   
  ,drugs_cost                    string    --只药费    
  ,total_drugs_cost              string    --药费总额   
  ,carriage_cost                 string    --苗运费    
  ,seeding_selling_price         string    --苗销售价格  
  ,seeding_buying_price          string    --苗采购价格
  ,drugs_std                     string    --兽药标准
  ,avg_weight                    string    --平均只重
  ,feed_profit                   string    --饲料利润
  ,single_feed_cnt               string    --单只理论用料量
  ,cub_profit                    string    --苗利润
  ,drugs_profit                  string    --兽药利润
  ,trans_profit                  string    --运费利润
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWP_BIRD_PUT_CONTRACT_DD="
INSERT OVERWRITE TABLE $DWP_BIRD_PUT_CONTRACT_DD PARTITION(op_day='$OP_DAY')
SELECT month_id                           --期间(月)
       ,day_id                            --期间(日)
       ,level1_org_id                     --组织1级(股份)  
       ,level1_org_descr                  --组织1级(股份)  
       ,level2_org_id                     --组织2级(片联)  
       ,level2_org_descr                  --组织2级(片联)  
       ,level3_org_id                     --组织3级(片区)  
       ,level3_org_descr                  --组织3级(片区)  
       ,level4_org_id                     --组织4级(小片)  
       ,level4_org_descr                  --组织4级(小片)  
       ,level5_org_id                     --组织5级(公司)  
       ,level5_org_descr                  --组织5级(公司)  
       ,level6_org_id                     --组织6级(OU)  
       ,level6_org_descr                  --组织6级(OU)  
       ,level7_org_id                     --组织7级(库存组织)
       ,level7_org_descr                  --组织7级(库存组织)
       ,level1_businesstype_id            --业态1级
       ,level1_businesstype_name          --业态1级
       ,level2_businesstype_id            --业态2级
       ,level2_businesstype_name          --业态2级
       ,level3_businesstype_id            --业态3级
       ,level3_businesstype_name          --业态3级
       ,level4_businesstype_id            --业态4级
       ,level4_businesstype_name          --业态4级
       ,currency_id                       --币种
       ,contract_no                       --合同号
       ,production_line_id                --产线代码
       ,production_line_descr             --产线描述
       ,farmer_id                         --养户号
       ,farmer_name                       --养殖单位
       ,farm_addr                         --饲养地址
       ,phone_no                          --联系方式
       ,salesman_name                     --销售员
       ,breed_type_descr                  --养殖类型
       ,if_dsp                            --是否直供
       ,contract_date                     --合同日期
       ,contract_kill_date                --合同宰杀日期
       ,contract_type_descr               --合同类型
       ,put_type_id                       --投放类型
       ,put_type_descr                    --投放类型
       ,distance                          --养殖距离
       ,recyle_price                      --基础回收单价 
       ,factory_price                     --饲料综合出厂价
       ,put_cost                          --投放成本(公司投放成本)
       ,longitude                         --经度
       ,latitude                          --纬度
       ,material_id                       --物料编码
       ,material_descr                    --物料名称
       ,m_factory_descr                   --苗厂
       ,contract_price                    --苗单价
       ,contract_qty                      --投放数量
       ,dmt_plan_qty                      --计划投放数量(投放目标)
       ,put_date                          --投放日期
       ,put_start_date                    --投放开始日期
       ,put_end_date                      --投放结束日期
       ,buy_amount                        --金额(苗厂供应金额)     
       ,unit_deposit_amt                  --只押金    
       ,deposit_balance_amt               --押金总额   
       ,drugs_cost                        --只药费    
       ,total_drugs_cost                  --药费总额   
       ,carriage_cost                     --苗运费    
       ,seeding_selling_price             --苗销售价格
       ,seeding_buying_price              --苗采购价格
       ,drugs_std                         --兽药标准
       ,avg_weight                        --平均只重
       ,feed_profit                       --饲料利润
       ,single_feed_cnt                   --单只理论用料量
       ,cub_profit                        --苗利润
       ,drugs_profit                      --兽药利润
       ,trans_profit                      --运费利润
  FROM (SELECT *
          FROM $TMP_DWP_BIRD_PUT_CONTRACT_DD_1
         WHERE op_day='$OP_DAY') t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    set mapred.max.split.size=10000000;
    set mapred.min.split.size.per.node=10000000;
    set mapred.min.split.size.per.rack=10000000;
    set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
    set hive.hadoop.supports.splittable.combineinputformat=true;
    set hive.auto.convert.join=false;
    set mapred.reduce.tasks=20;

    $CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_00;
    $INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_00;
    $CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_01;
    $INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_01;
    $CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_0;
    $INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_0;
    $CREATE_TMP_DWP_BIRD_PUT_CONTRACT_DD_1;
    $INSERT_TMP_DWP_BIRD_PUT_CONTRACT_DD_1;
    $CREATE_DWP_BIRD_PUT_CONTRACT_DD;
    $INSERT_DWP_BIRD_PUT_CONTRACT_DD;
"  -v
