#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmf_bird_duck_charge_mm.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 月度禽屠宰鸭肠/鸭毛收费分析表
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmf_bird_duck_charge_mm.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

###########################################################################################
## 获取宰杀只数
## 变量声明
TMP_DMF_BIRD_DUCK_CHARGE_MM_1='TMP_DMF_BIRD_DUCK_CHARGE_MM_1'

CREATE_TMP_DMF_BIRD_DUCK_CHARGE_MM_1="
CREATE TABLE IF NOT EXISTS $TMP_DMF_BIRD_DUCK_CHARGE_MM_1(
  month_id                string    --期间(月)
  ,level1_org_id          string    --组织1级(股份)  
  ,level1_org_descr       string    --组织1级(股份)  
  ,level2_org_id          string    --组织2级(片联)  
  ,level2_org_descr       string    --组织2级(片联)  
  ,level3_org_id          string    --组织3级(片区)  
  ,level3_org_descr       string    --组织3级(片区)  
  ,level4_org_id          string    --组织4级(小片)  
  ,level4_org_descr       string    --组织4级(小片)  
  ,level5_org_id          string    --组织5级(公司)  
  ,level5_org_descr       string    --组织5级(公司)  
  ,level6_org_id          string    --组织6级(OU)  
  ,level6_org_descr       string    --组织6级(OU)  
  ,level7_org_id          string    --组织7级(库存组织)
  ,level7_org_descr       string    --组织7级(库存组织)
  ,production_line_id     string    --产线
  ,production_line_descr  string    --产线
  ,bird_killed_qty        string    --宰杀只数
)
PARTITIONED BY (op_month string)
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>获取鸭产品宰杀只数>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMF_BIRD_DUCK_CHARGE_MM_1="
INSERT OVERWRITE TABLE $TMP_DMF_BIRD_DUCK_CHARGE_MM_1 PARTITION(op_month='$OP_MONTH')
SELECT t1.period_id month_id            --期间(月)
       ,t2.level1_org_id                --组织1级(股份)  
       ,t2.level1_org_descr             --组织1级(股份)  
       ,t2.level2_org_id                --组织2级(片联)  
       ,t2.level2_org_descr             --组织2级(片联)  
       ,t2.level3_org_id                --组织3级(片区)  
       ,t2.level3_org_descr             --组织3级(片区)  
       ,t2.level4_org_id                --组织4级(小片)  
       ,t2.level4_org_descr             --组织4级(小片)  
       ,t2.level5_org_id                --组织5级(公司)  
       ,t2.level5_org_descr             --组织5级(公司)  
       ,t2.level6_org_id                --组织6级(OU)  
       ,t2.level6_org_descr             --组织6级(OU)  
       ,null level7_org_id              --组织7级(库存组织)
       ,null level7_org_descr           --组织7级(库存组织)
       ,t1.production_line_id           --产线
       ,t1.production_line_descr        --产线
       ,sum(t1.recycle_qty) bird_killed_qty  --宰杀只数
  FROM (SELECT contract_no                --合同号
               ,case when material_code='3501000002' then '1'
                else '2' end production_line_id       --产线
               ,case when material_code='3501000002' then '鸡线'
                else '鸭线' end production_line_descr --产线
               ,org_id
               ,substr(transaction_date,1,6) period_id
               ,sum(secondary_qty) recycle_qty        --辅助数量
               ,sum(quantity_received) recycle_weight --已接收数量
          FROM dwu_cg_buy_list_cg01_dd
         WHERE op_day='$OP_DAY'
           AND material_code in('3501000002','3502000002')
           AND release_num like 'BWP%'
           AND cancel_flag in('OPEN','CLOSED')
           AND quantity_received>0
         GROUP BY contract_no,material_code,org_id,substr(transaction_date,1,6)) t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t2
    ON (t1.org_id=t2.org_id)
 INNER JOIN (SELECT contractnumber contract_no          --合同号
                    ,case when meaning='CHICHEN' then '1'
                          when meaning='DUCK' then '2'
                     else null end production_line_id         --产线
                    ,case when meaning='CHICHEN' then '鸡线'
                          when meaning='DUCK' then '鸭线'
                     else null end production_line_descr      --产线
               FROM dwu_qw_contract_dd
              WHERE op_day='$OP_DAY') t3
    ON (t1.contract_no=t3.contract_no
    AND t1.production_line_id=t3.production_line_id)
  GROUP BY t1.period_id                 --期间(月)
       ,t2.level1_org_id                --组织1级(股份)  
       ,t2.level1_org_descr             --组织1级(股份)  
       ,t2.level2_org_id                --组织2级(片联)  
       ,t2.level2_org_descr             --组织2级(片联)  
       ,t2.level3_org_id                --组织3级(片区)  
       ,t2.level3_org_descr             --组织3级(片区)  
       ,t2.level4_org_id                --组织4级(小片)  
       ,t2.level4_org_descr             --组织4级(小片)  
       ,t2.level5_org_id                --组织5级(公司)  
       ,t2.level5_org_descr             --组织5级(公司)  
       ,t2.level6_org_id                --组织6级(OU)  
       ,t2.level6_org_descr             --组织6级(OU)
       ,t1.production_line_id           --产线
       ,t1.production_line_descr        --产线
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DMF_BIRD_DUCK_CHARGE_MM='DMF_BIRD_DUCK_CHARGE_MM'

CREATE_DMF_BIRD_DUCK_CHARGE_MM="
CREATE TABLE IF NOT EXISTS $DMF_BIRD_DUCK_CHARGE_MM(
  month_id                     string    --期间(月)
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
  ,production_line_id          string    --产线
  ,production_line_descr       string    --产线
  ,prod_type_id                string    --产品类型      
  ,prod_type_name              string    --产品类型  
  ,currency_type_id            string    --币种
  ,currency_type_descr         string    --币种    
  ,water_qty                   string    --用水量(立方)   
  ,water_amt                   string    --水费金额(元)   
  ,lights_qty                  string    --照明用电量(千瓦时)
  ,lights_amt                  string    --照明费(元)    
  ,freeze_qty                  string    --速冻重量(吨)   
  ,freeze_amt                  string    --速冻金额(元)   
  ,cold_store_qty              string    --冷藏重量(吨)   
  ,cold_store_amt              string    --冷藏费(元)    
  ,coal_gas_qty                string    --煤(汽)用量(立方)
  ,coal_gas_amt                string    --煤(汽)费(元)  
  ,load_unload_qty             string    --装卸量(吨)    
  ,load_unload_amt             string    --装卸费(元)    
  ,load_change_qty             string    --换装量(吨)    
  ,load_change_amt             string    --换装费(元)    
  ,transfer_rental_period      string    --租用周转盘(月)  
  ,transfer_rental_amt         string    --租用周转盘(元)  
  ,other_qty                   string    --其他数量
  ,other_amt                   string    --其他(元)     
  ,place_rental_period         string    --场地租凭月数(月) 
  ,place_rental_amt            string    --场地租赁费(元)  
  ,bird_killed_qty             string    --宰杀只数(只)
  ,create_time                 string    --创建时间
)
PARTITIONED BY (op_month string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMF_BIRD_DUCK_CHARGE_MM="
INSERT OVERWRITE TABLE $DMF_BIRD_DUCK_CHARGE_MM PARTITION(op_month='$OP_MONTH')
SELECT t1.month_id                       --期间(月)
       ,null day_id                      --期间(日)
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
       ,null level1_businesstype_id      --业态1级
       ,null level1_businesstype_name    --业态1级
       ,null level2_businesstype_id      --业态2级
       ,null level2_businesstype_name    --业态2级
       ,null level3_businesstype_id      --业态3级
       ,null level3_businesstype_name    --业态3级
       ,null level4_businesstype_id      --业态4级
       ,null level4_businesstype_name    --业态4级
       ,t1.production_line_id            --产线
       ,t1.production_line_descr         --产线
       ,t1.prod_type_id                  --产品类型      
       ,t1.prod_type_name                --产品类型  
       ,t1.currency_type_id              --币种
       ,t1.currency_type_descr           --币种    
       ,t1.water_qty                     --用水量(立方)   
       ,t1.water_amt                     --水费金额(元)   
       ,t1.lights_qty                    --照明用电量(千瓦时)
       ,t1.lights_amt                    --照明费(元)    
       ,t1.freeze_qty                    --速冻重量(吨)   
       ,t1.freeze_amt                    --速冻金额(元)   
       ,t1.cold_store_qty                --冷藏重量(吨)   
       ,t1.cold_store_amt                --冷藏费(元)    
       ,t1.coal_gas_qty                  --煤(汽)用量(立方)
       ,t1.coal_gas_amt                  --煤(汽)费(元)  
       ,t1.load_unload_qty               --装卸量(吨)    
       ,t1.load_unload_amt               --装卸费(元)    
       ,t1.load_change_qty               --换装量(吨)    
       ,t1.load_change_amt               --换装费(元)    
       ,t1.transfer_rental_period        --租用周转盘(月)  
       ,t1.transfer_rental_amt           --租用周转盘(元)  
       ,t1.other_qty                     --其他数量
       ,t1.other_amt                     --其他(元)     
       ,t1.place_rental_period           --场地租凭月数(月) 
       ,t1.place_rental_amt              --场地租赁费(元)  
       ,t2.bird_killed_qty               --宰杀只数(只)
       ,'$CREATE_TIME'                   --创建时间
  FROM (SELECT month_id                      --期间
               ,level1_org_id                 --组织1级(股份)  
               ,level1_org_descr              --组织1级(股份)  
               ,level2_org_id                 --组织2级(片联)  
               ,level2_org_descr              --组织2级(片联)  
               ,level3_org_id                 --组织3级(片区)  
               ,level3_org_descr              --组织3级(片区)  
               ,level4_org_id                 --组织4级(小片)  
               ,level4_org_descr              --组织4级(小片)  
               ,level5_org_id                 --组织5级(公司)  
               ,level5_org_descr              --组织5级(公司)  
               ,level6_org_id                 --组织6级(OU)  
               ,level6_org_descr              --组织6级(OU)  
               ,level7_org_id                 --组织7级(库存组织)
               ,level7_org_descr              --组织7级(库存组织)
               ,production_line_id            --产线
               ,production_line_descr         --产线
               ,prod_type_id                  --产品类型      
               ,prod_type_name                --产品类型  
               ,currency_type_id              --币种
               ,currency_type_descr           --币种    
               ,sum(water_qty) water_qty                              --用水量(立方)   
               ,sum(water_amt) water_amt                              --水费金额(元)   
               ,sum(lights_qty) lights_qty                            --照明用电量(千瓦时)
               ,sum(lights_amt) lights_amt                            --照明费(元)    
               ,sum(freeze_qty) freeze_qty                            --速冻重量(吨)   
               ,sum(freeze_amt) freeze_amt                            --速冻金额(元)   
               ,sum(cold_store_qty) cold_store_qty                    --冷藏重量(吨)   
               ,sum(cold_store_amt) cold_store_amt                    --冷藏费(元)    
               ,sum(coal_gas_qty) coal_gas_qty                        --煤(汽)用量(立方)
               ,sum(coal_gas_amt) coal_gas_amt                        --煤(汽)费(元)  
               ,sum(load_unload_qty) load_unload_qty                  --装卸量(吨)    
               ,sum(load_unload_amt) load_unload_amt                  --装卸费(元)    
               ,sum(load_change_qty) load_change_qty                  --换装量(吨)    
               ,sum(load_change_amt) load_change_amt                  --换装费(元)    
               ,sum(transfer_rental_period) transfer_rental_period    --租用周转盘(月)  
               ,sum(transfer_rental_amt) transfer_rental_amt          --租用周转盘(元)  
               ,sum(other_qty) other_qty                              --其他数量
               ,sum(other_amt) other_amt                              --其他(元)     
               ,sum(place_rental_period) place_rental_period          --场地租凭月数(月) 
               ,sum(place_rental_amt) place_rental_amt                --场地租赁费(元)  
               ,sum(bird_killed_qty) bird_killed_qty                  --宰杀只数(只)   
          FROM dwf_bird_duck_charge_dd
         WHERE op_day='$OP_DAY'
         GROUP BY month_id                    --期间
               ,level1_org_id                 --组织1级(股份)  
               ,level1_org_descr              --组织1级(股份)  
               ,level2_org_id                 --组织2级(片联)  
               ,level2_org_descr              --组织2级(片联)  
               ,level3_org_id                 --组织3级(片区)  
               ,level3_org_descr              --组织3级(片区)  
               ,level4_org_id                 --组织4级(小片)  
               ,level4_org_descr              --组织4级(小片)  
               ,level5_org_id                 --组织5级(公司)  
               ,level5_org_descr              --组织5级(公司)  
               ,level6_org_id                 --组织6级(OU)  
               ,level6_org_descr              --组织6级(OU)  
               ,level7_org_id                 --组织7级(库存组织)
               ,level7_org_descr              --组织7级(库存组织)
               ,production_line_id            --产线
               ,production_line_descr         --产线
               ,prod_type_id                  --产品类型      
               ,prod_type_name                --产品类型  
               ,currency_type_id              --币种
               ,currency_type_descr) t1
  LEFT JOIN (SELECT *
               FROM $TMP_DMF_BIRD_DUCK_CHARGE_MM_1
              WHERE op_month='$OP_MONTH') t2
    ON (t1.month_id=t2.month_id
    AND t1.level5_org_id=t2.level5_org_id
    AND t1.level6_org_id=t2.level6_org_id
    AND t1.production_line_id=t2.production_line_id)
  WHERE t2.bird_killed_qty>0
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMF_BIRD_DUCK_CHARGE_MM_1;
    $INSERT_TMP_DMF_BIRD_DUCK_CHARGE_MM_1;
    $CREATE_DMF_BIRD_DUCK_CHARGE_MM;
    $INSERT_DMF_BIRD_DUCK_CHARGE_MM;
"  -v