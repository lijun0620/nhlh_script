#!/bin/bash

######################################################################
#                                                                    
# 程    序: dwf_bird_duck_charge_dd.sh                               
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
    echo "输入参数错误，调用示例: dwf_bird_duck_charge_dd.sh 20180101"
    exit 1
fi

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
TMP_DWF_BIRD_DUCK_CHARGE_DD_1='TMP_DWF_BIRD_DUCK_CHARGE_DD_1'

CREATE_TMP_DWF_BIRD_DUCK_CHARGE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DWF_BIRD_DUCK_CHARGE_DD_1(
  settlement_no           string    --结算单号
  ,month_id               string    --期间
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
  ,prod_type_id           string    --产品类型      
  ,prod_type_name         string    --产品类型  
  ,currency_id            string    --币种
  ,currency_descr         string    --币种类型    
  ,water_qty              string    --用水量(立方)   
  ,water_amt              string    --水费金额(元)   
  ,lights_qty             string    --照明用电量(千瓦时)
  ,lights_amt             string    --照明费(元)    
  ,freeze_qty             string    --速冻重量(吨)   
  ,freeze_amt             string    --速冻金额(元)   
  ,cold_store_qty         string    --冷藏重量(吨)   
  ,cold_store_amt         string    --冷藏费(元)    
  ,coal_gas_qty           string    --煤(汽)用量(立方)
  ,coal_gas_amt           string    --煤(汽)费(元)  
  ,load_unload_qty        string    --装卸量(吨)    
  ,load_unload_amt        string    --装卸费(元)    
  ,load_change_qty        string    --换装量(吨)    
  ,load_change_amt        string    --换装费(元)    
  ,transfer_rental_period string    --租用周转盘(月)  
  ,transfer_rental_amt    string    --租用周转盘(元)  
  ,other_qty              string    --其他数量
  ,other_amt              string    --其他(元)     
  ,place_rental_period    string    --场地租凭月数(月) 
  ,place_rental_amt       string    --场地租赁费(元)  
  ,bird_killed_qty        string    --宰杀只数(只)   
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>将数据按业务类型转换为横表>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DWF_BIRD_DUCK_CHARGE_DD_1="
INSERT OVERWRITE TABLE $TMP_DWF_BIRD_DUCK_CHARGE_DD_1 PARTITION(op_day='$OP_DAY')
SELECT t1.settlement_no             --结算单号
       ,substr(t1.period_id,1,6) month_id       --周期
       ,t2.level1_org_id
       ,t2.level1_org_descr
       ,t2.level2_org_id
       ,t2.level2_org_descr
       ,t2.level3_org_id
       ,t2.level3_org_descr
       ,t2.level4_org_id
       ,t2.level4_org_descr
       ,t2.level5_org_id
       ,t2.level5_org_descr
       ,t2.level6_org_id
       ,t2.level6_org_descr
       ,t3.level7_org_id
       ,t3.level7_org_descr
       ,t1.production_line_id
       ,t1.production_line_descr
       ,case when t1.cat='肠' then '1'
             when t1.cat='毛' then '2'
             when t1.cat='血' then '3'
        end prod_type_id            --产品类型
       ,case when t1.cat='肠' then '鸭肠'
             when t1.cat='毛' then '鸭毛'
             when t1.cat='血' then '鸭血'
        end prod_type_name          --产品类型
       ,t1.currency_id              --币种(1-原币, 2-本位币)
       ,t1.currency_descr           --币种(币种类型)
       ,t1.water_qty                --用水量(立方)
       ,t1.water_amt                --水费金额(元)               
       ,t1.lights_qty               --照明用电量(千瓦时)
       ,t1.lights_amt               --照明费(元)
       ,t1.freeze_qty               --速冻重量(吨)
       ,t1.freeze_amt               --速冻金额(元)
       ,t1.cold_store_qty           --速冻重量(吨)
       ,t1.cold_store_amt           --速冻金额(元)
       ,t1.coal_gas_qty             --煤(气)用量(立方)
       ,t1.coal_gas_amt             --煤(气)费(元)
       ,t1.load_unload_qty          --装卸量(吨)
       ,t1.load_unload_amt          --装卸费(元)
       ,t1.load_change_qty          --换装量(吨)
       ,t1.load_change_amt          --换装费(元)
       ,t1.transfer_rental_period   --租用周转盘(月)
       ,t1.transfer_rental_amt      --租用周转盘(元)
       ,t1.other_qty                --其他数量
       ,t1.other_amt                --其他(元)
       ,t1.place_rental_period      --场地租凭月数(月)
       ,t1.place_rental_amt         --场地租赁费(元)
       ,'0' bird_killed_qty         --宰杀只数(只)
  FROM (SELECT period_id         --期间    
               ,org_id           --OU组织  
               ,inv_org_id       --库存组织ID
               ,currency_id      --币种
               ,currency_descr   --本位币
               ,settlement_no    --结算单号
               ,bus_type
               ,production_line_id
               ,production_line_descr
               ,cat              --产品类型
               ,sum(case when exp_item in('水费','水费-鸡','水费-鸭') then qty else 0 end) water_qty      --用水量(立方)
               ,sum(case when exp_item in('水费','水费-鸡','水费-鸭') then amount else 0 end) water_amt   --水费金额(元)               
               ,sum(case when exp_item in('照明费','照明费-鸡','照明费-鸭','电费','电费-鸡','电费-鸭') then qty else 0 end) lights_qty        --照明用电量(千瓦时)
               ,sum(case when exp_item in('照明费','照明费-鸡','照明费-鸭','电费','电费-鸡','电费-鸭') then amount else 0 end) lights_amt     --照明费(元)
               ,sum(case when exp_item in('速冻费','速冻费-鸡','速冻费-鸭') then qty else 0 end) freeze_qty        --速冻重量(吨)
               ,sum(case when exp_item in('速冻费','速冻费-鸡','速冻费-鸭') then amount else 0 end) freeze_amt     --速冻金额(元)
               ,sum(case when exp_item in('冷藏费','冷藏费-鸡','冷藏费-鸭') then qty else 0 end) cold_store_qty    --速冻重量(吨)
               ,sum(case when exp_item in('冷藏费','冷藏费-鸡','冷藏费-鸭') then amount else 0 end) cold_store_amt --速冻金额(元)
               ,sum(case when exp_item in('煤气费','煤气费-鸡','煤气费-鸭') then qty else 0 end) coal_gas_qty      --煤(气)用量(立方)
               ,sum(case when exp_item in('煤气费','煤气费-鸡','煤气费-鸭') then amount else 0 end) coal_gas_amt   --煤(气)费(元)
               ,sum(case when exp_item in('装卸费','装卸费-鸡','装卸费-鸭') then qty else 0 end) load_unload_qty      --装卸量(吨)
               ,sum(case when exp_item in('装卸费','装卸费-鸡','装卸费-鸭') then amount else 0 end) load_unload_amt   --装卸费(元)
               ,sum(case when exp_item in('换装费','换装费-鸡','换装费-鸭') then qty else 0 end) load_change_qty      --换装量(吨)
               ,sum(case when exp_item in('换装费','换装费-鸡','换装费-鸭') then amount else 0 end) load_change_amt   --换装费(元)
               ,sum(case when exp_item in('租用周转盘','租用周转盘-鸡','租用周转盘-鸭') then qty else 0 end) transfer_rental_period      --租用周转盘(月)
               ,sum(case when exp_item in('租用周转盘','租用周转盘-鸡','租用周转盘-鸭') then amount else 0 end) transfer_rental_amt   --租用周转盘(元)
               ,sum(case when exp_item in('其他','其他-鸡','其他-鸭') then qty else 0 end) other_qty         --其他数量
               ,sum(case when exp_item in('其他','其他-鸡','其他-鸭') then amount else 0 end) other_amt      --其他(元)
               ,sum(case when exp_item in('场地租用','场地租用-鸡','场地租用-鸭','场地租赁费','场地租赁费-鸡','场地租赁费-鸭') then qty else 0 end) place_rental_period      --场地租凭月数(月)
               ,sum(case when exp_item in('场地租用','场地租用-鸡','场地租用-鸭','场地租赁费','场地租赁费-鸡','场地租赁费-鸭') then amount else 0 end) place_rental_amt      --场地租赁费(元)
          FROM (SELECT period_id
                       ,org_id
                       ,null inv_org_id
                       ,'1' currency_id
                       ,currency_id currency_descr
                       ,settlement_no
                       ,bus_type
                       ,case when exp_item in('水费-鸡','电费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '1' else '2' end production_line_id
                       ,case when exp_item in('水费-鸡','电费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '鸡线' else '鸭线' end production_line_descr
                       ,cat
                       ,exp_item
                       ,qty
                       ,amount
                  FROM dwu_cw_mcx_dd
                 WHERE op_day='$OP_DAY'
                UNION ALL
                SELECT period_id
                       ,org_id
                       ,null inv_org_id
                       ,'2' currency_id
                       ,loc_currency_id currency_descr
                       ,settlement_no
                       ,bus_type
                       ,case when exp_item in('水费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '1' else '2' end production_line_id
                       ,case when exp_item in('水费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '鸡线' else '鸭线' end production_line_descr
                       ,cat
                       ,exp_item
                       ,qty
                       ,loc_amount amount
                  FROM dwu_cw_mcx_dd
                 WHERE op_day='$OP_DAY'
                UNION ALL
                SELECT period_id
                       ,org_id
                       ,null inv_org_id
                       ,'3' currency_id
                       ,'CNY' currency_descr
                       ,settlement_no
                       ,bus_type
                       ,case when exp_item in('水费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '1' else '2' end production_line_id
                       ,case when exp_item in('水费-鸡','照明费-鸡','速冻费-鸡','冷藏费-鸡','煤气费-鸡','装卸费-鸡','换装费-鸡','租用周转盘-鸡','其他-鸡','场地租用-鸡','场地租赁费-鸡')
                             then '鸡线' else '鸭线' end production_line_descr
                       ,cat
                       ,exp_item
                       ,qty
                       ,case when loc_currency_id='CNY' then loc_amount
                        else round(a2.conversion_rate*loc_amount,2) end amount
                  FROM dwu_cw_mcx_dd a1
                  LEFT JOIN (SELECT from_currency,
                                    to_currency,
                                    conversion_rate
                               FROM mreport_global.dmd_fin_period_currency_rate_mm
                              WHERE conversion_period='$OP_MONTH'
                                AND to_currency='CNY') a2
                    ON (a1.loc_currency_id=a2.from_currency)
                 WHERE a1.op_day='$OP_DAY') a
         GROUP BY period_id
               ,org_id
               ,inv_org_id
               ,currency_id
               ,currency_descr
               ,settlement_no
               ,bus_type
               ,production_line_id
               ,production_line_descr
               ,cat) t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t2
    ON (t1.org_id=t2.org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t3
    ON (t1.inv_org_id=t3.inv_org_id)
"

###########################################################################################
## 将数据从大表转换至目标表
## 变量声明
DWF_BIRD_DUCK_CHARGE_DD='DWF_BIRD_DUCK_CHARGE_DD'

CREATE_DWF_BIRD_DUCK_CHARGE_DD="
CREATE TABLE IF NOT EXISTS $DWF_BIRD_DUCK_CHARGE_DD(
  settlement_no           string    --结算单号
  ,month_id               string    --期间
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
  ,prod_type_id           string    --产品类型      
  ,prod_type_name         string    --产品类型  
  ,currency_type_id       string    --币种
  ,currency_type_descr    string    --币种    
  ,water_qty              string    --用水量(立方)   
  ,water_amt              string    --水费金额(元)   
  ,lights_qty             string    --照明用电量(千瓦时)
  ,lights_amt             string    --照明费(元)    
  ,freeze_qty             string    --速冻重量(吨)   
  ,freeze_amt             string    --速冻金额(元)   
  ,cold_store_qty         string    --冷藏重量(吨)   
  ,cold_store_amt         string    --冷藏费(元)    
  ,coal_gas_qty           string    --煤(汽)用量(立方)
  ,coal_gas_amt           string    --煤(汽)费(元)  
  ,load_unload_qty        string    --装卸量(吨)    
  ,load_unload_amt        string    --装卸费(元)    
  ,load_change_qty        string    --换装量(吨)    
  ,load_change_amt        string    --换装费(元)    
  ,transfer_rental_period string    --租用周转盘(月)  
  ,transfer_rental_amt    string    --租用周转盘(元)  
  ,other_qty              string    --其他数量
  ,other_amt              string    --其他(元)     
  ,place_rental_period    string    --场地租凭月数(月) 
  ,place_rental_amt       string    --场地租赁费(元)  
  ,bird_killed_qty        string    --宰杀只数(只)   
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>转换币种>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DWF_BIRD_DUCK_CHARGE_DD="
INSERT OVERWRITE TABLE $DWF_BIRD_DUCK_CHARGE_DD PARTITION(op_day='$OP_DAY')
SELECT t1.settlement_no             --结算单号
       ,t1.month_id                 --周期
       ,t1.level1_org_id
       ,t1.level1_org_descr
       ,t1.level2_org_id
       ,t1.level2_org_descr
       ,t1.level3_org_id
       ,t1.level3_org_descr
       ,t1.level4_org_id
       ,t1.level4_org_descr
       ,t1.level5_org_id
       ,t1.level5_org_descr
       ,t1.level6_org_id
       ,t1.level6_org_descr
       ,t1.level7_org_id
       ,t1.level7_org_descr
       ,t1.production_line_id       --产线
       ,t1.production_line_descr    --产线
       ,t1.prod_type_id             --产品类型
       ,t1.prod_type_name           --产品类型
       ,t1.currency_id currency_type_id       --币种
       ,case when t1.currency_id='1' then '原币'
             when t1.currency_id='2' then '本位币'
             when t1.currency_id='3' then '母币'
        else '未知' end currency_type_descr --币种类型
       ,t1.water_qty                --用水量(立方)
       ,t1.water_amt                --水费金额(元)               
       ,t1.lights_qty               --照明用电量(千瓦时)
       ,t1.lights_amt               --照明费(元)
       ,t1.freeze_qty               --速冻重量(吨)
       ,t1.freeze_amt               --速冻金额(元)
       ,t1.cold_store_qty           --速冻重量(吨)
       ,t1.cold_store_amt           --速冻金额(元)
       ,t1.coal_gas_qty             --煤(气)用量(立方)
       ,t1.coal_gas_amt             --煤(气)费(元)
       ,t1.load_unload_qty          --装卸量(吨)
       ,t1.load_unload_amt          --装卸费(元)
       ,t1.load_change_qty          --换装量(吨)
       ,t1.load_change_amt          --换装费(元)
       ,t1.transfer_rental_period   --租用周转盘(月)
       ,t1.transfer_rental_amt      --租用周转盘(元)
       ,t1.other_qty                --其他数量
       ,t1.other_amt                --其他(元)
       ,t1.place_rental_period      --场地租凭月数(月)
       ,t1.place_rental_amt         --场地租赁费(元)
       ,t1.bird_killed_qty          --宰杀只数(只)
  FROM (SELECT *
          FROM $TMP_DWF_BIRD_DUCK_CHARGE_DD_1
         WHERE op_day='$OP_DAY'
           AND currency_id in('2','3')
           AND level2_org_id NOT IN('1015')) t1
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DWF_BIRD_DUCK_CHARGE_DD_1;
    $INSERT_TMP_DWF_BIRD_DUCK_CHARGE_DD_1;
    $CREATE_DWF_BIRD_DUCK_CHARGE_DD;
    $INSERT_DWF_BIRD_DUCK_CHARGE_DD;
"  -v