#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_keep_price_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 禽旺-回收保本价
# 修改说明:                                                          
######################################################################

OP_DAY=$1
OP_MONTH=${OP_DAY:0:6}

# 判断时间输入参数
if [ $# -ne 1 ]
then
    echo "输入参数错误，调用示例: dmp_bird_keep_price_dd.sh 20180101"
    exit 1
fi

# 当前时间
CREATE_TIME=$(date -d " -0 day" +%Y%m%d%H%M)

# 下月初第一天
NEXT_MONTH_FIRST=$(date -d " +1 month" +%Y%m01)

# 当前月未时间
MONTH_END_DAY=$(date -d "$NEXT_MONTH_FIRST -1 day" +%Y%m%d)

# 当前月初时间
MONTH_FIRST_DAY=$(date -d " -0 day" +%Y%m01)

# 上个月未时间
LAST_END_DAY=$(date -d "$MONTH_FIRST_DAY -1 day" +%Y%m%d)
LAST_END_DAY=$OP_DAY    #  临时使用变量，上线时间删除

###########################################################################################
## 获取月未价
## 变量声明
TMP_DMP_BIRD_KEEP_PRICE_DD_0='TMP_DMP_BIRD_KEEP_PRICE_DD_0'

CREATE_TMP_DMP_BIRD_KEEP_PRICE_DD_0="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_KEEP_PRICE_DD_0(
  month_id                     string        --期间
  ,end_day_id                  string        --取end_day_id
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_KEEP_PRICE_DD_0="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_KEEP_PRICE_DD_0 PARTITION(op_day='$OP_DAY')
SELECT month_id
       ,case when substr('$MONTH_END_DAY',1,6)=month_id then '$MONTH_END_DAY'
        else end_day_id end end_day_id 
  FROM (SELECT month_id
               ,max(end_day_id) end_day_id
          FROM (SELECT substr(creation_date,1,6) month_id
                       ,creation_date end_day_id
                  FROM dwu_cw_cw29_dd                              --CW29
                 WHERE op_day='$OP_DAY'
                UNION ALL
                SELECT substr(creation_date,1,6) month_id
                       ,creation_date end_day_id
                  FROM dwu_cw_cw31_dd
                 WHERE op_day='$OP_DAY') a
         GROUP BY month_id) t1
"

###########################################################################################
## 计算日保本价
## 变量声明
TMP_DMP_BIRD_KEEP_PRICE_DD_1='TMP_DMP_BIRD_KEEP_PRICE_DD_1'

CREATE_TMP_DMP_BIRD_KEEP_PRICE_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_KEEP_PRICE_DD_1(
  period_id                     string        --期间
  ,org_id                       string        --组织id
  ,production_line_id           string        --产线
  ,amount_b                     string        --每日综合售价B金额(元)
  ,sum_amount_b                 string        --累计综合售额B(元)
  ,byproduct_amount             string        --副产品收入(元)
  ,input_tax                    string        --进项税(元)
  ,packing_material_cost        string        --包装材料
  ,excipient_material_cost      string        --辅助材料
  ,labor_material_cost          string        --人工材料
  ,fuel_cost                    string        --燃料
  ,water_power_cost             string        --水电
  ,fuel_power_cost              string        --燃料动力
  ,manufacture_change_cost      string        --制造费用变动
  ,manufacture_fixed_cost       string        --制造费用固定
  ,during_cost                  string        --期间费用
  ,day_inbuy_prod_weight        string        --日自购产量(重量)
  ,accum_inbuy_prod_weight      string        --月累计自购产量
  ,settlement_weight            string        --结算重量
)
PARTITIONED BY (op_day string)
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_KEEP_PRICE_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_KEEP_PRICE_DD_1 PARTITION(op_day='$OP_DAY')
SELECT period_id                                    --期间
       ,org_id                                      --组织id
       ,case when product_line='10' then '1'
             when product_line='20' then '2'
        else null end production_line_id            --产线
       ,sum(coalesce(amount_b,0))                   --每日综合售价B金额(元)
       ,sum(coalesce(sum_amount_b,0))               --累计综合售额B(元)
       ,sum(coalesce(byproduct_amount,0))           --副产品收入(元)
       ,sum(coalesce(input_tax,0))                  --进项税(元)
       ,sum(coalesce(packing_material_cost,0))      --包装材料
       ,sum(coalesce(excipient_material_cost,0))    --辅助材料
       ,sum(coalesce(labor_material_cost,0))        --人工材料
       ,sum(coalesce(fuel_cost,0))                  --燃料
       ,sum(coalesce(water_power_cost,0))           --水电
       ,sum(coalesce(fuel_power_cost,0))            --燃料动力
       ,sum(coalesce(manufacture_change_cost,0))    --制造费用变动
       ,sum(coalesce(manufacture_fixed_cost,0))     --制造费用固定
       ,sum(coalesce(during_cost,0))                --期间费用
       ,sum(coalesce(day_inbuy_prod_weight,0))      --日自购产量(重量)
       ,sum(coalesce(accum_inbuy_prod_weight,0))    --月累计自购产量
       ,sum(coalesce(settlement_weight,0))          --结算重量
  FROM (SELECT creation_date period_id                     --期间
               ,org_id                                     --组织id
               ,product_line                               --产线
               ,amount_b                                   --每日综合售价B金额(元)
               ,case when a1.creation_date=a2.end_day_id then 0
                else a1.sum_amount_b end sum_amount_b      --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定

               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,0 settlement_weight                        --结算重量
          FROM (SELECT *
                  FROM dwu_cw_cw29_dd
                 WHERE op_day='$OP_DAY') a1                --CW29
          LEFT JOIN (SELECT month_id
                            ,end_day_id
                       FROM $TMP_DMP_BIRD_KEEP_PRICE_DD_0
                      WHERE op_day='$OP_DAY') a2
            ON (a1.creation_date=a2.end_day_id)
        UNION ALL
        SELECT creation_date period_id                     --期间
               ,org_id                                     --组织id
               ,product_line                               --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,case when coalesce(amount_b,0)=0 then amount_a
                else amount_b end sum_amount_b             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定

               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,0 settlement_weight                        --结算重量
          FROM (SELECT month_id
                       ,end_day_id
                  FROM $TMP_DMP_BIRD_KEEP_PRICE_DD_0
                 WHERE op_day='$OP_DAY') a1
         INNER JOIN (SELECT *
                       FROM dwu_cw_cw31_dd
                      WHERE op_day='$OP_DAY') a2
            ON (a1.end_day_id=a2.creation_date)            --CW31         
        UNION ALL
        SELECT regexp_replace(cost_date,'-','') period_id       --期间
               ,ou_id org_id             --组织id
               ,line_type product_line   --产线
               ,0 amount_b               --每日综合售价B金额(元)
               ,0 sum_amount_b           --累计综合售额B(元)
               ,byproduct_amount         --副产品收入(元)
               ,0 input_tax              --进项税(元)

               ,packing_amount packing_material_cost       --包装材料
               ,0 excipient_material_cost                  --辅助材料(用不到，暂时不取)
               ,manual_amount labor_material_cost          --人工材料
               ,fuel_amount fuel_cost                      --燃料
               ,water_elec_amount water_power_cost         --水电
               ,power_amount fuel_power_cost               --燃料动力
               ,wip_chg_amount manufacture_change_cost     --制造费用变动
               ,wip_fix_amount manufacture_fixed_cost      --制造费用固定

               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,0 settlement_weight                        --结算重量
          FROM dwu_qw_account_cost_dd                      --CW30(月未成本分摊)
         WHERE op_day='$OP_DAY' 
           AND account_flag='Y'
        UNION ALL                 
        SELECT period_id                                   --期间
               ,org_id                                     --组织id
               ,product_line                               --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,0 sum_amount_b                             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,cost_amount17 input_tax                    --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定

               ,coalesce(selling_expense_fixed,0)+coalesce(selling_expense_change,0)+coalesce(fin_expense,0)+coalesce(admini_expense,0) during_cost  --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,0 settlement_weight                        --结算重量
          FROM dmd_fin_exps_profits                        --CW19
         WHERE currency_type='3'
        UNION ALL
        SELECT period_id                                   --期间
               ,org_id                                     --组织id
               ,product_line                               --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,0 sum_amount_b                             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定
               ,0 during_cost                              --期间费用
               ,self_buy_amount day_inbuy_prod_weight      --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,0 settlement_weight                        --结算重量
          FROM (SELECT *
                  FROM dwu_cw_cw26_dd
                 WHERE op_day='$OP_DAY') a1                --CW26
        UNION ALL
        SELECT period_id                                   --期间
               ,org_id                                     --组织id
               ,product_line                               --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,0 sum_amount_b                             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定
               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,case when t1.period_id=t2.end_day_id then 0
                else t1.accum_inbuy_prod_weight end accum_inbuy_prod_weight  --月累计自购产量(重量)
               ,0 settlement_weight                              --结算重量
          FROM (SELECT a1.period_id
                       ,a2.org_id
                       ,a2.product_line
                       ,sum(case when a1.period_id>=a2.period_id then a2.accum_inbuy_prod_weight
                            else 0 end) accum_inbuy_prod_weight  --月累计自购产量(重量)
                  FROM (SELECT period_id
                          FROM dwu_cw_cw26_dd
                         WHERE op_day='$OP_DAY'
                         GROUP BY period_id) a1                    --CW26
                  LEFT JOIN (SELECT period_id
                                    ,org_id
                                    ,product_line
                                    ,self_buy_amount accum_inbuy_prod_weight --月累计自购产量(重量)
                               FROM dwu_cw_cw26_dd
                              WHERE op_day='$OP_DAY') a2
                    ON (substr(a1.period_id, 1, 6)=substr(a2.period_id, 1, 6))
                GROUP BY a1.period_id
                      ,a2.org_id
                      ,a2.product_line) t1
          LEFT JOIN (SELECT month_id
                            ,end_day_id
                       FROM $TMP_DMP_BIRD_KEEP_PRICE_DD_0
                      WHERE op_day='$OP_DAY') t2
            ON (t1.period_id=t2.end_day_id)
        UNION ALL
        SELECT t2.creation_date period_id                  --期间
               ,t2.org_id                                  --组织id
               ,t2.product_line                            --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,0 sum_amount_b                             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定
               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,t2.accum_inbuy_prod_weight                 --月累计自购产量(重量)
               ,0 settlement_weight                        --结算重量
          FROM (SELECT org_id
                       ,product_line
                       ,'$MONTH_END_DAY' creation_date
                       ,inner_qty accum_inbuy_prod_weight
                  FROM dwu_cw_cw28_dd                 --CW28
                 WHERE op_day='$MONTH_END_DAY'
                   AND creation_date='$OP_MONTH'
                UNION ALL
                SELECT org_id
                       ,product_line
                       ,regexp_replace(date_add(concat(substr(date_add(concat(substr(creation_date,1,4),'-',substr(creation_date,5,2),'-27'),10),1,8),'01'),-1),'-','') creation_date
                       ,inner_qty accum_inbuy_prod_weight
                  FROM dwu_cw_cw28_dd                 --CW28
                 WHERE op_day='$OP_DAY') t2
        UNION ALL
        SELECT regexp_replace(substr(a1.js_date,1,10),'-','') period_id
               ,a2.org_id                                  --组织id
               ,case when a3.meaning='CHICHEN' then '10'
                     when a3.meaning='DUCK' then '20'
                end product_line                           --产线
               ,0 amount_b                                 --每日综合售价B金额(元)
               ,0 sum_amount_b                             --累计综合售额B(元)
               ,0 byproduct_amount                         --副产品收入(元)
               ,0 input_tax                                --进项税(元)

               ,0 packing_material_cost                    --包装材料
               ,0 excipient_material_cost                  --辅助材料
               ,0 labor_material_cost                      --人工材料
               ,0 fuel_cost                                --燃料
               ,0 water_power_cost                         --水电
               ,0 fuel_power_cost                          --燃料动力
               ,0 manufacture_change_cost                  --制造费用变动
               ,0 manufacture_fixed_cost                   --制造费用固定
               ,0 during_cost                              --期间费用
               ,0 day_inbuy_prod_weight                    --日自购产量(重量)
               ,0 accum_inbuy_prod_weight                  --月累计自购产量
               ,a1.buy_weight settlement_weight            --结算重量
          FROM (SELECT *
                  FROM dwu_qw_qw11_dd
                 WHERE op_day='$OP_DAY') a1                --QW11
          LEFT JOIN (SELECT org_id,
                            level6_org_id
                       FROM mreport_global.dim_org_management
                      GROUP BY org_id,level6_org_id) a2
            ON (a1.org_id=a2.level6_org_id)
          LEFT JOIN (SELECT *
                       FROM dwu_qw_contract_dd
                      WHERE op_day='$OP_DAY') a3
            ON (a1.pith_no=a3.contractnumber)) t1
 WHERE org_id is not null
   AND product_line is not null
 GROUP BY period_id
       ,org_id
       ,case when product_line='10' then '1'
             when product_line='20' then '2'
        else null end
"

###########################################################################################
## 计算日保本价
## 变量声明
DMP_BIRD_KEEP_PRICE_DD='DMP_BIRD_KEEP_PRICE_DD'

CREATE_DMP_BIRD_KEEP_PRICE_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_KEEP_PRICE_DD(
  month_id                       string       --期间(月)
  ,day_id                        string       --期间(日)
  ,level1_org_id                 string       --组织1级
  ,level1_org_descr              string       --组织1级
  ,level2_org_id                 string       --组织2级
  ,level2_org_descr              string       --组织2级
  ,level3_org_id                 string       --组织3级
  ,level3_org_descr              string       --组织3级
  ,level4_org_id                 string       --组织4级
  ,level4_org_descr              string       --组织4级
  ,level5_org_id                 string       --组织5级
  ,level5_org_descr              string       --组织5级
  ,level6_org_id                 string       --组织6级
  ,level6_org_descr              string       --组织6级
  ,production_line_id            string       --产线
  ,production_line_descr         string       --产线
  ,day_keep_amt                  string       --本日保本价(元)
  ,mon_keep_amt                  string       --本月保本价(元)
  ,create_time                   string       --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_KEEP_PRICE_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_KEEP_PRICE_DD PARTITION(op_day='$OP_DAY')
SELECT substr(t1.period_id,1,6) month_id       --期间(月)
       ,t1.period_id day_id                    --期间(日)
       ,t3.level1_org_id                       --组织1级
       ,t3.level1_org_descr                    --组织1级
       ,t3.level2_org_id                       --组织2级
       ,t3.level2_org_descr                    --组织2级
       ,t3.level3_org_id                       --组织3级
       ,t3.level3_org_descr                    --组织3级
       ,t3.level4_org_id                       --组织4级
       ,t3.level4_org_descr                    --组织4级
       ,t3.level5_org_id                       --组织5级
       ,t3.level5_org_descr                    --组织5级
       ,t3.level6_org_id                       --组织6级
       ,t3.level6_org_descr                    --组织6级
       ,t1.production_line_id                  --产线
       ,case when t1.production_line_id='1' then '鸡线'
             when t1.production_line_id='2' then '鸭线'
        else null end production_line_descr
       ,case when t4.level6_org_id is not null then coalesce(t1.day_tax_keep_price,0)
        else coalesce(round(t1.day_keep_price/(1-t2.rate),4),0) end day_keep_amt  --本日保本价
       ,case when t4.level6_org_id is not null then coalesce(t1.mon_tax_keep_price,0)
        else coalesce(round(t1.mon_keep_price/(1-t2.rate),4),0) end mon_keep_amt  --本月保本价
       ,'$CREATE_TIME'                         --当前时间
  FROM (SELECT period_id
               ,org_id
               ,production_line_id
               ,case when coalesce(settlement_weight,0)=0 then 0 else (amount_b+byproduct_amount+input_tax-(packing_material_cost+excipient_material_cost+labor_material_cost+fuel_cost+water_power_cost+manufacture_change_cost+manufacture_fixed_cost+during_cost))*(day_inbuy_prod_weight/settlement_weight)/2000 end day_tax_keep_price
               ,case when coalesce(settlement_weight,0)=0 then 0 else (amount_b+byproduct_amount-(packing_material_cost+excipient_material_cost+labor_material_cost+fuel_cost+water_power_cost+manufacture_change_cost+manufacture_fixed_cost+during_cost))*(day_inbuy_prod_weight/settlement_weight)/2000 end day_keep_price
               ,case when coalesce(settlement_weight,0)=0 then 0 else (sum_amount_b+byproduct_amount+input_tax-(packing_material_cost+excipient_material_cost+labor_material_cost+fuel_cost+water_power_cost+manufacture_change_cost+manufacture_fixed_cost+during_cost))*(accum_inbuy_prod_weight/settlement_weight)/2000 end mon_tax_keep_price
               ,case when coalesce(settlement_weight,0)=0 then 0 else (sum_amount_b+byproduct_amount-(packing_material_cost+excipient_material_cost+labor_material_cost+fuel_cost+water_power_cost+manufacture_change_cost+manufacture_fixed_cost+during_cost))*(accum_inbuy_prod_weight/settlement_weight)/2000 end mon_keep_price
          FROM $TMP_DMP_BIRD_KEEP_PRICE_DD_1
         WHERE op_day='$OP_DAY') t1
  LEFT JOIN (SELECT 1-meaning rate
               FROM mreport_global.ods_ebs_fnd_lookup_values
              WHERE lookup_type='CUX_QW_FINISHED_PRODUCT_RATE'
                AND language='ZHS') t2
    ON (1=1)
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
  LEFT JOIN (SELECT a2.level6_org_id
               FROM (SELECT lookup_code inv_org_code,
                            meaning org_name
                       FROM mreport_global.ods_ebs_fnd_lookup_values
                      WHERE lookup_type='BWP_TAX_RATE_CONVERSION'
                        AND language='ZHS') a1
              INNER JOIN (SELECT inv_org_code,
                                 level6_org_id
                            FROM mreport_global.dim_org_inv_management
                           WHERE inv_org_code is not null) a2
                 ON (a1.inv_org_code=a2.inv_org_code)
             GROUP BY a2.level6_org_id) t4
    ON (t3.level6_org_id=t4.level6_org_id)
  WHERE t3.level2_org_id NOT IN('1015')
    AND t1.production_line_id is not null
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_KEEP_PRICE_DD_0;
    $INSERT_TMP_DMP_BIRD_KEEP_PRICE_DD_0;
    $CREATE_TMP_DMP_BIRD_KEEP_PRICE_DD_1;
    $INSERT_TMP_DMP_BIRD_KEEP_PRICE_DD_1;
    $CREATE_DMP_BIRD_KEEP_PRICE_DD;
    $INSERT_DMP_BIRD_KEEP_PRICE_DD;
"  -v

