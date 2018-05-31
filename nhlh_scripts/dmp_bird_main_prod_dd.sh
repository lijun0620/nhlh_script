#!/bin/bash

######################################################################
#                                                                    
# 程    序: dmp_bird_main_prod_dd.sh                               
# 创建时间: 2017年08月16日                                            
# 创 建 者: zgh                                                      
# 参数:                                                              
#    参数1: 日期[yyyymmdd]                                             
# 补充说明: 
# 功    能: 每日主产品销量明细表
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
    echo "输入参数错误，调用示例: dmp_bird_main_prod_dd.sh 20180101"
    exit 1
fi



###########################################################################################
## 建立临时表，用于存放养殖户的棚舍地址数量
TMP_DMP_BIRD_MAIN_PROD_DD_1='TMP_DMP_BIRD_MAIN_PROD_DD_1'

CREATE_TMP_DMP_BIRD_MAIN_PROD_DD_1="
CREATE TABLE IF NOT EXISTS $TMP_DMP_BIRD_MAIN_PROD_DD_1(
      month_id                         string      --期间(月份)
      ,day_id                          string      --核销日期
      ,level1_org_id                   string      --组织1级(股份)
      ,level1_org_descr                string      --组织1级(股份)
      ,level2_org_id                   string      --组织2级(片联)
      ,level2_org_descr                string      --组织2级(片联)
      ,level3_org_id                   string      --组织3级(片区)
      ,level3_org_descr                string      --组织3级(片区)
      ,level4_org_id                   string      --组织4级(小片)
      ,level4_org_descr                string      --组织4级(小片)
      ,level5_org_id                   string      --组织5级(公司)
      ,level5_org_descr                string      --组织5级(公司)
      ,level6_org_id                   string      --组织6级(OU)
      ,level6_org_descr                string      --组织6级(OU)
      ,level7_org_id                   string      --组织7级(库存组织)
      ,level7_org_descr                string      --组织7级(库存组织)
      ,level1_businesstype_id          string     --业态1级id
      ,level1_businesstype_name        string     --业态1级
      ,level2_businesstype_id          string     --业态2级id
      ,level2_businesstype_name        string     --业态2级
      ,level3_businesstype_id          string     --业态3级id
      ,level3_businesstype_name        string     --业态3级
      ,level4_businesstype_id          string     --业态4级id
      ,level4_businesstype_name        string     --业态4级
      ,level1_material_id              string      --物料1级
      ,level1_material_descr           string      --物料1级
      ,level2_material_id              string      --物料2级
      ,level2_material_descr           string      --物料2级
      ,level3_material_id              string      --物料3级
      ,level3_material_descr           string      --物料3级
      ,level4_material_id              string      --物料4级
      ,level4_material_descr           string      --物料4级
      ,trade_type_id                   string      --交易关系
      ,trade_type_descr                string      --交易关系
      ,depart_id                       string      --系别ID
      ,depart_descr                    string      --系别名称
      ,selfa_chickens_sale_qty         string      --A大雏销量(自产)
      ,selfa_chickens_sale_amt         string      --A大雏销售金额(自产)
      ,selfa_chickens_discount_amt     string      --A大雏折扣金额(自产)
      ,selfa_growing_sale_qty          string      --A中雏销量(自产)
      ,selfa_growing_sale_amt          string      --A中雏销售金额(自产)
      ,selfa_growing_discount_amt      string      --A中雏折扣金额(自产)
      ,selfa_small_sale_qty            string    --A小雏销量(自产)
      ,selfa_small_sale_amt            string    --A小雏销售金额(自产)
      ,selfa_small_discount_amt        string    --A小雏折扣金额(自产)      
      ,selfb_sale_qty                  string      --B雏销量(自产)
      ,selfb_sale_amt                  string      --B雏销售金额(自产)
      ,selfb_discount_amt              string      --B雏折扣金额(自产)
      ,ina_chickens_sale_qty           string      --A大雏销量(NG)
      ,ina_chickens_sale_amt           string      --A大雏销售金额(NG)
      ,ina_chickens_discount_amt       string      --A大雏折扣金额(NG)
      ,ina_growing_sale_qty            string      --A中雏销量(NG)
      ,ina_growing_sale_amt            string      --A中雏销售金额(NG)
      ,ina_growing_discount_amt        string      --A中雏折扣金额(NG)
      ,ina_small_sale_qty              string    --A小雏销量(NG)
      ,ina_small_sale_amt              string    --A小雏销售金额(NG)
      ,ina_small_discount_amt          string    --A小雏折扣金额(NG)
      ,inb_sale_qty                    string      --B雏销量(NG)
      ,inb_sale_amt                    string      --B雏销售金额(NG)
      ,inb_discount_amt                string      --B雏折扣金额(NG)
      ,outa_chickens_sale_qty          string      --A大雏销量(WG)
      ,outa_chickens_sale_amt          string      --A大雏销售金额(WG)
      ,outa_chickens_discount_amt      string      --A大雏折扣金额(WG)
      ,outa_growing_sale_qty           string      --A中雏销量(WG)
      ,outa_growing_sale_amt           string      --A中雏销售金额(WG)
      ,outa_growing_discount_amt       string      --A中雏折扣金额(WG)
      ,outa_small_sale_qty             string    --A小雏销量(WG)
      ,outa_small_sale_amt             string    --A小雏销售金额(WG)
      ,outa_small_discount_amt         string    --A小雏折扣金额(WG)
      ,outb_sale_qty                   string      --B雏销量(WG)
      ,outb_sale_amt                   string      --B雏销售金额(WG) 
      ,outb_discount_amt               string      --B雏折扣金额(WG)
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS ORC
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_TMP_DMP_BIRD_MAIN_PROD_DD_1="
INSERT OVERWRITE TABLE $TMP_DMP_BIRD_MAIN_PROD_DD_1 PARTITION(op_day='$OP_DAY')
SELECT '$OP_MONTH' month_id             --期间(月份) 
       ,promise_date                    --期间(日)--出库时间
       ,t3.level1_org_id                        --组织1级(股份)  
       ,t3.level1_org_descr                     --组织1级(股份)  
       ,t3.level2_org_id                        --组织2级(片联)  
       ,t3.level2_org_descr                     --组织2级(片联)  
       ,t3.level3_org_id                        --组织3级(片区)  
       ,t3.level3_org_descr                     --组织3级(片区)  
       ,t3.level4_org_id                        --组织4级(小片)  
       ,t3.level4_org_descr                     --组织4级(小片)  
       ,t3.level5_org_id                        --组织5级(公司)  
       ,t3.level5_org_descr                     --组织5级(公司)  
       ,t3.level6_org_id                        --组织6级(OU)  
       ,t3.level6_org_descr                     --组织6级(OU)  
       ,t4.level7_org_id                        --组织7级(库存组织)
       ,t4.level7_org_descr                     --组织7级(库存组织)
       ,t5.level1_businesstype_id              --业态1级
       ,t5.level1_businesstype_name            --业态1级
       ,t5.level2_businesstype_id              --业态2级
       ,t5.level2_businesstype_name            --业态2级
       ,t5.level3_businesstype_id              --业态3级
       ,t5.level3_businesstype_name            --业态3级
       ,t5.level4_businesstype_id              --业态4级
       ,t5.level4_businesstype_name            --业态4级
       ,t1.material_segment1_id                 --物料1级
       ,t1.material_segment1_desc               --物料1级
       ,t1.material_segment2_id                 --物料2级
       ,t1.material_segment2_desc               --物料2级
       ,t1.material_segment3_id                 --物料3级
       ,t1.material_segment3_desc               --物料3级
       ,t1.material_segment4_id                 --物料4级
       ,t1.material_segment4_desc               --物料4级
       ,'缺失' trade_type_id
       ,t1.cust_trade_rltion
       ,t1.material_segment3_id    depart_id
       ,t1.material_segment3_desc  depart_descr
       ,t1.selfa_chickens_sale_qty               --A大雏销量(自产)
       ,t1.selfa_chickens_sale_amt               --A大雏销售金额(自产)
       ,t1.selfa_chickens_discount_amt           --A大雏折扣金额(自产)
       ,t1.selfa_growing_sale_qty                --A中雏销量(自产)
       ,t1.selfa_growing_sale_amt                --A中雏销售金额(自产)
       ,t1.selfa_growing_discount_amt            --A中雏折扣金额(自产)
       ,t1.selfa_small_sale_qty                --A小雏销量(自产)
       ,t1.selfa_small_sale_amt                --A小雏销售金额(自产)
       ,t1.selfa_small_discount_amt            --A小雏折扣金额(自产)      
       ,t1.selfb_sale_qty                        --B雏销量(自产)
       ,t1.selfb_sale_amt                        --B雏销售金额(自产)
       ,t1.selfb_discount_amt                    --B雏折扣金额(自产)
       ,t1.ina_chickens_sale_qty                 --A大雏销量(NG)
       ,t1.ina_chickens_sale_amt                 --A大雏销售金额(NG)
       ,t1.ina_chickens_discount_amt             --A大雏折扣金额(NG)
       ,t1.ina_growing_sale_qty                  --A中雏销量(NG)
       ,t1.ina_growing_sale_amt                  --A中雏销售金额(NG)
       ,t1.ina_growing_discount_amt              --A中雏折扣金额(NG)
       ,t1.ina_small_sale_qty                  --A小雏销量(NG)
       ,t1.ina_small_sale_amt                  --A小雏销售金额(NG)
       ,t1.ina_small_discount_amt              --A小雏折扣金额(NG)
       ,t1.inb_sale_qty                          --B雏销量(NG)
       ,t1.inb_sale_amt                          --B雏销售金额(NG)
       ,t1.inb_discount_amt                      --B雏折扣金额(NG)
       ,t1.outa_chickens_sale_qty                --A大雏销量(WG)
       ,t1.outa_chickens_sale_amt                --A大雏销售金额(WG)
       ,t1.outa_chickens_discount_amt            --A大雏折扣金额(WG)
       ,t1.outa_growing_sale_qty                 --A中雏销量(WG)
       ,t1.outa_growing_sale_amt                 --A中雏销售金额(WG)
       ,t1.outa_growing_discount_amt             --A中雏折扣金额(WG)
       ,t1.outa_small_sale_qty                 --A小雏销量(WG)
       ,t1.outa_small_sale_amt                 --A小雏销售金额(WG)
       ,t1.outa_small_discount_amt             --A小雏折扣金额(WG)
       ,t1.outb_sale_qty                         --B雏销量(WG)
       ,t1.outb_sale_amt                         --B雏销售金额(WG) 
       ,t1.outb_discount_amt                     --B雏折扣金额(WG)
  FROM (
        select 
               aa.org_id
               ,aa.inv_org_id
               ,aa.bus_type
               ,aa.material_segment1_id
               ,aa.material_segment1_desc
               ,aa.material_segment2_id
               ,aa.material_segment2_desc
               ,aa.material_segment3_id
               ,aa.material_segment3_desc
               ,aa.material_segment4_id
               ,aa.material_segment4_desc
               ,substr(regexp_replace(aa.OUT_DATE,'-',''),1,8) promise_date            --出库日期
               ,aa.cust_trade_rltion       --交易关系
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)            selfa_chickens_sale_qty        --A大雏销量(自产)
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)  selfa_chickens_sale_amt        --A大雏销售金额(自产)
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='自产'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)  selfa_chickens_discount_amt    --A大雏折扣金额(自产)
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)            selfa_growing_sale_qty         --A中雏销量(自产)   
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)  selfa_growing_sale_amt         --A中雏销售金额(自产) 
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='自产'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)  selfa_growing_discount_amt     --A中雏折扣金额(自产) 
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)            selfa_small_sale_qty           --A小雏销量(自产)   
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*LOC_PRICE else 0 end)  selfa_small_sale_amt           --A小雏销售金额(自产) 
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='自产'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)  selfa_small_discount_amt       --A小雏折扣金额(自产) 
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              selfb_sale_qty                 --B雏销量(自产)  
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='自产'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    selfb_sale_amt                 --B雏销售金额(自产)
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='自产'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    selfb_discount_amt             --B雏折扣金额(自产)
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              ina_chickens_sale_qty          --A大雏销量(NG)   
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    ina_chickens_sale_amt          --A大雏销售金额(NG) 
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='内购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    ina_chickens_discount_amt      --A大雏折扣金额(NG) 
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              ina_growing_sale_qty           --A中雏销量(NG)   
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    ina_growing_sale_amt           --A中雏销售金额(NG) 
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='内购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    ina_growing_discount_amt       --A中雏折扣金额(NG)
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)             ina_small_sale_qty             --A小雏销量(NG)   
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    ina_small_sale_amt             --A小雏销售金额(NG) 
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='内购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    ina_small_discount_amt         --A小雏折扣金额(NG) 
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)                inb_sale_qty                   --B雏销量(NG)   
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='内购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)      inb_sale_amt                   --B雏销售金额(NG) 
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='内购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)      inb_discount_amt               --B雏折扣金额(NG) 
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              outa_chickens_sale_qty         --A大雏销量(WG)  
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    outa_chickens_sale_amt         --A大雏销售金额(WG)
               ,sum(case when aa.material_segment5_desc='A大雏' and aa.product_recordname='外购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    outa_chickens_discount_amt     --A大雏折扣金额(WG)
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              outa_growing_sale_qty          --A中雏销量(WG)  
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    outa_growing_sale_amt          --A中雏销售金额(WG)
               ,sum(case when aa.material_segment5_desc='A中雏' and aa.product_recordname='外购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    outa_growing_discount_amt      --A中雏折扣金额(WG)
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)              outa_small_sale_qty            --A小雏销量(WG)  
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)    outa_small_sale_amt            --A小雏销售金额(WG)
               ,sum(case when aa.material_segment5_desc='A小雏' and aa.product_recordname='外购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)    outa_small_discount_amt        --A小雏折扣金额(WG)
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY) else 0 end)                outb_sale_qty                  --B雏销量(WG)   
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='外购'  then (aa.OUT_QUANTITY-aa.GIVEN_QUANTITY)*aa.LOC_PRICE else 0 end)      outb_sale_amt                  --B雏销售金额(WG) 
               ,sum(case when aa.material_segment5_desc='B雏' and aa.product_recordname='外购'  and aa.IS_SPECIAL_PRICE='Y' then (aa.OFF_QUANTITY*aa.OFF_PRICE) else 0 end)      outb_discount_amt              --B雏折扣金额(WG)               
             from
				        (     
				        select a.org_id,
				               a.inv_org_id,
				               a.bus_type,
				               b.material_segment1_id,
				               b.material_segment1_desc,
				               b.material_segment2_id,
				               b.material_segment2_desc,
				               b.material_segment3_id,
				               b.material_segment3_desc,
				               b.material_segment4_id,
				               b.material_segment4_desc,
				               a.out_date,
				               a.cust_trade_rltion,
				               b.material_segment5_desc,
				               b.product_recordname,
				               a.is_special_price,
				               a.out_quantity,
				               a.given_quantity,
				               case when a.loc_currency_id='CNY' then a.loc_price
				                    else c.conversion_rate*a.loc_price end  loc_price,
				               a.off_quantity,
				               case when a.loc_currency_id='CNY' then a.off_price
				                    else c.conversion_rate*a.off_price end  off_price
				         from mreport_poultry.dwu_xs_other_sale_dd a
				      inner join mreport_global.dwu_dim_material_new b on a.material_id=b.inventory_item_id
              left join (SELECT from_currency,
                                to_currency,
                                conversion_rate
                           FROM mreport_global.dmd_fin_daily_currency_rate_dd
                          WHERE conversion_date='$OP_DAY'
                            AND to_currency='CNY'
                         ) c
                      ON a.loc_currency_id=c.from_currency
				        where a.op_day='$OP_DAY'
				          --and a.given_quantity='-0.0'
				          and b.material_segment5_desc in ('A大雏','A中雏','A小雏','B雏')
				         )  aa
        group by  aa.org_id
               ,aa.inv_org_id
               ,aa.bus_type
               ,aa.material_segment1_id
               ,aa.material_segment1_desc
               ,aa.material_segment2_id
               ,aa.material_segment2_desc
               ,aa.material_segment3_id
               ,aa.material_segment3_desc
               ,aa.material_segment4_id
               ,aa.material_segment4_desc
               ,substr(regexp_replace(aa.OUT_DATE,'-',''),1,8)
               ,aa.cust_trade_rltion
       ) t1
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_management
              WHERE org_id is not null) t3
    ON (t1.org_id=t3.org_id and t1.bus_type=t3.bus_type_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_inv_management
              WHERE inv_org_id is not null) t4
    ON (t1.inv_org_id=t4.inv_org_id)
  LEFT JOIN (SELECT *
               FROM mreport_global.dim_org_businesstype
              WHERE level4_businesstype_name is not null) t5
    ON (t1.bus_type=t5.level4_businesstype_id)
"



###########################################################################################
## 将数据从大表转换至目标表
DMP_BIRD_MAIN_PROD_DD='DMP_BIRD_MAIN_PROD_DD'

CREATE_DMP_BIRD_MAIN_PROD_DD="
CREATE TABLE IF NOT EXISTS $DMP_BIRD_MAIN_PROD_DD(
      month_id                         string      --期间(月份)
      ,day_id                          string      --核销日期
      ,level1_org_id                   string      --组织1级(股份)
      ,level1_org_descr                string      --组织1级(股份)
      ,level2_org_id                   string      --组织2级(片联)
      ,level2_org_descr                string      --组织2级(片联)
      ,level3_org_id                   string      --组织3级(片区)
      ,level3_org_descr                string      --组织3级(片区)
      ,level4_org_id                   string      --组织4级(小片)
      ,level4_org_descr                string      --组织4级(小片)
      ,level5_org_id                   string      --组织5级(公司)
      ,level5_org_descr                string      --组织5级(公司)
      ,level6_org_id                   string      --组织6级(OU)
      ,level6_org_descr                string      --组织6级(OU)
      ,level7_org_id                   string      --组织7级(库存组织)
      ,level7_org_descr                string      --组织7级(库存组织)
      ,level1_businesstype_id          string     --业态1级
      ,level1_businesstype_name        string     --业态1级
      ,level2_businesstype_id          string     --业态2级
      ,level2_businesstype_name        string     --业态2级
      ,level3_businesstype_id          string     --业态3级
      ,level3_businesstype_name        string     --业态3级
      ,level4_businesstype_id          string     --业态4级
      ,level4_businesstype_name        string     --业态4级
      ,level1_material_id              string      --物料1级
      ,level1_material_descr           string      --物料1级
      ,level2_material_id              string      --物料2级
      ,level2_material_descr           string      --物料2级
      ,level3_material_id              string      --物料3级
      ,level3_material_descr           string      --物料3级
      ,level4_material_id              string      --物料4级
      ,level4_material_descr           string      --物料4级
      ,trade_type_id                   string      --交易关系
      ,trade_type_descr                string      --交易关系
      ,depart_id                       string      --系别ID
      ,depart_descr                    string      --系别名称
      ,selfa_chickens_sale_qty         string      --A大雏销量(自产)
      ,selfa_chickens_sale_amt         string      --A大雏销售金额(自产)
      ,selfa_chickens_discount_amt     string      --A大雏折扣金额(自产)
      ,selfa_growing_sale_qty          string      --A中雏销量(自产)
      ,selfa_growing_sale_amt          string      --A中雏销售金额(自产)
      ,selfa_growing_discount_amt      string      --A中雏折扣金额(自产)
      ,selfa_small_sale_qty            string    --A小雏销量(自产)
      ,selfa_small_sale_amt            string    --A小雏销售金额(自产)
      ,selfa_small_discount_amt        string    --A小雏折扣金额(自产)      
      ,selfb_sale_qty                  string      --B雏销量(自产)
      ,selfb_sale_amt                  string      --B雏销售金额(自产)
      ,selfb_discount_amt              string      --B雏折扣金额(自产)
      ,ina_chickens_sale_qty           string      --A大雏销量(NG)
      ,ina_chickens_sale_amt           string      --A大雏销售金额(NG)
      ,ina_chickens_discount_amt       string      --A大雏折扣金额(NG)
      ,ina_growing_sale_qty            string      --A中雏销量(NG)
      ,ina_growing_sale_amt            string      --A中雏销售金额(NG)
      ,ina_growing_discount_amt        string      --A中雏折扣金额(NG)
      ,ina_small_sale_qty              string    --A小雏销量(NG)
      ,ina_small_sale_amt              string    --A小雏销售金额(NG)
      ,ina_small_discount_amt          string    --A小雏折扣金额(NG)
      ,inb_sale_qty                    string      --B雏销量(NG)
      ,inb_sale_amt                    string      --B雏销售金额(NG)
      ,inb_discount_amt                string      --B雏折扣金额(NG)
      ,outa_chickens_sale_qty          string      --A大雏销量(WG)
      ,outa_chickens_sale_amt          string      --A大雏销售金额(WG)
      ,outa_chickens_discount_amt      string      --A大雏折扣金额(WG)
      ,outa_growing_sale_qty           string      --A中雏销量(WG)
      ,outa_growing_sale_amt           string      --A中雏销售金额(WG)
      ,outa_growing_discount_amt       string      --A中雏折扣金额(WG)
      ,outa_small_sale_qty             string    --A小雏销量(WG)
      ,outa_small_sale_amt             string    --A小雏销售金额(WG)
      ,outa_small_discount_amt         string    --A小雏折扣金额(WG)
      ,outb_sale_qty                   string      --B雏销量(WG)
      ,outb_sale_amt                   string      --B雏销售金额(WG) 
      ,outb_discount_amt               string      --B雏折扣金额(WG)
      ,create_time                     string      --创建时间
)
PARTITIONED BY (op_day string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\011'
STORED AS TEXTFILE
"

## 转换数据
echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
INSERT_DMP_BIRD_MAIN_PROD_DD="
INSERT OVERWRITE TABLE $DMP_BIRD_MAIN_PROD_DD PARTITION(OP_DAY='$OP_DAY')
SELECT   
      month_id                        
      ,day_id                         
      ,level1_org_id                  
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
      ,level7_org_id                  
      ,level7_org_descr
      ,level1_businesstype_id             
      ,level1_businesstype_name           
      ,level2_businesstype_id             
      ,level2_businesstype_name           
      ,level3_businesstype_id             
      ,level3_businesstype_name           
      ,level4_businesstype_id             
      ,level4_businesstype_name                      
      ,level1_material_id             
      ,level1_material_descr          
      ,level2_material_id             
      ,level2_material_descr          
      ,level3_material_id             
      ,level3_material_descr          
      ,level4_material_id             
      ,level4_material_descr          
      ,trade_type_id                  
      ,trade_type_descr               
      ,depart_id                      
      ,depart_descr                   
      ,selfa_chickens_sale_qty        
      ,selfa_chickens_sale_amt        
      ,selfa_chickens_discount_amt    
      ,selfa_growing_sale_qty         
      ,selfa_growing_sale_amt         
      ,selfa_growing_discount_amt     
      ,selfa_small_sale_qty           
      ,selfa_small_sale_amt           
      ,selfa_small_discount_amt       
      ,selfb_sale_qty                 
      ,selfb_sale_amt                 
      ,selfb_discount_amt             
      ,ina_chickens_sale_qty          
      ,ina_chickens_sale_amt          
      ,ina_chickens_discount_amt      
      ,ina_growing_sale_qty           
      ,ina_growing_sale_amt           
      ,ina_growing_discount_amt       
      ,ina_small_sale_qty             
      ,ina_small_sale_amt             
      ,ina_small_discount_amt         
      ,inb_sale_qty                   
      ,inb_sale_amt                   
      ,inb_discount_amt               
      ,outa_chickens_sale_qty         
      ,outa_chickens_sale_amt         
      ,outa_chickens_discount_amt     
      ,outa_growing_sale_qty          
      ,outa_growing_sale_amt          
      ,outa_growing_discount_amt      
      ,outa_small_sale_qty            
      ,outa_small_sale_amt            
      ,outa_small_discount_amt        
      ,outb_sale_qty                  
      ,outb_sale_amt                  
      ,outb_discount_amt              
      ,'$CREATE_TIME' create_time        --创建时间
  FROM (SELECT *
          FROM $TMP_DMP_BIRD_MAIN_PROD_DD_1
         WHERE OP_DAY='$OP_DAY') t1
  WHERE (t1.selfa_chickens_sale_qty        
      +t1.selfa_chickens_sale_amt        
      +t1.selfa_chickens_discount_amt    
      +t1.selfa_growing_sale_qty         
      +t1.selfa_growing_sale_amt         
      +t1.selfa_growing_discount_amt     
      +t1.selfa_small_sale_qty           
      +t1.selfa_small_sale_amt           
      +t1.selfa_small_discount_amt       
      +t1.selfb_sale_qty                 
      +t1.selfb_sale_amt                 
      +t1.selfb_discount_amt             
      +t1.ina_chickens_sale_qty          
      +t1.ina_chickens_sale_amt          
      +t1.ina_chickens_discount_amt      
      +t1.ina_growing_sale_qty           
      +t1.ina_growing_sale_amt           
      +t1.ina_growing_discount_amt       
      +t1.ina_small_sale_qty             
      +t1.ina_small_sale_amt             
      +t1.ina_small_discount_amt         
      +t1.inb_sale_qty                   
      +t1.inb_sale_amt                   
      +t1.inb_discount_amt               
      +t1.outa_chickens_sale_qty         
      +t1.outa_chickens_sale_amt         
      +t1.outa_chickens_discount_amt     
      +t1.outa_growing_sale_qty          
      +t1.outa_growing_sale_amt          
      +t1.outa_growing_discount_amt      
      +t1.outa_small_sale_qty            
      +t1.outa_small_sale_amt            
      +t1.outa_small_discount_amt        
      +t1.outb_sale_qty                  
      +t1.outb_sale_amt                  
      +t1.outb_discount_amt)<>0
"

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>执行语句>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
hive -e "
    use mreport_poultry;
    $CREATE_TMP_DMP_BIRD_MAIN_PROD_DD_1;
    $INSERT_TMP_DMP_BIRD_MAIN_PROD_DD_1;
    $CREATE_DMP_BIRD_MAIN_PROD_DD;
    $INSERT_DMP_BIRD_MAIN_PROD_DD;
"  -v