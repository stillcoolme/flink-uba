### 模块简介

#### hotitem 热门商品统计

##### HotItem

##### HotItemWithSql


#### orderdetect 订单支付实时监控

基本需求：
1. 用户下单之后，应设置订单失效时间，以提高用户支付的意愿，并降低系统风险；用户下单后15分钟未支付，则输出监控信息；
2. 对于订单支付事件，用户支付完成还得确认平台账户上是否到账了。

方案选型：
1.1 业务系统自己不断检测订单是否时效；redis设置key有效时间，查订单的时候去redis查是否还有该订单号；时效性不好，订单多的话业务系统压力大；
1.2 flink实时处理；

解决思路：
1.1 利用 CEP 库进行事件流的模式匹配，并设定匹配的时间间隔
1.2 也可以利用状态编程，用 process function 实现处理逻辑

2.1 要同时读入两条流的数据来做合并处理。利用 connect将两条流进行连接，用自定义的CoProcessFunction 进行处理。


代码：
1. OrderPayTimeout  CEP检测超时未支付的订单
2. OrderTimeoutWithoutCep  processFunction来检测超时未支付的订单
3. TxPayMatch 交易对账
