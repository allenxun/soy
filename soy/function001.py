#1、连接clickhouse，按照sku纬度计算订单总量，计算生产线生产总能力
#2、比对生产订单需求和生产能力供应的关系。
#3、给出初步排产结果
#4、减少订单需求数量最少的工单，重新计算生产能力，重新计算订单需求，重新计算排产结果

clickhouse_host = "localhost"
clickhouse_port = "8123"
clickhouse_database = "default"
clickhouse_usename = ""
clickhouse_password = ""

def count_orders(spark):
    # ClickHouse连接配置
    clickhouse_options = {
        "url": "jdbc:clickhouse://"+clickhouse_host+":"+clickhouse_port+"/"+clickhouse_database,
        "query": "SELECT COUNT(*) AS order_count FROM orders_table",
        "driver": "ru.yandex.clickhouse.ClickHouseDriver",
        "user": clickhouse_usename,
        "password": clickhouse_password
    }

    # 从ClickHouse中读取数据
    orders_df = spark.read.format("jdbc").options(**clickhouse_options).load()

    # 统计订单数量
    order_count = orders_df.collect()[0]["order_count"]
    print("Total number of orders:", order_count)

