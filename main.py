#######
#使用pyspark进行数据处理
#1.检核总的生产线和人天是否满足
#2.获取初步的排程结果
#3.检核包材是否满足
#4.计算排程的评价结果
#整体上，每个worker的工作是独立的，不需要进行数据的交互，其中包括多个ExecutorBackend，每个包含独立的excetor执行相应的排程任务
####

import concurrent.futures
from pyspark.sql import SparkSession

from soy import function001

# 初始化SparkSession
spark = SparkSession.builder.appName("ConcurrentSparkFunctions").getOrCreate()

# 定义需要并发运行的Spark函数
def function1(spark):
    function001.count_orders(spark)



# 使用ThreadPoolExecutor并发执行多个Spark函数
with concurrent.futures.ThreadPoolExecutor() as executor:
    # 提交任务给Executor并发执行
    future1 = executor.submit(function1, spark)

    # 等待所有任务完成
    concurrent.futures.wait([future1])

# 关闭SparkSession
spark.stop()
