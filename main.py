# This is a sample Python script.
from pyspark.sql import SparkSession
from lib import aggregator
from lib import utils


# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
def hello_spark_without_conf_file(app_name):
    spark_session = SparkSession.builder.appName(app_name) \
        .master("local[*]") \
        .getOrCreate()

    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    data_frame = spark_session.createDataFrame(data_list).toDF("Name", "Age")
    data_frame.show(n=3)
    spark_session.stop()


def hello_spark_with_conf_file():
    spark_session = utils.create_spark_session_from_config_file(config_file_location="config/spark.conf",
                                                                enable_hive_support=False)

    data_list = [("Ravi", 28), ("David", 45), ("Abdul", 37)]
    data_frame = spark_session.createDataFrame(data_list).toDF("Name", "Age")
    data_frame.show(n=3)

    utils.stop_spark_session(spark_session=spark_session)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # hello_spark_without_conf_file("Spark Aggregations")
    # hello_spark_with_conf_file()
    # aggregator.spark_aggregate_functions_example()
    # aggregator.analyse_invoices_using_simple_aggregation_functions()
    # aggregator.analyse_invoices_group_invoices_by_country_and_weeknumber()
    aggregator.analyse_invoices_rolling_sum_of_weeks_using_window_aggregation()
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
