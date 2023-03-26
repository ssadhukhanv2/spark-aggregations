from gc import collect

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import monotonically_increasing_id, approx_count_distinct, first, last, mean, skewness, min, \
    max, stddev, stddev_samp, stddev_pop, variance, var_samp, var_pop, sum, sum_distinct, count, \
    count_distinct, avg, expr, round, to_date, weekofyear, col


def spark_aggregate_functions_example():
    # Demo of some common Aggregation Functions available in Spark
    # approx_count_distinct() first() last() max() min() avg() min() mean() skewness() stddev() variance() etc

    # https://sparkbyexamples.com/spark/spark-sql-aggregate-functions/
    spark_session = lambda: SparkSession.builder.appName("Spark By Examples") \
        .master("local") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()

    spark = spark_session()
    employee_list = [("James", "Sales", 3000),
                     ("Michael", "Sales", 4600),
                     ("Robert", "Sales", 4100),
                     ("Maria", "Finance", 3000),
                     ("James", "Sales", 3000),
                     ("Scott", "Finance", 3300),
                     ("Jen", "Finance", 3900),
                     ("Jeff", "Marketing", 3000),
                     ("Kumar", "Marketing", 2000),
                     ("Saif", "Sales", 4100)]

    employee_df = spark.createDataFrame(employee_list).toDF("employee_name", "department", "salary")
    employee_df.withColumn("id", monotonically_increasing_id())

    employee_df.filter(employee_df.salary > 4000).show(n=10)

    print(f"Spark Version: {spark.sparkContext.version}")
    print(f"Employee count: {employee_df.count()}")

    # approx_count_distinct
    # Using .agg() method
    distinct_salaries_count_df = employee_df.agg(approx_count_distinct("salary") \
                                                 .alias("Number of Distinct Salaries"))

    # Using .select()
    # distinct_salaries_count_df=employee_df.select(approx_count_distinct("salary"))

    print(
        f"distinct_salaries_count: {distinct_salaries_count_df}, Type: {type(distinct_salaries_count_df)}")
    distinct_salaries_count_df.show()

    # first
    # Using .agg() method
    first_salary_df = employee_df.select(first("salary")).alias("First Salary")
    # Using .select()
    # first_salary_df = employee_df.agg(first("salary")) \
    #     .alias("First Salary")
    print(
        f"first_salary First Salary: {first_salary_df}, Type: {type(first_salary_df)}")
    first_salary_df.show()

    # last
    employee_df.select(last("salary").alias("Last Salary")).show()

    # max
    employee_df.select(max("salary").alias("Max Salary")).show()

    # min
    employee_df.select(min("salary").alias("Min Salary")).show()

    # mean
    employee_df.select(mean("salary").alias("Mean Salary")).show()

    # skewness
    employee_df.select(skewness("salary")).alias("Skewness of Salary").show()

    # including multiple columns
    #  different standard deviation values using select()
    employee_df.select(stddev("salary").alias(" Standard Deviation"), \
                       stddev_samp("salary").alias(" Standard Deviation samp"), \
                       stddev_pop("salary").alias(" Standard Deviation pop")).show()
    #  different standard deviation values using agg()
    employee_df.agg(stddev("salary").alias(" Standard Deviation"), \
                    stddev_samp("salary").alias(" Standard Deviation samp"), \
                    stddev_pop("salary").alias(" Standard Deviation pop")).show()

    # sum
    employee_df.agg(sum("salary").alias("Sum of Salaries")).show()

    # sum_distinct (sum of distinct salaries)
    employee_df.agg(sum_distinct("salary").alias("Sum of Distinct Salaries")).show()

    # variance
    employee_df.select(variance("salary").alias("Variance"), \
                       var_samp("salary").alias("Variance samp"), \
                       var_pop("salary").alias("Variance pop")) \
        .show()

    spark.stop()


def analyse_invoices_using_simple_aggregation_functions():
    spark = SparkSession.builder.appName("Analyse Invoices using Simple Aggregations") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()

    invoices_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("path", "data\invoices.csv") \
        .load()

    invoices_df.show(5)

    # Using df.select()
    invoices_df.select(count("*").alias("Count of Rows in Invoices"),
                       sum("Quantity").alias("Total Qty"),
                       avg("UnitPrice").alias("Average Unit Price"),
                       count_distinct("InvoiceNo").alias("Number of Distinct Invoices")) \
        .show()

    # using df.selectExpr()
    # note count(1) and count(*) are both same, they count the total number of tables
    # note count(field-name) like count(StockCode) only counts the values that have null values
    invoices_df.selectExpr("count(1) as `count(1)`",
                           "count(*) as `count(*)`",
                           "count(StockCode) as `count(StockCode)`",
                           "sum(Quantity) as `Total Qty`",
                           "avg(UnitPrice) as `Average Unit Price`",
                           "approx_count_distinct(InvoiceNo) as `Number of Distinct Invoices`").show()

    # create a temp view sales from the dataframe
    invoices_df.createOrReplaceTempView("sales")

    # using spark sql
    spark.sql("""
    SELECT Country, InvoiceNo as InvoiceNo,
    sum(Quantity) as Qty,
    round(sum(Quantity*UnitPrice),2) as InvoiceValue
    from sales
    GROUP BY
    Country,InvoiceNo
    """) \
        .show()

    invoices_df.show()

    # using df.agg()
    invoices_df.groupby("Country", "InvoiceNo") \
        .agg(sum("Quantity").alias("Total Quantity"), \
             round(sum(expr("Quantity*UnitPrice")), 2),
             expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")) \
        .show()

    # spark.stop()


def analyse_invoices_group_invoices_by_country_and_weeknumber():
    spark = SparkSession.builder.appName("Group Invoices by Country and Week Number") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()

    invoices_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("path", "data\invoices.csv") \
        .load()

    invoices_df.printSchema()
    invoices_df.show()

    agg_count_distinct_invoices = count_distinct("InvoiceNo").alias("Number of Distinct Invoices")
    agg_sum_of_quantity = count_distinct("InvoiceNo").alias("Total Quantity")
    agg_sum_of_invoice_value = expr("round(sum(Quantity*UnitPrice),2) as InvoiceValue")

    print(type(agg_count_distinct_invoices))
    print(type(agg_sum_of_quantity))
    print(type(agg_sum_of_invoice_value))

    summary_invoice_for_2010 = invoices_df \
        .withColumn("InvoiceDate", to_date("InvoiceDate", "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate)==2010") \
        .withColumn("WeekNumber", weekofyear(col("InvoiceDate"))) \
        .groupby("Country", "WeekNumber") \
        .agg(agg_count_distinct_invoices, agg_sum_of_quantity, agg_sum_of_invoice_value)

    summary_invoice_for_2010.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")

    summary_invoice_for_2010.sort("Country", "WeekNumber").show(n=200)


def analyse_invoices_rolling_sum_of_weeks_using_window_aggregation():
    spark = SparkSession.builder.appName("Rolling Sum using Window Aggregation") \
        .master("local[3]") \
        .config("spark.sql.shuffle.partitions", 2) \
        .config("spark.default.parallelism", 2) \
        .getOrCreate()

    summary_df = spark.read.format("parquet") \
        .option("path", "data\summary.parquet") \
        .load() \
        .toDF("country", "week_number", "distinct_invoices", "total_qty", "invoice_value")

    summary_df.printSchema()
    summary_df.show(n=10)

    # This window sums the current row with all the rows before it for a particular country
    running_total_window_all = Window.partitionBy("country") \
        .orderBy("week_number") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # This window sums the current row with the last row  for a particular country
    running_total_window_last_1 = Window.partitionBy("country") \
        .orderBy("week_number") \
        .rowsBetween(-1, Window.currentRow)

    summary_df.withColumn("running_total", sum("invoice_value") \
                          .over(running_total_window_all)) \
        .show()

    summary_df.withColumn("running_total", sum("invoice_value") \
                          .over(running_total_window_last_1)) \
        .show()
