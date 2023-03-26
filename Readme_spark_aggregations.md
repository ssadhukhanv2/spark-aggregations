## Important Links
* [Spark Documentation](https://spark.apache.org/docs/latest/index.html)
* [Overview of the spark cluster](https://spark.apache.org/docs/latest/cluster-overview.html)

## Spark Setup in Windows

* Install JDK11 from [here](https://jdk.java.net/archive/), set it as JAVA_HOME and add `%JAVA_HOME%\bin` to the system path.
* Install [winutils](https://github.com/steveloughran/winutils) for Hadoop. 
  * [Latest version of winutils]((https://github.com/cdarlint/winutils)) install from here 
  * Download or clone the repo and copy the folder of the latest version available(in our case hadoop-3.2.2) to a location.
  * Set this has HADOOP_HOME to this folder and add `%HADOOP_HOME%\bin` to the system path
* Install Spark 
  * Download Spark from [here](https://spark.apache.org/downloads.html) and extract it in a folder location
  * Set `SPARK_HOME` variable and add `%SPARK_HOME%\bin` to the system path variable
* Install pyspark in the venv using pip
  * Activate the virtual environment `.\venv\Scripts\activate`
  * Install the pyspark package `pip install pyspark`
* Incase multiple versions of python installed on a machine, ensure Python 3.10 is installed and configure the following environment variables
  * Set `PYTHONPATH` to `C:\Users\subhr\Softwares\spark-3.3.2-bin-hadoop3\python;C:\Users\subhr\Softwares\spark-3.3.2-bin-hadoop3\python\lib\py4j-0.10.9.5-src.zip`
  * Set `PYSPARK_PYTHON` to `C:\Program Files\Python310\python.exe` 
    * Without this `PYSPARK_PYTHON` environment variable, running code form pycharm doesn't work, but with this environment variable pyspark from commandline doesn't work `%SPARK_HOME%\bin\pyspark --version`. So while using the commandline change the environment variable name to `PYSPARK_PYTHON_XXXXX`

## Project Setup 


* [**Course GitHub Link**](https://github.com/LearningJournal/Spark-Programming-In-Python/tree/master/01-HelloSpark)
* [**SparkBy{Examples} Link**](https://sparkbyexamples.com/spark/how-to-create-an-rdd-using-parallelize/)
* **Spark UI Available at [http://localhost:4040/](http://localhost:4040/)**

* [**Anaconda**](https://www.youtube.com/watch?v=MUZtVEDKXsk&t=625s&ab_channel=PythonSimplified): Install Anaconda and use it as the package manager for creating the project. 
  * Launch the Anaconda shell activate the hello-spark environment `conda activate hello-spark` 
  * Install Pyspark using conda `conda install -c conda-forge pyspark`
  * Install Pytest using conda `conda install -c anaconda pytest`
  
  [Anaconda difference with pip](https://www.reddit.com/r/Python/comments/w564g0/can_anyone_explain_the_differences_of_conda_vs_pip/)

* [Using spark-shell in client mode locally](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162098#overview) Using the spark-shell in client mode. UI launches in [http://localhost:4040/executors/](http://localhost:4040/executors/) 

      ## check spark version
      %SPARK_HOME%\bin\pyspark
      
      ## Launch sparkshell
      %SPARK_HOME%\bin\pyspark --master local[3] --driver-memory 2G

*  [Create a multinode spark cluster in GCP](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20218636#overview)
* [Connect to the multi node spark cluster using `spark-shell` and `Zeppelin`](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162104#overview) 
  * Lauch pyspark: `pyspark --master yarn --driver-memory 1G --executor-memory 500M --num-executors 2 --executor-cores 1` 
  * Spark History Server: All applications which have completed their execution on the spark are displayed here.
  * The application which are currently running applications may be displayed within incomplete applications under spark history server. But to get a view of both incomplete and the inactive(completed) application you can view under the resource manager.
* [Submitting jobs using the spark-submit](https://capgemini.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20162116#overview) [Need to practice]
  
      spark-submit --master yarn --deploy-mode cluster pi.py 100


* To use formats like AVRO we need to add the below jars to the spark in the `spark-defaults.conf` file. Read more [Apache AVRO Datasource Guide](https://spark.apache.org/docs/latest/sql-data-sources-avro.html)

      spark.jars.packages                org.apache.spark:spark-avro_2.12:3.3.2

### [Spark Basic Concepts](Readme_spark_basics.md) 

### [Spark Working with File based Data Sources and Sink](Readme_spark_read_write.md)

### [Spark Transformations Concepts](Readme_spark_data_transformation.md)

## Resources
* [**sort() vs orderBy()**](https://towardsdatascience.com/sort-vs-orderby-in-spark-8a912475390)

## Spark Data Aggregations

* [**Data Aggregations with Spark**](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20715134#questions) Spark Aggregations can be of 3 types:
  * **Simple Aggregations**: see [utils.aggregator::spark_aggregate_functions_example()](lib/utils.py) and [utils.aggregator::analyse_invoices_using_simple_aggregation_functions()](lib/utils.py)
  * **Grouping Aggregations**: see [utils.aggregator::group_invoices_by_country_and_weeknumber()](lib/utils.py)
  * **Windowing Aggregations**: see [utils.aggregator::group_invoices_by_country_and_weeknumber()](lib/utils.py)
   
     All aggregations in spark are implemented as built-in functions. they are available within `pyspark.sql.functions` package
      * **Built-in Aggregating Functions** These are used for simple and grouping aggregations. Example- `avg() count() max() min() sum()`
      * **Built-in Window Functions** These are used for windowing aggregations. Example- `lead() lag() rank() dense_rank() cume_dist()`
* [**Different Spark Aggregate Functions**](https://sparkbyexamples.com/spark/spark-sql-aggregate-functions/) Some commonly used built-in Spark Aggregation Functions avaiable within `pyspark.sql.functions` are `monotonically_increasing_id, approx_count_distinct, first, last, mean, skewness, min, \
    max, stddev, stddev_samp, stddev_pop, sumDistinct, variance, var_samp, var_pop,sum` . The Spark Aggregation functions in spark may be called two ways:
  * Passing the Aggregation Functions in `DataFrame.agg()`
    
        employee_df.agg(approx_count_distinct("salary") \
        .alias("Number of Distinct Salaries")).show()
        
        invoices_df.groupby("Country", "InvoiceNo") \
        .agg(sum("Quantity").alias("Total Quantity"), \
             round(sum(expr("Quantity*UnitPrice")), 2),
             expr("round(sum(Quantity*UnitPrice),2) as InvoiceValueExpr")) \
        .show()
  * Passing the Aggregation Functions in `DataFrame.select()`
    
        employee_df.select(approx_count_distinct("salary")).show()
  
  * using df.selectExpr()
        
        invoices_df.selectExpr("count(1) as `count(1)`",
                           "count(StockCode) as `count(StockCode)`",
                           "sum(Quantity) as `Total Qty`",
                           "avg(UnitPrice) as `Average Unit Price`",
                           "approx_count_distinct(InvoiceNo) as `Number of Distinct Invoices`").show()
  *  using spark sql: while using spark sql we need to create the view
  
         invoices_df.createOrReplaceTempView("sales")
         
         spark.sql("""
         SELECT Country, InvoiceNo as InvoiceNo,
         sum(Quantity) as Qty,
         round(sum(Quantity*UnitPrice),2) as InvoiceValue
         from sales
         GROUP BY
         Country,InvoiceNo
         """) \
         .show()