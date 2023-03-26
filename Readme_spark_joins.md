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

### [Spark Data Aggregations](Readme_spark_aggregations.md)

## Spark Joins
* [Inner Joins](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20930430#questions)
  * Concept and Scenarios to use inner join
  * Dealing with Column Name Ambiguity while applying inner join 
  * Code available [here](https://github.com/LearningJournal/Spark-Programming-In-Python/blob/master/17-SparkJoinDemo/SparkJoinDemo.py)

* [Outer Joins](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20949086#questions)
  * Concept of Outer Join
  * Scenarios where we need to use outer joins(full outer, left outer, right outer)
  * syntax of applying outer joins(full outer, left outer, right outer) in spark
  * supplying a default value from other columns for null values
  * Code available [here](https://github.com/LearningJournal/Spark-Programming-In-Python/blob/master/18-OuterJoinDemo/OuterJoinDemo.py)

* [Shuffle Join](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/20968646#questions)
  * Internal working of shuffle join
  * How shuffling actuall happens
  * Observe inner join performance inside the Spark UI
  * Code available [here](https://github.com/LearningJournal/Spark-Programming-In-Python/blob/master/19-ShuffleJoinDemo/SuffleJoinDemo.py)

* [Broadcast Join](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/21019950#questions)
  * Large vs small datasets
  * Some options we can use to fine tune the performance of join
  * Why Shuffle joins are good for large to large datasets
  * How Broadcast joins are a much better option for joins between a large dataset and a small sets
  * Observe shuffle join performance within spark ui
  * Code copy from video(Not available in git)

* [Bucket Join](https://www.udemy.com/course/apache-spark-programming-in-python-for-beginners/learn/lecture/21052336#reviews)
  * How to preprocess dataframes by partitioning/bucketing them in advance, need of time shuffling is eliminated
  * Turn off Shuffle operation for joins so that all joins would become merge join.
  * Code available [here](https://github.com/LearningJournal/Spark-Programming-In-Python/blob/master/20-BucketJoinDemo/BucketJoinDemo.py)

## Resources
* [**sort() vs orderBy()**](https://towardsdatascience.com/sort-vs-orderby-in-spark-8a912475390)