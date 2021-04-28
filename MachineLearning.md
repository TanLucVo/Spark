# DATAFRAME
### DataFrames là gì?
DataFrames thường đề cập đến một cấu trúc dữ liệu, có bản chất là dạng bảng.Nó đại diện cho các hàng, mỗi hàng bao gồm một số quan sát
Các hàng có thể có nhiều định dạng dữ liệu khác nhau (không đồng nhất), trong khi một cột có thể có dữ liệu có cùng kiểu dữ liệu (đồng nhất).
DataFrames usually contain some metadata in addition to data; for example, column and row names.
![Image of Yaktocat](https://d1jnx9ba8s6j9r.cloudfront.net/blog/wp-content/uploads/2018/06/table-528x215.jpg)
### Tại sao chúng ta cần DataFrames

1. **Xử lý dữ liệu có cấu trúc và bán cấu trúc**
  - DataFrames được thiết kế để **xử lý một bộ sưu tập lớn dữ liệu có cấu trúc cũng như bán cấu trúc.**
  - Các quan sát trong Spark DataFrame được tổ chức dưới các cột được đặt tên, giúp __Apache Spark__ hiểu được lược đồ của Dataframe
  - Điều này giúp Spark tối ưu hóa kế hoạch thực thi trên các truy vấn này. Nó cũng có thể xử lý hàng __petabyte__ dữ liệu.
2. **Slicing and Dicing**
  - DataFrames thường hỗ trợ các phương pháp phức tạp để cắt và phân loại dữ liệu. Nó bao gồm các hoạt động như "chọn" hàng,
  cột và ô theo tên hoặc theo số, lọc ra các hàng, v.v.
  - Dữ liệu thống kê thường rất lộn xộn và chứa nhiều giá trị bị thiếu và không chính xác cũng như vi phạm phạm vi
3. **Data Sources**
  - DataFrames hỗ trợ nhiều định dạng và nguồn dữ liệu
4. **Hỗ trợ nhiều ngôn ngữ**
  - Nó có hỗ trợ API cho các ngôn ngữ khác nhau như Python, R, Scala, Java, giúp những người có nền tảng lập trình khác nhau sử dụng dễ dàng hơn.

### Một số API của DataFrame trong Pyspark

#### Create DataFrame
* __Create DataFrame from RDD__
  * Using toDF()
  ```python
  dfFromRDD1 = rdd.toDF()
  dfFromRDD1.printSchema()
  ```
  * Using createDataFrame() from SparkSession
  ```python
  dfFromRDD2 = spark.createDataFrame(rdd).toDF(*columns)
  ```

* __Create DataFrame from List Collection__
   * Using createDataFrame() with the Row type
  ```python
  rowData = map(lambda x: Row(*x), data) 
  dfFromData3 = spark.createDataFrame(rowData,columns)
  ```
  * Create DataFrame with schema
  ```python
    from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    data2 = [("Nguyễn Văn","","A","36636","Male",3000000),
        ("Lê Văn","B","","40288","Male",4000000),
      ]

    schema = StructType([ \
        StructField("firstname",StringType(),True), \
        StructField("middlename",StringType(),True), \
        StructField("lastname",StringType(),True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
      ])

    df = spark.createDataFrame(data=data2,schema=schema)
    df.printSchema()
    df.show(truncate=False)
  ```
* __Create DataFrame from Data sources__
  * Creating DataFrame from CSV
  ```python
    df2 = spark.read.csv("/src/resources/file.csv")
  ```
  * Creating from text (TXT) file
  ```python
    df2 = spark.read.text("/src/resources/file.txt")
  ```
  * Creating from JSON file
  ```python
    df2 = spark.read.json("/src/resources/file.json")
  ```
  #### Display DataFrame Contents
  > PySpark DataFrame show () được sử dụng để hiển thị nội dung của DataFrame ở Định dạng Hàng & Cột của Bảng.
  >  Theo mặc định, nó chỉ hiển thị 20 Hàng và giá trị cột bị cắt bớt 20 ký tự.
  ```python
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName('SparkDataFrames').getOrCreate()
    columns = ["id","message"]
    data = [("1", "Hello"),
        ("2", "How are you? what your name"),
        ("3", "i luv you"),
        ("4", "Be cool.")]
    df = spark.createDataFrame(data,columns)
    df.show()
    +---+--------------------+
    | id|             message|
    +---+--------------------+
    |  1|               Hello|
    |  2|How are you? what...|
    |  3|           i luv you|
    |  4|            Be cool.|
    +---+--------------------+
  ```
  ```python
    #Display full column contents
    df.show(truncate=False)
    +---+---------------------------+
    |id |message                    |
    +---+---------------------------+
    |1  |Hello                      |
    |2  |How are you? what your name|
    |3  |i luv you                  |
    |4  |Be cool.                   |
    +---+---------------------------+
  ```
#### Select Columns From DataFrame
>Trong PySpark, hàm select () được sử dụng để chọn một, nhiều, từng cột theo chỉ mục, tất cả các cột từ danh sách và các cột lồng nhau từ DataFrame
* __Select Single & Multiple Columns From PySpark__
```python
df.select("firstname","lastname").show()
df.select(df.firstname,df.lastname).show()
df.select(df["firstname"],df["lastname"]).show()

#By using col() function
from pyspark.sql.functions import col
df.select(col("firstname"),col("lastname")).show()

#Select columns by regular expression
df.select(df.colRegex("`^.*name*`")).show()
```
* __Select All Columns From List__
```python
# Select All columns from List
df.select(*columns).show()

# Select All columns
df.select([col for col in df.columns]).show()
df.select("*").show()
```
* __Select Columns by Index__
```python
  
#Selects first 3 columns and top 3 rows
df.select(df.columns[:3]).show(3)

#Selects columns 2 to 4  and top 3 rows
df.select(df.columns[2:4]).show(3)
```
#### Retrieve data from DataFrame
>PySpark RDD / DataFrame collect () được sử dụng để truy xuất tất cả các phần tử của tập dữ liệu (từ tất cả các nút) đến nút trình điều khiển
```python
dept = [("Finance",10), \
    ("Marketing",20), \
    ("Sales",30), \
    ("IT",40) \
  ]
deptColumns = ["dept_name","dept_id"]
deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
deptDF.show(truncate=False)
#+---------+-------+
#|dept_name|dept_id|
#+---------+-------+
#|Finance  |10     |
#|Marketing|20     |
#|Sales    |30     |
#|IT       |40     |
#+---------+-------+
dataCollect = deptDF.collect()
print(dataCollect)
#[
  #Row(dept_name='Finance', dept_id=10),
  #Row(dept_name='Marketing', dept_id=20),
  #Row(dept_name='Sales', dept_id=30),
  #Row(dept_name='IT', dept_id=40)
#]
```
Thông thường, collect() được sử dụng để truy xuất đầu ra hành động khi bạn có tập kết quả rất nhỏ và việc gọi collect() trên RDD /
DataFrame có tập kết quả lớn hơn gây ra bộ nhớ vì nó trả về toàn bộ tập dữ liệu (từ tất cả các worker) đến trình điều khiển do đó 
chúng ta nên tránh gọi collect () trên một tập dữ liệu lớn hơn.

#### WithColumn
>PySpark withColumn() là một hàm chuyển đổi của DataFrame được sử dụng để thay đổi giá trị, chuyển đổi kiểu dữ liệu của một cột hiện có,
> tạo một cột mới và nhiều hơn nữa
* **Change DataType using PySpark withColumn()**
  ```python
  df.withColumn("salary",col("salary").cast("Integer")).show()
  ```
* **Update The Value of an Existing Column**
  ```python
  df.withColumn("salary",col("salary")*100).show()
  ```
* **Create a Column from an Existing**
```python
df.withColumn("CopiedColumn",col("salary")* -1).show()
```
* **Rename Column Name**
```python
df.withColumnRenamed("name","firstName").show(truncate=False) 
```
#### Rename Column on DataFrame
> Sử dụng PySpark withColumnRename () để đổi tên một cột DataFrame, chúng ta thường cần đổi tên một cột hoặc nhiều (hoặc tất cả) cột trên PySpark DataFrame,
>  có thể thực hiện việc này bằng một số cách. Khi các cột được lồng vào nhau, nó trở nên phức tạp
```python
#root
 #|-- name: struct (nullable = true)
 #|    |-- firstname: string (nullable = true)
 #|    |-- middlename: string (nullable = true)
 #|    |-- lastname: string (nullable = true)
 #|-- dob: string (nullable = true)
 #|-- gender: string (nullable = true)
 #|-- salary: integer (nullable = true)
 df.withColumnRenamed("dob","DateOfBirth").printSchema()
 #root
 #|-- name: struct (nullable = true)
 #|    |-- firstname: string (nullable = true)
 #|    |-- middlename: string (nullable = true)
 #|    |-- lastname: string (nullable = true)
 #|-- DateOfBirth: string (nullable = true)
 #|-- gender: string (nullable = true)
 #|-- salary: integer (nullable = true)
```
#### PySpark Where Filter Function
> Hàm PySpark filter() được sử dụng để lọc các hàng từ RDD/DataFrame dựa trên điều kiện đã cho hoặc biểu thức SQL
```python
# Using equals condition
df.filter(df.salary > 3000).show(truncate=False)

#+--------------------+----------+------+------+
#|name                |dob       |gender|salary|
#+--------------------+----------+------+------+
#|{Michael, Rose, }   |2000-05-19|M     |4000  |
#|{Robert, , Williams}|1978-09-05|M     |4000  |
#|{Maria, Anne, Jones}|1967-12-01|F     |4000  |
#+--------------------+----------+------+------+
```
 **Filter with Multiple Conditions**
 ```python
   df.filter( (df.salary > 3000) & (df.gender  == "M") ).show(truncate=False)  
  #+--------------------+----------+------+------+
  #|name                |dob       |gender|salary|
  #+--------------------+----------+------+------+
  #|{Michael, Rose, }   |2000-05-19|M     |4000  |
  #|{Robert, , Williams}|1978-09-05|M     |4000  |
  #+--------------------+----------+------+------+
  ```
  **Filter on an Array column**
  ```python
  from pyspark.sql.functions import array_contains
  df.filter(array_contains(df.languages,"Java")) \
      .show(truncate=False)  
  ```
  **Filter Based on Starts With, Ends With, Contains**
  ```python
  # Using startswith
  df.filter(df.gender.startswith("M")).show()
  #+--------------------+----------+------+------+
  #|                name|       dob|gender|salary|
  #+--------------------+----------+------+------+
  #|    {James, , Smith}|1991-04-01|     M|  3000|
  #|   {Michael, Rose, }|2000-05-19|     M|  4000|
  #|{Robert, , Williams}|1978-09-05|     M|  4000|
  #+--------------------+----------+------+------+
  
  #using endswith
  df.filter(df.name.firstname.endswith("s")).show()
  #+----------------+----------+------+------+
  #|            name|       dob|gender|salary|
  #+----------------+----------+------+------+
  #|{James, , Smith}|1991-04-01|     M|  3000|
  #+----------------+----------+------+------+
  ```
  #### PySpark orderBy() and sort()
  ```python
  df.sort("salary").show(truncate=False)
  #+--------------------+----------+------+------+
  #|name                |dob       |gender|salary|
  #+--------------------+----------+------+------+
  #|{Jen, Mary, Brown}  |1980-02-17|F     |-1    |
  #|{James, , Smith}    |1991-04-01|M     |3000  |
  #|{Michael, Rose, }   |2000-05-19|M     |4000  |
  #|{Maria, Anne, Jones}|1967-12-01|F     |4000  |
  #|{Robert, , Williams}|1978-09-05|M     |4000  |
  #+--------------------+----------+------+------+
  
  #orderBy() 
  df.orderBy(df.salary.desc(),df.gender.asc()).show(truncate=False)
  #+--------------------+----------+------+------+
  #|name                |dob       |gender|salary|
  #+--------------------+----------+------+------+
  #|{Maria, Anne, Jones}|1967-12-01|F     |4000  |
  #|{Michael, Rose, }   |2000-05-19|M     |4000  |
  #|{Robert, , Williams}|1978-09-05|M     |4000  |
  #|{James, , Smith}    |1991-04-01|M     |3000  |
  #|{Jen, Mary, Brown}  |1980-02-17|F     |-1    |
  #+--------------------+----------+------+------+
  ```
  
  #### PySpark map() Transformation
  > PySpark(map()) là một phép biến đổi RDD được sử dụng để áp dụng hàm chuyển đổi (lambda) trên mọi phần tử của RDD/DataFrame và trả về một RDD mới
  ```python
  data = ["JS","Python","Java","C", "C++"]
  rdd=spark.sparkContext.parallelize(data)
  rdd2=rdd.map(lambda x: (x,1))
  for element in rdd2.collect():
      print(element)

  #('JS', 1)
  #('Python', 1)
  #('Java', 1)
  #('C', 1)
  #('C++', 1)
  ```
  #### PySpark flatMap() Transformation
  > PySpark flatMap() là một hoạt động chuyển đổi làm phẳng RDD/DataFrame
  > sau khi áp dụng hàm trên mọi phần tử và trả về PySpark RDD/DataFrame mới
  ```python
  print(rdd.collect())
  #['Project Gutenberg’s', 'Alice’s Adventures in Wonderland', 'Project Gutenberg’s', 'Adventures in Wonderland', 'Project Gutenberg’s']
  rdd2=rdd.flatMap(lambda x: x.split(" "))
  for element in rdd2.collect():
      print(element)
  #Project
  #Gutenberg’s
  #Alice’s
  #Adventures
  #in
  #Wonderland
  #Project
  #Gutenberg’s
  #Adventures
  #in
  #Wonderland
  #Project
  #Gutenberg’s
  ```
  
  #### Replace NULL/None Values
  ```python
  df.show(truncate=False)
  #+--------------------+----------+------+------+
  #|name                |dob       |gender|salary|
  #+--------------------+----------+------+------+
  #|{James, , Smith}    |1991-04-01|M     |3000  |
  #|{Michael, Rose, }   |2000-05-19|M     |4000  |
  #|{Robert, , Williams}|1978-09-05|M     |4000  |
  #|{Maria, Anne, Jones}|1967-12-01|F     |4000  |
  #|{Jen, Mary, Brown}  |1980-02-17|F     |null  |
  #+--------------------+----------+------+------+
  
  df.na.fill(value=0).show()
  #+--------------------+----------+------+------+
  #|                name|       dob|gender|salary|
  #+--------------------+----------+------+------+
  #|    {James, , Smith}|1991-04-01|     M|  3000|
  #|   {Michael, Rose, }|2000-05-19|     M|  4000|
  #|{Robert, , Williams}|1978-09-05|     M|  4000|
  #|{Maria, Anne, Jones}|1967-12-01|     F|  4000|
  #|  {Jen, Mary, Brown}|1980-02-17|     F|     0|
  #+--------------------+----------+------+------+
  ```
