![Image of Apache Spark](https://viblo.asia/uploads/e458bfb3-2876-490e-8456-d1b03f87600c.jpg)

# Sprak Properties
Spark properties kiểm soát hầu hết các cài đặt ứng dụng và được cấu hình riêng cho từng ứng dụng.Các thuộc tính này có thể được đặt trực tiếp trên SparkConf được chuyển tới SparkContext.
SparkConf cho phép bạn định cấu hình một số thuộc tính phổ biến (ví dụ: URL chính và tên ứng dụng), cũng như các cặp khóa-giá trị tùy ý thông qua phương thức set ().
Ví dụ: chúng ta có thể khởi tạo một ứng dụng với hai luồng như sau:

```python
  conf = new SparkConf()
               .setMaster("local[2]")
               .setAppName("CountingSheep")
  sc = new SparkContext(conf)

```
### Dynamically Loading Spark Properties
Trong một số trường hợp, bạn có thể muốn tránh mã hóa cứng các cấu hình nhất định trong SparkConf. Ví dụ: Nếu muốn chạy cùng một ứng dụng với các bản chính khác nhau hoặc số lượng bộ nhớ khác nhau. Spark cho phép bạn chỉ cần tạo một conf trống
  
  ```python
  sc = new SparkContext(new SparkConf())
  ```
  Sau đó, bạn có thể cung cấp các giá trị cấu hình
    ```python
    ./bin/spark-submit --name "My app" --master local[4] --conf spark.eventLog.enabled=false
  --conf "spark.executor.extraJavaOptions=-XX:+PrintGCDetails -XX:+PrintGCTimeStamps" myApp.jar
    ```
### Viewing Spark Properties
  Giao diện người dùng web ứng dụng tại http: // <driver>: 4040 liệt kê các thuộc tính Spark trong tab "Environment".
  Đây là một nơi hữu ích để kiểm tra để đảm bảo rằng các thuộc tính của bạn đã được đặt chính xác.
  Lưu ý rằng chỉ các giá trị được chỉ định rõ ràng thông qua spark-defaults.conf, SparkConf hoặc dòng lệnh mới xuất hiện.
  Đối với tất cả các thuộc tính cấu hình khác, bạn có thể giả sử giá trị mặc định được sử dụng.
### Application Properties
  Thiết lập tên cho ứng dụng xuất hiện trong giao diện người dùng và trong dữ liệu nhật ký.
  ```python
  conf = new SparkConf().setAppName("CountingSheep")
  sc = new SparkContext(conf)
  ```
  Số core để sử dụng cho quy trình trình điều khiển, chỉ ở chế độ cụm
  ```python
  spark.driver.cores
  ```
  Dung lượng bộ nhớ sẽ sử dụng cho mỗi quá trình thực thi, có cùng định dạng với chuỗi bộ nhớ JVM với hậu tố đơn vị kích thước
  ```python
    spark.executor.pyspark.memory
  ```
  Giới hạn tổng kích thước của các kết quả được tuần tự hóa của tất cả các phân vùng cho mỗi hành động Spark tính bằng byte. Tối thiểu phải là 1M hoặc 0 cho không giới hạn. Công việc sẽ bị hủy bỏ nếu tổng kích thước vượt quá giới hạn này.
  ```python
    spark.driver.memoryOverhead
  ```
  Ghi lại SparkConf hiệu quả dưới dạng LOG khi một SparkContext được khởi động.
  ```python
    spark.logConf
  ```
  Số lượng phân vùng mặc định trong RDD được trả về bởi các phép biến đổi như nối, ReduceByKey và parallelize khi người dùng không đặt
  ```python
    spark.default.parallelism
  ```
  # Resilient Distributed Datasets(RDD)
  Spark xoay quanh khái niệm về tập dữ liệu phân tán có khả năng phục hồi (RDD), là một tập hợp các phần tử có khả năng chịu lỗi và có thể hoạt động song song. Có hai cách để tạo RDD: song song một bộ sưu tập hiện có trong chương trình trình điều khiển của bạn hoặc tham chiếu tập dữ liệu trong hệ thống lưu trữ bên ngoài, chẳng hạn như hệ thống tệp được chia sẻ, HDFS, HBase hoặc bất kỳ nguồn dữ liệu nào cung cấp Hadoop InputFormat.
  ### Parallelized Collections
  Các tập hợp song song được tạo bằng cách gọi phương thức song song của SparkContext trên một tập hợp hoặc tập hợp có thể lặp lại hiện có trong chương trình trình điều khiển của bạn. Các phần tử của bộ sưu tập được sao chép để tạo thành một tập dữ liệu phân tán có thể được vận hành song song. Ví dụ: đây là cách tạo một tập hợp song song chứa các số từ 1 đến 5:
  ```python
  data = [1, 2, 3, 4, 5]
  distData = sc.parallelize(data)
  ```
  RDD của tệp văn bản có thể được tạo bằng cách sử dụng phương thức textFile của SparkContext. Phương thức này lấy một URI cho tệp và đọc nó như một tập hợp các dòng.
  ```python
  distFile = sc.textFile("data.txt")
  ```
  Sau khi được tạo, distFile có thể được thực hiện bằng các hoạt động của tập dữ liệu. Ví dụ, chúng ta có thể cộng kích thước của tất cả các dòng bằng cách sử dụng bản đồ và các phép toán giảm như sau: 
  ```python
  distFile.map(lambda s: len (s)).reduce(lambda a, b: a + b).
  ```
  ### RDD Operations
  RDD hỗ trợ hai loại hoạt động: biến đổi, tạo ra một tập dữ liệu mới từ một tập dữ liệu hiện có và các hành động, trả về một giá trị cho chương trình trình điều khiển sau khi chạy một tính toán trên tập dữ liệu. Ví dụ, bản đồ là một phép biến đổi chuyển từng phần tử tập dữ liệu qua một hàm và trả về một RDD mới đại diện cho kết quả. Mặt khác, Reduce là một hành động tổng hợp tất cả các phần tử của RDD bằng cách sử dụng một số chức năng và trả về kết quả cuối cùng cho chương trình điều khiển (mặc dù cũng có một hàm ReduceByKey song song trả về một tập dữ liệu phân tán).
  ### Working with Key-Value Pairs
  Trong Python, các hoạt động này hoạt động trên RDD có chứa các bộ giá trị Python tích hợp sẵn như (1, 2). Đơn giản chỉ cần tạo các bộ giá trị như vậy và sau đó gọi hoạt động mong muốn của bạn.Ví dụ đoạn mã sau sử dụng thao tác ReduceByKey trên các cặp khóa-giá trị để đếm số lần mỗi dòng văn bản xuất hiện trong một tệp
  ```python
  lines = sc.textFile("data.txt")
  pairs = lines.map(lambda s: (s, 1))
  counts = pairs.reduceByKey(lambda a, b: a + b)
  ```
### Transformations
Bảng sau liệt kê một số phép biến đổi phổ biến được Spark hỗ trợ.
Transformation | Meaning
------------ | -------------
map(func) | Trả về tập dữ liệu phân tán mới được hình thành bằng cách chuyển từng phần tử của nguồn thông qua một hàm chức năng.
filter(func) | Trả về một tập dữ liệu mới được hình thành bằng cách chọn các phần tử của nguồn mà func trả về true.
flatMap(func) | Tương tự như bản đồ, nhưng mỗi mục đầu vào có thể được ánh xạ tới 0 hoặc nhiều mục đầu ra
mapPartitions(func) | Tương tự như bản đồ, nhưng chạy riêng biệt trên từng phân vùng (khối) của RDD, vì vậy func phải là kiểu Iterator <T> => Iterator <U> khi chạy trên RDD kiểu T.
mapPartitionsWithIndex(func) | Tương tự như mapPartitions, nhưng cũng cung cấp func với một giá trị nguyên đại diện cho chỉ mục của phân vùng, vì vậy func phải có kiểu (Int, Iterator <T>) => Iterator <U> khi chạy trên RDD kiểu T.
union(otherDataset) | Trả về một tập dữ liệu mới có chứa sự kết hợp của các phần tử trong tập dữ liệu nguồn và đối số
reduceByKey(func, [numPartitions]) | Khi được gọi trên tập dữ liệu của các cặp (K, V), trả về tập dữ liệu của các cặp (K, V) trong đó các giá trị cho mỗi khóa được tổng hợp bằng cách sử dụng hàm giảm đã cho
sortByKey([ascending], [numPartitions]) | Khi được gọi trên tập dữ liệu của các cặp (K, V) trong đó K triển khai Có thứ tự, trả về tập dữ liệu của các cặp (K, V) được sắp xếp theo các khóa theo thứ tự tăng dần hoặc giảm dần, như được chỉ định trong đối số tăng dần boolean.
join(otherDataset, [numPartitions]) | Khi được gọi trên tập dữ liệu kiểu (K, V) và (K, W), trả về tập dữ liệu gồm các cặp (K, (V, W)) với tất cả các cặp phần tử cho mỗi khóa. Tham gia bên ngoài được hỗ trợ thông qua leftOuterJoin, rightOuterJoin và fullOuterJoin.

### Action
Action | Meaning
------------ | -------------
reduce(func) | Tổng hợp các phần tử của tập dữ liệu bằng cách sử dụng một hàm func (nhận hai đối số và trả về một). Hàm phải có tính chất giao hoán và liên kết để nó có thể được tính toán song song một cách chính xác.
collect() | Trả về tất cả các phần tử của tập dữ liệu dưới dạng một mảng tại chương trình điều khiển. Điều này thường hữu ích sau khi bộ lọc hoặc hoạt động khác trả về một tập hợp con đủ nhỏ của dữ liệu.
count() | Trả về số phần tử trong tập dữ liệu.
first() | Trả về phần tử đầu tiên của tập dữ liệu (tương tự như lấy (1)).
take(n) | Trả về một mảng có n phần tử đầu tiên của tập dữ liệu.
takeSample(withReplacement, num, [seed]) | Trả về một mảng với một mẫu ngẫu nhiên gồm num phần tử của tập dữ liệu, có hoặc không thay thế, tùy chọn chỉ định trước một hạt tạo số ngẫu nhiên.
saveAsTextFile(path) | Viết các phần tử của tập dữ liệu dưới dạng tệp văn bản (hoặc tập hợp tệp văn bản) trong một thư mục nhất định trong hệ thống tệp cục bộ, HDFS hoặc bất kỳ hệ thống tệp nào khác được Hadoop hỗ trợ
foreach(func) | Chạy một hàm func trên mỗi phần tử của tập dữ liệu. Điều này thường được thực hiện đối với các tác dụng phụ như cập nhật Bộ tích lũy hoặc tương tác với hệ thống lưu trữ bên ngoài.

# Spark Dataframe
DataFrame là một Tập dữ liệu được tổ chức thành các cột được đặt tên. Về mặt khái niệm, nó tương đương với một bảng trong cơ sở dữ liệu quan hệ hoặc một khung dữ liệu trong R / Python, nhưng với các tối ưu hóa phong phú hơn. DataFrames có thể được xây dựng từ nhiều nguồn như: tệp dữ liệu có cấu trúc, bảng trong Hive, cơ sở dữ liệu bên ngoài hoặc RDD hiện có. API DataFrame có sẵn trong Scala, Java, Python và R. Trong Scala và Java, DataFrame được đại diện bởi Tập dữ liệu của các hàng. Trong API Scala, DataFrame chỉ đơn giản là một bí danh kiểu của Dataset [Row]. Trong khi, trong Java API, người dùng cần sử dụng Dataset <Row> để đại diện cho DataFrame 
 ### Creating DataFrames
  Sử dụng createDataFrame()
  ```python
      data = Seq(('James','','Smith','1991-04-01','M',3000),
    ('Michael','Rose','','2000-05-19','M',4000),
    ('Robert','','Williams','1978-09-05','M',4000),
    ('Maria','Anne','Jones','1967-12-01','F',4000),
    ('Jen','Mary','Brown','1980-02-17','F',-1))
    columns = Seq("firstname","middlename","lastname","dob","gender","salary")
    df = spark.createDataFrame(data), schema = columns).toDF(columns:_*)
  ```
  Với SparkSession, các ứng dụng có thể tạo DataFrames từ RDD hiện có, hive table hoặc từ các nguồn dữ liệu Spark.
  ```python
    # spark is an existing SparkSession
    df = spark.read.json("examples/src/main/resources/people.json")
    # Displays the content of the DataFrame to stdout
    df.show()
    # +----+-------+
    # | age|   name|
    # +----+-------+
    # |null|Michael|
    # |  30|   Andy|
    # |  19| Justin|
    # +----+-------+
  ```
  ### Read JSON file into DataFrame
  ```python
    # Read JSON file into dataframe
    df = spark.read.json("resources/zipcodes.json")
    df.printSchema()
    df.show()
  ```
  ### Read JSON file from multiline
  Nguồn dữ liệu PySpark JSON cung cấp nhiều tùy chọn để đọc tệp trong các tùy chọn khác nhau, sử dụng tùy chọn đa dòng để đọc các tệp JSON nằm rải rác trên nhiều dòng. Theo mặc định, tùy chọn nhiều dòng được đặt thành false.
  ```javascript 
    [{
      "RecordNumber": 2,
      "Zipcode": 704,
      "ZipCodeType": "STANDARD",
      "City": "PASEO COSTA DEL SUR",
      "State": "PR"
    },
    {
      "RecordNumber": 10,
      "Zipcode": 709,
      "ZipCodeType": "STANDARD",
      "City": "BDA SAN LUIS",
      "State": "PR"
    }]
  ```
  Using read.option("multiline","true")
  ```python 
    # Read multiline json file
    multiline_df = spark.read.option("multiline","true") \
          .json("resources/multiline-zipcode.json")
    multiline_df.show()  
  ```
  ### Reading all files in a directory
  ```python
  # Read all JSON files from a folder
  df3 = spark.read.json("resources/*.json")
  df3.show()
  ```
  ### Write PySpark DataFrame to JSON file
  Sử dụng phương thức "ghi" đối tượng PySpark DataFrameWriter trên DataFrame để ghi tệp JSON
  ```python 
  df2.write.json("/tmp/spark_output/zipcodes.json")
  ```
  
  ### Untyped Dataset Operations
  Trong Python, có thể truy cập các cột của DataFrame theo thuộc tính (df.age) hoặc bằng cách lập chỉ mục (df ['age']). Mặc dù biểu mẫu trước thuận tiện cho việc khám phá dữ liệu tương tác, nhưng người dùng được khuyến khích sử dụng biểu mẫu thứ hai, đây là bằng chứng trong tương lai và sẽ không bị ảnh hưởng bởi các tên cột cũng là thuộc tính trên lớp DataFrame.
  ```python
    # spark, df are from the previous example
    # Print the schema in a tree format
    df.printSchema()
    # root
    # |-- age: long (nullable = true)
    # |-- name: string (nullable = true)

    # Select only the "name" column
    df.select("name").show()
    # +-------+
    # |   name|
    # +-------+
    # |Michael|
    # |   Andy|
    # | Justin|
    # +-------+

    # Select everybody, but increment the age by 1
    df.select(df['name'], df['age'] + 1).show()
    # +-------+---------+
    # |   name|(age + 1)|
    # +-------+---------+
    # |Michael|     null|
    # |   Andy|       31|
    # | Justin|       20|
    # +-------+---------+

    # Select people older than 21
    df.filter(df['age'] > 21).show()
    # +---+----+
    # |age|name|
    # +---+----+
    # | 30|Andy|
    # +---+----+

    # Count people by age
    df.groupBy("age").count().show()
    # +----+-----+
    # | age|count|
    # +----+-----+
    # |  19|    1|
    # |null|    1|
    # |  30|    1|
    # +----+-----+
  
  ```
  ### Sort DataFrame column
  ```python
    df.sort("department","state").show(false)
    df.sort(col("department"),col("state")).show(false)
  ```
  
  Sắp xếp tăng dần
  
  ```python
  df.sort(col("department").asc,col("state").asc).show(false)
  df.orderBy(col("department").asc,col("state").asc).show(false)
   ```
    
 ### Replace NULL Values on DataFrame
 ```python 
    df.na.fill("unknown",Array("city"))
    .na.fill("",Array("type"))
    .show(false)
 ```
 ### Drop Rows with NULL Values in Any Columns
   ```python 
     df.na.drop().show(false)
   ```
