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
# Machine Learning
#### Pipeline
Trong học máy, người ta thường chạy một chuỗi các thuật toán để xử lý và học hỏi từ dữ liệu. 
Ví dụ: một quy trình xử lý tài liệu văn bản đơn giản có thể bao gồm một số giai đoạn:
- Chia văn bản của từng tài liệu thành các từ.
- Chuyển đổi từng từ của tài liệu thành vectơ đặc trưng số.
- Tìm hiểu mô hình dự đoán bằng cách sử dụng các vectơ và nhãn đặc trưng.
> Pipeline được chỉ định là một chuỗi các giai đoạn và mỗi giai đoạn là Transformer hoặc Estimator. Các giai đoạn này được chạy theo thứ tự và DataFrame đầu vào được chuyển đổi khi nó đi qua từng giai đoạn

Đối với các giai đoạn của Estimator, phương thức fit() được gọi để tạo ra một Transformer(trở thành một phần của PipelineModel, hoặc fitted Pipeline) và phương thức biến đổi của Transformer đó được gọi trên DataFrame.
![Image of Yaktocat](https://spark.apache.org/docs/latest/img/ml-Pipeline.png)

Một Pipeline là một Estimator. Do đó, sau khi phương thức Pipeline’s fit() chạy, nó tạo ra một PipelineModel, là một Transformer. PipelineModel này được sử dụng tại thời điểm thử nghiệm.

![Image of Yaktocat](https://spark.apache.org/docs/latest/img/ml-PipelineModel.png)

#### Extracting, transforming and selecting features
- TF-IDF
  > Term frequency-inverse document frequency (TF-IDF) là một phương pháp vector hóa tính năng được sử dụng rộng rãi trong khai thác văn bản để phản ánh tầm quan trọng của một thuật ngữ đối với một tài liệu trong kho ngữ liệu
  
  Nếu một thuật ngữ xuất hiện rất thường xuyên trên toàn bộ kho tài liệu, điều đó có nghĩa là nó không mang thông tin đặc biệt về một tài liệu cụ thể. Tần suất tài liệu nghịch đảo là một thước đo bằng số về lượng thông tin mà một thuật ngữ cung cấp: 
  IDF(t,D)=log(|D|+1)/(DF(t,D)+1)
  
  Phép đo TF-IDF chỉ đơn giản là tích của TF và IDF: TFIDF(t,d,D)=TF(t,d)⋅IDF(t,D).
  ```python
    from pyspark.ml.feature import HashingTF, IDF, Tokenizer

    sentenceData = spark.createDataFrame([
    (0.0, "Hi I heard about Spark"),
    (0.0, "I wish Java could use case classes"),
    (1.0, "Logistic regression models are neat")
    ], ["label", "sentence"])

    tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
    wordsData = tokenizer.transform(sentenceData)

    hashingTF = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
    featurizedData = hashingTF.transform(wordsData)

    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)

    rescaledData.select("label", "features").show()
    
    #+-----+--------------------+
    #|label|            features|
    #+-----+--------------------+
    #|  0.0|(20,[6,8,13,16],[...|
    #|  0.0|(20,[0,2,7,13,15,...|
    #|  1.0|(20,[3,4,6,11,19]...|
    #+-----+--------------------+
  ```
  
  - Word2Vec
    Word2Vec là Công cụ ước tính lấy chuỗi các từ đại diện cho tài liệu và đào tạo Word2VecModel.Mô hình ánh xạ mỗi từ thành một vectơ có kích thước cố định duy nhất.
    Word2VecModel biến mỗi tài liệu thành một vectơ bằng cách sử dụng giá trị trung bình của tất cả các từ trong tài liệu; vectơ này sau đó có thể được sử dụng làm các tính năng để dự đoán, tính toán độ tương đồng của tài liệu
    
    ```python 
      from pyspark.ml.feature import Word2Vec

      # Input data: Each row is a bag of words from a sentence or document.
      documentDF = spark.createDataFrame([
          ("Hi I heard about Spark".split(" "), ),
          ("I wish Java could use case classes".split(" "), ),
          ("Logistic regression models are neat".split(" "), )
      ], ["text"])

      # Learn a mapping from words to Vectors.
      word2Vec = Word2Vec(vectorSize=3, minCount=0, inputCol="text", outputCol="result")
      model = word2Vec.fit(documentDF)

      result = model.transform(documentDF)
      for row in result.collect():
          text, vector = row
          print("Text: [%s] => \nVector: %s\n" % (", ".join(text), str(vector)))

      #Text: [Hi, I, heard, about, Spark] => 
      #Vector: [-0.013848860561847687,0.004904018342494965,-0.06304796114563942]

      #Text: [I, wish, Java, could, use, case, classes] => 
      #Vector: [0.028715904053699757,0.005890350788831711,-0.06727825477719307]

      #Text: [Logistic, regression, models, are, neat] => 
      #Vector: [0.005417859554290772,0.04880434982478619,0.0038779648020863533]
    ```
  - Tokenizer
    > Tokenization là quá trình lấy văn bản (chẳng hạn như một câu) và chia nó thành các thuật ngữ riêng lẻ (thường là các từ). Một lớp Tokenizer đơn giản cung cấp chức năng này. Ví dụ dưới đây cho thấy cách chia câu thành các chuỗi từ.
    
    RegexTokenizer cho phép mã hóa nâng cao hơn dựa trên đối sánh biểu thức chính quy (regex). Theo mặc định, tham số “pattern” (regex, default: "\\ s +") được sử dụng làm dấu phân tách để tách văn bản đầu vào
    
    ```python
      sentenceDataFrame = spark.createDataFrame([
      (0, "Hello how are you"),
      (1, "What is your name"),
      ], ["id", "sentence"])

      tokenizer = Tokenizer(inputCol="sentence", outputCol="words")
      tokenized = tokenizer.transform(sentenceDataFrame)
      tokenized.select("sentence", "words").show(truncate=False)
      
      #+-----------------+----------------------+
      #|sentence         |words                 |
      #+-----------------+----------------------+
      #|Hello how are you|[hello, how, are, you]|
      #|What is your name|[what, is, your, name]|
      #+-----------------+----------------------+
    ```


  - StopWordsRemover
    > Stop words là những từ cần được loại trừ khỏi đầu vào, thường là vì các từ xuất hiện thường xuyên và không mang nhiều ý nghĩa.
    
    ```python 
      sentenceData = spark.createDataFrame([
      (0, ["I", "saw", "the", "red", "balloon"]),
      (1, ["Mary", "had", "a", "little", "lamb"])
      ], ["id", "raw"])

      remover = StopWordsRemover(inputCol="raw", outputCol="filtered")
      remover.transform(sentenceData).show(truncate=False)
      #+---+----------------------------+--------------------+
      #|id |raw                         |filtered            |
      #+---+----------------------------+--------------------+
      #|0  |[I, saw, the, red, balloon] |[saw, red, balloon] |
      #|1  |[Mary, had, a, little, lamb]|[Mary, little, lamb]|
      #+---+----------------------------+--------------------+
    ```
   - StandardScaler và MinMaxScaler
    > StandardScaler biến đổi tập dữ liệu gồm các hàng Vectơ, chuẩn hóa từng tính năng để có độ lệch chuẩn đơn vị và / hoặc giá trị trung bình bằng 0.
    > MinMaxScaler biến đổi một tập dữ liệu gồm các hàng Vectơ, thay đổi tỷ lệ từng đối tượng thành một phạm vi cụ thể (thường là [0, 1])
    
    ```python
      dataFrame = spark.createDataFrame([
    (0, Vectors.dense([1.0, 0.1, -1.0]),),
    (1, Vectors.dense([2.0, 1.1, 1.0]),),
    (2, Vectors.dense([3.0, 10.1, 3.0]),)
    ], ["id", "features"])

    scaler = MinMaxScaler(inputCol="features", outputCol="scaledFeatures")

    scalerModel = scaler.fit(dataFrame)

    scaledData = scalerModel.transform(dataFrame)

    scaledData.select("features", "scaledFeatures").show()
    
    #+--------------+--------------+
    #|      features|scaledFeatures|
    #+--------------+--------------+
    #|[1.0,0.1,-1.0]|     (3,[],[])|
    #| [2.0,1.1,1.0]| [0.5,0.1,0.5]|
    #|[3.0,10.1,3.0]| [1.0,1.0,1.0]|
    #+--------------+--------------+
    ```
#### Classification and regression
- Logistic regression
  > Hồi quy logistic là một phương pháp phổ biến để dự đoán một phản ứng phân loại. Đây là một trường hợp đặc biệt của mô hình Tuyến tính Tổng quát dự đoán xác suất của các kết quả. Trong hồi quy logistic spark.ml có thể được sử dụng để dự đoán kết quả nhị phân bằng cách sử dụng hồi quy logistic nhị thức.hoặc nó có thể được sử dụng để dự đoán một kết quả đa lớp bằng cách sử dụng hồi quy logistic đa thức. Sử dụng tham số họ để chọn giữa hai thuật toán này hoặc không đặt nó và Spark sẽ suy ra biến thể chính xác.
  
  ```python
    from pyspark.ml.classification import LogisticRegression
    # Load training data
    training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))

    # We can also use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

    # Fit the model
    mlrModel = mlr.fit(training)
  ```

- Decision tree classifier
  > Cây quyết định là một họ phổ biến của các phương pháp phân loại và hồi quy
  
  ```python
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    featureIndexer =VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

    model = pipeline.fit(trainingData)
  ```
- Linear Support Vector Machine
  > Support Vector Machine xây dựng một siêu phẳng hoặc tập hợp các siêu phẳng trong không gian chiều cao hoặc vô hạn, có thể được sử dụng để phân loại, hồi quy hoặc các tác vụ khác. Theo trực quan, một siêu phẳng có khoảng cách lớn nhất đến điểm dữ liệu huấn luyện gần nhất của bất kỳ lớp nào (được gọi là lề chức năng), vì nói chung, lề càng lớn thì lỗi tổng quát của bộ phân loại càng thấp.
  
  ```python
    from pyspark.ml.classification import LinearSVC

    # Load training data
    training = spark.read.format("libsvm").load("data.txt")

    lsvc = LinearSVC(maxIter=10, regParam=0.1)

    # Fit the model
    lsvcModel = lsvc.fit(training)

    # Print the coefficients and intercept for linear SVC
    print("Coefficients: " + str(lsvcModel.coefficients))
    print("Intercept: " + str(lsvcModel.intercept))
  ```

- Naive Bayes
  > Naive Bayes classifiers là một họ các bộ phân loại đa lớp, theo xác suất đơn giản dựa trên việc áp dụng định lý Bayes với các giả định về tính độc lập mạnh mẽ (naive) giữa mọi cặp đối tượng.
  
  Naive Bayes có thể được đào tạo rất hiệu quả. Với một lần chuyển qua dữ liệu huấn luyện, nó sẽ tính toán phân phối xác suất có điều kiện của từng tính năng cho từng nhãn. Để dự đoán, nó áp dụng định lý Bayes để tính toán phân phối xác suất có điều kiện của mỗi nhãn cho một quan sát.
  
  ```python
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")

    # Split the data into train and test
    splits = data.randomSplit([0.6, 0.4], 1234)
    train = splits[0]
    test = splits[1]

    # create the trainer and set its parameters
    nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

    # train the model
    model = nb.fit(train)

    # select example rows to display.
    predictions = model.transform(test)
    predictions.show()
  ```
#### Demo for iris dataset
```python
  # load data 
  df = spark.read.csv('iris.data', header = False, inferSchema = True)
  df.printSchema()
  class_name = df.columns[len(df.columns)-1]
  
  # convert string to numeric 
  indexer = StringIndexer()
  indexer.setInputCol(class_name).setOutputCol("label")
  df1 = indexer.fit(df).transform(df)
  
  # features name and label name
  class_name = 'label'
  feature_names = df.columns[:-1]
  
  #add all feature to 1 vector
  assembler = VectorAssembler()
  assembler.setInputCols(feature_names).setOutputCol('features')
  transformed_data = assembler.transform(df1)
  
  # Split the data
  (training_data, test_data) = transformed_data.randomSplit([0.8,0.2])
  
  #model
  model = LogisticRegression(featuresCol = 'features',labelCol=class_name, maxIter=30)
  M = model.fit(training_data)
  
  # Predict with the test dataset
  predictions = M.transform(test_data)
  
  multi_evaluator = MulticlassClassificationEvaluator(labelCol = 'label', metricName = 'accuracy')
  print('Logistic Regression Accuracy:', multi_evaluator.evaluate(predictions))

  #Logistic Regression Accuracy: 0.896551724137931
  ```
