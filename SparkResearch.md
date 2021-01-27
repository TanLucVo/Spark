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
  
