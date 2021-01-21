![TanLuc's github stats](https://github-readme-stats.vercel.app/api?username=TanLucVo&theme=dark&show_icons=true)
# Tổng quan về Apache Spark
![Image of Apache Spark](https://viblo.asia/uploads/e458bfb3-2876-490e-8456-d1b03f87600c.jpg)

  Apache Spark là một open source cluster computing framework được phát triển sơ khởi vào năm 2009 bởi AMPLab tại đại học California. Sau này, Spark đã được trao cho Apache Software Foundation vào năm 2013 và được phát triển cho đến nay. Nó cho phép xây dựng các mô hình dự đoán nhanh chóng với việc tính toán được thực hiện trên một nhóm các máy tính, có có thể tính toán cùng lúc trên toàn bộ tập dữ liệu mà không cần phải trích xuất mẫu tính toán thử nghiệm. Tốc độ xử lý của Spark có được do việc tính toán được thực hiện cùng lúc trên nhiều máy khác nhau. Đồng thời việc tính toán được thực hiện ở bộ nhớ trong (in-memories) hay thực hiện hoàn toàn trên RAM.

### Thành phần của Apache Spark
  Thành phần trung của Spark là Spark Core: cung cấp những chức năng cơ bản nhất của Spark như lập lịch cho các tác vụ, quản lý bộ nhớ, fault recovery, tương tác với các hệ thống lưu trữ…Đặc biệt, Spark Core cung cấp API để định nghĩa RDD (Resilient Distributed DataSet) là tập hợp của các item được phân tán trên các node của cluster và có thể được xử lý song song.
![Image of Thành phần Apache Spark](https://viblo.asia/uploads/c3ff2905-9c18-40cf-9319-c4ebcd2acdb2.jpg)

Spark có thể chạy trên nhiều loại Cluster Managers như Hadoop YARN, Apache Mesos hoặc trên chính cluster manager được cung cấp bởi Spark được gọi là Standalone Scheduler.
  - Spark SQL cho phép truy vấn dữ liệu cấu trúc qua các câu lệnh SQL. Spark SQL có thể thao tác với nhiều nguồn dữ liệu như Hive tables, Parquet, và JSON.

  - Spark Streaming cung cấp API để dễ dàng xử lý dữ liệu stream,

  - MLlib Cung cấp rất nhiều thuật toán của học máy như: classification, regression, clustering, collaborative filtering…

  - GraphX là thư viện để xử lý đồ thị.
  
### Những tính năng nổi bật
  - Spark as a Service: Giao diện REST để quản lí (submit, start, stop, xem trạng thái) spark job, spark context
  
  - Tăng tốc, giảm độ trễ thực thi job xuống mức chỉ tính bằng giây bằng cách tạo sẵn spark context cho các job dùng chung.
  
  - Stop job đang chạy bằng cách stop spark context.
  
  - Bỏ bước upload gói jar lúc start job làm cho job được start nhanh hơn.
  
  - Cung cấp hai cơ chế chạy job đồng bộ và bất đồng bộ
  
  - Cho phép cache RDD theo tên , tăng tính chia sẻ và sử dụng lại RDD giữa các job.
  
  - Hỗ trợ viết spark job bằng cú pháp SQL.
  
  - Dễ dàng tích hợp với các công cụ báo cáo như: Business Intelligence, Analytics, Data Integration Tools.

### Hiệu năng
  Về tốc độ xử lý thì Spark nhanh hơn Hadoop. Spark được cho là nhanh hơn Hadoop gấp 100 lần khi chạy trên RAM, và gấp 10 lần khi chạy trên ổ cứng. Hơn nữa, người ta cho rằng Spark sắp xếp (sort) 100TB dữ liệu nhanh gấp 3 lần Hadoop trong khi sử dụng ít hơn 10 lần số lượng hệ thống máy tính.

  ![Image of Language Spark](https://images.viblo.asia/full/de17071c-f13c-41c9-80ad-b39401d16cc2.jpg)
  
  Sở dĩ Spark nhanh là vì nó xử lý mọi thứ ở RAM. Nhờ xử lý ở bộ nhớ nên Spark cung cấp các phân tích dữ liệu thời gian thực cho các chiến dịch quảng cáo, machine learning (học máy), hay các trang web mạng xã hội.

### Spark Languages
  Lập trình viên có thể viết các ứng dụng Spark bằng nhiều ngôn ngữ khác nhau
  
  ![Image of Language Spark](https://viblo.asia/uploads/d1771afd-6556-4e39-8b93-dc85f44a692d.jpg)
  
### Các công ty đang sử dụng Spark 
  
  ![Image of Language Spark](  https://viblo.asia/uploads/fdac5aee-56e7-4a29-83f8-edc8dff6b9c8.jpg)


# Tổng quan về Mapreduce

### MapReduce là gì?
  MapReduce là mô hình được thiết kế độc quyền bởi Google, nó có khả năng lập trình xử lý các tập dữ liệu lớn song song và phân tán thuật toán trên 1 cụm máy tính. MapReduce trở thành một trong những thành ngữ tổng quát hóa trong thời gian gần đây. 

  ![Image of Language Spark](https://blog.itnavi.com.vn/wp-content/uploads/2020/06/Mapreduce-l%C3%A0-g%C3%AC-1.jpg)
  
  MapReduce sẽ  bao gồm những thủ tục sau: thủ tục 1 Map() và 1 Reduce(). Thủ tục Map() bao gồm lọc (filter) và phân loại (sort) trên dữ liệu khi thủ tục khi thủ tục Reduce() thực hiện quá trình tổng hợp dữ liệu. Đây là mô hình dựa vào các khái niệm biển đối của bản đồ và reduce những chức năng lập trình theo hướng chức năng. Thư viện của thủ tục Map() và Reduce() sẽ được viết bằng nhiều loại ngôn ngữ khác nhau. Thủ tục được cài đặt miễn phí và được sử dụng phổ biến nhất là là Apache Hadoop.

### Các hàm chính của MapReduce là gì?
  > MapReduce có 2 hàm chính là Map() và Reduce(), đây là 2 hàm đã được định nghĩa bởi người dùng và nó cũng chính là 2 giai đoạn liên tiếp trong quá trình xử lý dữ liệu của MapReduce. Nhiệm vụ cụ thể của từng hàm như sau: 
  - Hàm Map(): có nhiệm vụ nhận Input cho các cặp giá trị/  khóa và output chính là tập những cặp giá trị/khóa trung gian. Sau đó, chỉ cần ghi xuống đĩa cứng và tiến hành thông báo cho các hàm Reduce() để trực tiếp nhận dữ liệu. 
  - Hàm Reduce(): có nhiệm vụ tiếp nhận từ khóa trung gian và những giá trị tương ứng với lượng từ khóa đó. Sau đó, tiến hành ghép chúng lại để có thể tạo thành một tập khóa khác nhau. Các cặp khóa/giá trị này thường sẽ thông qua một con trỏ vị trí để đưa vào các hàm reduce. Quá trình này sẽ giúp cho lập trình viên quản lý dễ dàng hơn một lượng danh sách cũng như  phân bổ giá trị sao cho  phù hợp nhất với bộ nhớ hệ thống. 
  - Ở giữa Map và Reduce thì còn 1 bước trung gian đó chính là Shuffle. Sau khi Map hoàn thành  xong công việc của mình thì Shuffle sẽ làm nhiệm vụ chính là thu thập cũng như tổng hợp từ khóa/giá trị trung gian đã được map sinh ra trước đó rồi chuyển qua cho Reduce tiếp tục xử lý.
  
![Image of Language Spark](  https://blog.itnavi.com.vn/wp-content/uploads/2020/06/Mapreduce-l%C3%A0-g%C3%AC-2.jpg)

### Các ưu điểm nổi bật của MapReduce
  - MapReduce có khả năng xử lý dễ dàng mọi bài toán có lượng dữ liệu lớn nhờ khả năng tác vụ phân tích và tính toán phức tạp. Nó có thể xử lý nhanh chóng cho ra kết quả dễ dàng chỉ trong khoảng thời gian ngắn.
  - Mapreduce có khả năng chạy song song trên các máy có sự phân tán  khác nhau. Với khả năng hoạt động độc lập kết hợp  phân tán, xử lý các lỗi kỹ thuật để mang lại nhiều hiệu quả cho toàn hệ thống. 
  - MapRedue có khả năng thực hiện trên nhiều nguồn ngôn ngữ lập trình khác nhau như: Java, C/ C++, Python, Perl, Ruby,… tương ứng với nó là những thư viện hỗ trợ. 
  - Mã độc trên internet ngày càng nhiều hơn nên việc xử lý những đoạn mã độc này cũng trở nên rất phức tạp và tốn kém nhiều thời gian. Chính vì vậy, các ứng dụng MapReduce dần hướng đến quan tâm nhiều hơn cho việc phát hiện các mã độc để có thể xử lý chúng. Nhờ vậy, hệ thống mới có thể vận hành trơn tru và được bảo mật nhất.

### MapReduce được ứng dụng cho việc thống kê hàng loạt những số liệu cụ thể: 

  - Thực hiện thống kê cho các từ khóa được xuất hiện ở trong các tài liệu, bài viết, văn bản hoặc được cập nhật trên hệ thống fanpage, website,…
  - Khi số lượng các bài viết đã được thống kê thì tài liệu sẽ có chứa các từ khóa đó. 
  - Sẽ có thể thống kê được những câu lệnh match, pattern bên trong các tài liệu đó
  - Khi thống kê được số lượng các URLs có xuất hiện bên trong một webpages. 
  - Sẽ thống kê được các lượt truy cập của khách hàng sao cho nó có thể tương ứng với các URLs.
  - Sẽ thống kê được tất cả từ khóa có trên website, hostname,…


  
  
    
