# geo_benchmark

[Han-Sub Shin, Kisung Lee, and Hyuk-Yoon Kwon, "Performance Evaluation of Spatial Data Management Systems Using GeoSpark," In Proc. 2020 IEEE International Conference on Big Data and Smart Computing, pp. 197-200, Feb. 2020.](https://ieeexplore.ieee.org/document/9070530)

GeoSpark의 storage 에 따른 성능 평가를 진행한 논문의 코드 저장소입니다.
본 논문에서는 Cloud storage로 AWS S3, 분산 storage로 HDFS, NoSQL storage로 MongoDB를 사용하였습니다. 
각각의 Storage에서 Box(Range), Circle(Distance), Distance Join, Knn 총 4가지 쿼리를 구현하였습니다. 

각각의 코드는 해당 directory에서 확인 가능합니다. 

# 사용법
Spark 작업 제출 방법 중 Jar(Java Archive)파일로 변환 후, Spark-submit 명령어를 통해 작업을 제출하는 방법을 사용했습니다.
사용하려는 쿼리의 폴더로 이동 후, Maven 을 통해 project packaging을 진행 후, 작업 제출을 진행합니다. 

명령어는 다음과 같습니다. 

### 1. Maven을 통한 packaging
* mvn clean
* mvn compile
* mvn package

### 2. Spark 작업 제출
* spark-submit \
  --class [class-name] \
  --deploy-mode [cluster] \
  --master [yarn] \ 
  --conf <key>=<value> \
  ./target/[jar-name].jar
