package com.zhuinden.sparkexperiment;



import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;


/**
 * Created by achat1 on 9/23/15.
 * Just an example to see if it works.
 */
@Component
public class WordCount implements Serializable {
    @Autowired
    private SparkSession sparkSession;
    static final String path = "/home/jojang/dev/workspace/spark/data/";
    static final String fileName = "/home/jojang/dev/workspace/spark/data/schedule.csv";

    public List<Count> count() throws Exception {

//
//
////        String input = "hello world hello hello hello";
////        String[] _words = input.split(" ");
////        List<Word> words = Arrays.stream(_words).map(Word::new).collect(Collectors.toList());
////         words.forEach(x -> System.out.println( " word :" + x.getWord()) );
////
////        Dataset<Row> dataFrame = sparkSession.createDataFrame(words, Word.class);
////        dataFrame.collectAsList().forEach(x->  System.out.println("xxx:" + x) );
//
//        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
//        JavaRDD<String> dataSet =jsc.textFile(path+ "schedule.csv",8);
//        dataSet.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
//        System.out.println(" get0:"  + dataSet.collect().get(0));
//        System.out.println(" get1:"  + dataSet.collect().get(1));
//
//
////
////
////        SQLContext sqlContext= sparkSession.sqlContext();
//////        sqlContext.read().option("header","true").csv(path+ "schedule.csv").toDF();
////
//////        sqlContext.read().format("jdbc").option("url", "jdbc:mysql://opus365-dev01.cbqbqnguxslu.ap-northeast-2.rds.amazonaws.com:3306/ftr?autoReconnection=true&useUnicode=true&characterEncoding=UTF-8&characterSetResults=UTF-8&useSSL=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=Asia/Seoul").option("dbtable", "ftr.ftr_ofer").option("user", "ftradm").option("password", "12345678").load();
////        jsc.stop();
//
//        JavaRDD<String> textFile = sparkSession.sparkContext().textFile(path+ "schedule.csv",8).toJavaRDD() ;
//        System.out.println("Text file count:" + textFile.count() );
////        List<String> list= textFile.collect();
////        System.out.println("list.get(0): " + list.get(0) );
////        System.out.println("list.get(1): " + list.get(1) );
//
////        for(String in: list ) {
////            System.out.println(in);
////        }
//
//
//        Dataset<Row> data = sparkSession.createDataFrame(textFile, Schedule.class);
//        data.createOrReplaceTempView("my_schedule");
//        Dataset<Row> sqlDS=sparkSession.sql("select * from my_schedule");
//
//
//
//
//        Encoder<Schedule> scheduleEncoder = Encoders.bean(Schedule.class);
//
//        Dataset<Schedule> scheduleDataset = sqlDS.map(new MapFunction<Row, Schedule>() {
//            @Override
//            public Schedule call(Row row) throws Exception {
//                System.out.println("Data======"+ row.getString(0));
//                return new Schedule(row.getString(0));
//            }
//        }, scheduleEncoder);
//
//        System.out.println( scheduleDataset.toString()) ;
//        Dataset<String> j= scheduleDataset.select("carrier_cd").toJSON();
//
//
//
//        System.out.println(   ) ;
//        System.out.println( "end!!!!!!!!!") ;
//
////        sqlDS.select("carrier_cd") ;
//
//
//
////        scheduleDataset.show();
//
//
//
//
////        RDD<String> textFile = sparkSession.sparkContext().textFile(path+ "schedule.csv",8) ;
////        System.out.println("Text file count:" + textFile.count() );
////        textFile.saveAsTextFile("output/test5.csv"); ;
//
////
////        Dataset<Row> ds= sparkSession.createDataset(textFile,Encoders.STRING()).toDF() ;
////        ds = ds.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
////        ds.createOrReplaceTempView("my_schedule");
////
//////        ds.write().saveAsTable("my_schedule");
////        Dataset<Row> sqlDS=sparkSession.sql("select count(*) as rCnt from my_schedule").toDF("rCnt");
////
////        System.out.println( sqlDS.toString());
////        System.out.println(sqlDS.col("rCnt"));
////        System.out.println(sqlDS.select("rCnt"));
//
//
////        Dataset<Integer> cntDS =sqlDS.map((MapFunction<Row,String>) row -> row.getInt(0), Encoders.INT() );
//
//
//
////        ds.write().mode(SaveMode.Overwrite).option("header", "true")
////                .format("com.databricks.spark.csv").save("/output/ds.csv");
//
//
//
////        C
////        System.out.println("ds2 - count:" + ds2.toString() );
////        ds2.write().format("csv").save("outputTest.csv");
//        try {
////         sparkSession.sql("select * from schedule").write().mode("overwrite").csv("output/test.csv");
//        }catch (Exception ex) {
//            ex.printStackTrace();
//        }
////         sparkSession.read().format("csv").option("header","true").text("schedule.csv");
////                 .createOrReplaceTempView("schedule"); ;
//
//
////        dataFrame.show();
//        //StructType structType = dataFrame.schema();
//
////        RelationalGroupedDataset groupedDataset = dataFrame.groupBy(col("word"));
////        groupedDataset.count().show();
////        List<Row> rows = groupedDataset.count().collectAsList();//JavaConversions.asScalaBuffer(words)).count();
////        return rows.stream().map(new Function<Row, Count>() {
////            @Override
////            public Count apply(Row row) {
////                return new Count(row.getString(0), row.getLong(1));
////            }
////        }).collect(Collectors.toList());
//
//
////        Dataset<Row> df = sparkSession.readStream().csv("schedule.csv");
////        df.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
////        df.cache();
////        df.show();
////        df.printSchema();
//
////        sparkSession.sparkContext().stop();
//
////         proc2() ;
////         proc3() ;
        proc4();
//        proc5();

        return null;
    }

//    private void queryData(JavaSparkContext sc,String query) {
//
//        CassandraConnector connector = CassandraConnector.apply(sc.getConf());
//        try (Session session = connector.openSession()) {
//
//
//            ResultSet results = (ResultSet) session.execute()
//
//            System.out.println("\nQuery all results from cassandra's todolisttable:\n" + results.all());
//
//
//        }
//
//    }


    private void proc2() {
        try {
            Dataset<Row> ds =  sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                    .option("header", "true").load("/home/jojang/dev/workspace/spark/data/schedule.csv");
            ds.show(false);
            System.out.println("csv loaded");
        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }


    private void proc3() {
        try {
            Dataset<Row> ds =  sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                    .option("header", "true").load("/home/jojang/dev/workspace/spark/data/schedule.csv");
//            ds.show(false);

            ds.createOrReplaceTempView("schedule_2");
            Dataset<Row> results=sparkSession.sql("select * from schedule_2");

            results.show(false);
            results.printSchema();

            Dataset<String> namesDS = results.map(
                    (MapFunction<Row, String>) row -> "Name: " + row.getString(0),
                    Encoders.STRING());

//            namesDS.show(false);

            Dataset<Row> results2=sparkSession.sql("select ServiceLane from schedule_2");
            results2.show(false);


            System.out.println("table loaded");
        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    private void proc4() {
        try {
            JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
            JavaRDD<String> dataSet =jsc.textFile(path+ "schedule.csv",8);
            dataSet.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
            dataSet.collect();


            Dataset<Row> ds =  sparkSession.sqlContext().read().format("com.databricks.spark.csv")
                    .option("header", "true").load("/home/jojang/dev/workspace/spark/data/schedule.csv").coalesce(8) ;
            ds.show(false);


            Row[] c= (org.apache.spark.sql.Row[])ds.collect();
            System.out.println("CSV Collect: " + c[1]);

            ds.createOrReplaceTempView("schedule_2");
            Dataset<Row> results=sparkSession.sql("select * from schedule_2");
            Row[] c2= (org.apache.spark.sql.Row[])results.collect();
            System.out.println("SQL Collect: " + c2[1]);

            System.out.println("=============================================================================================");
            for(int i=0; i< c2.length; i++) {
                System.out.println( c2[i]);
                if(i>9) break;
            }
            System.out.println("=============================================================================================");

            results.show(false);
            results.printSchema();


            ds.createOrReplaceGlobalTempView("schedule_2g");
            Dataset<Row> results2=sparkSession.sql("select * from global_temp.schedule_2g");
            Row[] c3= (org.apache.spark.sql.Row[])results2.collect();
            System.out.println("Global SQL Collect: " + c3[1]);


            System.out.println("csv sqlContext loaded. partition Num:" + ds.rdd().getNumPartitions() );
            System.out.println("table loaded");
        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }


    private void proc5() {
        try {
        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
        JavaRDD<String> dataSet =jsc.textFile(path+ "schedule.csv",8);
        dataSet.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
        dataSet.collect();

            StructField[] structFields = new StructField[]{
//                    new StructField("intColumn", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("stringColumn", DataTypes.StringType, true, Metadata.empty())
            };

            StructType yourStruct =new StructType(structFields);
            Dataset<Row> personDS = sparkSession.createDataFrame(dataSet.rdd(), Schedule.class);
            personDS.printSchema();

            Encoder<Schedule> scheduleEncoder = Encoders.bean(Schedule.class);

        Dataset<Schedule> scheduleDataset = personDS.map(new MapFunction<Row, Schedule>() {
            @Override
            public Schedule call(Row row) throws Exception {
                System.out.println("Data======"+ row.getString(0));
                return new Schedule(row.getString(0));
            }
        }, scheduleEncoder);

            scheduleDataset.show();

        }catch (Exception ex) {
            ex.printStackTrace();
        }

    }


//    private void proc6() {
//
//        JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());
//        JavaRDD<String> rows =jsc.textFile(path+ "schedule.csv",8);
//        rows.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
//        rows.collect();
//
//        StructType schema = new StructType(new StructField[] {
//                new StructField("words",
//                        DataTypes.createArrayType(DataTypes.StringType), false,
//                        Metadata.empty()) });
//        Dataset<Row> wordDF = new SQLContext(sparkSession).createDataFrame(rows.rdd(), schema);
//        // build a bigram language model
//        NGram transformer = new NGram().setInputCol("words")
//                .setOutputCol("ngrams").setN(2);
//        DataFrame ngramDF = transformer.transform(wordDF);
//        ngramDF.show(10, false);
//        return ngramDF;
//    }


    public void createTable() {

        Dataset<Row> results2=sparkSession.sql("select * from global_temp.schedule_2g");
        Row[] c3= (org.apache.spark.sql.Row[])results2.collect();
        System.out.println("create SQL Collect: " + c3[1]);
        results2.show();

        results2.writeTo("schedule_3g");
        results2.write().mode(SaveMode.Overwrite).parquet(path+"schedule_3g.parquet");

    }

    public void global_view() {

        Dataset<Row> results2=sparkSession.sql("select * from global_temp.schedule_2g");
        Row[] c3= (org.apache.spark.sql.Row[])results2.collect();
        System.out.println("Global SQL Collect: " + c3[1]);
        results2.show();


    }




}