package com.cowboysmall.scratch.twilio;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class SomethingTest {

    private SparkSession sparkSession;

    private JavaSparkContext sparkContext;

    @BeforeEach
    public void setUp() {

        sparkSession =
                SparkSession.builder()
                        .appName("Something App")
                        .master("local")
                        .getOrCreate();

        sparkContext = new JavaSparkContext(sparkSession.sparkContext());
    }

    @AfterEach
    public void tearDown() {

        sparkContext.close();
        sparkSession.close();
    }


    //_________________________________________________________________________

    @Test
    public void testSomething() {

        Dataset<Row> dataset = createDataset();
        assertThat(dataset.count(), equalTo(10L));


        Dataset<Row> cleaned =
                dataset.na().drop("any");
        assertThat(cleaned.count(), equalTo(7L));
        cleaned.show();


        Dataset<Row> filtered =
                cleaned.filter(cleaned.col("score").gt(150));
        assertThat(filtered.count(), equalTo(5L));
        filtered.show();


        Dataset<Row> grouped =
                filtered.groupBy("name", "year").max("score");
        assertThat(grouped.count(), equalTo(4L));
        grouped.show();
    }


    //_________________________________________________________________________

    private Dataset<Row> createDataset() {

        StructType schema = DataTypes.createStructType(

                new StructField[]{
                        DataTypes.createStructField("id", DataTypes.StringType, true),
                        DataTypes.createStructField("name", DataTypes.StringType, true),
                        DataTypes.createStructField("year", DataTypes.StringType, true),
                        DataTypes.createStructField("score", DataTypes.IntegerType, true)
                }
        );

        List<Object[]> data = new ArrayList<>();
        data.add(new Object[]{"1", "First", "2020", 175});
        data.add(new Object[]{"2", "Second", "2021", 100});
        data.add(new Object[]{"3", "Third", "2021", 225});
        data.add(new Object[]{"4", null, "2022", 100});
        data.add(new Object[]{"5", "Second", "2021", 150});
        data.add(new Object[]{"6", "Third", null, 100});
        data.add(new Object[]{"7", "Fourth", "2020", 250});
        data.add(new Object[]{"8", null, null, 100});
        data.add(new Object[]{"9", "First", "2020", 200});
        data.add(new Object[]{"10", "Second", "2021", 250});

        JavaRDD<Row> rows = sparkContext.parallelize(data).map(RowFactory::create);

        return sparkSession.sqlContext().createDataFrame(rows, schema).toDF();
    }
}
