package graphframe.sharedSparkContext;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Created by wso2 on 20/1/17.
 */
public class SoleSc {


    static JavaSparkContext sparkContext = new JavaSparkContext(
            new SparkConf().setAppName("SOME APP NAME").setMaster("local[2]").set("spark.executor.memory","1g")
    );

    static SQLContext sqlCtx = new SQLContext(sparkContext);
    public static Dataset getVertexDataFrame(){
        StructType customSchema = new StructType(new StructField[] {
                new StructField("vertex", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset dataframe = sqlCtx.read()
                .format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("header", "true")
                .load("/home/wso2/graph_data.csv");
        return dataframe;
    }
    public static Dataset getEdgeDataFrame(){
        StructType customSchema = new StructType(new StructField[] {
                new StructField("src", DataTypes.StringType, true, Metadata.empty()),
                new StructField("dst", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset dataframe = sqlCtx.read()
                .format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("header", "true")
                .load("/home/wso2/edge_data.csv");
        return dataframe;
    }
    public static Dataset getSolutionsWithVertex(){
        StructType customSchema = new StructType(new StructField[] {
                new StructField("solution_id", DataTypes.StringType, true, Metadata.empty()),
                new StructField("end_vertex", DataTypes.StringType, true, Metadata.empty())
        });
        Dataset dataframe = sqlCtx.read()
                .format("com.databricks.spark.csv")
                .schema(customSchema)
                .option("header", "true")
                .load("/home/wso2/solution_id_with_vertex.csv");
        return dataframe;
    }

    Dataset dataframe = sqlCtx.load("/home/wso2/graph_data.txt");
    public JavaSparkContext getSparkContext(){
        return this.sparkContext;
    }
}
