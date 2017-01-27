package graphframe;


import com.google.gson.JsonObject;
import graphframe.patternMine.PatternMining;
import graphframe.sharedSparkContext.SoleSc;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.graphframes.GraphFrame;



public class FirstGraph{
    SoleSc sc = new SoleSc();
    Dataset<Row> vertex_dataset;
    Dataset<Row> edge_dataset;
    Dataset<Row> solution_ids;
    GraphFrame gFrame;


    public FirstGraph(){
        vertex_dataset = SoleSc.getVertexDataFrame();
        edge_dataset = SoleSc.getEdgeDataFrame();
        solution_ids = SoleSc.getSolutionsWithVertex();
        vertex_dataset = vertex_dataset.withColumn("vertex", functions.trim(vertex_dataset.col("vertex")));
        Dataset<Row> joined = vertex_dataset.join(solution_ids,vertex_dataset.col("vertex").equalTo(solution_ids.col("end_vertex")),"left");
        gFrame = new GraphFrame(joined.drop("end_vertex"), edge_dataset);
    }
    public JsonObject findSolutions(String[] strarr){
        JsonObject a = PatternMining.patternMine(strarr,gFrame);
        return a;
    }
    public static void main(String[] args){
        FirstGraph hhh = new FirstGraph();

        String[] arr = new String[]{"1:3","1:3","6:4","5:2"};
        System.out.print(hhh.findSolutions(arr));
    }

}