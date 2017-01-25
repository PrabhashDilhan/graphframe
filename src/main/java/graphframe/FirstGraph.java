package graphframe;

import graphframe.serializableFunction1.SerialiFunJRdd;
import graphframe.serializableFunction1.SerializableFunction;
import graphframe.sharedSparkContext.SoleSc;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.graphframes.GraphFrame;
import org.graphframes.lib.PageRank;
import scala.Option;
import scala.Tuple2;

import java.util.List;


public class FirstGraph{
    SoleSc sc = new SoleSc();
    String stringlist;
public static void main(String[] args) {
    try {

        //DataFrame edge_dataframe = SharedSC.getEdgeDataFrame("EDGE_RDD", -1234, sc.getSparkContext());
        //DataFrame vertex_dataframe = SharedSC.getVertexDataFrame("VERTEX_RDD", -1234, sc.getSparkContext());

        Dataset<Row> vertex_dataset = SoleSc.getVertexDataFrame();

        Dataset<Row> edge_dataset = SoleSc.getEdgeDataFrame();

        GraphFrame gFrame = new GraphFrame(vertex_dataset, edge_dataset);

        //gFrame.vertices().show();
        //gFrame.edges().show();

        gFrame.edges().filter("src = '7'").show();






    } catch (Exception e) {
        System.out.print(e);
    }
}
public static String[] patternMine(String[] str, GraphFrame gg){
    String[] minedPatterns = null;
    String ll = null;
    for(int i=0; i<str.length-1; i++) {
        List<Row> dataset = gg.edges().filter("src = '8'").collectAsList();
        for (int j = 0; j < dataset.size(); j++) {
            if(dataset.get(1).toString()==str[i+1]){
                ll = dataset.get(1).toString()+"-"+str[i+1];
            }
        }

    }
    return minedPatterns;
}
}