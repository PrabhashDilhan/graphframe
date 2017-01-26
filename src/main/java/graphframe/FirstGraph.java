package graphframe;

import com.google.gson.JsonObject;
import graphframe.sharedSparkContext.SoleSc;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
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

        Dataset<Row> solution_ids = SoleSc.getSolutionsWithVertex();
        solution_ids.show();

        Dataset<Row> joined = vertex_dataset.join(solution_ids,vertex_dataset.col("vertex").equalTo(solution_ids.col("end_vertex")),"left");

        joined.drop("end_vertex").show();

        GraphFrame gFrame = new GraphFrame(joined.drop("end_vertex"), edge_dataset);

       gFrame.vertices().show();
        //gFrame.edges().show();

        gFrame.edges().filter("src='1:3'").show();

        String[] str = new String[]{"6:4","6:4","5:2"};
        gFrame.vertices().filter("vertex='1:3'").show();

        System.out.print(patternMine(str,gFrame));
        //gFrame.edges().filter("dst = "+str[1].toString()+"").show();


    } catch (Exception e) {
        System.out.print(e);
    }
}
public static String patternMine(String[] str, GraphFrame gg){

    List<String> minedSolutions = new ArrayList<String>();
    List<String> ending_vertex_ids = new ArrayList<String>();
    List<List<String>> minedPatterns = new ArrayList<List<String>>();
    List<String> temp = new ArrayList<String>();

    JsonObject event = new JsonObject();
    JsonObject solutions = new JsonObject();
    String ll = null;


    for(int i=0; i<str.length-1; i++) {
        List<Row> dataset = gg.edges().filter("src = \""+str[i]+"\"").collectAsList();
        innerLoop:for (int j = 0; j < dataset.size(); j++) {
            if(dataset.get(j).getString(1).trim().equals(str[i+1])){
                temp.add(str[i]);
                if(gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(1)!=null){
                    String sol = gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(1);
                    String id = gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(0);
                    minedSolutions.add(sol);
                    ending_vertex_ids.add(id);
                    temp.add(str[i+1]);
                    minedPatterns.add(temp);
                    String rr = temp.get(temp.size()-1);
                    temp.remove(rr);
                    break innerLoop;
                }else{
                    break innerLoop;
                }
            }
            if(dataset.get(j).getString(1).trim().equals(str[i+1])==false && j==dataset.size()-1){
                temp.clear();
            }
        }
    }

    return  ll;
}
}