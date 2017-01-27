package graphframe.patternMine;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.sql.Row;
import org.graphframes.GraphFrame;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by wso2 on 27/1/17.
 */
public class PatternMining {
    public static JsonObject patternMine(String[] str, GraphFrame gg){

        List<String> minedSolutions = new ArrayList<String>();
        List<String> ending_vertex_ids = new ArrayList<String>();
        List<String> minedPatterns = new ArrayList<String>();
        List<String> temp = new ArrayList<String>();

        JsonObject finalArnswer = new JsonObject();
        JsonObject solutions = new JsonObject();
        String kk = "";


        for(int i=0; i<str.length-1; i++) {
            List<Row> dataset = gg.edges().filter("src = \""+str[i]+"\"").collectAsList();
            innerLoop:for (int j = 0; j < dataset.size(); j++) {
                if(dataset.get(j).getString(1).trim().equals(str[i+1])){
                    temp.add(str[i]);
                    kk = kk+temp.toString()+"@@@";
                    if(gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(1)!=null){
                        String sol = gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(1);
                        String id = gg.vertices().filter("vertex=\"" + str[i+1] + "\"").collectAsList().get(0).getString(0);
                        minedSolutions.add(sol);
                        ending_vertex_ids.add(id);
                        temp.add(str[i+1]);
                        kk = kk+temp.toString()+"#####";
                        minedPatterns.add(temp.toString());
                        temp.remove(temp.size()-1);
                        break innerLoop;
                    }else{
                        break innerLoop;
                    }
                }
                if(dataset.get(j).getString(1).trim().equals(str[i+1])==false && j==dataset.size()-1){
                    temp.clear();
                    kk = kk+">>>>";
                }
            }
        }
        String minedSolutionsJson = new Gson().toJson(minedSolutions);
        String ending_vertex_idsJson = new Gson().toJson(ending_vertex_ids);
        String minedPatternsJson = new Gson().toJson(minedPatterns);

        solutions.addProperty("minedSolutions",minedSolutionsJson);
        solutions.addProperty("ending_vertex_idsJson",ending_vertex_idsJson);
        solutions.addProperty("minedPatternsJson",minedPatternsJson);
        finalArnswer.add("message",solutions);


        return  finalArnswer;
    }
}
