package spark;
 
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
 
import com.google.common.collect.Iterables;
 
import scala.Tuple2;
import scala.Tuple3;
import utils.ISO8601;
 
public class PageRankaki {
    public static void main(String[] args) throws ParseException {
        //long input_date = ISO8601.toTimeMS("2006-06-09T07:26:21Z");
        long input_date = ISO8601.toTimeMS(args[3]);
        int numIterations = Integer.parseInt(args[2]);
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Sparkaki"));
        Configuration conf = new Configuration();
        conf.set("textinputformat.record.delimiter", "\n\n");
        /*
         * Reading the files by Revision (Key, Value) = (Long ID, 14 lines of Revision)
         */
        JavaPairRDD<LongWritable, Text> lines =  sc.newAPIHadoopFile(args[0],TextInputFormat.class, LongWritable.class, Text.class, conf);
        System.out.println(lines.count());
        // Keeps the revisions with valid date
        lines =lines.filter(x-> ISO8601.toTimeMS(x._2.toString().split("\\s+")[4]) <= input_date);
        System.out.println(lines.count());
        /*
         * mapToPair : This functions splits the 14 Revision into 14 lines. Then, it splits the first line in order to save the Article_title and Date.
         * 			    Furthermore, it saves the line of the outlinks, it removes the duplicates,  and finally, it returns (Key, Value) = (Article_title, "date outlinks")
         *
         * reduceByKey: This function finds the most recent valid revision and its outlinks, 
         *
         * mapToPair: This function removes the selfloops and returns (Key, Value) = (Article_title, outlinks)
         */
        JavaPairRDD<String, String> links = lines.mapToPair(s-> {String parts[] =s._2.toString().split("\n");
                                                                           String Article_title = parts[0].split("\\s+")[3] ;
                                                                           String date_ = parts[0].split("\\s++")[4];
                                                                           String outlinks = parts[3].substring(0, parts[3].length());
                                                                           /*
                                                                            * removing duplicates
                                                                            */
                                                                           ArrayList<String> links_ = new ArrayList<String>();
                                                                           links_.addAll(Arrays.asList(outlinks.split("\\s+")));
                                                                           links_ = (ArrayList<String>) links_.stream().distinct().collect(Collectors.toList());
                                                                           outlinks = StringUtils.join(links_," ");
                                                                           /*
                                                                            * removing self-loops, keeping the most recent date and returning the result
                                                                            */
                                                                           return new Tuple2<String, String>(Article_title, date_+" "+outlinks.replaceAll("MAIN", "") ); })
                                                    .reduceByKey((x,y)->ISO8601.toTimeMS(x.split("\\s+")[0]) >ISO8601.toTimeMS(y.split("\\s+")[0]) ? x : y)
                                                    .mapToPair(z-> { return new Tuple2<String,String>(z._1, z._2.substring(z._2.toString().split("\\s+")[0].length(),z._2.toString().length()).
                                                                                                                                                        replaceAll(z._1.toString(), "").trim());})
                                                    .cache();
         
        /*
         * please uncomment if you want to see the (Article, outlinks)
         */
       // System.out.println(links.collect());
        
        /*
         * Initialise Page Rank with 1.0
         */
        JavaPairRDD<String, Double> ranks = links.mapValues(s-> 1.0);
        /*
         * Page rank Function:
         * 					   Join : With this function help us find each contribution of a node. It returns (Article_id, (outlinks, PR))
         * 					   FlatMapToPair: This function computes the Contribution of each node(PR/#outlinks) and add them to the returned List. (Key, Value) =(Article_title,contr)
         * 									  It also adds a (Key, Value) = (Article_title, 0.0) in order to pass the nodes without inlinks to the reduce function.
         * 					   ReduceByKey: It computes the PageRank with the given formula.
         * 							
         */
        for (int current = 0; current < numIterations; current++) {
        JavaPairRDD<String, Double> contribs = links.join(ranks)
                									 .flatMapToPair(s -> { List<Tuple2<String, Double>> res = new ArrayList<Tuple2<String, Double>>();
               
                //add every node in order to compute every pagerank(nodes without inlinks)
                res.add(new Tuple2<String, Double>(s._1.toString(),0.0));
             // s2 = (link1 link2, 1.0)
                String s2 = s._2.toString();
                // s2 = link1 link2 1.0
                s2 = s2.substring(1,s2.length()-1).replaceAll(",", " ");
               
                String[] outlinks = s2.split("\\s+");
               
                int url_count = outlinks.length-1;
               
                for (int i=0; i<outlinks.length-1; i++) {
                    res.add(new Tuple2<String, Double>(outlinks[i], Double.parseDouble(outlinks[url_count])/ url_count));
                }
                
                return res;
                });
//        System.out.println("Contributions : "+contribs.collect());
        ranks = contribs.filter(x-> !(x._1.equals(""))).reduceByKey((a, b) -> a + b).mapValues(v -> 0.15 + v * 0.85);
        }
        /*
         * Sorting by descending order the output File. ( Sorting steps: (Article_title, PR) -> (PR, Article_title) ~ SortbyKey(PR) -> (Article_title, PR)
         */
        ranks=ranks.mapToPair(p->p.swap()).sortByKey(false).mapToPair(p->p.swap());
        /*
         * Removing comma and parenthesis
         */
        ranks.map(x -> x._1 + " " + x._2).saveAsTextFile(args[1]);
       
       /*
        * Closing the app
        */
        sc.close();
       
       
    }
}