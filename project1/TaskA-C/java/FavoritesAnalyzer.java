import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;

public class FavoritesAnalyzer {

    public static class AccessLogMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("AccessID,ByWho,WhatPage,TypeOfAccess,AccessTime")) return;

            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String userID = tokens[1];
                String pageID = tokens[2];

                context.write(new Text(userID), new Text("P" + pageID));
                context.write(new Text(userID), new Text("A"));
            }
        }
    }

    public static class FavoritesReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int totalAccesses = 0;
            HashSet<String> distinctPages = new HashSet<>();

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("P")) {
                    distinctPages.add(value.substring(1));
                } else if (value.equals("A")) {
                    totalAccesses++;
                }
            }

            context.write(key, new Text(totalAccesses + "\t" + distinctPages.size()));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Favorites Analyzer");

        job.setJarByClass(FavoritesAnalyzer.class);
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(FavoritesReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
