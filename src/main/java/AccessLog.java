import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Collections;

public class AccessLog extends Configured implements Tool {

    public static class AccessLogMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text pageID = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            if (fields.length == 5) {
                String pageIDValue = fields[2].trim();
                pageID.set(pageIDValue);

                context.write(pageID, one);
            }
        }
    }

    // Reducer class to aggregate the visit counts and sort the top 11
    public static class AccessLogReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private PriorityQueue<PageVisit> topPages;

        @Override
        public void setup(Context context) {
            // Priority queue to maintain the top 11 pages (increase to 11)
            topPages = new PriorityQueue<>(11, Collections.reverseOrder());
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;

            // Sum up the total visits for each page type
            for (IntWritable val : values) {
                sum += val.get();
            }

            // Add the page type and count to the priority queue
            topPages.add(new PageVisit(key.toString(), sum));

            // If the queue exceeds 11, remove the least frequent page
            if (topPages.size() > 11) {
                topPages.poll();
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            // Output the top 11 pages by visit count
            while (!topPages.isEmpty()) {
                PageVisit pageVisit = topPages.poll();
                context.write(new Text(pageVisit.pageID), new IntWritable(pageVisit.visitCount));
            }
        }

        // Helper class to store page visit data
        public static class PageVisit implements Comparable<PageVisit> {
            String pageID;
            int visitCount;

            PageVisit(String pageID, int visitCount) {
                this.pageID = pageID;
                this.visitCount = visitCount;
            }

            @Override
            public int compareTo(PageVisit other) {
                return Integer.compare(this.visitCount, other.visitCount);
            }
        }
    }

    // Driver class to configure and run the job
    public int run(String[] args) throws Exception {
        // Initialize a new Hadoop job
        Job job = Job.getInstance(getConf(), "Top 10 Most Visited Pages");
        job.setJarByClass(AccessLog.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(AccessLogMapper.class);
        job.setReducerClass(AccessLogReducer.class);

        // Set the output key and value types for the job
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Specify input and output paths (these should be passed as command-line arguments)
        FileInputFormat.addInputPath(job, new Path("C:/Users/19788/Desktop/project1/access_logs.csv"));  // Input path for access_logs.csv
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/19788/Desktop/project1/output_top10_pages"));  // Output path

        // Wait for job completion and return the status
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        // Run the job with the provided command-line arguments
        int res = ToolRunner.run(new AccessLog(), args);
        System.exit(res);
    }
}
