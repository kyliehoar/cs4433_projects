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
import org.apache.hadoop.fs.FileSystem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class Top10 extends Configured implements Tool {

    public static class EnrichWithMetadataMapper extends Mapper<Object, Text, Text, Text> {
        private HashMap<String, String> pageMetadata = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path pagesCsvPath = new Path("C:/Users/19788/Desktop/project1/pages.csv");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pagesCsvPath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] fields = line.split(",");
                    String personID = fields[0].trim();
                    String name = fields[1].trim();
                    String nationality = fields[2].trim();
                    pageMetadata.put(personID, name + "," + nationality);
                }
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split("\t");

            if (fields.length == 2) {
                String whatPage = fields[0].trim();
                String visitCount = fields[1].trim();

                String personID = whatPage;
                if (pageMetadata.containsKey(personID)) {
                    String metadata = pageMetadata.get(personID);
                    String[] metadataParts = metadata.split(",");
                    String name = metadataParts[0];
                    String nationality = metadataParts[1];

                    context.write(new Text(personID), new Text(name + "," + nationality + "," + visitCount));
                }
            }
        }
    }

    public static class EnrichWithMetadataReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key, value);
            }
        }
    }

    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(getConf(), "Top 10 Visited Pages");
        job.setJarByClass(Top10.class);

        job.setMapperClass(EnrichWithMetadataMapper.class);
        job.setReducerClass(EnrichWithMetadataReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("C:/Users/19788/Desktop/project1/output_top10_pages/part-r-00000"));
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/19788/Desktop/project1/output_taskb"));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        int res = ToolRunner.run(new Top10(), args);
        System.exit(res);
    }
}
