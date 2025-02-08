import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import org.apache.commons.lang3.StringUtils;

public class FacebookCount {
    public static class NationalityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if(fields[0].equals("PersonID")){
            return; //this just skips the header row
        }

        if (fields.length >= 3) {
            String nationality = fields[2].trim();

            if(!StringUtils.isEmpty(nationality)){
                context.write(new Text(nationality), one);
            }
        }
    }
}

    public static class NationalityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();//group the country count
            }
            result.set(sum);
            context.write(key, result); //output country and total count
        }
    }

public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Count Facebook Pages per Nationality");

    job.setJarByClass(FacebookCount.class);

    job.setMapperClass(NationalityMapper.class);
    job.setReducerClass(NationalityReducer.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setNumReduceTasks(1); //ensuring reducer is called

    FileInputFormat.addInputPath(job, new Path("C:/Users/19788/Desktop/project1/pages.csv"));
    FileOutputFormat.setOutputPath(job, new Path("C:/Users/19788/Desktop/project1/output_taskc"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
