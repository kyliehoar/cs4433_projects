import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

public class FilterByNationality {
    public static class NationalityMapper extends Mapper<LongWritable, Text, Text, Text>{ //mapper nationalityMapper extends Hadoop's mapper class. 2 inputs (byte offset and line of input as string), 2 outputs (name of person, hobby)

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{
            final String myNationality = "Canada"; //filter the nationality

            String[] fields = value.toString().split(",");

            String nationality = fields[2]; //taken from 3rd field

            String name = fields[1];
            String hobby = fields[4];

            if(nationality.equals(myNationality)){
                String outputValue = name + " - " + hobby;

                context.write(new Text(name), new Text(outputValue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Filter by Nationality"); //created job Hadoop

        job.setJarByClass(FilterByNationality.class); //JAR file
        job.setMapperClass(NationalityMapper.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path("C:/Users/19788/Desktop/project1/pages.csv")); //set input path for job
        FileOutputFormat.setOutputPath(job, new Path("C:/Users/19788/Desktop/project1/output_taska")); //set output path for job

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
