import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;


public class ConnectednessFactor {

    public static class FriendsMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("FriendRel,PersonID,MyFriend")) return;

            String[] tokens = line.split(",");
            if (tokens.length >= 3) {
                String friendID = tokens[2];
                context.write(new Text(friendID), new Text("F"));
            }
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.startsWith("PersonID,Name")) return;

            String[] tokens = line.split(",");
            if (tokens.length >= 2) {
                String personID = tokens[0];
                String name = tokens[1];
                context.write(new Text(personID), new Text("P" + name));
            }
        }
    }

    public static class ConnectednessReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String pageOwnerName = "Unknown";
            int friendCount = 0;

            for (Text val : values) {
                String value = val.toString();
                if (value.startsWith("P")) {
                    pageOwnerName = value.substring(1);
                } else if (value.equals("F")) {
                    friendCount++;
                }
            }

            if (!pageOwnerName.equals("Unknown")) {
                context.write(new Text(pageOwnerName), new IntWritable(friendCount));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Connectedness Factor Job");

        job.setJarByClass(ConnectednessFactor.class);
        job.setReducerClass(ConnectednessReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PagesMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        boolean success = job.waitForCompletion(true);

        System.exit(success ? 0 : 1);
    }
}
