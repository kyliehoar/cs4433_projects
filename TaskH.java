import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// ***** TASK H *****
// Report all people with a Facebook who are more ‘popular’ than the average person on
// the Facebook site, namely, those who have more friendship relationships than the
// average number of friend relationships per person across all people in the site.

public class TaskH {

    public static class PagesMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // get a line of input from pages.csv
            String line = value.toString();
            String[] split = line.split(",");

            // the foreign join key in the PersonID
            outkey.set(split[0]);

            // set custom output value to be the PersonID and Name
            String name = split[1];

            // flag this record for the reducer and then output
            outvalue.set("P" + name);

            context.write(outkey, outvalue);
        }
    }

    public static class FriendsMapper
            extends Mapper<Object, Text, Text, Text>{

        Map<String, Integer> userFriendCounts = new HashMap<>();
        private int totalFriendships = 0;

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // get a line of input from friends.csv
            String line = value.toString();
            String[] split = line.split(",");

            userFriendCounts.put(split[1], userFriendCounts.getOrDefault(split[1], 0) + 1);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : userFriendCounts.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(String.valueOf(entry.getValue())));
                totalFriendships+= entry.getValue();
            }
            context.write(new Text("-2"), new Text("F" + totalFriendships));
        }
    }

    public static class PopularReducer
        extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text();
        private Text tag = new Text();
        private String joinType = null;

        private int totalFriendships = 0;
        private int totalUsers = 0;
        private Map<String, Integer> userFriendCounts = new HashMap<>();
        private Map<String, String> userNames = new HashMap<>();

        public void setup(Context context) {
            // retrieve the type of join from our configuration
            joinType = context.getConfiguration().get("joinType");
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            int friendCount = 0;

            while (values.iterator().hasNext()) {
                tag = values.iterator().next();
                if (tag.charAt(0) == 'P') {
                    userNames.put(key.toString(), tag.toString().substring(1));
                    totalUsers++;
                } else if (tag.charAt(0) == 'F') {
                    totalFriendships += Integer.parseInt(tag.toString().substring(1));
                } else {
                    friendCount += Integer.parseInt(tag.toString());
                }
            }

            userFriendCounts.put(key.toString(), friendCount);
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            int averageFriendships = totalFriendships / totalUsers;
            if (joinType.equalsIgnoreCase("inner")) {
                for (Map.Entry<String, Integer> entry : userFriendCounts.entrySet()) {
                        if (entry.getValue() > averageFriendships && userNames.get(entry.getKey()) != null) {
                            Text userName = new Text(userNames.get(entry.getKey()));
                            context.write(new Text(entry.getKey()), userName);
                        }
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task H");
        job.setJarByClass(TaskH.class);
        job.setReducerClass(TaskH.PopularReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"), TextInputFormat.class, TaskH.PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv"),TextInputFormat.class, TaskH.FriendsMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskH.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task H");
        job.setJarByClass(TaskH.class);
        job.setReducerClass(TaskH.PopularReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"), TextInputFormat.class, TaskH.PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv"),TextInputFormat.class, TaskH.FriendsMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskH.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}