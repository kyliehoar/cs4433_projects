import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// ***** TASK F *****
// Identify people p1 that have declared someone as their friend p2 yet who have never
// accessed their respective friend p2’s Facebook page and return the PersonID and the
// Name of the person p1. This may indicate that they don’t care enough to find out any
// news about their friend -- at least not via Facebook.

public class TaskF {

    public static class FriendsMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");

            outKey.set(split[1]);
            outValue.set("F" + split[2]);
            context.write(outKey, outValue);
        }
    }

    public static class AccessLogMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");

            outKey.set(split[1]);
            outValue.set("A" + split[2]);
            context.write(outKey, outValue);
        }
    }

    public static class PagesMapper
        extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] split = line.split(",");

            outKey.set(split[0]);
            outValue.set("P" + split[1]);
            context.write(outKey, outValue);
        }
    }

    public static class FriendshipReducer
            extends Reducer<Text, Text, Text, Text> {

        private static final Text EMPTY_TEXT = new Text();
        private Text tag = new Text();
        private String joinType = null;

        private ArrayList<Text> friendsList = new ArrayList<Text>();
        private ArrayList<Text> accessList = new ArrayList<Text>();

        public void setup(Context context) {
            // retrieve the type of join from our configuration
            joinType = context.getConfiguration().get("joinType");
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            friendsList.clear();
            accessList.clear();
            Text name = new Text();
            Text fakeFriend = new Text();

            while (values.iterator().hasNext()) {
                tag = values.iterator().next();
                if (tag.charAt(0) == 'F') {
                    friendsList.add(new Text(tag.toString().substring(1)));
                } else if (tag.charAt(0) == 'A') {
                    accessList.add(new Text(tag.toString().substring(1)));
                } else if (tag.charAt(0) == 'P') {
                    name = new Text(tag.toString().substring(1));
                }
            }

            for (Text F : friendsList) {
                if (!accessList.contains(F)) {
                    fakeFriend = name;
                }
            }

            executeJoinLogic(context, key, fakeFriend);
        }

        protected void executeJoinLogic(Context context, Text key, Text name) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                if (!friendsList.isEmpty() && !accessList.isEmpty() && !name.toString().isEmpty()) {
                    context.write(key, name);
                }
            }
        }
    }

    public static void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task f");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(TaskF.FriendshipReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv"), TextInputFormat.class, TaskF.FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv"),TextInputFormat.class, TaskF.AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"),TextInputFormat.class, TaskF.PagesMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskF.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task f");
        job.setJarByClass(TaskF.class);
        job.setReducerClass(TaskF.FriendshipReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv"), TextInputFormat.class, TaskF.FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv"),TextInputFormat.class, TaskF.AccessLogMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"),TextInputFormat.class, TaskF.PagesMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskF.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
