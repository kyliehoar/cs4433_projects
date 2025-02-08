import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

// ***** TASK G *****
// Identify all "disconnected" people (and return their PersonID and Name) that have not
// accessed the Facebook site for 14 days or longer (i.e., meaning no entries in the
// AccessLog exist in the last 14 days).

public class TaskG {

    public static class PagesMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // get a line of input from pages.csv
            String line = value.toString();
            String[] split = line.split(",");

            // the foreign join key in the PersonID
            outkey.set(split[0]);

            // set custom output value to be the Name
            String name = split[1];

            // flag this record for the reducer and then output
            outvalue.set("P" + name);
            context.write(outkey, outvalue);
        }
    }

    public static class AccessLogsMapper
            extends Mapper<Object, Text, Text, Text>{

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            // get a line of input from access_logs.csv
            String line = value.toString();
            String[] split = line.split(",");

            // the foreign join key is the PersonID
            outkey.set(split[1]);

            // set the custom output value to be the AccessTime
            String accessTime = split[4];

            // flag this record for the reducer and then output
            outvalue.set("A" + accessTime);

            context.write(outkey, outvalue);
        }
    }

    public static class DisconnectedReducer
            extends Reducer<Text,Text,Text,Text> {

        private static final Text EMPTY_TEXT = new Text();
        private Text tag = new Text();
        private ArrayList<Text> pagesList = new ArrayList<Text>();
        private ArrayList<Text> accessList = new ArrayList<Text>();
        private String joinType = null;

        private static final long FOURTEEN_DAYS_MIN = 20160; // 14 days in minutes
        private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        private final LocalDate now = LocalDate.now();

        public void setup(Context context) {
            // retrieve the type of join from our configuration
            joinType = context.getConfiguration().get("joinType");
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            // clear list
            pagesList.clear();
            accessList.clear();

            // bin each record from both mappers based on the tag letter "P" or "A". Then, remove the tag.
            while (values.iterator().hasNext()) {
                tag = values.iterator().next();
                if (tag.charAt(0) == 'P') {
                    pagesList.add(new Text(tag.toString().substring(1)));
                } else if (tag.charAt(0) == 'A') {
                    accessList.add(new Text(tag.toString().substring(1)));
                }
            }

            long mostRecentAccessMinutes = 0;
            for (Text access : accessList) {
                try {
                    LocalDateTime dateTime = LocalDateTime.parse(access.toString(), formatter);
                    long accessTime = dateTime.atZone(ZoneId.systemDefault()).toEpochSecond() / 60;
                    mostRecentAccessMinutes = Math.max(mostRecentAccessMinutes, accessTime);
                } catch (Exception e) {
                    System.err.println("Error parsing accessTime from: " + access.toString());
                }
            }

            long todayStartInMinutes = now.atStartOfDay(ZoneId.systemDefault()).toEpochSecond() / 60;
            long minutesBeforeToday = todayStartInMinutes - mostRecentAccessMinutes;
            String status = (minutesBeforeToday > FOURTEEN_DAYS_MIN) ? "isDisconnected" : "isConnected";

            executeJoinLogic(context, key, status);
        }

        private void executeJoinLogic(Context context, Text key, String status) throws IOException, InterruptedException {
            if (joinType.equalsIgnoreCase("inner")) {
                if (!pagesList.isEmpty() && status.equalsIgnoreCase("isDisconnected")) {
                    for (Text P : pagesList) {
                        context.write(key, P);
                    }
                }
            }
        }
    }

    public void debug(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task g");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv"),TextInputFormat.class, AccessLogsMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskG.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task g");
        job.setJarByClass(TaskG.class);
        job.setReducerClass(DisconnectedReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv"), TextInputFormat.class, PagesMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv"),TextInputFormat.class, AccessLogsMapper.class);
        job.getConfiguration().set("joinType", "inner");
        FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project1/outputTaskG.csv"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}