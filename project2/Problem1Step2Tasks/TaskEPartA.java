import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// ***** TASK E, part a *****
// Return only cluster centers along with an indication if convergence has been
// reached (yes – it has converged, or, no – it has not yet converged).

public class TaskEPartA {

    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text> {
        private Map<Double, Double> centroidMap = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            centroidMap.clear(); // Clear to avoid any stale data
            Path centroidsPath = new Path(context.getConfiguration().get("centroids.path"));
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] split = line.split("\t");
                String[] tokens = split[0].split(",");
                double x = Double.parseDouble(tokens[0]);
                double y = Double.parseDouble(tokens[1]);
                centroidMap.put(x, y);
            }
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tokens = value.toString().split(",");
            double x = Double.parseDouble(tokens[0]);
            double y = Double.parseDouble(tokens[1]);

            double closestX = 0, closestY = 0;
            double minDistance = Double.MAX_VALUE;
            for (Map.Entry<Double, Double> point : centroidMap.entrySet()) {
                double centroidX = point.getKey();
                double centroidY = point.getValue();
                double distance = euclideanDistance(x, y, centroidX, centroidY);
                if (distance < minDistance) {
                    minDistance = distance;
                    closestX = centroidX;
                    closestY = centroidY;
                }
            }

            String centroidKey = closestX + "," + closestY;
            String pointKey = x + ":" + y;

            context.write(new Text(centroidKey), new Text(pointKey));
        }
    }

    public static class KMeansCombiner extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<String> points = new ArrayList<>();
            double Xsum = 0;
            double Ysum = 0;
            double count = 0;

            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                if (tokens.length == 2) {
                    double x = Double.parseDouble(tokens[0]);
                    double y = Double.parseDouble(tokens[1]);
                    Xsum += x;
                    Ysum += y;
                    count++;
                    points.add(x + ":" + y);
                }
            }

            context.write(key, new Text("S" + Xsum + "," + Ysum + "," + count));

            for (String point : points) {
                context.write(key, new Text("P" + point));
            }
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {

        private Text tag = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Map<Double, Double> centroidMap = new HashMap<>();
            double Xsum = 0;
            double Ysum = 0;
            double count = 0;

            while (values.iterator().hasNext()) {
                tag = values.iterator().next();
                if (tag.charAt(0) == 'S') {
                    String[] tokens = tag.toString().substring(1).split(",");
                    Xsum += Double.parseDouble(tokens[0]);
                    Ysum += Double.parseDouble(tokens[1]);
                    count += Double.parseDouble(tokens[2]);
                } else if (tag.charAt(0) == 'P') {
                    String[] tokens = tag.toString().substring(1).split(":");
                    centroidMap.put(Double.parseDouble(tokens[0]), Double.parseDouble(tokens[1]));
                }
            }

            double Xavg = Xsum / count;
            double Yavg = Ysum / count;

            context.write(new Text(Xavg + "," + Yavg), new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int maxIterations = 20;
        boolean converged = false;
        int iteration = 0;

        while (iteration < maxIterations && !converged) {
            conf.set("centroids.path", "/Users/kyliehoar/IdeaProjects/cs4433_project2/centroids_partE-a/iteration-" + iteration + "/part-r-00000");
            Job job = Job.getInstance(conf, "Task d" + iteration);
            job.setJarByClass(TaskD.class);
            job.setMapperClass(TaskD.KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class);
            job.setReducerClass(TaskD.KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project2/dataset_file.csv"));
//            FileInputFormat.addInputPath(job, new Path(args[1]));
            FileOutputFormat.setOutputPath(job, new Path("/Users/kyliehoar/IdeaProjects/cs4433_project2/centroids_partE-a/iteration-" + (iteration + 1)));
//            FileOutputFormat.setOutputPath(job, new Path("/user/cs4433/project2/centroids_partD/iteration-" + (iteration + 1)));
            job.waitForCompletion(true);

//            converged = checkConvergence("/user/cs4433/project2/centroids_partD/iteration-" + iteration + "/part-r-00000", "/user/cs4433/project2/centroids_partD/iteration-" + (iteration + 1) + "/part-r-00000", threshold, maxShift, conf);
            converged = checkConvergence("/Users/kyliehoar/IdeaProjects/cs4433_project2/centroids_partE-a/iteration-" + iteration + "/part-r-00000", "/Users/kyliehoar/IdeaProjects/cs4433_project2/centroids_partE-a/iteration-" + (iteration + 1) + "/part-r-00000", conf);
            if (iteration != 0 && !converged) {
                System.out.println("Iteration " + iteration + " did not converge. Starting iteration " + (iteration + 1));
            }
            if (iteration != 0 && converged) {
                System.out.println("Iteration " + iteration + " did converge. Ending iterations.");
            }
            iteration++;
        }
    }

    private static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
    }

    private static Queue<Double> maxShiftHistory = new LinkedList<>();
    private static final int STABILITY_WINDOW = 5; // Number of iterations to check
    private static final double VARIANCE_THRESHOLD = 5.0; // Allowable fluctuation in shifts

    private static boolean checkConvergence(String oldPath, String newPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(new Path(oldPath))));
        BufferedReader newReader = new BufferedReader(new InputStreamReader(fs.open(new Path(newPath))));
        String oldLine, newLine;

        Map<Double, Double> oldCentroids = new HashMap<>();
        double maxShift = 0;

        while ((oldLine = oldReader.readLine()) != null) {
            String[] oldParts = oldLine.split("\t");
            String[] oldCentroidParts = oldParts[0].split(",");
            oldCentroids.put(Double.parseDouble(oldCentroidParts[0]), Double.parseDouble(oldCentroidParts[1]));
        }

        while ((newLine = newReader.readLine()) != null) {
            String[] newParts = newLine.split("\t");
            String[] centroidParts = newParts[0].split(",");

            double newX = Double.parseDouble(centroidParts[0]);
            double newY = Double.parseDouble(centroidParts[1]);

            for (Map.Entry<Double, Double> point : oldCentroids.entrySet()) {
                double minDistance = Double.MAX_VALUE;
                String bestMatch = null;

                double dist = euclideanDistance(newX, newY, point.getKey(), point.getValue());
                if (dist < minDistance) {
                    minDistance = dist;
                    bestMatch = point.getKey() + "," + point.getValue();
                }

                if (bestMatch != null) {
                    maxShift = Math.max(maxShift, minDistance);
                }
            }
        }
        oldReader.close();
        newReader.close();

        // Track max shift history
        if (maxShiftHistory.size() >= STABILITY_WINDOW) {
            maxShiftHistory.poll(); // Remove oldest value
        }
        maxShiftHistory.offer(maxShift); // Add new value

        // Check variance in last STABILITY_WINDOW shifts
        if (maxShiftHistory.size() >= STABILITY_WINDOW) {
            double mean = maxShiftHistory.stream().mapToDouble(Double::doubleValue).sum() / STABILITY_WINDOW;
            double variance = maxShiftHistory.stream()
                    .mapToDouble(x -> Math.pow(x - mean, 2))
                    .sum() / STABILITY_WINDOW;

            if (variance < VARIANCE_THRESHOLD) {
                return true;
            }
        }

        return false;
    }
}