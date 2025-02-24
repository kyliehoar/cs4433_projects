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

// ***** TASK D *****
// An optimized multi-iteration K-Means algorithm that utilizes a combiner to reduce
// data transfer between mappers and reducers. This version ensures early stopping if
// centroids stabilize, but will run for at most R iterations.

public class TaskD_experiments {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> {
        private Map<Double, Double> centroidMap = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            centroidMap.clear();
            Path centroidsPath = new Path(context.getConfiguration().get("centroids.path"));
            FileSystem fs = FileSystem.get(context.getConfiguration());

            if (!fs.exists(centroidsPath)) {
                System.err.println("Centroid file missing: " + centroidsPath);
                return;
            }

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
            double Xsum = 0, Ysum = 0, count = 0;
            for (Text value : values) {
                String[] tokens = value.toString().split(":");
                Xsum += Double.parseDouble(tokens[0]);
                Ysum += Double.parseDouble(tokens[1]);
                count++;
            }
            context.write(key, new Text(Xsum + "," + Ysum + "," + count));
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double Xsum = 0, Ysum = 0, count = 0;

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                Xsum += Double.parseDouble(tokens[0]);
                Ysum += Double.parseDouble(tokens[1]);
                count += Double.parseDouble(tokens[2]);
            }

            double Xavg = Xsum / count;
            double Yavg = Ysum / count;

            context.write(new Text(Xavg + "," + Yavg), new Text());
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: java TaskD <K> <R>");
            System.exit(1);
        }

        int K = Integer.parseInt(args[0]);
        int maxIterations = Integer.parseInt(args[1]);
        boolean converged = false;
        int iteration = 0;

        Configuration conf = new Configuration();
        conf.setInt("kmeans.k", K);

        long startTime = System.currentTimeMillis(); // Track total execution time

        while (iteration < maxIterations && !converged) {
            conf.set("centroids.path", "centroids_partD/iteration-" + iteration + "/part-r-00000");

            System.out.println("Running iteration " + iteration + " for K=" + K);

            Job job = Job.getInstance(conf, "TaskD K=" + K + " Iteration=" + iteration);
            job.setJarByClass(TaskD_experiments.class);
            job.setMapperClass(KMeansMapper.class);
            job.setCombinerClass(KMeansCombiner.class); // Optimized with combiner
            job.setReducerClass(KMeansReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, new Path("dataset_file.csv"));
            Path outputPath = new Path("centroids_partD/iteration-" + (iteration + 1));

            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            FileOutputFormat.setOutputPath(job, outputPath);
            long iterStartTime = System.currentTimeMillis();
            job.waitForCompletion(true);
            long iterEndTime = System.currentTimeMillis();

            System.out.println("Iteration " + iteration + " took " + (iterEndTime - iterStartTime) + " ms");

            converged = checkConvergence("centroids_partD/iteration-" + iteration + "/part-r-00000",
                    "centroids_partD/iteration-" + (iteration + 1) + "/part-r-00000",
                    conf);
            iteration++;
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Total time for K=" + K + ", R=" + maxIterations + ": " + (endTime - startTime) + " ms");
    }

    private static double euclideanDistance(double x1, double y1, double x2, double y2) {
        return Math.sqrt(Math.pow(x2 - x1, 2) + Math.pow(y2 - y1, 2));
    }

    private static boolean checkConvergence(String oldPath, String newPath, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        Path oldCentroidPath = new Path(oldPath);

        if (!fs.exists(oldCentroidPath)) {
            System.err.println("Centroid file missing: " + oldPath);
            return false;
        }

        BufferedReader oldReader = new BufferedReader(new InputStreamReader(fs.open(oldCentroidPath)));
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

            for (Map.Entry<Double, Double> entry : oldCentroids.entrySet()) {
                double dist = euclideanDistance(newX, newY, entry.getKey(), entry.getValue());
                maxShift = Math.max(maxShift, dist);
            }
        }

        oldReader.close();
        newReader.close();

        return maxShift < 0.01;
    }
}
