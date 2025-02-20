import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KMeans {

    private static final Logger LOGGER = Logger.getLogger(KMeans.class.getName());

    public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> { //mapper
        private List<double[]> centroids; //centroid initialization

        @Override
        protected void setup(Context context) {
            centroids = loadCentroids();
        }

        @Override
        protected void map(Object key, Text value, Context context) {
            String[] tokens = value.toString().split(",");
            double[] point = new double[tokens.length];

            for (int i = 0; i < tokens.length; i++) {
                point[i] = Double.parseDouble(tokens[i]);
            }

            int closestCentroid = findClosestCentroid(point);

            try {
                context.write(new IntWritable(closestCentroid), value);
            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Error writing output in mapper", e);
            }
        }

        private int findClosestCentroid(double[] point) {
            int closest = 0;
            double minDistance = Double.MAX_VALUE;
            for (int i = 0; i < centroids.size(); i++) {
                double distance = calculateDistance(point, centroids.get(i));
                if (distance < minDistance) {
                    minDistance = distance;
                    closest = i;
                }
            }
            return closest;
        }

        private double calculateDistance(double[] point, double[] centroid) {
            double sum = 0;
            for (int i = 0; i < point.length; i++) {
                sum += Math.pow(point[i] - centroid[i], 2);
            }
            return Math.sqrt(sum);
        }

        private List<double[]> loadCentroids() {
            List<double[]> centroids = new ArrayList<>();
            Random rand = new Random();
            for (int i = 0; i < 3; i++) {
                double[] centroid = new double[2];
                for (int j = 0; j < 2; j++) {
                    centroid[j] = rand.nextDouble() * 100;
                }
                centroids.add(centroid);
            }
            return centroids;
        }
    } //generates random centroids

    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> { //reducer
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) {
            double[] sum = new double[2];
            int count = 0;

            for (Text value : values) {
                String[] tokens = value.toString().split(",");
                for (int i = 0; i < tokens.length; i++) {
                    sum[i] += Double.parseDouble(tokens[i]);
                }
                count++;
            }

            for (int i = 0; i < sum.length; i++) {
                sum[i] /= count;
            }

            try {
                context.write(key, new Text(sum[0] + "," + sum[1]));
            } catch (IOException | InterruptedException e) {
                LOGGER.log(Level.SEVERE, "Error writing output in reducer", e);
            }
        }
    }

    public static void main(String[] args) throws Exception { //driver
        Configuration conf = new Configuration();
        int R = 6;

        for (int i = 0; i < R; i++) {
            Job job = Job.getInstance(conf, "KMeans Iteration " + (i + 1));
            job.setJarByClass(KMeans.class);
            job.setMapperClass(KMeansMapper.class);
            job.setReducerClass(KMeansReducer.class);

            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class); //iterations

            FileInputFormat.addInputPath(job, new Path("C:/Users/19788/Desktop/cs4433_projects/project2/input_data/dataset_file.csv"));
            FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/output_multi" + "/iteration_" + (i + 1)));

            if (!job.waitForCompletion(true)) {
                LOGGER.severe("Job failed at iteration " + (i + 1));
                System.exit(1);
            }
        }
    }
}
