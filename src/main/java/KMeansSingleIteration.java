import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansSingleIteration {

    public static class KMeansMapper extends Mapper<Object, Text, Text, Text> { //mapper
        private final List<Point> centroids = new ArrayList<>(); //declaring list for centroids

        @Override
        protected void setup(Context context) throws IOException {
            //Load initial centroids from seeds.csv using FileSystem
            Path seedsPath = new Path("file:///C:/Users/19788/Desktop/cs4433_projects/project2/input_data/seeds.csv");
            FileSystem fs = FileSystem.get(context.getConfiguration());

            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(seedsPath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split(",");
                    if (parts.length == 2) {
                        try {
                            double x = Double.parseDouble(parts[0].trim());
                            double y = Double.parseDouble(parts[1].trim());
                            centroids.add(new Point(x, y));
                        } catch (NumberFormatException e) {
                            System.err.println("Skipping invalid centroid line: " + line);
                        }
                    }
                } //reading and storing the centroids
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            Point point = new Point(value.toString());
            String nearestCentroid = findNearestCentroid(point);
            context.write(new Text(nearestCentroid), new Text(point.toString()));
        }

        private String findNearestCentroid(Point point) {
            double minDistance = Double.MAX_VALUE;
            String nearestCentroid = null;

            for (Point centroid : centroids) {
                double distance = point.calculateDistance(centroid);
                if (distance < minDistance) {
                    minDistance = distance;
                    nearestCentroid = centroid.toString();
                }
            }

            return nearestCentroid;
        }
    }

    public static class KMeansReducer extends Reducer<Text, Text, Text, Text> { //reducer
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Point> points = new ArrayList<>();
            for (Text val : values) {
                points.add(new Point(val.toString()));
            }

            Point newCentroid = calculateNewCentroid(points);
            context.write(key, new Text(newCentroid.toString()));
        }

        private Point calculateNewCentroid(List<Point> points) {
            double sumX = 0, sumY = 0;

            for (Point point : points) {
                sumX += point.x;
                sumY += point.y;
            }

            double newX = sumX / points.size();
            double newY = sumY / points.size();

            return new Point(newX, newY);
        }
    } //calculating the new centroid

    public static class Point {
        double x, y;

        public Point(String data) {
            String[] parts = data.split(",");
            try {
                this.x = Double.parseDouble(parts[0].trim());
                this.y = Double.parseDouble(parts[1].trim());
            } catch (NumberFormatException e) {
                System.err.println("Skipping invalid point: " + data);
                this.x = 0;
                this.y = 0;
            }
        }

        public Point(double x, double y) {
            this.x = x;
            this.y = y;
        }

        public double calculateDistance(Point other) {
            return Math.sqrt(Math.pow(this.x - other.x, 2) + Math.pow(this.y - other.y, 2));
        }

        @Override
        public String toString() {
            return this.x + "," + this.y;
        }
    }

    public static void main(String[] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(KMeansSingleIteration.class);
        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Specify input and output paths
        FileInputFormat.addInputPath(job, new Path("file:///C:/Users/19788/Desktop/cs4433_projects/project2/input_data/dataset_file.csv"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/project2/output_single")); // Output Path

        // Run job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
