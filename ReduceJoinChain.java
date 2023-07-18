import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class ReduceJoinChain {

        private static double avgMin = 0.0;
        private static List<String> gameTypesAvgMin = new ArrayList<String>();

        // Có thể đổi tên hàm CustsMapper         
        public static class CustsMapper extends Mapper <Object, Text, Text, Text> {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String record = value.toString();
                        String[] parts = record.split(",");
                        context.write(new Text(parts[0]), new Text("age   " + parts[3]));
                }
        }

        // Có thể đổi tên hàm TxnsMapper
        public static class TxnsMapper extends Mapper <Object, Text, Text, Text> {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String record = value.toString();
                        String[] parts = record.split(",");
                        context.write(new Text(parts[2]), new Text("type   " + parts[4]));
                }
        }

        // Có thể đổi tên hàm ReduceJoinReducer
        public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text> {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        String age = "";
                        String types = "";
                        for (Text t : values) {
                                String parts[] = t.toString().split("   ");
                                if (parts[0].equals("type")) {
                                        types = types + parts[1] + ",";
                                } else if (parts[0].equals("age")) {
                                        age = parts[1];
                                }
                        }
                        context.write(new Text(age), new Text(types));
                }
        }

        // Có thể đổi tên hàm GameTypeStatMapper
        public static class GameTypeStatMapper extends Mapper <Object, Text, Text, Text> {
                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        String record = value.toString();
                        String[] parts = record.split("        ");
                        String[] types = parts[1].split(",");
                        for (String type : types) {
                                context.write(new Text(type.toString().trim()), new Text(parts[0].toString()));
                        }
                }
        }

        // Có thể đổi tên hàm GameTypeStatReducer
        public static class GameTypeStatReducer extends Reducer <Text, Text, Text, Text> {
                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                        int count = 0;
                        int min = 0;
                        int max = 0;
                        int total = 0;
                        double avg = 0.0;

                        for (Text v : values) {
                                count++;
                                int age = Integer.parseInt(v.toString());

                                if (age > max) {
                                        max = age;
                                }

                                if (min == 0 || age < min) {
                                        min = age;
                                }

                                total += age;
                        }

                        if (count != 0) {
                                avg = total/count;
                        }

                        if (avgMin == 0.0 || avg < avgMin) {
                                avgMin = avg;
                                gameTypesAvgMin = new ArrayList<String>();
                                gameTypesAvgMin.add(key.toString());
                        }

                        if (avg == avgMin) {
                                gameTypesAvgMin.add(key.toString());
                        }

                        String str = String.format("[Min: %d Max: %d Avg: %.1f]", min, max, avg);
                        context.write(key, new Text(str));
                }

                @Override
                protected void cleanup(Context context) throws IOException, InterruptedException {
                        List<String> types = new ArrayList<String>();
                        for (String type : gameTypesAvgMin) {
                                if (!types.contains(type)) {
                                        types.add(type);
                                }
                        }
                        context.write(new Text("Game Types with lowest average age of "), new Text(String.format("%.1f: %s", avgMin, StringUtils.join(types, ", "))));
                }
        }

        public static void main(String[] args) throws Exception {

                // job 1
                Configuration conf = new Configuration();
                Job job = new Job(conf, "Reduce-side join job 1");
                job.setJarByClass(ReduceJoinChain.class);
                job.setReducerClass(ReduceJoinReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, CustsMapper.class);
                MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, TxnsMapper.class);
                Path outputPath = new Path(args[2]);

                FileOutputFormat.setOutputPath(job, outputPath);
                outputPath.getFileSystem(conf).delete(outputPath);
                job.waitForCompletion(true);

                // job 2
                Configuration conf2 = new Configuration();
                Job job2 = new Job(conf2, "Reduce-side join job 2");
                job2.setJarByClass(ReduceJoinChain.class);
                job2.setMapperClass(GameTypeStatMapper.class);
                job2.setReducerClass(GameTypeStatReducer.class);
                job2.setOutputKeyClass(Text.class);
                job2.setOutputValueClass(Text.class);
                job2.setNumReduceTasks(1);
                FileInputFormat.addInputPath(job2, new Path(args[2]));
                FileOutputFormat.setOutputPath(job2, new Path(args[3]));
                System.exit(job2.waitForCompletion(true) ? 0 : 1);

        }

}
