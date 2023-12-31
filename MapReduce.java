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

public class Assignment1 {

        private static String maxMonth = "";
        private static double maxCost = 0.0;

        public static class Trans1Mapper extends Mapper <Object, Text, Text, Text> {
                 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                         String record = value.toString();
                         String[] parts = record.split(",");
                         // Date parts[1]
                         // CustomerId parts[2]
                         // Cost parts[3]
                         String date = parts[1].trim();
//                       String month = date.substring(0, 3);
                         context.write(new Text(date.substring(0, 3)), new Text(parts[2].trim() + "---" + parts[3].trim()));
                 }
         }

        public static class Trans2Mapper extends Mapper <Object, Text, Text, Text> {
                 public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                         String record = value.toString();
                         String[] parts = record.split(",");
                         // Date parts[1]
                         // CustomerId parts[2]
                         // Cost parts[3]
                         String date = parts[1];
//                       String month = date.substring(0, 3);
                         context.write(new Text(date.substring(0, 3)), new Text(parts[2] + "---" + parts[3]));
                 }
         }

        public static class ReduceJoinReducer extends Reducer <Text, Text, Text, Text> {

                public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                         double total = 0.0;
                         List<String> ids = new ArrayList<String>();
                         for (Text t : values) {
                                 String parts[] = t.toString().split("---");
                                 String id = parts[0].trim();
                                 String costStr = parts[1].trim();

                                 if (costStr != null && costStr != "") {
                                         total += Float.parseFloat(costStr);
                                 }

                                 if (!ids.contains(id)) {
                                         ids.add(id);
                                 }
                         }

                         if (maxMonth == "" || total > maxCost) {
                                 maxCost = total;
                                 maxMonth = key.toString();
                         }

                         context.write(key, new Text(String.format("%d   %f", ids.size(), total)));
                 }

                 @Override
                 protected void cleanup(Context context) throws IOException, InterruptedException {
                         context.write(new Text(String.format("%s is the month with highest cost", maxMonth)), new Text(String.format("(%f)", maxCost)));

                 }
         }



        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
                // TODO Auto-generated method stub
                Configuration conf = new Configuration();
                 Job job = new Job(conf, "Reduce-side join");
                 job.setJarByClass(Assignment1.class);
                 job.setReducerClass(ReduceJoinReducer.class);
                 job.setOutputKeyClass(Text.class);
                 job.setOutputValueClass(Text.class);

                 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, Trans1Mapper.class);
                 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, Trans2Mapper.class);
                 Path outputPath = new Path(args[2]);

                 FileOutputFormat.setOutputPath(job, outputPath);
                 outputPath.getFileSystem(conf).delete(outputPath);
                 System.exit(job.waitForCompletion(true) ? 0 : 1);

        }

}
