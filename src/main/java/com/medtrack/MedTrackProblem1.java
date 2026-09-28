package com.medtrack;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MedTrackProblem1 {

    public static class AdmissionMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final Map<String, String> departmentMap = new HashMap<>();
        private final Text departmentName = new Text();
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void setup(Context context)
                throws IOException {

            URI[] cacheFiles = context.getCacheFiles();

            if (cacheFiles != null) {
                for (URI uri : cacheFiles) {

                    try (BufferedReader reader =
                                 new BufferedReader(
                                         new FileReader("departments.csv"))) {

                        String line;

                        // Skip header
                        reader.readLine();

                        while ((line = reader.readLine()) != null) {

                            String[] fields = line.split(",");

                            if (fields.length >= 2) {
                                String deptCode = fields[0].trim();
                                String deptName = fields[1].trim();

                                departmentMap.put(deptCode, deptName);
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // Skip header
            if (line.startsWith("admissionId")) {
                return;
            }

            String[] fields = line.split(",");

            if (fields.length >= 3) {

                String deptCode = fields[2].trim();

                String deptName = departmentMap.get(deptCode);

                if (deptName != null) {
                    departmentName.set(deptName);
                    context.write(departmentName, ONE);
                }
            }
        }
    }

    public static class DepartmentReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key,
                               Iterable<IntWritable> values,
                               Context context)
                throws IOException, InterruptedException {

            int total = 0;

            for (IntWritable value : values) {
                total += value.get();
            }

            result.set(total);
            context.write(key, result);
        }
    }

    public static void main(String[] args)
            throws Exception {

        if (args.length != 3) {
            System.err.println(
                    "Usage: MedTrackProblem1 <input> <departments> <output>");
            System.exit(2);
        }

        Configuration conf = new Configuration();

        Job job = Job.getInstance(
                conf,
                "MedTrack Hospital Admissions Analytics");

        job.setJarByClass(MedTrackProblem1.class);

        job.setMapperClass(AdmissionMapper.class);
        job.setReducerClass(DepartmentReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Map-side Join: departments.csv goes to Distributed Cache
        job.addCacheFile(
                new URI(args[1] + "#departments.csv"));

        FileInputFormat.addInputPath(
                job,
                new Path(args[0]));

        FileOutputFormat.setOutputPath(
                job,
                new Path(args[2]));

        System.exit(
                job.waitForCompletion(true) ? 0 : 1);
    }
}