package org.apache.beam.examples;

import org.apache.beam.examples.common.ExampleUtils;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.transforms.Top;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;



public class TopPackages {

    private static final String inputFile = "*.java";
    private static final String outputFile = "topcounts";
    private static final String searchTerm = "import";

    // DoFn for extracting package names from each import statement
    static class extractPackagesFn extends DoFn<String,String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            // initialize an arraylist to store package names
            List<String> listOfPackages = new ArrayList<String>();
            //split the import statement and take only the name of package
            String[] arrOfLines = element.split(" ",2);
            element = arrOfLines[arrOfLines.length-1];
            //below loop splits the package name with 'period' in each iteration and builds the name of package by appending with existing elements in the list i.e. org.apache.beam.sdk becomes org, apache.beam.sdk and org goes to listOfPackages, then apache, beam.sdk and now listOfPackages becomes [org,org.apache] and so on.
            while (element.contains(".")) {
                String[] tmp = element.split("\\.",2);
                if (!(listOfPackages.size()==0)) {
                    listOfPackages.add(listOfPackages.get(listOfPackages.size()-1)+"."+tmp[0]);
                } else {
                    listOfPackages.add(tmp[0]);
                }
                element = tmp[tmp.length-1];
            }
            // convert to object
            Object[] objects = listOfPackages.toArray();
            //convert to string array
            String[] arrOfPackages = Arrays.copyOf(objects, objects.length, String[].class);
            // return arrOfPackages;
            // output each package name from listOfPackages
            for (String pkg : arrOfPackages) {
                if (!pkg.isEmpty()) {
                    receiver.output(pkg);
                }
            }
        }
    }

    // first extract the import statements, then extract package names, then count the packages
    public static class CountPackages extends PTransform<PCollection<String>,PCollection<KV<String,Long>>> {
        @Override
        public PCollection<KV<String,Long>> expand(PCollection<String> lines) {
            PCollection<String> outputlines = lines.apply(ParDo.of(new grepLogicFn()));
            PCollection<String> pkgs = outputlines.apply(ParDo.of(new extractPackagesFn()));
            PCollection<KV<String,Long>> packageCounts = pkgs.apply(Count.perElement());
            return packageCounts;
        }
    }

    // format as "k, v" pairs to write into file
    static class TopFormatAsTextFn extends DoFn<List<KV<String, Long>>,String> {
        @ProcessElement
        public void processElement(@Element List<KV<String, Long>> element, OutputReceiver<String> receiver) {
            StringBuffer sb = new StringBuffer();
            for (KV<String, Long> kv : element) {
                sb.append(kv.getKey()+","+kv.getValue()+'\n');
            }
            receiver.output(sb.toString());
        }
    }

    //DoFn for grep
    static class grepLogicFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            if (element.contains(searchTerm)) {
                receiver.output(element);
            }
        }
    }

    // build the pipeline
    public static void runTopPackages(PipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        p.apply("Read lines", TextIO.read().from(inputFile))
            .apply("Count'em", new CountPackages())
            .apply("Take top 5", Top.of(5,new KV.OrderByValue<>()))
            .apply("Format as k,v", ParDo.of(new TopFormatAsTextFn()))
            .apply("Write into file", TextIO.write().to(outputFile).withSuffix(".txt").withoutSharding());
        p.run().waitUntilFinish();
    }

    //
    public static void main(String[] args) {
        //
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        // execute the pipeline
        runTopPackages(options);
    }
}