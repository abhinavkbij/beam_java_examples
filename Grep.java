// importing packages
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


public class Grep {
    //
    private static String inputFile = "*.java";
    private static String outputFile = "grepJavaOutput";
    private static String searchTerm = "import";

    static class grepLogicFn extends DoFn<String, String> {
        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            if (element.contains(searchTerm)) {
                receiver.output(element);
            }
        }
    }

    static class applyGrep extends PTransform<PCollection<String>, PCollection<String>> {
        @Override
        public PCollection<String> expand(PCollection<String> lines) {
            PCollection<String> outputlines = lines.apply(ParDo.of(new grepLogicFn()));
            return outputlines;
        }
    }

    static void runGrep(PipelineOptions options) {
        Pipeline p = Pipeline.create(options);
        p.apply("Read Lines", TextIO.read().from(inputFile))
            .apply("Apply grep logic", new applyGrep())
            .apply("Write to text files", TextIO.write().to(outputFile).withSuffix(".txt").withoutSharding());

        p.run().waitUntilFinish();
    }



    public static void main (String[] args) {
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().create();
        runGrep(options);
    }
}
