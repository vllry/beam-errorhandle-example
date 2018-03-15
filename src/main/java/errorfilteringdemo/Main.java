package errorfilteringdemo;

import errorfilteringdemo.datum.Audit;
import errorfilteringdemo.datum.Failure;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;


public class Main {

    public static void main(String[] args) {

        // Create the pipeline.
        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        PCollection<String> rawAuditCollection = p.apply(
                "Read raw auditd logs",
                TextIO.read().from("./input/audit.log")
        );

        PCollectionTuple auditdCollections = Raw2Audit.process(rawAuditCollection);
        PCollection<Audit> auditCollection = auditdCollections.get(Raw2Audit.validTag);
        PCollection<Failure> auditFailures = auditdCollections.get(Raw2Audit.failuresTag);



        // Write out results to files - normally done to a message queue or DB.

        auditCollection.apply(ToString.elements()).apply(
                    "Write Audit",
                    TextIO.write().to("./output/audit").withSuffix(".txt")
        );

        auditFailures.apply(ToString.elements()).apply(
                "Write Audit Failures",
                TextIO.write().to("./output/failuresTag").withSuffix(".txt")
        );



        // Start the pipeline. All other actions are essentially the build the pipeline definition.
        p.run();
    }

}
