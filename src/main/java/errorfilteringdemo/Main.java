package errorfilteringdemo;

import errorfilteringdemo.datum.Audit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;


public class Main {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        PCollection<String> rawAuditCollection = p.apply(
                "Read raw audit logs",
                TextIO.read().from("./input/audit.log")
        );

        PCollectionTuple auditdCollections = Raw2Audit.process(rawAuditCollection);
        PCollection<Audit> auditCollection = auditdCollections.get(Raw2Audit.validTag);
        PCollection<String> auditFailures = auditdCollections.get(Raw2Audit.failuresTag);

        PCollection<String> auditStrings = auditCollection.apply(
                "Audit objects to string",
                ToString.elements()
        );

        auditStrings.apply(
                    "WriteAudit",
                    TextIO.write().to("./output/audit").withSuffix(".txt")
        );

        auditFailures.apply(
                "WriteAuditFailures",
                TextIO.write().to("./output/failuresTag").withSuffix(".txt")
        );

        p.run();
    }

}
