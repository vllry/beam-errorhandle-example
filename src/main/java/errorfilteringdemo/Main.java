package errorfilteringdemo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;

public class Main {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        PCollection<String> rawAuditCollection = p.apply(
                "Read raw audit logs",
                TextIO.read().from("./audit.log")
        );

        Raw2Audit auditTransformer = new Raw2Audit();
        PCollectionTuple auditCollection = auditTransformer.transform(rawAuditCollection);

        auditCollection.get(auditTransformer.failures).apply(
                "WriteToText",
                TextIO.write().to("audit").withSuffix(".txt")
        );

        p.run();
    }

}
