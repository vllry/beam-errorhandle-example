package errorfilteringdemo;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;


public class Main {

    public static void main(String[] args) {

        Pipeline p = Pipeline.create(PipelineOptionsFactory.create());

        PCollection<String> rawAuditCollection = p.apply(
                "Read raw audit logs",
                TextIO.read().from("./input/audit.log")
        );

        Raw2Audit auditSet = new Raw2Audit(rawAuditCollection);

        /*auditCollection.get(auditTransformer.valid).apply(
                "WriteAudit",
                TextIO.write().to("output/audit").withSuffix(".txt")
        );*/

        auditSet.getFailed().apply(
                "WriteAuditFailures",
                TextIO.write().to("./output/failuresTag").withSuffix(".txt")
        );

        p.run();
    }

}
