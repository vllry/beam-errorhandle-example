Running
===

Dependencies
---

1. An IDE (optional, I suggest IntelliJ IDEA).
2. The Java SDK.
3. [Maven](https://maven.apache.org/) package manager (may be bundled with the IDE).
4. Run `mvn install` via the IDE or command line. This will fetch Beam and its dependencies.


Running
---

Run `mvn exec:java -Dexec.mainClass="errorfilteringdemo.Main"` to execute the pipeline. You should see results written to `./output`.


About
===
This is a simple demo of per-element error handling in Apache Beam pipelines. The demo is sparse, but highlights a real-world case of handling critical log streams. This allows for bad data or pipeline bugs to be noticed and handled, without disrupting other operations by crashing the pipeline.

I tried to comment the code well. You can also read more about this example [here](https://vallery.don't.forget.to.update.this)