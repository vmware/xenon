Brief intro to how to use btrace with xenon tests

* Download btrace from [https://github.com/btraceio/btrace/releases]

* Extract to local directory

* Create a btrace script like described in https://github.com/btraceio/btrace/wiki/Trace-Scripts

* Compile the script:
```bash
/home/jvassev/programs/btrace/bin/btracec MyScript.java
```

A MyScript.class will be created in the current directory

* Start your test like this. Make sure to specify the correct path to `btrace-agent.jar`

```bash
./mvn test -Dtest=MyTest#myMethod \
   -DskipAnalysis=true \
   -DtestJvmOpts="-javaagent:/home/jvassev/programs/btrace/btrace-agent.jar=script=/path/to/MyScript.clas,noServer=true"

```

* The output of the trace is in the current directory and is named similar to {SCRIPT}.java-.default.{TIMESTAMP}.btrace
