A tool for running sample workloads so that hprof files with various characteristics can be generated.

Run `./gradlew run` to see the available subcommands (one per type of heap contents). To specify a subcommand, use `--args`, as in:

```
./gradlew run --args='superclasses'
```
