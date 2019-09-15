# beam-scala-examples
Apache Beam examples with Scala using official Java API, and it's different from
using [Scio](https://github.com/spotify/scio) API.

# Progress
## Word Count
- [x] Minimal Word Count
- [x] Word Count
- [x] Debugging Word Count
- [x] Windowed Word Count
## Complete
## Cookbook
- [x] BigQueryTornadoes
- [x] CombinePerKeyExamples
- [x] DistinctExample
- [x] FilterExamples
- [ ] JoinExamples
- [ ] MaxPerKeyExamples
- [ ] TriggerExample
## Snippets
## Subprocess

# Build
Clean up code formatting, treat warnings as errors and build the FatJar
```
$ gradle spotlessApply runtimeJar
```

# Run
## Word Count
### Minimal Word Count
```
$ java -cp build/libs/beam-scala-examples-runtime-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.MinimalWordCount
```

### Word Count
```
$ java -cp build/libs/beam-scala-examples-runtime-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.WordCount \
    --output=/tmp/wordcount
```

### Debugging Word Count
```
$ java -cp ./build/libs/beam-scala-examples-runtime-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.DebuggingWordCount \
    --filterPattern="Florish|stomach" \
    --output=/tmp/bogus-donot-need
```

### Windowed Word Count
```
$ java -cp ./build/libs/beam-scala-examples-runtime-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.WindowedWordCount \
    --windowSize=10 \
    --output=/tmp/wordcount-windowed
```


# License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
