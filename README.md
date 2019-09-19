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

- [x] AutoComplete
- [x] StreamingWordExtract
- [ ] TfIdf
- [ ] TopWikipediaSessions
- [ ] TrafficMaxLaneFlow
- [ ] TrafficRoutes
## Complete/Game

## Cookbook

- [x] BigQueryTornadoes
- [x] CombinePerKeyExamples
- [x] DistinctExample
- [x] FilterExamples
- [x] JoinExamples
- [x] MaxPerKeyExamples
- [ ] TriggerExample

## Snippets

## Subprocess

# Build

Clean up code formatting, treat warnings as errors and build the Assembly Jar

```
$ gradle licenseFormatMain spotlessApply assemblyJar
```

# Run

```
./gradlew exec \
    --PmainClass=org.apache.beam.examples.scala.WordCount \
    --args='--output=/tmp/wc/wc'
```

## Word Count

### Minimal Word Count

```
$ java -cp build/libs/beam-scala-examples-assembly-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.MinimalWordCount
```

### Word Count

```
$ java -cp build/libs/beam-scala-examples-assembly-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.WordCount \
    --output=/tmp/wordcount
```

### Debugging Word Count

```
$ java -cp ./build/libs/beam-scala-examples-assembly-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.DebuggingWordCount \
    --filterPattern="Florish|stomach" \
    --output=/tmp/bogus-donot-need
```

### Windowed Word Count

```
$ java -cp ./build/libs/beam-scala-examples-assembly-0.1.0-SNAPSHOT.jar \
    org.apache.beam.examples.scala.WindowedWordCount \
    --windowSize=10 \
    --output=/tmp/wordcount-windowed
```

# Local Dev

## Per work space settings with `coc.nvim` and `scalameta/metals` LSP

- `plugins {}` and version properties doesn't work with gradle 5.3.1 that metals
   was bundled with for bootstrapping
- Metals `.jvmopts` and `.sbtopts` per workspace doesn't work nicely with
  `coc.nvim`. Have to modify server settings directly

Both of the above changes requires modification for global settings. However,
`coc.nvim` provide configuration file resolved per workspace. Create `.vim`
folder with `coc-settings.json` file for per workspace configuration (point to
latest gradle version + `scalafmtConfigPath`) without polluting global settings.

```
{
  "languageserver": {
    "metals": {
      "command": "metals-vim",
      "rootPatterns": ["build.sbt", "build.sc", "build.gradle"],
      "filetypes": ["scala", "sbt", "gradle"],
      "settings": {
        "metals": {
          "gradleScript": "/usr/bin/gradle or /home/user/.sdkman/...",
          "scalafmtConfigPath": "codequality/scalafmt.conf"
        }
      }
    }
  }
}
```

# License

Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
