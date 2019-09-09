# beam-scala-examples
Apache Beam examples with Scala using official Java API, and it's different from using [Scio](https://github.com/spotify/scio) API.

# Java Interoperability Issues

## `Filter.by` couldn't pick the most specific method
```
> Task :compileScala
/beam-scala-examples/src/main/scala/org/apache/beam/examples/MinimalWordCount.scala:23: overloaded method value by with alternatives:
  [T, PredicateT <: org.apache.beam.sdk.transforms.SerializableFunction[T,Boolean]](predicate: PredicateT)org.apache.beam.sdk.transforms.Filter[T] <and>
  [T, PredicateT <: org.apache.beam.sdk.transforms.ProcessFunction[T,Boolean]](predicate: PredicateT)org.apache.beam.sdk.transforms.Filter[T]
 cannot be applied to (org.apache.beam.sdk.transforms.SimpleFunction[String,Boolean])
      .apply(Filter.by(filterNotEmpty))
```

### Workaround
* Create a static Java method to provide the required filter (ugh)
* Spin our own implementation of `PTransform` via `expand` method [WIP]



# License
Licensed under the Apache License, Version 2.0: http://www.apache.org/licenses/LICENSE-2.0
