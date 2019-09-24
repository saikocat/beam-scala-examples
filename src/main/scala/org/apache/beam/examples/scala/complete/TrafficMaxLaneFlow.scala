package org.apache.beam.examples.scala.complete

/**
  * A Beam Example that runs in both batch and streaming modes with traffic sensor data. You can
  * configure the running mode by setting --streaming to true or false.
  *
  * Concepts: The batch and streaming runners, sliding windows, use of the AvroCoder to encode a
  * custom class, and custom Combine transforms.
  *
  * This example analyzes traffic sensor data using SlidingWindows. For each window, it finds the
  * lane that had the highest flow recorded, for each sensor station. It writes those max values
  * along with auxiliary info to a BigQuery table.
  *
  * The pipeline reads traffic sensor data from  --inputFile.
  *
  * The example is configured to use the default BigQuery table from the example common package
  * (there are no defaults for a general Beam pipeline). You can override them by using the
  * --bigQueryDataset, and --bigQueryTable options. If the BigQuery table do not exist,
  * the example will try to create them.
  *
  * The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
  * and then exits.
  */
object TrafficMaxLaneFlow {
  final val WINDOW_DURATION = 60 // Default sliding window duration in minutes
  final val WINDOW_SLIDE_EVERY = 5 // Default window 'slide every' setting in minutes
}
