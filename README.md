Apache Spark custom receiver to stream Wiki edit events.

Build steps:
* sbt gen-idea
* sbt package
* sbt assembly : to generate deployable jars

Apache Spark Datasource V2
* Understand the MicroBatchReader interface
* Good pull request to go over
* Good built-in (V1 and V2) sources to study
  * V1 - MemoryStream, RateSourceProviderV2
  * V1 - TextSocketSource

Reference:
* https://github.com/pbassiner/sbt-multi-project-example