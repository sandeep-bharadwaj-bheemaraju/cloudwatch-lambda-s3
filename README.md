# cloudwatch-lambda-s3

*cloudwatch-lambda-s3* is a lambda piece of work which helps in data migration from On-Prem to RDS and provides an easy-and-fastway to migrate your data to AWS Cloud applications.

Currently, the focus is primarily on migrating data using CSV or Delimited file.

# Overview

*cloudwatch-lambda-s3* is a part of Lambda-Glue Architecture for data migration solution

![cloudwatch-lambda-s3](https://github.com/sandeep-bharadwaj-bheemaraju/cloudwatch-lambda-s3/blob/master/dashboard/web/img/cloudwatch-lambda-s3.png)

# Solution Design

To migrate the data from legacy system to AWS database, *cloudwatch-lambda-s3* acts as clean up process, triggered post glue execution. This lambda does below task.

* **AWS Glue upon its state change based on CloudWatch Event Rule triggers this lambda.**
* **It moves the processed files from in-process directory to succeeded/failed directory based on glue state change**
* **cloudwatch-lambda-s3 shall update the job state in Dynamo DB .**
* **Triggers one more lambda.**

## Requirements

* `java`/`javac` (Java 8 runtime environment and compiler)
* `gradle` (The build system for Java)

Step by Step installation procedure is clearly explained in the **Lambda Glue Architecture for Batch Processing.pdf** file in the repository.

