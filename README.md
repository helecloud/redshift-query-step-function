This project provides a clouformation template which creates a step function that runs Redshift queries via Redshift Data API without any idling lambdas with configurable
retries for error handling. Idea is based on [burtcorp/athena-runner](https://github.com/burtcorp/athena-runner) and [helecloud/athena-step-function-query](https://github.com/burtcorp/athena-runner)

In case of error, the query is ran again via [Decorrelated Jitter Algorithm](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

This is good for cases when queries can potentially take more than 15 minutes(lambda max runtime limit) or they need to be retried in case of errors.

This step function be started via AWS [step-functions.StartExecution](https://docs.aws.amazon.com/step-functions/latest/apireference/API_StartExecution.html) or [EventBridge Step functions targets](https://docs.aws.amazon.com/eventbridge/latest/userguide/eventbridge-targets.html), which means you can schedule queries or have them run based on events 

This project is available on [Serverless Application Repository](https://console.aws.amazon.com/lambda/home#/create/app?applicationId=arn:aws:serverlessrepo:eu-west-1:708984232979:applications/redshift-query-step-function).


## Step Function Input

The step function accepts an array of queries(objects), which can have the following attributes:

- ExecuteStatementParameters: Should be a json object with parameters for [redsfhit-data.ExecuteSttement](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_ExecuteStatement.html). In the case the Cloudformation template was deployed with the DefaultUser, DefaultClusterId and DefaultDb then this parameter can be simplified to be only a string.
- InitialWaitTime: The amount of time(seconds) to wait after starting query before checking the results. This should be configured to be the minimum amount of the time a query can take to be finished. Default is 30 seconds. 
- BaseWaitTime: After the InitialWaitTime has passed, the step function will continuously check if the query was completed or not. This parameter would be the amount of time to wait between the checks. Default is 15 seconds.
- WaitMultiplier: Between every check the BaseWaitTime is adjusted by multiplying it with this number. Default is 1.
- MaxWait: Maximum amount of time to wait between checks. Default is 450.
- MaxRetries: In the case query fails, this will be the number of it is retried. Default is 10.
- RetryErrorMatch: Python to match against errors, if error is matched, then it will be retried. Default is empty string which means all errors are retried.

Since the template can be configured with default user, database and cluster, to simplify you can also simply pass an array of queries text, and it will be converted to the following. The following inputs are logically identical:

```json
["vacuum"]
```

```json
[{
  "ExecuteStatementParameters": {
    "Sql": "vacuum"
  }
}]
```

Note that each query in the array is executed sequentially, if one fails after maximum retries, the execution stops and the step function will fail.

### Input Example

#### Simple Queries

```json
["copy favoritemovies from 'dynamodb://Movies'iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole' readratio 50;", 
"vacuum favoritemovies;"]
```

#### Queries with no retries

Sometimes retried failed queries is not required:

```json
[{
  "ExecuteStatementParameters": "vacuum favoritemovies;",
  "MaxRetries": 0
}]
```

#### Queries with different clusters

```json
[{
  "ExecuteStatementParameters": {
    "ClusterIdentifier": "somecluster-1",
    "Sql": "ANALYZE;VACUUM;",
    "Database": "db-1",
    "DbUser": "master"
  },
  "ExecuteStatementParameters": {
    "ClusterIdentifier": "somecluster-2",
    "Sql": "ANALYZE;VACUUM;",
    "Database": "db-2",
    "DbUser": "master"
  }
}]
```

## Checking for the queries results

In the case one of the queries passed, only contains select statements, then you can fetch the results via 
[redshift-data.GetStatementResult](https://docs.aws.amazon.com/redshift-data/latest/APIReference/API_GetStatementResult.html),

In the resulting output of the stepfunction there will be a StatementId for each query, which you can use to get the result, however since the redshift-data APIs are scoped to the roles that execute them you have to assume to the role that was used to execute the statement.

The cloudformation template creates this role and outputs it's ARN as ResultsRole. By default this role can be passed on to Lambda function
however you can also provide AWS account numbers to assume to this role by the ResultsRoleAwsPrincipals Cloudformation parameter.

There also some size limitations with this API, for large results best is to use [UNLOAD](https://docs.aws.amazon.com/redshift/latest/dg/r_UNLOAD.html) SQL command to write the results to S3. 

```
MIT License
                                                                              
Copyright (c) 2021 Helecloud
                                                                              
Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:
                                                                              
The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.
                                                                              
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
``
