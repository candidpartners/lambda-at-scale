# lambda-at-scale
Proof of concept lambda for massive parallelism using up to 20k concurrent lambdas

Currently to use this you need to go to the lambda that it creates and create a test event.  Then you can simply invoke it from the console

Metrics are emitted to a separate queue to be drained offline.  The metric generation rate can exceed that allowable by cloudwatch, so some throttling is needed.

## Deployment
The Makefile will create the serverless repo in your account.  By default it isn't public.  You'll have to have Amazon's aws cli and sam installed.  These tools require a bucket to create the serverless repo.  That is left as an exercise for the reader.  We leverage node and npm as well.  You'll need to update or override the variables at the top of the Makefile to adapt it to your environment.

The 'make deploy' target will create a serverless repo for you that you can choose to make public.  Alternately, you can run 'make deploy-cf' and it will deploy via CloudFront.

We used an Ubuntu 16.04-based Linux distribution for our development and testing.  It should be easily portable to similar environments.

## Running
The 'run' script takes a single argument, the name of the stack from the deployment stage.  It isn't strictly necessary, but it ties together the cleaning out of various queues and outputs links for the dashboards.  It is also useful to override the number of chunks to process or impose a concurrency limit.
