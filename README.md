# lambda-at-scale
Proof of concept lambda for massive parallelism using up to 50k concurrent lambdas

Currently to use this you need to go to the lambda that it creates and create a test event.  Then you can simply invoke it from the console

Metrics are emitted to a separate queue to be drained offline.  The metric generation rate can exceed that allowable by cloudwatch, so some throttling is needed.

# warming
The environment needs to be "warmed."  This means that we need to, over the course of an hour or so, constantly invoke more lambdas until we have the target number running.  To do that, from your workstation:

MAX_WORKERS=$TARGET OPERATION=warm FXN_NAME=$FXN_NAME nodejs index.js

Do note that this can be very expensive in terms of dollars and cents, as well as time.

# metrics
Run the dashboard script with the run id and the lambda size to generate a URL to get the details in cloud watch:
	dashboard 21dcc1a6dc49cdacd26671a951244d1d 1024

# steps
- configure lambda size in aws console
- purge the sqs queues
- FXN_NAME=foo MAX_WORKERS=5000 OPERATION=warm nodejs index.js
- METRIC_URL=url OPERATION=metric nodejs index.js

