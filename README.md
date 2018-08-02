# lambda-at-scale
Proof of concept lambda for massive parallelism using up to 50k concurrent lambdas

Currently to use this you need to go to the lambda that it creates and create a test event.  Then you can simply invoke it from the console

Metrics are emitted to a separate queue to be drained offline.  The metric generation rate can exceed that allowable by cloudwatch, so some throttling is needed.

# metrics
Run the dashboard script with the run id and the lambda size to generate a URL to get the details in cloud watch:
	dashboard 21dcc1a6dc49cdacd26671a951244d1d 1024
