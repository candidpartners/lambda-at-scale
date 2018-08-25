'use strict'

const AWS = require('aws-sdk')

// should be good enough to give us a id w/o collisions and not require a uuid lib

AWS.config.update({region: 'us-east-1'}); // TODO: pull from environment or something


const zlib = require('zlib')
const { Transform } = require('stream');

const cloudwatch = new AWS.CloudWatch()
const s3 = new AWS.S3()
const sqs = new AWS.SQS()
const lambda = new AWS.Lambda()

const BUCKET = process.env.CRAWL_INDEX_BUCKET || 'commoncrawl'
const KEY = process.env.CRAWL_INDEX_KEY || 'crawl-data/CC-MAIN-2018-17/warc.paths.gz'

const INPUT_URL = process.env.QUEUE_URL
const METRIC_URL = process.env.METRIC_URL
const MAX_WORKERS = process.env.MAX_WORKERS || 4
const DEFAULT_REGEX = '(\([0-9]{3}\) |[0-9]{3}-)[0-9]{3}-[0-9]{4}'
const DEFAULT_REGEX_FLAGS = 'gm'
const REGEX = new RegExp(process.env.REGEX || DEFAULT_REGEX, process.env.REGEX_FLAGS || DEFAULT_REGEX_FLAGS)

const REQUEST_REGEX = new RegExp('\nWARC-Type: request', 'gm')

const ENQUEUE_REQUEST = 'enqueue'
const METRIC_REQUEST = 'metric'
const WORK_REQUEST = 'work'
const WARM_REQUEST = 'warm'
const START_REQUEST = 'start'
const SANDBAG_REQUEST = 'sandbag'
const SINGLE_REQUEST = 'single'

let invocations = 0

async function sandbag(delay){
	return new Promise(resolve => setTimeout(resolve, delay))
}

// borrows from SO
function shuffle(input) {
	for (let i = input.length - 1; i > 0; i--) {
		const j = Math.floor(Math.random() * (i + 1));
		[input[i], input[j]] = [input[j], input[i]]; // eslint-disable-line no-param-reassign
	}
	return input
}

function annoying(err) {
	console.log("ANNOY: " + err)
}

function fatal(err) {
	console.log("FATAL: " + err)
	process.exit(1)
}

async function get_object(bucket, key) {
	const params = { Bucket: bucket, Key : key }
	return s3.getObject(params).promise()
		.catch(err => fatal("Unable to get S3 data"))

}

async function gunzipBuf(buffer) {
	return new Promise((resolve, reject) => {
		zlib.gunzip(buffer, (err, data) => {
			if (err) {
				reject(err)
			} else {
				resolve(data)
			}
		})
	})
}

async function run_lambda(fxn_name, type, run_id, worker_id) {
	const params = {
		FunctionName: fxn_name,
		Payload: JSON.stringify({ type, run_id, worker_id }),
		InvocationType: 'Event'
	}

	return lambda.invoke(params).promise()
		.catch(err => fatal('Something went wrong invoking lambda ' + err))
}

async function populate_queue(){
	const max = process.env.MAX_CHUNKS ? parseInt(process.env.MAX_CHUNKS) : 2

	const content = await get_object(BUCKET, KEY)
	const manifest = await gunzipBuf(content.Body)
	const all_archives = manifest.toString().split("\n")

	// mix things up so we can test random archives other than the first couple
	const input = process.env.SHUFFLE ?  shuffle(all_archives) : all_archives

	const lines = input.slice(0, max).filter(data => 0 != data.length) // limit for now

	console.log(`Populating with ${lines.length} archive entries`)
	const enqueues = lines.map(line => {
		return sqs.sendMessage({
			MessageBody: line,
			QueueUrl : INPUT_URL
		}).promise()
		.catch(err => fatal("Unable to fully populate queue: " + err))
	})


	// make sure we have our queue populated before we spin off
	// workers or we may race
	await Promise.all(enqueues)
		.catch(err => fatal("Unable to enqueue all messages"))

	// return what we did so we can wait for it
	return lines
}

async function await_queue(target){
	// the promises have all run, now we need to make sure SQS shows all of them
	// or our lambdas may start and immediately be done
	const attr_params = {
		QueueUrl: INPUT_URL,
		AttributeNames: [ 'ApproximateNumberOfMessages' ]
	}

	while (true){
		const results = await sqs.getQueueAttributes(attr_params).promise()
			.catch(err => fatal("Unable to determine queue depth: " + err))
		const ready = results.Attributes.ApproximateNumberOfMessages
		if (ready >= target){ // NB: type coercion here
			console.log("Queue reports " + ready + " messages available")
			break
		}
		console.log("Waiting for messages to show in SQS, see " + ready + ", want " + target)
	}

	return true
}

async function driver(fxn_name, memorySize, run_id) {
	const full_start_time = new Date().getTime()

	console.log("Starting ", fxn_name, run_id)
	const lines = await populate_queue()
	await await_queue(lines.length)

	const workers = []
	for (var worker_id = 0; worker_id != MAX_WORKERS; worker_id++) {
		workers.push(run_lambda(fxn_name, WORK_REQUEST, run_id, worker_id))
	}
	const launch_start_time = new Date().getTime()

	const metrics = [
		create_metric('memory_size', memorySize, 'Megabytes'),
		create_metric('full_start_time', full_start_time, 'Milliseconds'),
		create_metric('launch_start_time', launch_start_time, 'Milliseconds'),
		create_metric('total_workers', workers.length),
		create_metric('total_chunks', lines.length)
	]
	await on_metrics(metrics, fxn_name, run_id)

	return Promise.all(workers)
		.catch(err => fatal("Something went wrong starting workers: " + err))
		.then(() => "All launched for " + run_id)
}

async function handle_stream(stream){
	var uncompressed_bytes = 0
	var compressed_bytes = 0
	var total_requests = 0
	var count = 0

	const start_time = new Date().getTime()
	const extractor = new Transform({
		transform(chunk, encoding, callback) {
			try {
				const matches = chunk.toString().match(REGEX) || []
				matches.forEach(data => this.push(data))
			} catch (error){
				annoying("Failed to extract: " + error)
			} finally {
				callback()
			}
		}
	})

	const gunzipper = zlib.createGunzip()
	let data_ts = 0

	const log_traffic = () => {
		const ts = new Date().getTime() / 1000
		const elapsed = ts - data_ts
		if (10 <= elapsed){
			console.log("Received data: " + compressed_bytes)
			data_ts = ts
		}
	}

	await new Promise((resolve, _reject) => {
		const reject = message => {
			console.log("Failed: " + message)
			_reject(message)
		}

		const gunzipStream = stream
			.on('error', err => fatal("GZip stream error " + err))
			.on('data', log_traffic)
			.on('data', data => compressed_bytes += data.length)
			.on('end', () => console.log("End of base stream"))
			.pipe(gunzipper)

		const extractorStream = gunzipStream
			.on('error', err => fatal("Extract stream error " + err))
			.on('data', data => {
				// technically our stream could split our request
				// marker, but that will be rare, and over the total
				// number of requests we have we should be ok
				try {
					const requests = data.toString().match(REQUEST_REGEX) || []
					total_requests += requests.length
				} catch (error) {
					annoying("Failed to match batches: " + error)
				}
			})
			.on('data', data => uncompressed_bytes += data.length)
			.on('end', () => console.log("End of gzip stream"))
			.pipe(extractor)

		const extractedStream = extractorStream
			.on('error', err => fatal("Extracted stream error " + err))
			.on('data', data => count++)
			.on('end', () => {
				console.log("Streaming complete")
				resolve("complete")
			})
	})

	const now = new Date().getTime()
	// we're measuring time internally, this isn't 100%, but go ahead
	// and round up to the nearest 100ms
	const elapsed = Math.ceil((now - start_time) / 100) * 100

	return [
		create_metric('start_time', start_time, 'Milliseconds'),
		create_metric('regex_hits', count),
		create_metric('total_requests', total_requests),
		create_metric('compressed_bytes', compressed_bytes, 'Bytes'),
		create_metric('uncompressed_bytes', uncompressed_bytes, 'Bytes'),
		create_metric('elapsed_ms', elapsed, 'Milliseconds')
	]

}

async function handle_path(path) {
	const params = {
		Bucket : BUCKET,
		Key : path
	}

	const stream = s3.getObject(params)
		.on('httpHeaders', (code, headers) => {
			const requestId = headers['x-amz-request-id']
			const amzId = headers['x-amz-id-2']
			console.log("Streaming as x-amz-id-2=" + amzId + ", x-amz-request-id=" + requestId + "/" + JSON.stringify(params))
		}).createReadStream()

	return handle_stream(stream)
}

function create_metric(key, value, unit){
	return {
		key,
		value,
		unit: unit || 'Count'
	}
}

async function on_metrics(metrics, fxn_name, run_id){
	const date = new Date()
	const metricList = metrics.map(metric => {
		console.log(metric.key, ' -> ', metric.value)
		return {
			MetricName: metric.key,
			Dimensions: [
				{ Name: 'run_id', Value: run_id }
			],
			Timestamp: date,
			Unit: metric.unit,
			Value: metric.value
		}
	})

	const update = {
		'MetricData' : metricList,
		'Namespace' : fxn_name
	}

	// ... so for now, shunt to this queue
	return sqs.sendMessage({
			MessageBody: JSON.stringify(update),
			QueueUrl : METRIC_URL
		}).promise()
		.catch(err => fatal("Unable to send metric: " + err))
}

async function handle_message(fxn_name, run_id, worker_id) {
	const response = await sqs.receiveMessage({ QueueUrl : INPUT_URL }).promise()
		.catch(err => fatal("Failed to receive message from queue: " + err))
	const messages = response.Messages || []

	// we're at the end of our queue, so send our done time
	// sometimes SQS gives us no work when we hammer it, so try a couple times before give up
	if (0 == messages.length && 0 !== invocations){
		console.log(worker_id + ": No work to do")
		const metric = create_metric('end_time', new Date().getTime(), 'Milliseconds')
		await on_metrics([metric], fxn_name, run_id)
		return "All done"
	}

	invocations++

	for(const message of messages){
		let metrics = []
		try {
			metrics = await handle_path(message.Body)
		} catch (error) {
			annoying("Failed to handle path: " + error)
			await sqs.changeMessageVisibility({ QueueUrl : INPUT_URL, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: 0 }).promise()
				.catch(err => annoying("Failed to reset the visibility: " + err))
			break // don't delete, punt immediately
		}
		await sqs.deleteMessage({ QueueUrl : INPUT_URL, ReceiptHandle: message.ReceiptHandle }).promise()
			.catch(err => fatal("Failed to delete message from queue: " + err))
		await on_metrics(metrics, fxn_name, run_id)
	}

	const run = await run_lambda(fxn_name, WORK_REQUEST, run_id, worker_id)
	console.log(run)
	return "All done"
}

async function warming_environment(fxn_name){
	const initial = process.env.INITIAL_WORKERS || 3000
	const target = process.env.MAX_WORKERS || 4000
	const step = process.env.SCALE_STEP || 500
	const delay = process.env.SCALE_DELAY || 59

	console.log("Warming environment " + fxn_name + " to " + target + ", starting at " + new Date())
	for (var current = initial; current <= target; current += step){
		const workers = []
		for (var worker_id = 0; worker_id < current; worker_id++) {
			workers.push(run_lambda(fxn_name, SANDBAG_REQUEST))
		}

		await Promise.all(workers)
		const iteration = 1 + (current - initial) / step
		console.log(iteration + " : " + new Date() + ": Spawned " + workers.length + " (of " + target + ") workers for warmup...")
		await sandbag(delay * 1000)
	}

	return "Done"
}

async function persist_metric_batch(messages){
	for (const message of messages){
		await cloudwatch.putMetricData(message).promise()
			.catch(err => fatal("Error putting metric data: " + err))
	}

	return true
}

// NB: this could be made much better, but isn't the point
async function persist_metrics(){
	while (true){
		const response = await sqs.receiveMessage({ QueueUrl : METRIC_URL, MaxNumberOfMessages: 10 })
			.promise()
			.catch(err => fatal("Failed to receive message from queue: " + err))
		const messages = response.Messages || []

		if (0 === messages.length){
			break
		}

		const metrics = messages.map(message => {
			const raw_payload = message.Body
			return JSON.parse(raw_payload)
		})

		await persist_metric_batch(metrics)

		for (var message of messages){
			await sqs.deleteMessage({ QueueUrl : METRIC_URL, ReceiptHandle: message.ReceiptHandle })
				.promise()
				.catch(err => fatal("Failed to delete metric message from queue: " + err))
		}
	}

	return true
}

function get_run_id(){
	const epoch = new Date()
	return "" + Math.floor(epoch.getTime() / 1000)
}

exports.sqs_driver = async (event, context) => {
	const records = event.Records || []
	for (let record of records){
		const body = record.body
		const metrics = await handle_path(body)
		await on_metrics(metrics, context.functionName, '0')
	}
}

async function console_driver(operation){
	switch (operation){
		case ENQUEUE_REQUEST:
			const full_start_time = new Date().getTime()
			const lines = await populate_queue()
			const launch_start_time = new Date().getTime()

			const metrics = [
				create_metric('memory_size', process.env.SIZE, 'Megabytes'),
				create_metric('full_start_time', full_start_time, 'Milliseconds'),
				create_metric('launch_start_time', launch_start_time, 'Milliseconds'),
				create_metric('total_workers', process.env.MAX_WORKERS),
				create_metric('total_chunks', lines.length)
			]

			await on_metrics(metrics, process.env.FXN_NAME, process.env.RUN_ID)
			break
		case WARM_REQUEST:
			const run_id = get_run_id()
			console.log("Warming/Starting " + run_id)
			await warming_environment(process.env.FXN_NAME)
			await run_lambda(process.env.FXN_NAME, START_REQUEST, run_id)
			console.log("Purged, Warmed and launched!")
			break
		case METRIC_REQUEST:
			return persist_metrics()
		case SINGLE_REQUEST:
			const name = process.env.SINGLE_BUNDLE
			const stream = require('fs').createReadStream(name)
			const results = await handle_stream(stream)
			console.log(results)
			return results
	}
}



exports.handler = async (args, context) => {
	const { type, run_id, worker_id } = args

	switch (type){
		case SANDBAG_REQUEST:
			return await sandbag(60 * 1000)
		case WORK_REQUEST:
			return handle_message(context.functionName, run_id, worker_id)
		case START_REQUEST:
			const memorySize = parseInt(context.memoryLimitInMB)
			// if we have a run id, use it, else, make one
			const id = run_id || get_run_id()
			return driver(context.functionName, memorySize, id)
		case SINGLE_REQUEST:
			const name = process.env.SINGLE_BUNDLE
			return handle_path(name)
	}
}

if (!module.parent && process.env.OPERATION){
	console_driver(process.env.OPERATION)
}
