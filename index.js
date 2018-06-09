'use strict'

const AWS = require('aws-sdk')

// should be good enough to give us a id w/o collisions and not require a uuid lib
const crypto = require('crypto')

AWS.config.update({region: 'us-east-1'}); // TODO: pull from environment or something


const zlib = require('zlib')
const fs = require('fs');
const { Transform } = require('stream');

const cloudwatch = new AWS.CloudWatch()
const s3 = new AWS.S3()
const sqs = new AWS.SQS()
const lambda = new AWS.Lambda()

const BUCKET = process.env.CRAWL_INDEX_BUCKET || 'commoncrawl'
const KEY = process.env.CRAWL_INDEX_KEY || 'crawl-data/CC-MAIN-2018-17/warc.paths.gz'

const QUEUE_URL = process.env.QUEUE_URL
const MAX_WORKERS = process.env.MAX_WORKERS || 4
const DEFAULT_REGEX = '(\([0-9]{3}\) |[0-9]{3}-)[0-9]{3}-[0-9]{4}'
const DEFAULT_REGEX_FLAGS = 'gm'
const REGEX = new RegExp(process.env.REGEX || DEFAULT_REGEX, process.env.REGEX_FLAGS || DEFAULT_REGEX_FLAGS)

const REQUEST_REGEX = new RegExp('\nWARC-Type: request', 'gm')

async function get_object(bucket, key) {
	const params = { Bucket: bucket, Key : key }
	return s3.getObject(params).promise()
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

async function run_lambda(fxn_name, url, run_id, worker_id) {
	const params = {
		FunctionName: fxn_name,
		Payload: JSON.stringify({ url, run_id, worker_id }),
		InvocationType: 'Event'
	}

	return lambda.invoke(params).promise()
		.catch(err => console.log('Something went wrong', err))
}

async function driver(fxn_name) {
	const start_time = new Date().getTime()
	const run_id = crypto.randomBytes(16).toString("hex")

	console.log("Starting ", fxn_name, run_id)

	const max = process.env.MAX_CHUNKS ? parseInt(process.env.MAX_CHUNKS) : 2

	const content = await get_object(BUCKET, KEY)
	const manifest = await gunzipBuf(content.Body)
	const lines = manifest.toString().split("\n")
		.slice(0, max) // limit for now

	const enqueues = lines.map(line => {
		return sqs.sendMessage({
			MessageBody: line,
			QueueUrl : QUEUE_URL
		}).promise()
	})


	// make sure we have our queue populated before we spin off
	// workers or we may race
	await Promise.all(enqueues)

	const workers = []
	for (var worker_id = 0; worker_id != MAX_WORKERS; worker_id++) {
		workers.push(run_lambda(fxn_name, QUEUE_URL, run_id, worker_id))
	}

	const metrics = [
		create_metric('start_time', start_time, 'Milliseconds'),
		create_metric('total_workers', workers.length),
		create_metric('total_chunks', lines.length)
	]
	await on_metrics(metrics, fxn_name, run_id)

	return Promise.all(workers).then(() => "All launched")
}

async function handle_path(url, path) {
	const start_time = new Date().getTime()
	const extractor = new Transform({
		transform(chunk, encoding, callback) {
			const raw_matches = chunk.toString().match(REGEX)
			const matches = raw_matches || []
			matches.forEach(data => this.push(data))
			callback()
		}
	})

	const gunzipper = zlib.createGunzip()
	const params = {
		Bucket : BUCKET,
		Key : path
	}

	console.log("Streaming ", params)

	var uncompressed_bytes = 0
	var compressed_bytes = 0
	var total_requests = 0
	var count = 0
	const stream = s3.getObject(params).createReadStream()
		.on('error', err => console.log("Stream error", err))
		.on('data', data => compressed_bytes += data.length)
		.pipe(gunzipper)
		.on('data', data => {
			// technically our stream could split our request
			// marker, but that will be rare, and over the total
			// number of requests we have we should be ok
			const requests = data.toString().match(REQUEST_REGEX) || []
			total_requests += requests.length
		})
		.on('data', data => uncompressed_bytes += data.length)
		.pipe(extractor)
		.on('data', data => count++)

	// wait for the stream to finish, probably a better idiom to use
	await new Promise((resolve, reject) => {
		stream.on("end", () => {
			resolve("complete")
		})
	})

	const now = new Date().getTime()
	return [
		create_metric('total_requests', total_requests),
		create_metric('compressed_bytes', compressed_bytes, 'Bytes'),
		create_metric('uncompressed_bytes', uncompressed_bytes, 'Bytes'),
		create_metric('elapsed_ms', now - start_time, 'Milliseconds'),
		create_metric('end_time', now, 'Milliseconds'),
	]
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
	return cloudwatch.putMetricData(update).promise()
}

async function handle_message(fxn_name, url, run_id, worker_id) {
	const response = await sqs.receiveMessage({ QueueUrl : url }).promise()
	const messages = response.Messages || []

	if (0 == messages.length){
		console.log(worker_id + ": No work to do")
		return "All done"
	}

	for(const message of messages){
		const metrics = await handle_path(url, message.Body)
		await sqs.deleteMessage({ QueueUrl : url, ReceiptHandle: message.ReceiptHandle }).promise()
		await on_metrics(metrics, fxn_name, run_id)
	}

	const run = await run_lambda(fxn_name, url, run_id, worker_id)
	console.log(run)
	return "All done"
}

exports.handler = async (args,event,context) => {
	const { url, run_id, worker_id } = args
	const handler = url ? handle_message : driver
	return handler(process.env.WORKER_FXN || event.functionName, url, run_id, worker_id)
}
