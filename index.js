'use strict'

const AWS = require('aws-sdk')

AWS.config.update({region: 'us-east-1'}); // TODO: pull from environment or something

const zlib = require('zlib')
const { Transform } = require('stream');

const cloudWatch = new AWS.CloudWatch()
const s3 = new AWS.S3()
const sqs = new AWS.SQS()
const lambda = new AWS.Lambda()
const firehose = new AWS.Firehose()

const BUCKET = process.env.CRAWL_INDEX_BUCKET || 'commoncrawl'
const KEY = process.env.CRAWL_INDEX_KEY || 'crawl-data/CC-MAIN-2018-17/warc.paths.gz'

const INPUT_URL = process.env.QUEUE_URL
const METRIC_URL = process.env.METRIC_URL
const MAX_WORKERS = process.env.MAX_WORKERS || 4
const DEFAULT_REGEX = '(\([0-9]{3}\) |[0-9]{3}-)[0-9]{3}-[0-9]{4}'
const DEFAULT_REGEX_FLAGS = 'gm'
const REGEX = new RegExp(process.env.REGEX || DEFAULT_REGEX, process.env.REGEX_FLAGS || DEFAULT_REGEX_FLAGS)

const REQUEST_REGEX = new RegExp('\nWARC-Type: request', 'gm')

const WORK_REQUEST = 'work'
const START_REQUEST = 'start'

const ONE_MINUTE_MILLIS = 60 * 1000

const REGEX_HIT_QUEUE = []
const REGEX_FLUSH_SIZE = 500

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
    process.exit(0)
}

async function get_object(bucket, key) {
    const params = { Bucket: bucket, Key : key }
    return s3.getObject(params).promise()
        .catch(err => fatal("Unable to get S3 data: " + err))

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

async function run_lambda(fxn_name, type, run_id, worker_id, launch_count) {
    const params = {
        FunctionName: fxn_name,
        Payload: JSON.stringify({ type, run_id, worker_id, launch_count }),
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

    const lines = input.slice(0, max).filter(data => 0 !== data.length) // limit for now

    console.log(`Populating with ${lines.length} archive entries`)

    const enqueuers = []
    let count = 0
    for (let index = 0; index < lines.length; index = index + 10) {
        let counter = 0
        const entries = lines.slice(index, index + 10).map(line => {
            return {
                MessageBody: line,
                Id: `${counter++}` // There has to be a better way to do this
            }
        })

        const message = { Entries: entries, QueueUrl: INPUT_URL }
        count += entries.length
        enqueuers.push(sqs.sendMessageBatch(message).promise())
    }

    await Promise.all(enqueuers)
        .catch(err => fatal("Something bad happened while enqueueing: " + err))

    // return what we did so we can wait for it
    return lines
}

async function get_queue_size(){
    const attr_params = {
        QueueUrl: INPUT_URL,
        AttributeNames: [ 'ApproximateNumberOfMessages', 'ApproximateNumberOfMessagesNotVisible' ]
    }

    const results = await sqs.getQueueAttributes(attr_params).promise()
            .catch(err => fatal("Unable to determine queue depth: " + err))
    return parseInt(results.Attributes.ApproximateNumberOfMessages) + parseInt(results.Attributes.ApproximateNumberOfMessagesNotVisible)
}

async function await_queue(target){
    // the promises have all run, now we need to make sure SQS shows all of them
    // or our lambdas may start and immediately be done

    while (true){
        const ready = await get_queue_size()
        if (ready >= target){ // NB: type coercion here
            console.log("Queue reports " + ready + " messages available")
            break
        }
        console.log("Waiting for messages to show in SQS, see " + ready + ", want " + target)
    }

    return true
}

async function warm_target(launch_count){
    const depth = await get_queue_size()
    const remaining = Math.max(0, MAX_WORKERS - launch_count)
    const initial = process.env.INITIAL_COUNT || 3000
    const step = process.env.INCREMENT_STEP || 500
    const limit = 0 === launch_count ? initial : step
    // if we don't have a full step to do, do what we need
    const full_limit = Math.min(limit, remaining)

    // if our queue is empty then we don't want spin any more up,
    // or if we have half the increment step in the queue, don't spin up the full step
    console.log(`Calculating warm target from ${limit}, ${remaining} and ${depth}`)
    return Math.min(full_limit, depth)
}

async function driver(fxn_name, memorySize, run_id, launch_count) {
    const full_start_time = new Date().getTime()
    const first_run = 0 === launch_count

    let lines = []
    if (first_run) {
        console.log("Starting ", fxn_name, run_id)
        lines = await populate_queue()
        await await_queue(lines.length)
    }

    const target = await warm_target(launch_count)

    console.log(`Launching ${target} workers`)
    const workers = []
    for (let worker_id = 0; worker_id !== target; worker_id++) {
        workers.push(run_lambda(fxn_name, WORK_REQUEST, run_id, launch_count + worker_id))
    }

    if (first_run) {
        const launch_start_time = new Date().getTime()

        const metrics = [
            create_metric('memory_size', memorySize, 'Megabytes'),
            create_metric('full_start_time', full_start_time, 'Milliseconds'),
            create_metric('launch_start_time', launch_start_time, 'Milliseconds'),
            create_metric('total_workers', MAX_WORKERS),
            create_metric('total_chunks', lines.length)
        ]

        await on_metrics(metrics, fxn_name, run_id)
    }

    console.log("Waiting for " + workers.length + " workers to spin up...")
    await Promise.all(workers)
        .catch(err => fatal("Something went wrong starting workers: " + err))
        .then(() => "All launched for " + run_id)

    const current_count = target + launch_count
    if (0 === target){
       console.log(`All workers launched`)
       return
    }

    // we want to launch every minute as close as possible
    const next_run_time = full_start_time + ONE_MINUTE_MILLIS
    const sleep_time = Math.max(0, next_run_time - new Date().getTime() )

    await sandbag(sleep_time)
    console.log(`Recursing with launch count of ${current_count}`)
    return run_lambda(fxn_name, START_REQUEST, run_id, 0, current_count)
}

function flush_regex_queue(run_id){
    if (!process.env.HIT_STREAM || 0 === REGEX_HIT_QUEUE.length){
        return
    }

    const records = REGEX_HIT_QUEUE.map(buf => { Data: buf })

    const params = {
        DeliveryStreamName: process.env.HIT_STREAM,
        Records: records
    }

    firehose.putRecordBatch(params)
    REGEX_HIT_QUEUE.length = 0
}

function on_regex(data, run_id){
    if (!process.env.HIT_STREAM){
        return
    }

    if (data && run_id){
        REGEX_HIT_QUEUE.push(data)
    }

    if (REGEX_FLUSH_SIZE <= REGEX_HIT_QUEUE.length){
        flush_regex_queue(run_id)
    }
}

async function handle_stream(stream, run_id){
    let uncompressed_bytes = 0
    let compressed_bytes = 0
    let total_requests = 0
    let count = 0

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
            .on('data', () => count++)
            .on('data', data => on_regex(data, run_id))
            .on('end', () => {
                console.log("Streaming complete")
                flush_regex_queue()
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

async function handle_path(path, run_id) {
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

    return handle_stream(stream, run_id)
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
        console.log(metric.key + "." + run_id, ' -> ', metric.value)
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

async function setVisibilityTimeout(message, time){
    return sqs.changeMessageVisibility({ QueueUrl : INPUT_URL, ReceiptHandle: message.ReceiptHandle, VisibilityTimeout: time })
        .promise()
        .then(() => console.log(`Message ${message.ReceiptHandle} timeout set to ${time}`))
        .catch(err => annoying("Failed to reset the visibility: " + err))
}

async function handle_message(fxn_name, run_id, worker_id, end_time) {
    const response = await sqs.receiveMessage({ QueueUrl : INPUT_URL }).promise()
        .catch(err => fatal("Failed to receive message from queue: " + err))
    const messages = response.Messages || []

    // we're at the end of our queue, so send our done time
    // sometimes SQS gives us no work when we hammer it, so try a couple times before give up
    if (0 === messages.length && 0 !== invocations){
        console.log(worker_id + ": No work to do")
        const metric = create_metric('end_time', new Date().getTime(), 'Milliseconds')
        await on_metrics([metric], fxn_name, run_id)
        return "All done"
    }

    invocations++

    for(const message of messages){
        let metrics = [create_metric('messages_attempted', 1)]

        // if we aren't done by panic_time then we need to push the message back
        const panic_time = end_time - new Date().getTime()

        // with high concurrency, S3 gets mad causing timeouts, this handles that for us by pushing the message back on the queue
        const timer = setTimeout(async () => await setVisibilityTimeout(message, 0), panic_time)
        try {
            const message_metrics = await handle_path(message.Body, run_id)
            metrics.push(create_metric("metrics_handled", 1))
            metrics = metrics.concat(message_metrics)
            clearTimeout(timer)
        } catch (error) {
            metrics.push(create_metric("messages_error", 1))
            annoying("Failed to handle path: " + error)
            await Promise.all([
                setVisibilityTimeout(message, 0),
                on_metrics(metrics, fxn_name, run_id)
            ])
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

exports.metric_handler = async (event) => {
    const records = event.Records || []

    const metrics = records.map(record => {
        const raw_payload = record.body
        return JSON.parse(raw_payload)
    })

    await persist_metric_batch(metrics)
    console.log(`Inserted ${metrics.length} metrics`)
}

async function persist_metric_batch(messages){
    for (const message of messages){
        await cloudWatch.putMetricData(message).promise()
            .catch(err => fatal("Error putting metric data: " + err))
    }

    return true
}

exports.handler = async (args, context) => {
    const { type, run_id, worker_id, launch_count } = args

    // peel off N seconds to give us a bit of a buffer for the event to clear and code to run
    const grace_period = process.env.PANIC_GRACE_PERIOD || 3000
    const end_time = new Date().getTime() + context.getRemainingTimeInMillis() - grace_period

    switch (type){
        case WORK_REQUEST:
            return handle_message(context.functionName, run_id, worker_id, end_time)
        case START_REQUEST:
            const memorySize = parseInt(context.memoryLimitInMB)
            // if we have a run id, use it, else, make one
            const id = process.env.RUN_ID || run_id
            return driver(context.functionName, memorySize, id, launch_count || 0)
    }
}

