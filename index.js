const path = require('path');
const Bree = require('bree');
const Cabin = require('cabin');
const Graceful = require('@ladjs/graceful');
const celery = require('celery-node');

const RABBITMQ_HOST = 'localhost';
const RABBITMQ_USER = 'user';
const RABBITMQ_PASS = 'bitnami';
const RABBITMQ_PORT = '5672';
// const RABBITMQ_ZILLOW_QUEUE = 'dallas-tx-zillow';

const node_zillow_handler = path.resolve(__dirname, '..', '../../../scrapers/05_dallas_zillow_redfin_data/zillow.js');

const rabbitmq_broker = `amqp://${RABBITMQ_USER}:${RABBITMQ_PASS}@${RABBITMQ_HOST}:${RABBITMQ_PORT}`;

const worker = celery.createWorker(
    rabbitmq_broker,
    rabbitmq_broker
);

const processDataBree = async (data) => {
    const filename = data.url.split('/').pop();
    const bree = new Bree({
        root: false,
        logger: new Cabin(),
        jobs: [
            {
                name: 'zillow',
                path: node_zillow_handler, // jobs/zillow.js
                worker: {
                    workerData: {
                        filename: filename
                    }
                }
            }
        ]
    });

    const graceful = new Graceful({ brees: [bree]});
    graceful.listen();
    bree.start('zillow');

    bree.on('worker created', (name) => {
        console.log(`new worker ${name}`);
    });  

    bree.on('worker deleted', (name) => {
        console.log('worker deleted', name);
    });
}

worker.register("tasks.dallas-tx-zillow", async(data) => {
    if(!data.url) { console.log('Error: no url is defined!'); return; }
    await processDataBree(data);
    return data;
});

worker.start();