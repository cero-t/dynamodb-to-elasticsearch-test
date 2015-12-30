var AWS = require("aws-sdk");
var Promise = require("bluebird");
var http = require('http');

var TABLE_NAME = 'mytest';
var HOST = 'xxx.xxx.xxx.xxx'
var PORT = 9200;

var docClient = new AWS.DynamoDB.DocumentClient();

function processRecord(record) {
    return new Promise(function (resolve, reject) {
        var params = {
            Key: {id: record.dynamodb.Keys.id.S},
            TableName: TABLE_NAME
        };

        docClient.get(params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                data.id = params.Key.id;
                data.eventName = record.eventName;
                data.tableName = params.TableName;
                resolve(data);
            }
        });
    });
}

function prepareBulk(docs) {
    console.log(docs);

    var bulkRequestBody = '';

    docs.forEach(function (doc) {
        var timestamp = new Date();
        var source = doc.Item;

        var action = {"index": {}};
        action.index._index = TABLE_NAME;
        action.index._type = TABLE_NAME;
        action.index._id = doc.id;

        bulkRequestBody += [
                JSON.stringify(action),
                JSON.stringify(source),
            ].join('\n') + '\n';
    });
    return bulkRequestBody;
}

function toElasticsearch(body, context) {
    var requestParams = {
        host: HOST,
        port: PORT,
        method: 'POST',
        path: '/_bulk',
        body: body,
        headers: {
            'Content-Type': 'application/json',
            'Host': HOST + ':' + PORT,
            'Content-Length': Buffer.byteLength(body)
        }
    };

    var request = http.request(requestParams, function (response) {
        var responseBody = '';
        response.on('data', function (chunk) {
            responseBody += chunk;
        });
        response.on('end', function () {
            console.log('Result: ' + responseBody);
            context.succeed('Success');
        });
    }).on('error', function (e) {
        context.fail('Http error: ' + e);
    });
    request.end(requestParams.body);
}

exports.handler = function (event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    Promise.all(event.Records.map(function (event) {
            return processRecord(event);
        }))
        .then(function (docs) {
            return prepareBulk(docs);
        })
        .then(function (body) {
            return toElasticsearch(body, context);
        })
        .catch(function (err) {
            context.fail("Error occurred: " + err);
        });
};
