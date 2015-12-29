var AWS = require("aws-sdk");
var Promise = require("bluebird");
var docClient = new AWS.DynamoDB.DocumentClient();

function processRecord(record) {
    return new Promise(function (resolve, reject) {
        var params = {
            Key: {id: record.dynamodb.Keys.Id.S},
            // Key: {id: 'test'},
            TableName: 'mytest'
        };

        docClient.get(params, function (err, data) {
            if (err) {
                reject(err);
            } else {
                data.eventName = record.eventName;
                data.tableName = params.TableName;
                resolve(data);
            }
        });
    });
}

function prepareBulk(docs) {
    console.log(docs);
    //return results.map(function (result) {
    //    console.log(result);
    //    return result;
    //});
}

exports.handler = function (event, context) {
    console.log('Received event:', JSON.stringify(event, null, 2));

    Promise.all(event.Records.map(function (event) {
            return processRecord(event);
        }))
        .then(function (docs) {
            prepareBulk(docs);
        })
        .then(function (results) {
            context.succeed("Successfully processed " + event.Records.length + " records.");
        })
        .catch(function (err) {
            console.log(err);
        });
};
