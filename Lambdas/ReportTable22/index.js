'use strict';

var AWS = require('aws-sdk');
var Promise = require('promise');
var wrapper = require('co-express');
var attr = require('dynamodb-data-types').AttributeValue;
var json2csv = require('json2csv');

/*
AWS.config.update({ accessKeyId: 'AKIAI55QVOCDGWZHWBJA',
 secretAccessKey: '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',
  region: 'us-east-1' });
*/

var docClient = new AWS.DynamoDB.DocumentClient();

var events =  {
  "Records": [
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys": {
          "uniqueid": {
            "S": "yark"
          }
        },
        "NewImage": {
          "codeuserdatehourminuteseconduniqueid": {
            "S": "20170722416415#yark"
          },
        },
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "111",
        "SizeBytes": 26
      },
      "awsRegion": "us-west-2",
      "eventName": "INSERT",
      "eventSourceARN": "arn:aws:dynamodb:us-west-2:account-id:table/ExampleTableWithStream/stream/2015-06-27T00:48:05.899",
      "eventSource": "aws:dynamodb"
    },
  ]
};

function getHMSFromID(id){
    var HMS = '';

    for(var i = 0; i < id.length; i ++){
        if(id[i] === '#'){
            return HMS;
        }
        else{
            HMS += id[i];
        }
    }
    return HMS;
}

function getSpecificItemOnTable22(datehourminutesecondid)
{
    var HMS = getHMSFromID(datehourminutesecondid);
    const params = {    
        TableName: 'table22_message_ads',
        KeyConditionExpression: "#attr = :firstKey and dateHourMinutesSecond = :value",
        ExpressionAttributeNames: {
            "#attr": "primary"
        },
        ExpressionAttributeValues: {
            ":firstKey" : 'primary',
            ":value" : HMS,
        }
    };

    return docClient.query(params).promise()
      .then(data =>  data.Items[0] ? data.Items[0] : undefined);
}

let handler = function(event, context){
    var newRecord = attr.unwrap(event.Records[0].dynamodb.NewImage);
        
    if(event.Records[0].eventName != 'REMOVE')
    {
        console.log(newRecord);
        
        let datehourminutesecond = newRecord.codeuserdatehourminuteseconduniqueid;
    
        console.log(datehourminutesecond);
        if(!datehourminutesecond || (datehourminutesecond === ''))
        {
            console.log('Invalid record');
            return -1;
        }
        return getSpecificItemOnTable22( datehourminutesecond ).then(wrapper(function*(tableItem){
            if(tableItem == undefined)
            {
                console.log('datehourminuteseconduniqueid not existed');
                return -1;
            }
            else
            {
                console.log('Update report about ', datehourminutesecond );
                let reportCount = tableItem.report1 ? tableItem.report1 : 0;
                reportCount = reportCount + 1;
                
                let update_params = {
                    TableName: 'table22_message_ads',
                    Key: {
                        primary: tableItem.primary,
                        dateHourMinutesSecond : tableItem.dateHourMinutesSecond,
                    },
                    UpdateExpression: 'SET report1 = :value',
                    ExpressionAttributeValues : {
                        ':value' : reportCount
                    }
                };
                console.log(update_params);

                yield docClient.update(update_params).promise().then(resp => {
                    console.log('Updating table22 : Success!');
                });
                return 1;
            }
        }));
    }
}

/*
if(require.main === module) {
    handler(events, {});
}
*/

module.exports.handler = handler;