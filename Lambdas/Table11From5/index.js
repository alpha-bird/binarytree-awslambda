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
            "S": "abc"
          }
        },
        "NewImage": {
            "requestdatehourminutesecond": {
                "S": "2017081017"
            },
            "amount" : {
                "N" : "230"
            },
            "uniqueid": {
                "S": "markin"
            }
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

function getLatestItemOnTable11(uniqueid)
{
    const params = {    
        TableName: 'table11_accountbalance',
        FilterExpression: "uniqueid = :value",
        ExpressionAttributeValues: {
            ":value": uniqueid,
        },
    };

    return docClient.scan(params).promise()
      .then(data => {
        var items = data.Items ? data.Items : undefined;

        if(items === undefined)
            return undefined;
        else{
            console.log('sorting...');
            items.sort((a, b) => {
                if (a.monthly_in_dateHour > b.monthly_in_dateHour)
                    return -1;
                if (a.monthly_in_dateHour < b.monthly_in_dateHour)
                    return 1;
                return 0;   
            });
            return items[0];
        }
      });
}

function getSpecificTime(location) {
    var moment = require('moment-timezone');
    var date = moment().tz(location).format();

    var date_json = {
        year : 0,
        month : 0,
        day : 0,
        hour : 0,
        minute : 0,
        second : 0,
    }

    var year_str = '';
    var month_str = '';
    var day_str = '';
    var hour_str = '';
    var minute_str = '';
    var second_str = '';

    for(var i = 0; i < 4; i ++)
        year_str += date[i];
    for(var i = 5; i < 7; i ++)
        month_str += date[i];
    for(var i = 8; i < 10; i ++)
        day_str += date[i];
    for(var i = 11; i < 13; i ++)
        hour_str += date[i];
    for(var i = 14; i < 16; i ++)
        minute_str += date[i];
    for(var i = 17; i < 19; i ++)
        second_str += date[i];

    date_json.year = parseInt(year_str);
    date_json.month = parseInt(month_str);
    date_json.day = parseInt(day_str);
    date_json.hour = parseInt(hour_str);
    date_json.minute = parseInt(minute_str);
    date_json.second = parseInt(second_str);

    return date_json;
}

function send_notification_email(table_info){
    var email = 'marklyw@gmail.com';//'marklyw@gmail.com';//
    var nodemailer = require('nodemailer');

    var transporter = nodemailer.createTransport({
        service: 'Gmail',
        auth: {
            user: 'p.rochest99@gmail.com', // Your email id
            pass: 'ghost123!@#' // Your password
        }
    });
    var text = 'table5_transfer_record is updated with this record  \n \n' + table_info;

    var mailOptions = {
        from: 'p.rochest99@gmail.com', // sender address
        to: email, // list of receivers
        subject: 'Table Updated', // Subject line
        text: text //, // plaintext body
        // html: '<b>Hello world ✔</b>' // You can choose to send an HTML body instead
    };

    transporter.sendMail(mailOptions, function(error, info){
        if(error){
            console.log(error);
        }else{
            console.log('Message sent: ' + info.response);
        };
    });
}

function getLatestItemOnTable9(uniqueid)
{
    const params = {    
        TableName: 'table9_report',
        FilterExpression: "uniqueid = :value",
        ExpressionAttributeValues: {
            ":value": uniqueid,
        },
    };

    return docClient.scan(params).promise()
      .then(data => {
        var items = data.Items ? data.Items : undefined;

        if(items === undefined)
            return undefined;
        else{
            items.sort((a, b) => {
                if (a.reportdatehour > b.reportdatehour)
                    return -1;
                if (a.reportdatehour < b.reportdatehour)
                    return 1;
                return 0;   
            });
            return items[0];
        }
      });
}

let handler = function(event, context){
    var newRecord = attr.unwrap(event.Records[0].dynamodb.NewImage);
    //var oldRecord = attr.unwrap(event.Records[1].dynamodb.OldImage);

    if(event.Records[0].eventName !== 'REMOVE')
    {
        send_notification_email(JSON.stringify(newRecord) + '\n \n'  );

        return getLatestItemOnTable11(newRecord.uniqueid).then(
            wrapper(function*(tableItem){
                var uniqueid = newRecord.uniqueid;
                var transactiondatehourminutes = newRecord.requestdatehourminutesecond;
                var balance = 0;
                var monthly_in = 0;

                if(tableItem !== undefined){
                    var lastBalance = tableItem.balance;
                    
                    balance = lastBalance - newRecord.amount;
                    monthly_in = tableItem.monthly_in;
                }
                
                const date = getSpecificTime('Asia/Hong_Kong');
                const year = date.year;
                const month = date.month;
                const day = date.day;
                const hours = date.hour;
                const minutes = date.minute;

                const reportdatehour = `${year}${month > 9 ? month : '0' + month}` +
                    `${day > 9 ? day : '0' + day}` +
                    `${hours > 9 ? hours : '0' + hours}` + `${minutes > 9 ? minutes : '0' + minutes}`;

                var monthly_in_dateHour = reportdatehour;
                
                var transactionRequestAmount = newRecord.amount;

                let new_params = {
                    TableName: 'table11_accountbalance',
                    Item: {
                        uniqueid,
                        transactiondatehourminutes,
                        balance,
                        monthly_in,
                        monthly_in_dateHour,
                        transactionRequestAmount,
                    }
                };
                docClient.put(new_params).promise()
                    .then(data => {
                        console.log('Record added', new_params);
                    })
                    .catch(err => {
                        console.log(err);
                    });
                return 1;                
            })
        );
    }
}

/*
if(require.main === module) {
   handler(events, {});
}
*/
module.exports.handler = handler;