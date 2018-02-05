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
var s3Client = new AWS.S3({apiVersion: '2006-03-01'});

const fields_table3 = [
    'uniqueid',
    'registeddatehourminutesecond',
    'GMT',
    'advanceuser',
    'businessname',
    'contractDays',
    'email',
    'fullhouse',
    'paymentmethod',
    'paymentrecordnumber',
    'paymentstatus',
    'paymentvaliddays',
    'phonenumber',
    'reentrycompleted',
    'relatedabove',
    'userlocationlatitude',
    'userlocationlongitude',
    'paymentdatehourminutesecond',
    'category',
    'city',
    'country',
    'dateHourMinutesSecond',
    'description',
    'price',
    'productservice',
    'street1',
    'street2',
    'street3',
    'webpage',
];

const fields_table5 = [
    'uniqueid',
    'requestdatehourminutesecond',
    'amount',
    'bank',
    'bankaccount',
    'bankcode',
    'bankname01',
    'bankname02',
    'paypal',
    'transferMadeDateHourMinutes',
];

const fields_table6 = [
    'uniqueid',
    'paymentdatehourminutesecond',
    'fullHouse',
    'path',
    'reEntryCompleted',
    'relatedLeftCount',
    'relatedRightCount',
    'relatedabove',
    'directAbove',
    'relatedLeft',
    'relatedRight',
    'isWithdraw',
];

const fields_table11 = [
    'uniqueid',
    'transactiondatehourminutes',
    'balance',
    'monthly_in',
    'monthly_in_dateHour',
    'transactionRequestAmount',
];

const fields_table21 = [
    'uniqueid',
    'petAccumulatedExperience',
    'petAngry',
    'petBegging',
    'petFood',
    'petHappy',
    'petImage',
    'petImageAngryFood',
    'petImageAngryPlay',
    'petImageAngryToilet',
    'petImageAngryWater',
    'petImageFood',
    'petImageLeave',
    'petImagePlay',
    'petImageReturn',
    'petImageToilet',
    'petImageWater',
    'petLeaveReturnRecord',
    'petLevel',
    'petPlay',
    'petRemainder',
    'petSoundAngryFood',
    'petSoundAngryPlay',
    'petSoundAngryToilet',
    'petSoundAngryWater',
    'petSoundHappyFood',
    'petSoundHappyPlay',
    'petSoundHappyToilet',
    'petSoundHappyWater',
    'petSoundLeave',
    'petSoundReturn',
    'petTarget',
    'petToilet',
    'petWater',
];

const fields_table23 = [
    'uniqueid',
    'accumulatedScanNumber',
    'level1',
    'remainder1',
    'target',
];

function uploadToS3(bucketPath, fileName, tableData , done){
    var uploadParams = {Bucket: bucketPath, Key: '', Body: ''};
    
    var file = fileName;
/*
    var fs = require('fs');
    var fileStream = fs.createReadStream(file);

    fileStream.on('error', function(err) {
        console.log('File Error', err);
    });
    */
    uploadParams.Body = tableData;

    var path = require('path');
    uploadParams.Key = path.basename(file);

    // call S3 to retrieve upload file to specified bucket
    
    return s3Client.upload ( uploadParams, done);
}

function getTableCSVData(tableName, fields){
    var params = {
        TableName: tableName,
    };

    return docClient.scan(params).promise().then(function(resp){
        var items = resp.Items;
        var tableCSVFormatData = json2csv({ data: items, fields: fields });

        return tableCSVFormatData;
    });
}

function deleteItem(params){
    return docClient.delete(params).promise().then(resp => 1);
}

function resetTable5(){
    var params = {
        TableName: 'table5_transfer_record',
    }
    return docClient.scan(params).promise().then(wrapper(function*(resp){
        var items = resp.Items;
        for(var i = 0; i < items.length ; i ++){
            var item_para = {
                TableName : 'table5_transfer_record',
                Key:{
                    'uniqueid' : items[i].uniqueid,
                    'requestdatehourminutesecond' : items[i].requestdatehourminutesecond,
                },
            }
            
            var rst = yield deleteItem(item_para);
        }
        return 1;
    }));
}

function OptimizeTable5(){
    var table5_params = {
        TableName: "table5_transfer_record",
    };

    return docClient.scan(table5_params).promise().then(wrapper(function*(resp){
        var reset_result = yield resetTable5();
        if(reset_result == 1){
            console.log('Reset table5 : Success');
        }

        var items = resp.Items;
        var optimizedData = { };
        for(var i = 0 ;i < items.length ;i ++){
            optimizedData[items[i].uniqueid] = items[i];
        }

        for(var key in optimizedData){
            var params = {
                TableName : 'table5_transfer_record',
                Item : optimizedData[key], 
            };
            
            docClient.put(params, function(err, data) {
                if (err) {
                    console.error("Optimization : Add item occured errors : ", JSON.stringify(err, null, 2));
                } else {
                    console.log('Optimization Add item : Success : ', JSON.stringify(data, null, 2));
                }
            });
        }
        return 1;
    }));
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
let handler = function(event, context)
{
    var table3_params = {
        TableName: "table3_advanceuserinfo_spendingrecord",
    };
    
    docClient.scan(table3_params).promise().then(wrapper(function*(resp){
        try {
            const date = getSpecificTime('Asia/Hong_Kong');
            const year = date.year;
            const month = date.month;
            const day = date.day;
            const hours = date.hour;
            const minutes = date.minute;

            let bitBucketPath = 'binarytree-backup/dailybackup/';

            bitBucketPath += year;
            bitBucketPath += '-';
            bitBucketPath += month;
            bitBucketPath += '-';
            bitBucketPath += day;
            bitBucketPath += '-';
            bitBucketPath += hours;
            bitBucketPath += '-';
            bitBucketPath += minutes;

            console.log(bitBucketPath);

            var table3Data = json2csv({ data: resp.Items, fields: fields_table3 });
            uploadToS3(bitBucketPath,'table3_advanceuserinfo_spendingrecord.csv', table3Data, function(error, data){
                if(error){
                    console.log('Backup Table3 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table3_advanceuserinfo_spendingrecord : Success!');
                }
            });

            var table5Data = yield getTableCSVData('table5_transfer_record', fields_table5);
            var optimize_result = yield OptimizeTable5();
            if(optimize_result == 1){
                console.log('Optimize Table5 : Success');
            }
            uploadToS3(bitBucketPath,'table5_transfer_record.csv', table5Data, function(error, data){
                if(error){
                    console.log('Backup Table5 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table5_transfer_record : Success!');
                }
            });
           
            var table6Data = yield getTableCSVData('table6_relatedrecords', fields_table6);
            uploadToS3(bitBucketPath,'table6_relatedrecords.csv', table6Data, function(error, data){
                if(error){
                    console.log('Backup Table6 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table6_relatedrecords : Success!');
                }
            });
            
            var table11Data = yield getTableCSVData('table11_accountbalance', fields_table11);
            uploadToS3(bitBucketPath,'table11_accountbalance.csv', table11Data, function(error, data){
                if(error){
                    console.log('Backup Table11 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table11_accountbalance : Success!');
                }
            });

            var table21Data = yield getTableCSVData('table21_petDetails', fields_table21);
            uploadToS3(bitBucketPath,'table21_petDetails.csv', table21Data, function(error, data){
                if(error){
                    console.log('Backup Table21 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table21_petDetails : Success!');
                }
            });

            var table23Data = yield getTableCSVData('table23_experience', fields_table23);
            uploadToS3(bitBucketPath,'table23_experience.csv', table23Data, function(error, data){
                if(error){
                    console.log('Backup Table23 : Error Occured');
                    console.log(error);
                }
                if(data){
                    console.log('Backup table23_experience : Success!');
                }
            });

        } catch (err) {
            console.log(err);
        }
    }));
};

/*
if(require.main === module) {
  handler({}, {
    succeed: function (res) {
      console.log("Succeeded");
      console.log(res);
    }
  });
}
*/

module.exports.handler = handler;
