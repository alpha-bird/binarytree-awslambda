'use strict';

var AWS = require('aws-sdk');
var Promise = require('promise');
var wrapper = require('co-express');
var attr = require('dynamodb-data-types').AttributeValue;
var json2csv = require('json2csv');

AWS.config.update({ accessKeyId: 'AKIAI55QVOCDGWZHWBJA',
 secretAccessKey: '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',
  region: 'us-east-1' });

var docClient = new AWS.DynamoDB.DocumentClient();
var s3Client = new AWS.S3({apiVersion: '2006-03-01'});

//Read table8 references
function readLimits(){
  const params = {
    TableName: 'table8_reference'
  };
  return docClient.scan(params).promise().then(res => res.Items);
}

let adsIncome = 0;
let limits = [];
let scanNumbers = [];
let totalScans = 0;
let limitObjects;
let adsIncomes = {};

function getRecordsFromTable6() {
  let params = {
    TableName: 'table6_relatedrecords',
    FilterExpression: 'fullHouse=:value',
    ExpressionAttributeValues: {
      ':value': false
    }
  };
  return docClient.scan(params).promise().then(data => data.Items ? data.Items : []);
}

function getRecordsFromTable1(uniqueid){
  let params = {
    TableName: 'table1_geoqrcoderecord',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    },
    ProjectionExpression: 'uniqueid, fixedornot'
  };
  return docClient.query(params).promise().then(data => data.Items ? data.Items : []);
}

function getAdsIncomes(){
  let params = {
    TableName: 'table20_ads_income'
  };
  return docClient.scan(params).promise()
    .then(data => {
      if(data.Items){
        return adsIncome = data.Items[data.Items.length - 1].ads_income;
      }
      return 0;
    })
    .catch(err => {
      console.log(err);
    })
}
function getLimit(leftCount, rightCount) {
  const min = Math.min(leftCount, rightCount);
  for (let i = limits.length - 1; i >= 0; i--) {
    if (min >= limits[i]) {
      return limits[i];
    }
  }
  return limits[0];
}

function getReferenceFromLimit(limit) {
  for (let i = 0; i < limits.length; i++){
    if(limitObjects[i].relatedbelow01number == limit)
      return limitObjects[i].reference;
  }
  return 0;
}
function getFixedOrNotCount(records, value){
  let count = 0;
  records.forEach(record => {
    if(record.fixedornot === value)
      count++;
  });
  return count;
}

function getRecordFromTable3(uniqueid){
  let params = {
    TableName: 'table3_advanceuserinfo_spendingrecord',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    }
  };
  return docClient.query(params).promise()
    .then(data => data.Items[0] ? data.Items[0] : undefined);
}

function getPastReports(uniqueid){
  let params = {
    TableName: 'table9_report',
    KeyConditionExpression: 'uniqueid=:value',
    ExpressionAttributeValues: {
      ':value': uniqueid
    }
  };
  return docClient.query(params).promise()
    .then(data => data.Items ? data.Items : []);
}

function getRelatedBelowCount(uniqueid) {
  let params = {
    TableName: "table6_relatedrecords",
    IndexName: "RELATEDABOVE",
    KeyConditionExpression: "#attr = :value",
    ExpressionAttributeNames: {
      "#attr": "relatedabove"
    },
    ExpressionAttributeValues: {
      ":value": uniqueid
    }
  };
  return docClient.query(params).promise().then(data => data.Items[0] ? data.Items.length : 0);
}

function getScanNumbers(){
  let params = {
    TableName: "table12_sync_table1"
  };
  return docClient.scan(params).promise().then(data => data.Items);
}

function getScanNumberFromUniqueId(uniqueid){
  if(scanNumbers){
    for (let i = 0; i < scanNumbers.length; i++)
      if(scanNumbers[i].uniqueid == uniqueid)
        return scanNumbers[i];
  }
  return {
    uniqueid,
    fixedscannumber: 0,
    mobilescannumber: 0
  }
}

function obArrayToObject(array, key) {
  let res = {};
  if(!array || !(array.length > 0))
    return res;
  array.forEach((record) => {
    if(record[key])
      res[record[key]] = record;
  });
  return res;
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

let toTable9 = function(event, context) {
  let rowsCountOfTable1 = 1;
  return getAdsIncomes()
    .then(() => {
      console.log('GetScanNumbers');
      return getScanNumbers();
    })
    .then(scans => {
      scanNumbers = scans;
      totalScans = 0.0;
      scanNumbers.forEach(scan => {
        totalScans += (scan.fixedscannumber + scan.mobilescannumber + scan.scannedbynumber);
      });     

      console.log('ReadLimits');
      return readLimits();
    })
    .then(res => {
      limitObjects = res;
      console.log(res);
      res.sort((a, b) => {
        if (a.point > b.point)
          return 1;
        if (a.point < b.point)
          return -1;
        return 0;
      });
      limits = [];
      res.forEach(function (a) {
        limits.push(a.relatedbelow01number);
      });
      console.log("Limits: ", limits);
      return getRecordsFromTable6();
    })
    .then(wrapper(function*(records){
      console.log(records);
      const recordsFromTable6 = obArrayToObject(records, 'uniqueid');
      console.log(recordsFromTable6);
      const scanObjects = obArrayToObject(scanNumbers, 'uniqueid');
    
      let scans = scanNumbers.concat();
    
      records.forEach(function(advanceUser){
        if(!scanObjects[advanceUser.uniqueid])
          scans.push(recordsFromTable6[advanceUser.uniqueid]);
      })

      var scanData = {   };
      for(var i = 0 ; i < scans.length ; i ++){
            scanData[scans[i].uniqueid] = scans[i];
      }

      for(var key in scanData ) {
        var record = scanData[key];
        try {
          //Get fixedornot=true count
          const uniqueid = record.uniqueid;
          const scan = getScanNumberFromUniqueId(uniqueid);
          let fixedscannumber = scan.fixedscannumber;

          var scanNum = scan.scannedbynumber ? scan.scannedbynumber : 0;
         
          let mobilescannumber = scan.mobilescannumber + scanNum;
          let scannedbynumber = scanNum;
         
          let recordFromTable3 = yield getRecordFromTable3(uniqueid);
          let pastReports = yield getPastReports(uniqueid);
          let pastAccumulatedTotal = pastReports.length > 0 ? pastReports[pastReports.length - 1].accumulatedtotal : 0;
          const paymentstatus = recordFromTable3 ? recordFromTable3.paymentstatus : false;
          const ads_income = adsIncome || 0;
          const totalscanon28 = totalScans;

          const scanratio = (fixedscannumber + scan.mobilescannumber + scannedbynumber) / totalscanon28;

          const amountfortotalscan = scanratio * ads_income;
          
          let amountforadvanceuser = 0;
          const relatedLeftCount = recordsFromTable6[uniqueid] ? recordsFromTable6[uniqueid].relatedLeftCount : 0;
          const relatedRightCount = recordsFromTable6[uniqueid] ? recordsFromTable6[uniqueid].relatedRightCount : 0;
          //Get from table6, 8
          let qualifier = false;
          let paymentqualifer = false;
          let monthgrandtotal = 0;
          const relatedBelowCount = yield getRelatedBelowCount(uniqueid);
          if (paymentstatus && fixedscannumber >= 30) {
            qualifier = true;
            if (relatedBelowCount >= 2){
              paymentqualifer = true;
              const recordLimit = getLimit(relatedLeftCount, relatedRightCount);
              amountforadvanceuser = getReferenceFromLimit(recordLimit);
              monthgrandtotal = amountfortotalscan + amountforadvanceuser;
            } else {
              paymentqualifer = false;
              monthgrandtotal = amountfortotalscan;
            }
          }
          if (!paymentstatus && fixedscannumber >= 30) {
            qualifier = true;
            // const recordLimit = getLimit(relatedLeftCount, relatedRightCount);
            // amountforadvanceuser = getReferenceFromLimit(recordLimit);
            monthgrandtotal = amountfortotalscan;
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

          let accumulatedtotal = pastAccumulatedTotal + monthgrandtotal;

          if(day != 28){
            accumulatedtotal = 0;
            monthgrandtotal = 0;
          }

          let params = {
            TableName: 'table9_report',
            Item: {
              uniqueid,
              accumulatedtotal,
              ads_income,
              paymentstatus,
              amountforadvanceuser,
              amountfortotalscan,
              fixedscannumber,
              mobilescannumber,
              scannedbynumber,
              monthgrandtotal,
              qualifier,
              reportdatehour,
              scanratio,
              totalscanon28,
              paymentqualifer
            }
          };
          
          var putResult = yield docClient.put(params).promise()
            .then(data => {
              console.log('Generated report for ', params.Item);
              return 1;
            })
            .catch(err => {
              console.log(err);
              return -1;
            });
        } catch(err) {
          console.trace(err);
        }
      }
      return 1;
    }))
    .catch(err => {
      console.trace(err);
    });
};

const fields_table9 = [
    'uniqueid',
    'reportdatehour',
    'accumulatedtotal',
    'ads_income',
    'amountforadvanceuser',
    'amountfortotalscan',
    'fixedscannumber',
    'mobilescannumber',
    'monthgrandtotal',
    'paymentqualifer',
    'paymentstatus',
    'qualifier',
    'scanratio',
    'totalscanon28',
    'scannedbynumber',
];

const fields_table12 = [
    'uniqueid',
    'datehourminuteseconduniqueid',
    'fixedscannumber',
    'mobilescannumber',
    'scannedbynumber',
];

const fields_table20 = [
    'primary',
    'datehourminute',
    'adminid',
    'ads_income',
];

const fields_table22 = [
    'primary',
    'dateHourMinutesSecond',
    'businessname',
    'category',
    'city',
    'country',
    'datehourminuteseconduniqueid',
    'description',
    'email',
    'fixedornot',
    'phonenumber',
    'price',
    'productservice',
    'specialcode',
    'street1',
    'street2',
    'street3',
    'uniqueid',
    'userlocationlatitude',
    'userlocationlongitude',
    'webpage',
    'isSelect',
    'imageUrl',
    'movingnumber',
    'levelStr',
    'report1',
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

function backupTable(tableName, tableField, done){
    var tableParam = {
        TableName: tableName,
    };

    return docClient.scan(tableParam).promise().then(wrapper(function*(resp){
        const date = getSpecificTime('Asia/Hong_Kong');
        const year = date.year;
        const month = date.month;
        const day = date.day;
        const hours = date.hour;
        const minutes = date.minute;

        let bitBucketPath = 'binarytree-backup/monthlybackup/';

        bitBucketPath += year;
        bitBucketPath += '-';
        bitBucketPath += month;
        bitBucketPath += '-';
        bitBucketPath += day;

        console.log(bitBucketPath);

        var tableData = yield getTableCSVData(tableName, tableField);
        uploadToS3(bitBucketPath, tableName + '.csv', tableData, done);
    }));
}

function resetTable1(){
    var params = {
        TableName: 'table1_geoqrcoderecord',
    }
    return docClient.scan(params).promise().then(wrapper(function*(resp){
        var items = resp.Items;
        for(var i = 0; i < items.length ; i ++){
            var item_para = {
                TableName : 'table1_geoqrcoderecord',
                Key:{
                    'uniqueid' : items[i].uniqueid,
                    'datehourminuteseconduniqueid' : items[i].datehourminuteseconduniqueid,
                },
            }
            
            var rst = yield deleteItem(item_para);
        }
        return 1;
    }));
}

function deleteItem(params){
    return docClient.delete(params).promise().then(resp => 1);
}

function resetTable9(){
    var params = {
        TableName: 'table9_report',
    }
    return docClient.scan(params).promise().then(wrapper(function*(resp){
        var items = resp.Items;
        for(var i = 0; i < items.length ; i ++){
            var item_para = {
                TableName : 'table9_report',
                Key:{
                    'uniqueid' : items[i].uniqueid,
                    'reportdatehour' : items[i].reportdatehour,
                },
            }
            
            var rst = yield deleteItem(item_para);
        }
        return 1;
    }));
}

function resetTable12(){
    var params = {
        TableName: 'table12_sync_table1',
    }
    return docClient.scan(params).promise().then(wrapper(function*(resp){
        var items = resp.Items;
        for(var i = 0; i < items.length ; i ++){
            var item_para = {
                TableName : 'table12_sync_table1',
                Key:{
                    'uniqueid' : items[i].uniqueid,
                },
            }
            
            var rst = yield deleteItem(item_para);
        }
        return 1;
    }));
}

function OptimizeTable9(){
    var table9_params = {
        TableName: "table9_report",
    };

    return docClient.scan(table9_params).promise().then(wrapper(function*(resp){
        var reset_result = yield resetTable9();
        if(reset_result == 1){
            console.log('Reset table9 : Success');
        }

        var items = resp.Items;
        var optimizedData = { };
        for(var i = 0 ;i < items.length ;i ++){
            optimizedData[items[i].uniqueid] = items[i];
        }

        for(var key in optimizedData){
            var params = {
                TableName : 'table9_report',
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

function setFixedorNotTable22(){
    var table22_params = {
        TableName: "table22_message_ads",
    };

    return docClient.scan(table22_params).promise().then(wrapper(function*(resp){
        var items = resp.Items;
        for(var i = 0; i < items.length ; i ++){
          if(items[i].fixedornot != 1){
            var params = {
              TableName: "table22_message_ads",
              Key: {
                primary: items[i].primary,
                dateHourMinutesSecond: items[i].dateHourMinutesSecond
              },
              UpdateExpression: 'SET fixedornot = :value',
              ExpressionAttributeValues : {
                ':value' : 1
              }
            };
            yield docClient.update(params).promise().then();
          }
        }
        return 1;
    }));
}

let handler = function(event, context){
    var generateTable9 = wrapper(function*(){
        var result = yield toTable9( { }, { } );
        console.log('Generating table9 : Success');
        yield resetTable1(); //reset table1
        console.log('Rest table1 : success');
        if(result == 1){ 
            yield backupTable('table9_report', fields_table9, function(err, data){
                if(data){
                    console.log('Backup Table9 : Success');
                }
            });
            var opt_table9_result = yield OptimizeTable9();
            console.log('Optimize table9 : success');
        }
        
        yield backupTable('table12_sync_table1', fields_table12, function(err, data){
                if(data){
                    console.log('Backup Table12 : Success');
                }
            });
        yield resetTable12();
        
        yield backupTable('table20_ads_income', fields_table20, function(err, data){
                if(data){
                    console.log('Backup Table20 : Success');
                }
            });
        yield backupTable('table22_message_ads', fields_table22, function(err, data){
                if(data){
                    console.log('Backup Table22 : Success');
                }
            });
        yield setFixedorNotTable22();
    });

    generateTable9();
}

if(require.main === module) {
  handler({}, {});
}

module.exports.handler = handler;