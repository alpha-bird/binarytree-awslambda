'use strict';

let AWS = require('aws-sdk');
let Promise = require('promise');
let wrapper = require('co-express');
let attr = require('dynamodb-data-types').AttributeValue;

/*
AWS.config.update({ accessKeyId: 'AKIAI55QVOCDGWZHWBJA',
 secretAccessKey: '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',
  region: 'us-east-1' });
*/

let docClient = new AWS.DynamoDB.DocumentClient();

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

const transactiondatehourminutes_man = '201706091851';

function putItemToTable11(param){
    const params = {    
        TableName: 'table11_accountbalance',
        //ProjectionExpression: "uniqueid",
        FilterExpression: "uniqueid = :value",
        ExpressionAttributeValues: {
            ":value": param.Item.uniqueid,
        },
    };
    const date = getSpecificTime('Asia/Hong_Kong');
    const year = date.year;
    const month = date.month;
    const day = date.day;
    const hours = date.hour;
    const minutes = date.minute;

    const reportdatehour = `${year}${month > 9 ? month : '0' + month}` +
        `${day > 9 ? day : '0' + day}` +
        `${hours > 9 ? hours : '0' + hours}` + `${minutes > 9 ? minutes : '0' + minutes}`;

    return docClient.scan(params).promise().then(wrapper(function*(data){
      var items = data.Items ? data.Items : undefined;
      var tableItem = undefined;

      if(items !== undefined)
      {
        items.sort((a, b) => {
          if (a.monthly_in_dateHour > b.monthly_in_dateHour)
              return -1;
          if (a.monthly_in_dateHour < b.monthly_in_dateHour)
              return 1;
          return 0;
        });
        tableItem = items[0];
      }      
      
      if( tableItem == undefined )
      {
        var uniqueid = param.Item.uniqueid;
        var transactiondatehourminutes = transactiondatehourminutes_man;
        var balance = param.Item.monthgrandtotal;
        var monthly_in = param.Item.monthgrandtotal;
        var monthly_in_dateHour = reportdatehour;

        let new_params = {
            TableName: 'table11_accountbalance',
            Item: {
              uniqueid,
              transactiondatehourminutes,
              balance,
              monthly_in,
              monthly_in_dateHour
            }
        };
        docClient.put(new_params).promise()
          .then(data => {
            console.log('new item on table11', new_params);
          })
          .catch(err => {
            console.log(err);
          });
      }
      else{
        let update_params = {
          TableName: 'table11_accountbalance',
          Key: {
              uniqueid: tableItem.uniqueid,
              transactiondatehourminutes : tableItem.transactiondatehourminutes,
          },
          UpdateExpression: 'SET monthly_in = :value , balance = :value1, monthly_in_dateHour = :value2',
          ExpressionAttributeValues : {
              ':value' : param.Item.monthgrandtotal,
              ':value1' : param.Item.monthgrandtotal + tableItem.balance,
              ':value2' : reportdatehour,
          }
        };
        yield docClient.update(update_params).promise().then(resp => {
            console.log('Updating table11 : Success!');
        });
      }
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
//updated with cron job
let handler = function(event, context) {
  let rowsCountOfTable1 = 1;
  getAdsIncomes()
    .then(() => {
      console.log('GetScanNumbers');
      return getScanNumbers();
    })
    .then(scans => {
      scanNumbers = scans;
      totalScans = 0;
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
    .then(records => {
      console.log(records);
      const recordsFromTable6 = obArrayToObject(records, 'uniqueid');
      console.log(recordsFromTable6);
      const scanObjects = obArrayToObject(scanNumbers, 'uniqueid');
      let scans = scanNumbers.concat();
      records.forEach(function(advanceUser){
        if(!scanObjects[advanceUser.uniqueid])
          scans.push(recordsFromTable6[advanceUser.uniqueid]);
      })
      scans.forEach(wrapper(function*(record){
        try {
          //Get fixedornot=true count
          const uniqueid = record.uniqueid;
          const scan = getScanNumberFromUniqueId(uniqueid);
          let fixedscannumber = scan.fixedscannumber;
          let scannNumber = scan.scannedbynumber ? scan.scannedbynumber : 0;
          
          let mobilescannumber = scan.mobilescannumber + scannNumber;
          let recordFromTable3 = yield getRecordFromTable3(uniqueid);
          let pastReports = yield getPastReports(uniqueid);
          let pastAccumulatedTotal = pastReports.length > 0 ? pastReports[pastReports.length - 1].accumulatedtotal : 0;
          const paymentstatus = recordFromTable3 ? recordFromTable3.paymentstatus : false;
          const ads_income = adsIncome || 0;
          const totalscanon28 = totalScans;

          const scanratio = (fixedscannumber + mobilescannumber) / totalscanon28;

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
              monthgrandtotal,
              qualifier,
              reportdatehour,         
              scanratio,
              totalscanon28,
              paymentqualifer
            }
          };

          if(day == 28){
            putItemToTable11(params);
          }

          docClient.put(params).promise()
            .then(data => {
              console.log('Generated report for ', params.Item);
            })
            .catch(err => {
              console.log(err);
            });
        } catch(err) {
          console.trace(err);
        }
      }))
    })
    .catch(err => {
      console.trace(err);
    });
};
module.exports.handler = handler;
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