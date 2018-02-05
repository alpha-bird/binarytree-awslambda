'use strict';

var AWS = require('aws-sdk');
var Promise = require('promise');
var wrapper = require('co-express');
var attr = require('dynamodb-data-types').AttributeValue;
/*
AWS.config.update({ accessKeyId: 'AKIAI55QVOCDGWZHWBJA',
 secretAccessKey: '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',
  region: 'us-east-1' });
*/
///AWS Optimized Table1 to Table 12, Table23

var docClient = new AWS.DynamoDB.DocumentClient();

const initialLevel = 1;
var sevents = {
  "Records": [
    {
      "eventID": "1",
      "eventVersion": "1.0",
      "dynamodb": {
        "Keys": {
          "uniqueid": {
            "S": "mark"
          }
        },
        "NewImage": {
          "uniqueid" : {
              "S" : "mark"
          },
          "datehourminuteseconduniqueid" : {
              "S" : "20170710196298#mark"
          },
          "fixedscannumber" : {
              "N" : "7.5"
          },
          "mobilescannumber" : {
              "N" : "0"
          },
          "codeuseruniqueid" : {
              "S" : "peter"
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
]};

function getItem(tbName, uniqueid)
{
    const params = {    
        TableName: tbName,
        KeyConditionExpression: "uniqueid = :value",
        ExpressionAttributeValues: {
        ":value": uniqueid
        }
    };

    return docClient.query(params).promise()
      .then(data =>  data.Items[0] ? data.Items[0] : undefined);
}

// get highest level between  scanner and scanned

function getLevelFromTable23(scannerid, scannedid){
    return getItem('table23_experience', scannerid).then(wrapper(function*(resp){
        let scanner_level = 1;
        let scanned_level = 1;
        
        if(resp != undefined)
            scanner_level = resp.level1;
        var scannedItem = yield getItem('table23_experience', scannedid);
        if(scannedItem != undefined)
            scanned_level = scannedItem.level1;
        return scanner_level >= scanned_level ? scanner_level : scanned_level;
    }));
}

function updateScannerItemTable12(newRecord){
    let datehourminuteseconduniqueid = newRecord.datehourminuteseconduniqueid;
    let fixedscannumber = newRecord.fixedscannumber ? newRecord.fixedscannumber : 0;
    let mobilescannumber = newRecord.mobilescannumber ? newRecord.mobilescannumber : 0;

    let scannerid = newRecord.uniqueid;
    let scannedid = newRecord.codeuseruniqueid;

    return getItem('table12_sync_table1', scannerid).then(wrapper(function*(scannerItem){
        let level = yield getLevelFromTable23(scannerid, scannedid);
        let levelValue = 1;

        if(level != 1)
            levelValue = 1 + level / 1000;
        
        if(scannerItem == undefined){
            console.log('create new one');
            
            let scannerParam = {
                TableName: 'table12_sync_table1',
                Item : {
                    uniqueid : scannerid,
                    datehourminuteseconduniqueid : datehourminuteseconduniqueid,
                    fixedscannumber : fixedscannumber * levelValue,
                    mobilescannumber : mobilescannumber * levelValue,
                    scannedbynumber : 0,
                }
            };
            console.log(scannerParam);

            yield docClient.put(scannerParam).promise().then(resp => {
                console.log('success adding table12 scanner');
            });
        }
        else{
            console.log('scanner information updating...');

            let scannerParamUpdate = {
                TableName: 'table12_sync_table1',
                Key : {
                    uniqueid : scannerid,
                },
                UpdateExpression :'SET datehourminuteseconduniqueid = :dateHour, fixedscannumber = :fixNum, mobilescannumber = :mNum',
                ExpressionAttributeValues : {
                    ':dateHour' : datehourminuteseconduniqueid,
                    ':fixNum' : fixedscannumber * levelValue + scannerItem.fixedscannumber,
                    ':mNum' : mobilescannumber * levelValue + scannerItem.mobilescannumber,
                },
            };
            
            yield docClient.update(scannerParamUpdate).promise().then(node => {
                console.log('success update table12 scanner');
            });
        }
    }));
}

function updateScannedItemTable12(newRecord){
    let scannerId = newRecord.uniqueid;
    let scannedId = newRecord.codeuseruniqueid;
    
    return getItem('table12_sync_table1', scannedId).then(wrapper(function*(scannedItem){
        let level = yield getLevelFromTable23(scannerId, scannedId);
        
        let levelValue = 1;
        if(level != 1)
            levelValue = 1 + level / 1000;

        if(scannedItem == undefined){
            console.log('create scanned : ');
            let scannedParam = {
                TableName: 'table12_sync_table1',
                Item : {
                    uniqueid : scannedId,
                    datehourminuteseconduniqueid : newRecord.datehourminuteseconduniqueid,
                    fixedscannumber : 0,
                    mobilescannumber : 0,
                    scannedbynumber : levelValue,
                }
            }
            console.log(scannedParam);
            yield docClient.put(scannedParam).promise().then(resp => {
                console.log('success adding scanned on table12');
            });
        }
        else{
            console.log('update scanned');
            let scannedParam_update = {
                TableName: 'table12_sync_table1',
                Key : {
                    uniqueid : scannedId,
                },
                UpdateExpression :'SET datehourminuteseconduniqueid = :date, scannedbynumber = :scnbyNum',
                ExpressionAttributeValues : {
                    ':date' : newRecord.datehourminuteseconduniqueid,
                    ':scnbyNum' : scannedItem.scannedbynumber + levelValue,
                }
            }
            yield docClient.update(scannedParam_update).promise().then(node => {
                console.log('success update scanned on table12');
            });
        }
    }));
}

function updateTable12(event, context){
    console.log('updating table12....');
    
    var newRecord = attr.unwrap(event.Records[0].dynamodb.NewImage);

    console.log('NewImage : ');
    console.log(newRecord);

    let scannerId = newRecord.uniqueid;
    let scannedId = newRecord.codeuseruniqueid;

    return getItem('table12_sync_table1', scannerId).then(wrapper(function*(resp){
        //update Scanner
        yield updateScannerItemTable12(newRecord);
       
        //update Scanned
        yield updateScannedItemTable12(newRecord);
    }));
}

//uid : scanner or scanned uuid
//table23Item : scanner or scanned on table23

function updateTable23Item(uid){    
    return getItem('table23_experience', uid).then(wrapper(function*(table23Item){
        let table12Item = yield getItem('table12_sync_table1', uid);

        let fixedscannumber = table12Item.fixedscannumber;
        let mobilescannumber = table12Item.mobilescannumber;
        let scannedbynumber = table12Item.scannedbynumber;

        let accumulatedScanNumber = fixedscannumber + mobilescannumber + scannedbynumber;
        let target = 100;

        let level = Math.round(accumulatedScanNumber/target);
        
        let low = Math.floor(accumulatedScanNumber/target);
        let high = Math.ceil(accumulatedScanNumber/target);

        if(level == 0)
            level = 1;

        let addition = 1;
        if(high == level)
            addition = 0;
        
        let remainder = (accumulatedScanNumber + target)/target - level - addition;

        if(table23Item == undefined){
            //create new one
            let params = {
                TableName: 'table23_experience',
                Item : {
                    uniqueid : uid,
                    accumulatedScanNumber : accumulatedScanNumber,
                    level1 : level,
                    target : target,
                    remainder1 : remainder,
                }
            }

            console.log('create new one');
            console.log(params);

            yield docClient.put(params).promise().then(resp => {
                console.log("sucessfully added to table23_experience");
            });
        }
        else{
            let updateparams = {
                TableName: 'table23_experience',
                Key : {
                    uniqueid : uid,
                },
                UpdateExpression :'SET accumulatedScanNumber = :acNum, level1 = :levelNum, target = :targetNum, remainder1 = :remainNum',
                ExpressionAttributeValues : {
                    ':acNum' : accumulatedScanNumber,
                    ':levelNum' : level,
                    ':targetNum' : target,
                    ':remainNum' : remainder,
                }
            }
            console.log('updating...');
            console.log(updateparams);

            yield docClient.update(updateparams).promise().then(node => {
                console.log('successfully updated');
            });
        }
    }));    
}

function updateTable23(event, context){
    console.log('Updating table23....');
    var newRecord = attr.unwrap(event.Records[0].dynamodb.NewImage);
    
    console.log('new image : ');
    console.log(newRecord);    

    return getItem('table23_experience', newRecord.uniqueid).then(wrapper(function*(scannerItemTable23){
        //scanner update or add
        
        yield updateTable23Item(newRecord.uniqueid);

        //scanned update
        yield updateTable23Item(newRecord.codeuseruniqueid);
    }));
}

function deleteItem(params){
    return docClient.delete(params).promise().then(resp => 1);
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

function OptimizeTable1(){
    var table1_params = {
        TableName: "table1_geoqrcoderecord",
    };

    return docClient.scan(table1_params).promise().then(wrapper(function*(resp){
        var reset_result = yield resetTable1();
        if(reset_result == 1){
            console.log('Reset table1 : Success');
        }

        var items = resp.Items;
        var optimizedData = { };
        for(var i = 0 ;i < items.length ;i ++){
            optimizedData[items[i].uniqueid] = items[i];
        }

        for(var key in optimizedData){
            var params = {
                TableName : 'table1_geoqrcoderecord',
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

var handler = wrapper(function*(event, context){
    if(event.Records[0].eventName != "REMOVE")
    {
        //yield OptimizeTable1();
        yield updateTable12(event, context);
        yield updateTable23(event, context);
    }
});
/*
handler(sevents, {
   succeed: function(res){
     console.log("Succeeded");
     console.log(res);
   }
 });
*/
module.exports.handler = handler;
