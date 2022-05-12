import csv
from zeep import Client
from zeep import xsd
from datetime import date, datetime
from time import gmtime, strftime
from collections import defaultdict
from pytz import timezone
import os, time
import json
import csv

os.environ['TZ'] = 'America/Asuncion'
ctz=time.tzset()

wsdl = "http://clvmweb.clyfsa.com:81/SEP2WebServices/ManagementService.svc?singleWsdl"
client = Client(wsdl)

request_data ={
    "groupReference": {
      "Name": "MT880-Mineras",
    }
  }
response = client.service.QueryGroupMembers(**request_data)
#print (response)
#Here 'request_data' is the request parameter dictionary.
#Assuming that the operation named 'sendData' is defined in the passed wsdl.

#print (response)
lim = len(response["Devices"]["DeviceReference"])
alldvcs=defaultdict(dict)
dv=[]

with open('/var/www/html/etap/reports/data.csv','w') as f:
    writer = csv.writer(f)
    writer.writerow(('MeterReading_DateTime', 'Meter_ID', 'MW', 'MW Phase A', 'MW Phase B', 'MW Phase C', 'Mvar', 'Mvar Phase A', 'Mvar Phase B', 'Mvar Phase C', 'Vmag', 'Vmag Phase A', 'Vmag Phase B', 'Vmag Phase C', 'Amp', 'Amp Phase A', 'Amp Phase B', 'Amp Phase C', 'PF', 'PF Phase A', 'PF Phase B', 'PF Phase C', 'PF Phase C', 'Quality'))
    for x in range(lim):
        arequest_data ={
        "deviceReference": {
          "Name": response["Devices"]["DeviceReference"][x]["Name"],
        },
        "queryAll": "false",
        "attributeReferences": {
          "AttributeReferencesByGroup": [
            {
              "AttributeGroupType": "DeviceParameters",
              "AttributeReferences": {
                "AttributeReference": {
                  "Name": "Potencia contratada"
                }
              },
              "AllAttributes": "false"
            },
            {
              "AttributeGroupType": "DeviceParameters",
              "AttributeReferences": {
                "AttributeReference": {
                  "Name": "Numero de medidor"
                }
              },
              "AllAttributes": "false"
            },
            {
              "AttributeGroupType": "DeviceParameters",
              "AttributeReferences": {
                "AttributeReference": {
                  "Name": "DeviceID"
                }
              },    
              "AllAttributes": "false"
            }
          ]
        }
        }
        #aresponse = client.service.QueryDeviceAttributes(**arequest_data)
        bwsdl = "http://clvmweb.clyfsa.com:81/SEP2WebServices/DataService.svc?singleWsdl"
        bclient = Client(bwsdl)

        brequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "ActivePowerComb(|+A|+|-A|)_INST_LP1",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        crequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "ReactivePowerPlus_INST_LP1",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        drequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "PowerFactor_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }  
        erequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Voltage_L1_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        } 
        frequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Voltage_L2_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        grequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Voltage_L3_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }  
        hrequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Current_L1_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        irequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Current_L2_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        jrequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "Current_L3_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        } 
        krequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "PowerFactor_L1_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        lrequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "PowerFactor_L2_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }
        mrequest_data ={
            "measurementPointResultTypes": {
            "MeasurementPointResultTypeReferences": {
                "MeasurementPointName": response["Devices"]["DeviceReference"][x]["Name"],
                "AgencyId": "0",
                "ResultTypeNames":  "PowerFactor_L3_INST_LP2",
                
            }
            },
            "intervalStart": "2021-02-01T00:00:00",
            "intervalEnd": "2021-12-31T23:45:00",
            "statusFilter": "null",
            "sourceFilter": {
            "Measured": "true",
            "Manual": "false",
            "Aggregated": "false",
            "Imported": "false",
            "Estimated": "false"
            },
            "resultOrigin": "PreferRaw",
            "lastResultOnly": "true"
        }         
        #bresponse = bclient.service.QueryResults(**brequest_data)
        
        aresponse = client.service.QueryDeviceAttributes(**arequest_data)
        bresponse = bclient.service.QueryResults(**brequest_data)
        cresponse = bclient.service.QueryResults(**crequest_data)
        dresponse = bclient.service.QueryResults(**drequest_data)
        eresponse = bclient.service.QueryResults(**erequest_data)
        fresponse = bclient.service.QueryResults(**frequest_data)
        gresponse = bclient.service.QueryResults(**grequest_data)
        hresponse = bclient.service.QueryResults(**hrequest_data)
        iresponse = bclient.service.QueryResults(**irequest_data)
        jresponse = bclient.service.QueryResults(**jrequest_data)
        kresponse = bclient.service.QueryResults(**krequest_data)
        lresponse = bclient.service.QueryResults(**lrequest_data)
        mresponse = bclient.service.QueryResults(**mrequest_data)
        meterid=response["Devices"]["DeviceReference"][x]["Name"]
        print(aresponse, bresponse, cresponse, dresponse, eresponse, fresponse, gresponse, hresponse, meterid, x)
        if hasattr(bresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    MW=(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    MW=0
            else:
                MW=0
        else:
            MW=0				
        if hasattr(bresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    ts=(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Timestamp)
                else:
                    ts=0
            else:
                ts=0
        else:
            ts=0	    
        if hasattr(cresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(cresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(cresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    MVAR=(cresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    MVAR=0	
            else:
                MVAR=0	
        else:
            MVAR=0
        if hasattr(dresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(dresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(dresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    pf=(dresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    pf=0
            else:
                pf=0
        else:
            pf=0				
        if hasattr(eresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(eresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(eresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    Vmag_a=(eresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    Vmag_a=0
            else:
                Vmag_a=0
        else:
            Vmag_a=0
        if hasattr(fresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(fresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(fresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    Vmag_b=(fresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    Vmag_b=0
            else:
                Vmag_b=0
        else:
            Vmag_b=0			
        if hasattr(gresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(gresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(gresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    Vmag_c=(gresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    Vmag_c=0
            else:
                Vmag_c=0
        else:
            Vmag_c=0
        if hasattr(hresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(hresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(hresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    amp_a=(hresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    amp_a=0	
            else:
                amp_a=0	
        else:
            amp_a=0	
        if hasattr(iresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(iresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(iresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    amp_b=(iresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    amp_b=0
            else:
                amp_b=0
        else:
            amp_b=0
        if hasattr(jresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(jresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(jresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    amp_c=(jresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    amp_c=0
            else:
                amp_c=0
        else:
            amp_c=0
        if hasattr(kresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(kresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(kresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    pf_a=(kresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    pf_a=0
            else:
                pf_a=0
        else:
            pf_a=0
        if hasattr(lresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(lresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(lresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    
                    pf_b=(lresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    pf_b=0
            else:
                pf_b=0
        else:
            pf_b=0			
        if hasattr(mresponse[0].ResultsByResultType, 'ResultTypeResults'):
            if hasattr(mresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
                if hasattr(mresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
                    pf_c=(mresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
                else:
                    pf_c=0
            else:
                pf_c=0
        else:
            pf_c=0
        row = (
            ts,
            meterid,
            MW,
            0, 
            0, 
            0, 
            MVAR, 
            0, 
            0, 
            0, 
            0, 
            Vmag_a, 
            Vmag_b, 
            Vmag_c, 
            0, 
            amp_a, 
            amp_b, 
            amp_b, 
            pf, 
            pf_a, 
            pf_b, 
            pf_c, 
            1,)
        writer.writerow(row)
print(bresponse)