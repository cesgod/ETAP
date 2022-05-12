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
      "Name": "Algesa-Activos",
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
#print(lim)
#lim=20
#def myconverter(o):
#    if isinstance(o, datetime.datetime):
#        return o.__str__()
 
with open('/var/www/html/etap/reports/data.csv', mode='w') as csv_file:
    fieldnames = ['MeterReading_DateTime', 'Meter_ID', 'MW', 'MW Phase A', 'MW Phase B', 'MW Phase C', 'MW Phase A', 'Mvar', 'Mvar Phase A', 'Mvar Phase B', 'Mvar Phase C', 'Vmag', 'Vmag Phase A', 'Vmag Phase B', 'Vmag Phase C', 'Amp', 'Amp Phase A', 'Amp Phase B', 'Amp Phase C', 'PF', 'PF Phase A', 'PF Phase B', 'PF Phase C', 'Quality']
    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerow({'MeterReading_DateTime': '10/3/2016 23:45:00', 'Meter_ID': '9547724000', 'MW': '5.852', 'MW Phase A': '1.950666667', 'MW Phase B': '1.950666667', 'MW Phase C': '1.950666667', 'Mvar': '0.5', 'Mvar Phase A': '0.166666667', 'Mvar Phase B': '0.166666667', 'Mvar Phase C': '0.166666667', 'Vmag': '0.44', 'Vmag Phase A': '0.22', 'Vmag Phase B': '0,22', 'Vmag Phase C': '0.22', 'Amp': '5', 'Amp Phase A': '1.666666667', 'Amp Phase B': '1.666666667', 'Amp Phase C': '1.666666667', 'PF': '95', 'PF Phase A': '95', 'PF Phase B': '95', 'PF Phase C': '95', 'PF Phase C': '95', 'Quality': '1'})
ddd=0
for x in range (1):
	#print(response["Devices"]["DeviceReference"][x]["Name"])  
	#print(x)
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


	ct=strftime("%Y-%m-%d %H:%M:%S", gmtime())
	alldvcs["_id"]=""
	alldvcs["t"]=ct
	alldvcs["EtapId"]='VN_SLV_Load1'
	alldvcs["MeterId"]=(response["Devices"]["DeviceReference"][x]["Name"])
	#print(alldvcs)
	#alldvcs.append(dv)
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][0]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][1]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][2]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][3]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][4]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][5]['Value']['Value'])
	#alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][6]['Value']['Value'])
	#if  aresponse[0]['Attributes']['AttributeInfo'][6]['Value']['Value'] == 'ALGESA 08' or aresponse[0]['Attributes']['AttributeInfo'][6]['Value']['Value'] == 'ALGESA 06' :
	#	alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][6]['Value']['Value'])
	#else:
	#	alldvcs.append(aresponse[0]['Attributes']['AttributeInfo'][7]['Value']['Value'])	
	#print(aresponse[0]['Attributes']['AttributeInfo'][7]['Value']['Value'])
	if hasattr(bresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				MW=(bresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				MW=('n/a')
		else:
			MW=('n/a')
	else:
		MW=('n/a')				
		
	if hasattr(cresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(cresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(cresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				MVAR=(cresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				MVAR=('n/a')	
		else:
			MVAR=('n/a')	
	else:
		MVAR=('n/a')	
	if hasattr(dresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(dresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(dresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				pf=(dresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				pf=('n/a')
		else:
			pf=('n/a')
	else:
		pf=('n/a')				
	if hasattr(eresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(eresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(eresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				MWh=(eresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				MWh=('n/a')
		else:
			MWh=('n/a')
	else:
		MWh=('n/a')
	if hasattr(fresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(fresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(fresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				Vmag_a=(fresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				Vmag_a=('n/a')
		else:
			Vmag_a=('n/a')
	else:
		Vmag_a=('n/a')			
	if hasattr(gresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(gresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(gresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				Vmag_b=(gresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				Vmag_b=('n/a')
		else:
			Vmag_b=('n/a')
	else:
		Vmag_b=('n/a')
	if hasattr(hresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(hresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(hresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				amp_a=(hresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				amp_a=('n/a')	
		else:
			amp_a=('n/a')	
	else:
		amp_a=('n/a')	
	if hasattr(iresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(iresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(iresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				amp_b=(iresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				amp_b=('n/a')
		else:
			amp_b=('n/a')
	else:
		amp_b=('n/a')
	if hasattr(jresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(jresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(jresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				amp_c=(jresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				amp_c=('n/a')
		else:
			amp_c=('n/a')
	else:
		amp_c=('n/a')
	if hasattr(kresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(kresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(kresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				pf_a=(kresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				pf_a=('n/a')
		else:
			pf_a=('n/a')
	else:
		pf_a=('n/a')
	if hasattr(lresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(lresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(lresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				
				pf_b=(lresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				pf_b=('n/a')
		else:
			pf_b=('n/a')
	else:
		pf_b=('n/a')			
	if hasattr(mresponse[0].ResultsByResultType, 'ResultTypeResults'):
		if hasattr(mresponse[0].ResultsByResultType.ResultTypeResults[0], 'Results'):
			if hasattr(mresponse[0].ResultsByResultType.ResultTypeResults[0].Results, 'Result'):
				pf_c=(mresponse[0].ResultsByResultType.ResultTypeResults[0].Results.Result[0].Value.Value)
			else:
				pf_c=('n/a')
		else:
			pf_c=('n/a')
	else:
		pf_c=('n/a')			

	writer.writerow({'MeterReading_DateTime': ct, 'Meter_ID': '9547724000', 'MW': MW, 'MW Phase A': Vmag_a, 'MW Phase B': Vmag_b, 'MW Phase C': Vmag_b, 'Mvar': MVAR, 'Mvar Phase A': '0.166666667', 'Mvar Phase B': '0.166666667', 'Mvar Phase C': '0.166666667', 'Vmag': '0.44', 'Vmag Phase A': '0.22', 'Vmag Phase B': '0,22', 'Vmag Phase C': '0.22', 'Amp': '5', 'Amp Phase A': '1.666666667', 'Amp Phase B': '1.666666667', 'Amp Phase C': '1.666666667', 'PF': '95', 'PF Phase A': '95', 'PF Phase B': '95', 'PF Phase C': '95', 'PF Phase C': '95', 'Quality': '1'})	
	#print(bresponse[0]["ResultsByResultType"]["ResultTypeResults"][0]["Results"]["Result"][0]["Value"]["Value"])
	#alldvcs.append('NOPE')
	#ele = (input("Name : ")) 
    #d = json.loads(s)
print(writer)
print (alldvcs)

#with open('/var/www/html/etap/reports/2021-01.json', 'w', encoding='utf-8') as f:
#    json.dump(alldvcs, f, ensure_ascii=False, indent=4)

	# prints [1,3,5]    
#aresponse = aclient.service.QueryDeviceAttributes(**raequest_data)

#print (alldvcs)

