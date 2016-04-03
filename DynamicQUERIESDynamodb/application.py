#Team of 3
#Team Member :Abhitej Date (1001113870),Rasika Dhanurkar (1001110582),Sagar Lakhia (1001123182)
#Course Number : CSE 6331-002
#Lab Number : Programming Assignment 5

from flask import Flask, render_template, request, url_for
import boto
import urllib2
import sys
import argparse
import time
import datetime
import csv, StringIO
import os
import time
from random import randint
import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
import datetime
import os


# Initialize the Flask application
application = Flask(__name__)

# Define a route for the default URL, which loads the form
@application.route('/')
def form():
    #insertData()
    tableStatus=getTableStatus()
    return render_template('form_submit.html',tableStatus=tableStatus)
def downloadData():
    s3 = boto.connect_s3()
    key = s3.get_bucket('cloudabhitejcs6331').get_key('Consumer_Complaints.csv')
    reader = csv.reader(StringIO.StringIO(key.get_contents_as_string()), csv.excel)
    return reader
def getTableStatus():
    conn = boto.dynamodb.connect_to_region('us-east-1');
    try:
        tdescr=conn.describe_table('consumer_complaint')
        consumer_complaint = Table('consumer_complaint')
        consumer_complaint = consumer_complaint.scan(
        Complaint_ID__null=False,
        limit=2)
        count=0;
        for consumer_complaints in consumer_complaint:
             count=count+1
        if count==0:
             return 'false'
        else:
             return 'true'
        return 'true'
    except:
        return 'false'
@application.route('/', methods=['POST'])   
def goBack():
    tableStatus=getTableStatus()
    return render_template('form_submit.html',tableStatus=tableStatus)
    
def createTable():
    consumer_complaint = Table.create('consumer_complaint', schema=[
    HashKey('Complaint_ID'), # defaults to STRING data_type

    ], throughput={
    'read': 5,
    'write': 15,
    }, global_indexes=[
    GlobalAllIndex('EverythingIndex', parts=[
     HashKey('State'),
    ],
    throughput={
    'read': 1,
    'write': 1,
    })
    ],
    # If you need to specify custom parameters, such as credentials or region,
    # use the following:
    connection=boto.dynamodb2.connect_to_region('us-east-1')
    )
    return consumer_complaint

@application.route('/insertData/', methods=['POST'])
def insertData():
    conn = boto.dynamodb.connect_to_region('us-east-1');
    try:
        tdescr=conn.describe_table('consumer_complaint')
        consumer_complaint=Table('consumer_complaint')
    except:     
        consumer_complaint = createTable()
    try:    
        isTableActive='false'
        while isTableActive=='false':
            tdescr=conn.describe_table('consumer_complaint')
            if(((tdescr['Table'])['TableStatus']) == 'ACTIVE'):
                #consumer_complaint=Table('consumer_complaint')
                start_time = time.time()
                reader=downloadData()
                i=0
                for row in reader:
                                if i==0:
                                    i=i+1
                                else:
                                    with consumer_complaint.batch_write() as batch:
                                        batch.put_item(data={
                                                'Complaint_ID' : row[0],
                                                'Product' : row[1],
                                                'Sub-product' : row[2],
                                                'Issue' : row[3],
                                                'State' : row[4],
                                                'ZIP_code' : row[5],
                                                'Company' : row[6],
                                                'Company_response' : row[7],
                                                'Timely_response?' : row[8],
                                                'Consumer_disputed': row[9],
                                        })
                print("--- Time %s in seconds for Insert Query ---" % (time.time() - start_time))
                time=time.time() - start_time
                isTableActive='true'
    except:
        consumer_complaint.delete()
        return render_template('form_submit.html',tableStatus='false')
   #consumer_complaint.delete()
    return render_template('form_submit.html',tableStatus='true')

# Define a route for the action of the form, for example '/hello/'
# We are also defining which type of requests this route is 
# accepting: POST requests in this case
@application.route('/getData/', methods=['POST'])
def getData():
    state=request.form['state']
    consumer_complaint = Table('consumer_complaint')
#    consumer_complaint=getState(lat)               
   # query=request.form['query']
    start_time = time.time()
    many_consumer_complaint = [consumer_complaint.scan(State__eq=state),consumer_complaint.scan(Product='Bank account or service')]
    printdata=[]


    for consumer_complaint in many_consumer_complaint[0]:
           #print consumer_complaint['Complaint_ID']
           printdata.append(dict([('Complaint_ID',consumer_complaint['Complaint_ID']),('Product',consumer_complaint['Product']),('Sub-product',consumer_complaint['Sub-product']),
                                  ('Issue',consumer_complaint['Issue']),('State',consumer_complaint['State']),('ZIP_code',consumer_complaint['Zip_code']),('Company',consumer_complaint['Company']),
                                  ('Company_response',consumer_complaint['Company_response']),('Timely_response?',consumer_complaint['Timely_response?']),('Consumer_disputed',consumer_complaint['Consumer_disputed'])]))

    return render_template('form_action.html', state=state, query=printdata,time=time.time() - start_time)

# Run the app :)
if __name__ == '__main__':
  application.run( 
        host="localhost",
        port=int("8888"),debug=True
        
  )
