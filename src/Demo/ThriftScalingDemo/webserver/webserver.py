from flask import Flask
from flask import request
from flask import send_from_directory
import base64
import requests
import pyotp
import json
import copy

data = []

app = Flask(__name__, static_url_path='/static')

@app.route("/", methods=["get"])
def display():
    return send_from_directory('static', 'index.html')

@app.route("/postData", methods=["POST"])
def postData():
    time = request.form['time']
    inqueueSize = request.form['inqueueSize']
    numCore = request.form['numCore']

    dataitem = {}
    dataitem["time"] = str(time)
    dataitem["inqueueSize"] = str(inqueueSize)
    dataitem["numCore"] = str(numCore)

    appendData(dataitem)
        
    return "200"

def appendData(dataitem):
    global data
    data.append(dataitem)


@app.route("/getData", methods=["GET"])
def getData():
    dataDump = json.dumps(data)
    clearData()
    return dataDump

def clearData():
    global data
    data.clear()

if __name__ == '__main__':
    app.run(host= '0.0.0.0', port=5000)
