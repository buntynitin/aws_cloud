import os
import yaml
from flask import Flask
from flask import request, jsonify
from delete import delete
from download import download
from ls import ls
from upload import upload
from datetime import datetime, timedelta
from validation import ListRequestSchema, DeleteRequestSchema, UploadRequestSchema, DownloadRequestSchema

app = Flask(__name__)


def load_config():
    dir_root = os.path.dirname(os.path.abspath(__file__))
    with open(dir_root + "/config.yaml", "r") as yamlfile:
        return yaml.load(yamlfile, Loader=yaml.FullLoader)


'''
Generates a SAS for uploading to a bucket in aws

 Endpoint: /upload
 Method: Get
 Content-type: Application/json
 Request-body: { 
                    "container_name" : <Bucket_NAME>,
                    "blob_name" : <KEY_NAME>
               }
 Response: 
            1. OK - 200
                    Response-body : {
                                        "uri": <URI>,
                                        "container_name": <CONTAINER_NAME>,
                                        "blob_name": <BLOB_NAME>,
                                        "expiration_time": <EXPIRATION_TIME>
                                    }

            2. BAD_REQUEST - 400
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
            3. SERVER_ERROR - 500
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
'''


@app.route('/upload', methods=['GET'])
def get_upload_sas():
    try:
        upload_request_schema = UploadRequestSchema()
        errors = upload_request_schema.validate(request.json)
        if errors:
            return jsonify({"error": errors}), 400
        else:
            container_name = request.json['container_name']
            blob_name = request.json['blob_name']
            expiration_time = config['aws_sas_expiration_time']

            uri = upload(
                bucket_name=container_name,
                file_name=blob_name,
                expiration_time=expiration_time
            )

            return jsonify({
                "uri": uri,
                "container_name": container_name,
                "blob_name": blob_name,
                "expiration_time":  datetime.now() + timedelta(minutes=config['aws_sas_expiration_time'])
            })

    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


'''
Generates a SAS for downloading a file from a bucket in aws

 Endpoint: /download
 Method: Get
 Content-type: Application/json
 Request-body: { 
                    "container_name" : <BUCKET_NAME>,
                    "blob_name" : <KEY_NAME>
               }
 Response: 
            1. OK - 200
                    Response-body : {
                                        "uri": <URI>,
                                        "container_name": <CONTAINER_NAME>,
                                        "blob_name": <BLOB_NAME>,
                                        "expiration_time": <EXPIRATION_TIME>
                                    }

            2. BAD_REQUEST - 400
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
            3. SERVER_ERROR - 500
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
'''


@app.route('/download', methods=['GET'])
def get_download_sas():
    try:
        download_request_schema = DownloadRequestSchema()
        errors = download_request_schema.validate(request.json)
        if errors:
            return jsonify({"error": errors}), 400
        else:
            container_name = request.json['container_name']
            blob_name = request.json['blob_name']
            expiration_time = config['aws_sas_expiration_time']

            uri = download(
                bucket_name=container_name,
                file_name=blob_name,
                expiration_time=expiration_time
            )

            return jsonify({
                "uri": uri,
                "container_name": container_name,
                "blob_name": blob_name,
                "expiration_time": datetime.now() + timedelta(minutes=config['aws_sas_expiration_time'])

            })

    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


'''
Lists the files in a bucket

 Endpoint: /list
 Method: Get
 Content-type: Application/json
 Request-body: { 
                    "container_name" : <BUCKET_NAME>,
               }
 Response: 
            1. OK - 200
                    Response-body : [
                                        {
                                            "last_modified": <LAST_MODIFIED_TIME>,
                                            "name": <FILE_NAME>,
                                            "size": <SIZE_IN_BYTES>
                                         },
                                         ....   
                                    ]

            2. BAD_REQUEST - 400
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
            3. SERVER_ERROR - 500
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
'''


@app.route('/list', methods=['GET'])
def list_blobs():
    try:
        list_request_schema = ListRequestSchema()
        errors = list_request_schema.validate(request.json)
        if errors:
            return jsonify({"error": errors}), 400
        else:
            container_name = request.json['container_name']
            res = ls(container_name)
            return jsonify(res)
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


'''
Delete a blob in a container

 Endpoint: /delete
 Method: Delete
 Content-type: Application/json
 Request-body: { 
                    "container_name" : <BUCKET_NAME>,
                    "blob_name" : <FILE_NAME>
               }
 Response: 
            1. OK - 200
                    Response-body : {
                                        "message": "Deleted"
                                     }

            2. BAD_REQUEST - 400
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
            3. SERVER_ERROR - 500
                    Response-body : {
                                        "error" : <ERROR_DESCRIPTION>
                                    }
'''


@app.route('/delete', methods=['DELETE'])
def delete_blob():
    try:
        delete_request_schema = DeleteRequestSchema()
        errors = delete_request_schema.validate(request.json)
        if errors:
            return jsonify({"error": errors}), 400
        else:
            container_name = request.json['container_name']
            blob_name = request.json['blob_name']
            delete(container_name, blob_name)
            return jsonify({
                "message": "Deleted"
            })
    except Exception as ex:
        return jsonify({"error": str(ex)}), 500


if __name__ == '__main__':
    config = load_config()
    app.run(debug=True, port=5002)
