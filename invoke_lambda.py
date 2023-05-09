from boto3 import client
import common
import json


def invoke_lambda(lambda_name, payload, invocation_type="RequestResponse"):

    function_name = lambda_name + "-" + common.selected_environment

    response = client("lambda").invoke(FunctionName=function_name, InvocationType=invocation_type, LogType="Tail", Payload=json.dumps(payload))
    if invocation_type == "Event":
        return response  # if it is "Event" is not waiting for response, so response is just metadata
    return json.loads(response["Payload"].read())
