import logging
import boto3
import common
from boto3.dynamodb.conditions import Key
from default_ddb_persistent_data_response import default_persistent_ddb_response

mandatory_fields = {
    'TENANTCONFIG':
    [
        'newOrderStatus',
        'excessRTODefinition',
        'updatedAt',
        'excessNonRTODefinition',
        'nonManageableOrderStatuses',
        'selectionKey',
        'type'
    ],
}
persistent_ddb_types = [
    'TENANTCONFIG', 'USERINFO', 'ROLE', 'REASONCODE', 'PLACE', 'PLACETAG', 'SELECTOR'
]
persistent_data = {}


def delete_ddb():

    # DDB set-up
    dyn_resource = boto3.resource("dynamodb")
    table = dyn_resource.Table(common.default_table)

    for ddb_type in persistent_ddb_types:

        # Read persistent information
        ddb_persistent_data_response = table.query(KeyConditionExpression=Key("type").eq(ddb_type))["Items"]
        logging.info(
            '[delete_ddb] Table: %s, Type: %s, Persistent actual values: %s',
            common.default_table,
            ddb_type,
            ddb_persistent_data_response
        )

        # Default values if applicable:
        for ddb_type_check in persistent_ddb_types:
            if ddb_type == ddb_type_check and len(ddb_persistent_data_response) == 0 and ddb_type_check in default_persistent_ddb_response:

                ddb_persistent_data_response = default_persistent_ddb_response[
                    ddb_type_check
                ]
                logging.info(
                    '[delete_ddb] Defaults taken for ddb_type: %s; with values: %s',
                    ddb_type_check,
                    ddb_persistent_data_response
                )

        # Check that all necessary fields are included
        if ddb_type in mandatory_fields:
            for necessary_field in mandatory_fields[ddb_type]:
                if necessary_field not in ddb_persistent_data_response[0]:
                    raise Exception(
                        "Missing necessary fields in ddb_type:",
                        ddb_type,
                        ". Importers cannot proceed. Missing field:",
                        necessary_field
                    )

        # Add the persistent data to persistent_data object:
        persistent_data[ddb_type] = ddb_persistent_data_response

    logging.info(
        '[delete_ddb] persistent_data: %s',
        persistent_data
    )

    # Delete table
    logging.info('[delete_ddb] Deleting table: %s', common.default_table)
    table.delete()
    table.wait_until_not_exists()
    logging.info('[delete_ddb] Table deleted and confirmed it does not exist. Now: creating table...')

    # Create table https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.create_table
    params = {
        "TableName": common.default_table,
        "KeySchema": [{"AttributeName": "type", "KeyType": "HASH"}, {"AttributeName": "selectionKey", "KeyType": "RANGE"}],
        "AttributeDefinitions": [{"AttributeName": "type", "AttributeType": "S"}, {"AttributeName": "selectionKey", "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": {"StreamEnabled": True, "StreamViewType": "NEW_AND_OLD_IMAGES"}
        # 'ProvisionedThroughput': {
        #     'ReadCapacityUnits': 500,
        #     'WriteCapacityUnits': 4000,
        # },
    }
    table = dyn_resource.create_table(**params)
    table.wait_until_exists()
    logging.info('[delete_ddb] Table created')

    # Batch write persistent data
    with table.batch_writer() as writer:
        for ddb_type in persistent_ddb_types:
            ddb_persistent_data_response = persistent_data[ddb_type]
            for row in ddb_persistent_data_response:
                # print("Persistent data, saving:", row)
                writer.put_item(Item=row)
