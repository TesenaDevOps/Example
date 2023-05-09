import logging
from invoke_lambda import invoke_lambda


def delete_recreate_opensearch_indices(index_name):

    logging.info('[delete_opensearch_indices] deleting index %s...', index_name)
    invoke_lambda("WriteOpense", {
        "verbose": True,
        "payload": [
            {
                "task": "DELETEINDEX",
                "id": index_name
            }
        ]
    })

    logging.info('[delete_opensearch_indices] Deleted! Creating index %s...', index_name)
    invoke_lambda("WriteOpense", {
        "verbose": True,
        "payload": [
            {
                "task": "CREATEINDEX",
                "id": index_name
            }
        ]
    })

    return True
