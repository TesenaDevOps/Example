import logging
import common
from delete_ddb_table import delete_ddb
from delete_opensearch_index import delete_recreate_opensearch_indices
from shared.get_logging_level import get_logging_level  # inside a layer


def lambda_handler(event, context):

    # Set-up logger
    logging.getLogger().setLevel(get_logging_level(context))
    logging.info(
        '[handler] event: %s; context: %s',
        event,
        vars(context)
    )

    if event.get("DELETE_DDB", False):
        delete_ddb()

    if event.get("DELETE_OPENSEARCH", False):
        delete_recreate_opensearch_indices(common.env)


# a hack to be able to run it locally
if __name__ == "__main__":
    event = {}
    event = {**event, "DELETE_DDB": False}
    event = {**event, "DELETE_OPENSEARCH": False}
    lambda_handler(event, "")
