import apache_beam as beam
from apache_beam import PTransform

def get_partition_conf(destination):
    """
    Define the partition information of each table as follows:
    bq_partition_parameter = {
        "User": {'timePartitioning': {'field': 'createdAt', 'type': 'DAY'}, 'clustering': {'fields': ['teamId']} },
        "Team": {'timePartitioning': {'field': 'createdAt', 'type': 'DAY'} }
    }

    :param destination: {dataset}.{table} as string
    :return: dictionary including partition information of each table
    """
    bq_partition_parameter = {}

    # get table name by dividing destination as ["dataset", "table"]
    table = destination.split(".")[1]

    if table in bq_partition_parameter:
        return bq_partition_parameter[table]
    else:
        return None

class GetBqTableMap(PTransform):
    """
    Get table information of BigQuery as dictionary
    """
    def __init__(self, project_id, dataset_opt):
        self.project_id = project_id
        self.dataset_opt = dataset_opt

    def expand(self, pbegin):
        """

        :return: PCollection[{"kind_name": "bigquery_table_name"}]
        """
        from google.cloud import datastore
        from apache_beam.transforms import Impulse
        client = datastore.Client(self.project_id)
        query = client.query(kind='__kind__')
        query.keys_only()
        kinds = [entity.key.id_or_name for entity in query.fetch()]
        return (pbegin
                | Impulse()
                | beam.FlatMap(lambda _: [(kind, "{}.{}".format(self.dataset_opt.get(), kind)) for kind in kinds])
                )
