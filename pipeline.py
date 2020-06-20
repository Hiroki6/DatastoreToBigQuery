import logging

from ds_transform import convert, CreateQuery, GetKinds
from bq_transform import GetBqTableMap, get_partition_conf
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

class Ds2bqOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--dataset')

def run(argv=None):
    from apache_beam.io.gcp.bigquery_file_loads import BigQueryBatchFileLoads
    from apache_beam.io.gcp.datastore.v1new.datastoreio import ReadFromDatastore
    from datetime import datetime

    options = Ds2bqOptions(flags=argv)
    options.view_as(beam.options.pipeline_options.GoogleCloudOptions).region = "asia-northeast1"
    options.view_as(beam.options.pipeline_options.WorkerOptions).num_workers = 2
    options.view_as(beam.options.pipeline_options.WorkerOptions).disk_size_gb = 50

    # Setup
    options.view_as(beam.options.pipeline_options.StandardOptions).runner = 'DataflowRunner'
    options.view_as(beam.options.pipeline_options.SetupOptions).setup_file = './setup.py'

    logging.info(options)

    project_id = options.view_as(beam.options.pipeline_options.GoogleCloudOptions).project
    gcs_dir = "gs://{}-dataflow/temp/{}".format(project_id, datetime.now().strftime("%Y%m%d%H%M%S"))

    with beam.Pipeline(options=options) as p:
        table_names_dict = beam.pvalue.AsDict(
            p | "Get BigQuery Table Map" >> GetBqTableMap(project_id, options.dataset)
        )

        entities = (p
         | 'Get Kinds' >> GetKinds(project_id)
         | 'Create Query' >> beam.ParDo(CreateQuery(project_id))
         | 'Get Entity' >> beam.ParDo(ReadFromDatastore._QueryFn())
         | 'Convert Entity' >> beam.Map(convert))

        _ = (entities
             | 'BigQuery Load' >> BigQueryBatchFileLoads(destination=lambda row, table_dict: table_dict[row["__key__"]["kind"]],
                                custom_gcs_temp_location=gcs_dir,
                                write_disposition='WRITE_TRUNCATE',
                                table_side_inputs=(table_names_dict,),
                                additional_bq_parameters=get_partition_conf,
                                schema='SCHEMA_AUTODETECT')
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
