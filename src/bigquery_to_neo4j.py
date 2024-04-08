import abc
import logging
import os
from databuilder.task.task import DefaultTask
from databuilder.transformer.base_transformer import NoopTransformer

logging.basicConfig(level=logging.DEBUG)


# *************************************************************************************
# Abstract Job
# *************************************************************************************
class AbstractJob(metaclass=abc.ABCMeta):
    def __init__(self, params) -> None:
        self.params = params

    @abc.abstractmethod
    def _config(self):
        return

    @abc.abstractmethod
    def _task(self):
        return

    @abc.abstractmethod
    def _get_publisher(self):
        return

    def launch(self):
        from databuilder.job.job import DefaultJob

        job = DefaultJob(
            conf=self._config(), task=self._task(), publisher=self._get_publisher()
        )

        job.launch()


# *************************************************************************************
# Extractor Job
# *************************************************************************************


class BigqueryExtractorJob(AbstractJob):
    from databuilder.extractor.bigquery_metadata_extractor import (
        BigQueryMetadataExtractor,
    )
    from databuilder.loader.file_system_neo4j_csv_loader import FsNeo4jCSVLoader
    from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
    from databuilder.publisher import neo4j_csv_publisher

    def _config(self):

        from pyhocon import ConfigFactory

        tmp_folder = "/var/tmp/amundsen/{}".format(self.params["metadata_type"])

        node_files_folder = "{}/nodes".format(tmp_folder)

        relationship_files_folder = "{}/relationships".format(tmp_folder)

        job_config = {
            "extractor.bigquery_table_metadata.{}".format(
                self.BigQueryMetadataExtractor.PROJECT_ID_KEY
            ): self.params["project_id_key"],
            "loader.filesystem_csv_neo4j.{}".format(
                self.FsNeo4jCSVLoader.NODE_DIR_PATH
            ): node_files_folder,
            "loader.filesystem_csv_neo4j.{}".format(
                self.FsNeo4jCSVLoader.RELATION_DIR_PATH
            ): relationship_files_folder,
            "loader.filesystem_csv_neo4j.{}".format(
                self.FsNeo4jCSVLoader.SHOULD_DELETE_CREATED_DIR
            ): True,
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.NODE_FILES_DIR
            ): node_files_folder,
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.RELATION_FILES_DIR
            ): relationship_files_folder,
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.NEO4J_END_POINT_KEY
            ): self.params["neo4j_endpoint"],
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.NEO4J_USER
            ): self.params["neo4j_user"],
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.NEO4J_PASSWORD
            ): self.params["neo4j_password"],
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.NEO4J_VALIDATE_SSL
            ): "false",
            "publisher.neo4j.{}".format(
                self.neo4j_csv_publisher.JOB_PUBLISH_TAG
            ): self.params["job_publish_tag"],
        }

        job_config[
            "extractor.bigquery_table_metadata.{}".format(
                self.BigQueryMetadataExtractor.FILTER_KEY
            )
        ] = "labels.type:csv"

        return ConfigFactory.from_dict(job_config)

    def _task(self):
        task = DefaultTask(
            extractor=self.BigQueryMetadataExtractor(),
            loader=self.FsNeo4jCSVLoader(),
            transformer=NoopTransformer(),
        )

        return task

    def _get_publisher(self):
        return self.Neo4jCsvPublisher()


# *************************************************************************************
# Update Elasticsearch Job
# *************************************************************************************
class UpdateElasticsearchWithNeo4jJob(AbstractJob):
    from databuilder.extractor.neo4j_extractor import Neo4jExtractor
    from databuilder.extractor.neo4j_search_data_extractor import (
        Neo4jSearchDataExtractor,
    )
    from databuilder.loader.file_system_elasticsearch_json_loader import (
        FSElasticsearchJSONLoader,
    )
    from databuilder.publisher.elasticsearch_publisher import ElasticsearchPublisher
    from databuilder.publisher import neo4j_csv_publisher
    from databuilder.publisher.neo4j_csv_publisher import Neo4jCsvPublisher
    from amundsen_common.models import index_map

    def _config(self):
        import uuid
        from datetime import datetime
        from pyhocon import ConfigFactory

        date = datetime.now().strftime("%Y-%m-%d")

        tmp_es_file_path = (
            "/var/tmp/amundsen_dashboard/elasticsearch_dashboard_upload/es_data.json"
        )

        es_index_name = "dashboard_search_index_{ds}_{hex_str}".format(
            ds=date, hex_str=uuid.uuid4().hex
        )

        elasticsearch_doc_type = "dashboard"
        elasticsearch_index_alias = "dashboard_search_index"

        job_config = ConfigFactory.from_dict(
            {
                "extractor.search_data.extractor.neo4j.{}".format(
                    self.Neo4jExtractor.GRAPH_URL_CONFIG_KEY
                ): self.params["neo4j_endpoint"],
                "extractor.search_data.extractor.neo4j.{}".format(
                    self.Neo4jExtractor.MODEL_CLASS_CONFIG_KEY
                ): "databuilder.models.dashboard_elasticsearch_document.DashboardESDocument",
                "extractor.search_data.extractor.neo4j.{}".format(
                    self.Neo4jExtractor.NEO4J_AUTH_USER
                ): self.params["neo4j_user"],
                "extractor.search_data.extractor.neo4j.{}".format(
                    self.Neo4jExtractor.NEO4J_AUTH_PW
                ): self.params["neo4j_password"],
                "extractor.search_data.extractor.neo4j.{}".format(
                    self.Neo4jExtractor.NEO4J_ENCRYPTED
                ): False,
                "extractor.search_data.{}".format(
                    self.Neo4jSearchDataExtractor.CYPHER_QUERY_CONFIG_KEY
                ): self.Neo4jSearchDataExtractor.DEFAULT_NEO4J_DASHBOARD_CYPHER_QUERY,
                "extractor.search_data.{}".format(
                    self.neo4j_csv_publisher.JOB_PUBLISH_TAG
                ): self.params["job_publish_tag"],
                "loader.filesystem.elasticsearch.{}".format(
                    self.FSElasticsearchJSONLoader.FILE_PATH_CONFIG_KEY
                ): tmp_es_file_path,
                "loader.filesystem.elasticsearch.{}".format(
                    self.FSElasticsearchJSONLoader.FILE_MODE_CONFIG_KEY
                ): "w",
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.FILE_PATH_CONFIG_KEY
                ): tmp_es_file_path,
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.FILE_MODE_CONFIG_KEY
                ): "r",
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.ELASTICSEARCH_CLIENT_CONFIG_KEY
                ): self.params["elasticsearch_client"],
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.ELASTICSEARCH_NEW_INDEX_CONFIG_KEY
                ): es_index_name,
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.ELASTICSEARCH_DOC_TYPE_CONFIG_KEY
                ): elasticsearch_doc_type,
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.ELASTICSEARCH_MAPPING_CONFIG_KEY
                ): self.index_map.DASHBOARD_ELASTICSEARCH_INDEX_MAPPING,
                "publisher.elasticsearch.{}".format(
                    self.ElasticsearchPublisher.ELASTICSEARCH_ALIAS_CONFIG_KEY
                ): elasticsearch_index_alias,
            }
        )

        return job_config

    def _task(self):
        task = DefaultTask(
            extractor=self.Neo4jSearchDataExtractor(),
            loader=self.FSElasticsearchJSONLoader(),
            transformer=NoopTransformer(),
        )

        return task

    def _get_publisher(self):
        return self.ElasticsearchPublisher()


# *************************************************************************************
# run jobs


def run_job_bigquery_extractor(params):
    job = BigqueryExtractorJob(params)
    job.launch()


def run_job_update_elasticsearch(params):
    job = UpdateElasticsearchWithNeo4jJob(params)
    job.launch()


# *************************************************************************************
if __name__ == "__main__":
    from elasticsearch import Elasticsearch
    from datetime import datetime

    es_client = Elasticsearch(
        [
            {
                "host": os.getenv("ELASTICHSEARCH_HOST"),
                "port": os.getenv("ELASTICHSEARCH_PORT"),
            },
        ]
    )

    params = {
        "project_id_key": "project_id",
        "metadata_type": "bigquery_metadata",
        "neo4j_user": os.getenv("NEO4J_USERNAME"),
        "neo4j_password": os.getenv("NEO4J_PASSWORD"),
        "neo4j_endpoint": os.getenv("NEO4J_ENDPOINT"),
        "job_publish_tag": datetime.now().strftime("%Y-%m-%d"),
        "elasticsearch_client": es_client,
    }

    # conn_neo4j()
    run_job_bigquery_extractor(params)

    run_job_update_elasticsearch(params)
