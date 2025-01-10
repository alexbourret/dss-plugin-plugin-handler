from dataiku.connector import Connector
from plugin_handler_common import RecordsLimit
import dataikuapi
import dataiku
import logging


logging.basicConfig(level=logging.INFO, format='dss-plugin-handler %(levelname)s - %(message)s')
logger = logging.getLogger()


class PluginsUsageConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.presets = config.get("presets", [])

    def get_read_schema(self):
        return {
            "columns": [
                {"name": "dss_client", "type": "string"},
                {"name": "plugin_id", "type": "string"},
                {"name": "plugin_version", "type": "string"},
                {"name": "element_type", "type": "string"},
                {"name": "element_kind", "type": "string"},
                {"name": "project_key", "type": "string"},
                {"name": "object_id", "type": "string"},
                {"name": "raw_params", "type": "object"}
            ]
        }

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                      partition_id=None, records_limit=-1):
        limit = RecordsLimit(records_limit)
        if not self.presets:
            self.presets = [{}]
        for preset in self.presets:
            dss_client_url = preset.get("dss_client_url")
            dss_client_api_key = preset.get("dss_client_api_key")
            if dss_client_url:
                client = dataikuapi.DSSClient(dss_client_url, dss_client_api_key)
            else:
                dss_client_url = "Local"
                client = dataiku.api_client()
            plugins = client.list_plugins()
            for plugin in plugins:
                plugin_id = plugin.get("id")
                plugin_version = plugin.get("version")
                if plugin_id:
                    plugin_handle = client.get_plugin(plugin_id)
                    plugin_usages = plugin_handle.list_usages()
                    if plugin_usages.usages:
                        for plugin_usage in plugin_usages.usages:
                            raw_params = None
                            project = client.get_project(plugin_usage.project_key)
                            if plugin_usage.object_type == "RECIPE":
                                recipe = project.get_recipe(plugin_usage.object_id)
                                recipe_settings = recipe.get_settings()
                                raw_params = recipe_settings.raw_params
                            elif plugin_usage.object_type == "DATASET":
                                dataset = project.get_dataset(plugin_usage.object_id)
                                raw_params = None
                                try:
                                    dataset_settings = dataset.get_settings()
                                    raw_params = dataset_settings.get_raw_params()
                                except Exception as exception:
                                    logger.error("Dataset {} could not be retrieved".format(plugin_usage.object_id))
                                    continue
                            yield {
                                "dss_client": dss_client_url,
                                "plugin_id": plugin_id,
                                "plugin_version": plugin_version,
                                "element_type": plugin_usage.element_type,
                                "element_kind": plugin_usage.element_kind,
                                "project_key": plugin_usage.project_key,
                                "object_id": plugin_usage.object_id,
                                "raw_params": raw_params
                            }
                            if limit.is_reached():
                                return
                    else:
                        yield {
                            "dss_client": dss_client_url,
                            "plugin_id": plugin_id,
                            "plugin_version": plugin_version,
                            "element_type": None,
                            "element_kind": None,
                            "project_key": None,
                            "object_id": None,
                            "raw_params": None
                        }
                        if limit.is_reached():
                            return

    def get_writer(self, dataset_schema=None, dataset_partitioning=None,
                   partition_id=None):
        """
        Returns a writer object to write in the dataset (or in a partition).

        The dataset_schema given here will match the the rows given to the writer below.

        Note: the writer is responsible for clearing the partition, if relevant.
        """
        raise NotImplementedError

    def get_partitioning(self):
        """
        Return the partitioning schema that the connector defines.
        """
        raise NotImplementedError

    def list_partitions(self, partitioning):
        """Return the list of partitions for the partitioning scheme
        passed as parameter"""
        return []

    def partition_exists(self, partitioning, partition_id):
        """Return whether the partition passed as parameter exists

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError

    def get_records_count(self, partitioning=None, partition_id=None):
        """
        Returns the count of records for the dataset (or a partition).

        Implementation is only required if the corresponding flag is set to True
        in the connector definition
        """
        raise NotImplementedError
