from six.moves import xrange
from dataiku.connector import Connector
from plugin_usage_common import RecordsLimit
import dataikuapi
import dataiku


class PluginsUsageConnector(Connector):

    def __init__(self, config, plugin_config):
        Connector.__init__(self, config, plugin_config)  # pass the parameters to the base class
        self.presets = config.get("presets", [])

    def get_read_schema(self):
        return {"columns" : [ 
            {"name": "Plugin ID", "type" : "string"},
            {"name" :"Plugin version", "type" : "string"},
            {"name" :"Plugin element ID", "type" : "string"},
            {"name" :"Element kind", "type" : "string"},
            {"name" :"Project key", "type" : "string"},
            {"name" :"Used in", "type" : "string"}
        ]}

    def generate_rows(self, dataset_schema=None, dataset_partitioning=None,
                            partition_id=None, records_limit = -1):
        limit = RecordsLimit(records_limit)
        if not self.presets:
            self.presets = [{}]
        for preset in self.presets:
            dss_client_url = preset.get("dss_client_url")
            dss_client_api_key = preset.get("dss_client_api_key")
            if dss_client_url:
                client = dataikuapi.DSSClient(dss_client_url, dss_client_api_key)
            else:
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
                            yield {
                                "Plugin ID": plugin_id,
                                "Plugin version": plugin_version,
                                "Plugin element ID": plugin_usage.element_type,
                                "Element kind": plugin_usage.element_kind,
                                "Project key": plugin_usage.project_key,
                                "Used in": plugin_usage.object_id
                            }
                            if limit.is_reached():
                                break
                    else:
                        yield {
                            "Plugin ID": plugin_id,
                            "Plugin version": plugin_version,
                            "Plugin element ID": None,
                            "Element kind": None,
                            "Project key": None,
                            "Used in": None
                        }
                        if limit.is_reached():
                            break

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
