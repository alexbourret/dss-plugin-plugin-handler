import dataiku
import dataikuapi
import math
import ast
import json
from dataiku.customrecipe import get_input_names_for_role, get_recipe_config, get_output_names_for_role


def denanify_fraking_pandas(value_from_fraking_pandas):
    if isinstance(value_from_fraking_pandas, float) and math.isnan(value_from_fraking_pandas):
        value_from_fraking_pandas = None
    return value_from_fraking_pandas


def copy_dict_from_to(new_raw_params_dict, current_raw_params):
    for current_key in current_raw_params:
        current_value = current_raw_params[current_key]
        if isinstance(current_value, dict):
            current_raw_params[current_key] = copy_dict_from_to(new_raw_params_dict.get(current_key), current_raw_params[current_key])
        else:
            current_raw_params[current_key] = new_raw_params_dict.get(current_key)
    return current_raw_params


input_names = get_input_names_for_role('input_dataset')
config = get_recipe_config()

element_kind_column = config.get("element_kind")
project_key_column = config.get("project_key")
dataset_id_column = config.get("dataset_id")
object_id_column = config.get("object_id")
old_raw_params_column = config.get("old_raw_params")
new_raw_params_column = config.get("new_raw_params")

input_parameters_dataset = dataiku.Dataset(input_names[0])
input_parameters_dataframe = input_parameters_dataset.get_dataframe()

output_names = get_output_names_for_role('api_output')
output_dataset = dataiku.Dataset(output_names[0])
output_schema = list(input_parameters_dataset.read_schema())

output_schema.append({'name': 'project_key', 'type': 'string'})
output_schema.append({'name': 'element_kind', 'type': 'string'})
output_schema.append({'name': 'dataset_id', 'type': 'string'})
output_schema.append({'name': 'error_message', 'type': 'string'})
output_schema.append({'name': 'message', 'type': 'object'})
output_schema.append({'name': 'final_raw_parameters', 'type': 'object'})
output_dataset.write_schema(output_schema)

preset = config.get("preset", {})
dss_client_url = preset.get("dss_client_url")
dss_client_api_key = preset.get("dss_client_api_key")

if dss_client_url:
    client = dataikuapi.DSSClient(dss_client_url, dss_client_api_key)
else:
    dss_client_url = "Local"
    client = dataiku.api_client()

with output_dataset.get_writer() as writer:
    for index, input_parameters_row in input_parameters_dataframe.iterrows():
        data = {}
        project_key = input_parameters_row.get(project_key_column)
        element_kind = input_parameters_row.get(element_kind_column)
        dataset_id = input_parameters_row.get(dataset_id_column)
        object_id = input_parameters_row.get(object_id_column)
        old_raw_params = input_parameters_row.get(old_raw_params_column)
        old_raw_params_dict = None
        data["project_key"] = project_key
        data["element_kind"] = element_kind
        data["dataset_id"] = dataset_id
        try:
            old_raw_params_dict = ast.literal_eval(old_raw_params)
        except Exception as err:
            print("ALX:error={}".format(err))
            pass
        current_raw_params = None
        project = client.get_project(project_key)
        object_handle = None
        if element_kind == "custom-recipes":
            object_handle = project.get_recipe(object_id)
            recipe_settings = object_handle.get_settings()
            current_raw_params = recipe_settings.raw_params
        elif element_kind == "python-connectors":
            object_handle = project.get_dataset(object_id)
            dataset_settings = object_handle.get_settings()
            current_raw_params = dataset_settings.get_raw_params()
        if current_raw_params == old_raw_params_dict:
            data["message"] = "Matching"
            new_raw_params = input_parameters_row.get(new_raw_params_column)
            new_raw_params_dict = None
            try:
                #new_raw_params_dict = ast.literal_eval(new_raw_params)
                new_raw_params_dict = json.loads(new_raw_params)
            except Exception as err:
                print("ALX:error2={}".format(err))
                pass
            print("ALX:new_raw_params_dict={}".format(new_raw_params_dict))
            print("ALX:type new_raw_params_dict={}".format(type(new_raw_params_dict)))
            if isinstance(new_raw_params_dict, dict):
                current_raw_params = copy_dict_from_to(new_raw_params_dict, current_raw_params)
                dataset_settings.save()
                data["message"] = "OK"
            else:
                data["error_message"] = "New params is not a valid dictionary"
                writer.write_row_dict(data)
                continue
        else:
            data["error_message"] = "Non matching parameters"
            writer.write_row_dict(data)
            continue
        writer.write_row_dict(data)
