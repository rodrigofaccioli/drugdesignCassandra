import json

def load_config(configFile):
    with open(configFile) as json_file:
        data = json.load(json_file)
    return data
