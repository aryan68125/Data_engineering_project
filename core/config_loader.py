import yaml
import os

class LoadAppConfigs:
    def __init__(self,env : str = None, config_path : str = None):
        self.env = env
        self.config_path = config_path

    def load_app_config(self):
        with open(self.config_path, "r") as f:
            full_config = yaml.safe_load(f)
        if self.env not in full_config:
            raise ValueError(f"Environment '{self.env}' not found in YAML config.")
        
        return full_config[self.env]