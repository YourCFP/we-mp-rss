import yaml
import sys
import os
import argparse
from string import Template
config = {}
class Config: 
    config_path=None
    def __init__(self,config_path=None):
        self.args=self.parse_args()
        self.config_path = config_path or self.args.config
    def parse_args(self):
        parser = argparse.ArgumentParser()
        parser.add_argument('-config', help='配置文件', default='config.yaml')
        parser.add_argument('-job', help='启动任务', default=False)
        parser.add_argument('-init', help='初始化数据库,初始化用户', default=False)
        args, _ = parser.parse_known_args()
        return args
    def save_config(self):
        global config
        with open(self.config_path, 'w', encoding='utf-8') as f:
            yaml.dump(config, f)
    def replace_env_vars(self,data):
            if isinstance(data, dict):
                return {k: self.replace_env_vars(v) for k, v in data.items()}
            elif isinstance(data, list):
                return [self.replace_env_vars(item) for item in data]
            elif isinstance(data, str):
                try:
                    import re
                    # 匹配 ${VAR:-default} 或 ${VAR} 格式
                    pattern = re.compile(r'\$\{([^}:]+)(?::-([^}]*))?\}')
                    def replace_match(match):
                        var_name = match.group(1)
                        default_value = match.group(2)
                        return os.getenv(var_name, default_value) if default_value is not None else os.getenv(var_name, '')
                    return pattern.sub(replace_match, data)
                except:
                    return data
            return data
    def get_config(self):
        global config
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                # config = self.replace_env_vars(config)
                return config
        except Exception as e:
            print(f"Error loading configuration file {self.config_path}: {e}")
            sys.exit(1)
    def set(self,key,default:any=None):
        global config
        config[key] = default
        self.save_config()
    def get(self,key,default:any=None):
        global config
        _config=self.replace_env_vars(config)
        if key in _config:
            return _config[key]
        else:
            print("Key {} not found in configuration".format(key))
            if default is not None:
                return default
        return None
cfg=Config()
config = cfg.get_config()
DEBUG=cfg.get("debug",False)
APP_NAME=cfg.get("app_name","we-mp-rss")


VERSION='1.3.1'

print(VERSION)