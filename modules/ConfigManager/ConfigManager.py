import yaml
import logging

logger = logging.getLogger("app")


class ConfigManager:
    __instance = None

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super(ConfigManager, cls).__new__(cls)
        return cls.__instance

    def __init__(self, env, desc_bu):
        if not hasattr(self, 'initialized'):
            self.env = env
            self.desc_bu = desc_bu
            self.cfg = self.load_config()
            self.initialized = True

    def load_config(self):
        logger.info(f"Reading the config file for desc_bu: {self.desc_bu} and env: {self.env}")
        with open(f'./config/{self.env}/{self.desc_bu}_config.yml', 'r') as file:
            cfg = yaml.safe_load(file)
            logger.info(f"Successfully loaded the config for desc_bu: {self.desc_bu} and env: {self.env}")
        return cfg

