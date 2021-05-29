#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""This module contains class Config that creates and parses a config.

originally from lib_bgp_data
"""

__author__ = "Justin Furuness"
__credits__ = ["Justin Furuness"]
__maintainer__ = "Justin Furuness"
__email__ = "jfuruness@gmail.com"


from datetime import datetime
import time
import logging
import os

import pytest
import configparser
from configparser import NoSectionError, RawConfigParser


class Config:
    """Interact with config file"""

    default_path = "/etc/conf/main.conf"
    default_section = "main"

    def __init__(self, section=None, path=None):
        """Initialize it with a specific section to work with"""

        self._section = section
        self._path = path
        self._create_config_dir()

    def create_config_section(self, **kwargs):
        """Creates the config file and adds a section to it"""

        conf: RawConfigParser = self._get_conf_dict()
        conf[self.section] = kwargs
        self._write_conf(conf)

    def create_postgres_config_section(self, password, **kwargs):
        """Creates the config file and adds a postgres section to it"""

        db_kwargs = {
            "host": kwargs.get("host", "localhost"),
            "database": self.section,
            "password": password,
            "user": kwargs.get("user", section + "_user"),
            "restart_db_cmd": kwargs.get("restart_db_cmd", self.restart_db_cmd)
        }
        self.create_config_section(**db_kwargs)

    def _create_config_dir(self):
        """Creates the config dir"""

        _dir = os.path.split(self.path)[0]

        try:
            os.makedirs(os.path.split(self.path)[0])
        except FileExistsError:
            logging.debug(f"{_dir} exists, not creating new dir for conf")
        except PermissionError as e:
            logging.warning(f"{_dir} requires permissions to create")
            logging.warning(f"Run: sudo mkdir {_dir} && "
                            f"sudo chmod -R $USER:$USER {_dir}")
            raise e

    def remove_old_config_section(self):
        """Removes the old config file if it exists."""

        conf = self._get_conf_dict()
        # Try to delete the section
        try:
            del _conf[self.section]
        # If it doesn' exist, doesn't matter
        except KeyError:
            logging.warning("Tried to delete {self.section} in {self.path}"
                            " but it did not exist")

        self._write_conf(conf)
 
    def _read_config(self, section: str, tag: str, raw: bool = False):
        """Reads the specified section from the configuration file."""

        conf = self._get_conf_dict()
        _parser = SCP()
        _parser.read(self.path)
        string = _parser.get(section, tag, raw=raw)
        try:
            return int(string)
        except ValueError:
            return string

    def get_db_creds(self, error=False) -> dict:
        """Returns database credentials from the config file."""

        # section = "bgp"
        subsections = ["user", "host", "database"]
        args = {x: self._read_config(self.section, x) for x in subsections}
        args["password"] = self._read_config(self.section,
                                             "password",
                                             raw=True)
        return args

    def _write_to_config(self, section, subsection, string):
        """Writes to a config file."""

        _conf = SCP()
        _conf.read(self.path)
        _conf[section][subsection] = str(string)
        with open(self.path, 'w') as configfile:
            _conf.write(configfile)

    def _get_conf_dict(self) -> RawConfigParser:
        """Reads in the config as a dict like object"""

        conf = RawConfigParser()
        conf.read(self.path)
        return conf

    def _write_conf(self, conf: RawConfigParser):
        """Writes the config"""

        with open(self.path, "w+") as config_file:
            conf.write(config_file)

    @property
    def section(self) -> str:
        """Returns the section that this config is working in"""

        return self._section if self._section else self.default_section

    @property
    def path(self) -> str:
        """Returns the path of the config file"""

        return self._path if self._path else self.default_path

    @property
    def restart_db_cmd(self) -> str:
        """Returns restart postgres cmd or writes it if none exists."""

        subsection = "restart_db_cmd"

        try:
            cmd = self._read_config(self.section, subsection)
        except NoSectionError:
            typical_cmd = "sudo systemctl restart postgresql"

            prompt = ("Enter the command to restart postgres\n"
                      f"Enter: {typical_cmd}\n"
                      "Custom: Enter cmd for your machine\n")
            # https://stackoverflow.com/a/58866220
            if "PYTEST_CURRENT_TEST" in os.environ:
                 return typical_cmd
            else:
                cmd = input(prompt)
                if cmd == "":
                    cmd = typical_cmd
        return cmd
