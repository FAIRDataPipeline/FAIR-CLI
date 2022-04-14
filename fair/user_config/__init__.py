#!/usr/bin/python3
# flake8: noqa
# -*- coding: utf-8 -*-
"""
User Config Management
======================

Contains classes for the parsing and preparation of the user's 'config.yaml'
prior to the execution of a run or synchronisation

Contents
========

Constants
---------

    - JOB2CLI_MAPPINGS: mappings from CLI configuration to config.yaml keys
    - SHELLS: commands for executing scripts depending on specified shell


Classes
-------

    - JobConfiguration: handles the setup of the configuration file

"""

__date__ = "2021-09-10"

import copy
import datetime
import json
import logging
import os
import os.path
import platform
import re
import subprocess
import sys
import typing
from collections.abc import MutableMapping

import click
import git
import pydantic
import yaml

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.history as fdp_hist
import fair.register as fdp_reg
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.versioning as fdp_ver
import fair.run as fdp_run
import fair.user_config.globbing as fdp_glob
import fair.user_config.validation as fdp_valid
import fair.utilities as fdp_util
from fair.common import CMD_MODE
from fair.registry import SEARCH_KEYS

JOB2CLI_MAPPINGS = {
    "run_metadata.local_repo": "git.local_repo",
    "run_metadata.default_input_namespace": "namespaces.input",
    "run_metadata.default_output_namespace": "namespaces.output",
    "run_metadata.local_data_registry_url": "registries.local.uri",
    "run_metadata.remote_data_registry_url": "registries.origin.uri",
    "run_metadata.write_data_store": "registries.local.data_store",
}

SHELLS: typing.Dict[str, str] = {
    "pwsh": {"exec": "pwsh -command \". '{0}'\"", "extension": "ps1"},
    "batch": {"exec": "{0}", "extension": "bat"},
    "powershell": {
        "exec": "powershell -command \". '{0}'\"",
        "extension": "ps1",
    },
    "python2": {"exec": "python2 {0}", "extension": "py"},
    "python3": {"exec": "python3 {0}", "extension": "py"},
    "python": {"exec": "python {0}", "extension": "py"},
    "R": {"exec": "R -f {0}", "extension": "R"},
    "julia": {"exec": "julia {0}", "extension": "jl"},
    "bash": {
        "exec": "bash -eo pipefail {0}",
        "extension": "sh",
    },
    "java": {"exec": "java {0}", "extension": "java"},
    "sh": {"exec": "sh -e {0}", "extension": "sh"},
}


class JobConfiguration(MutableMapping):
    _logger = logging.getLogger("FAIRDataPipeline.ConfigYAML")
    _block_types = ("register", "write", "read")
    _final_permitted = {
        "all": ("data_product", "public", "use", "description"),
        "write": ("file_type",),
    }
    _status_tags = ("registered",)

    def __init__(self, config_yaml: str = None) -> None:
        self._config = {"run_metadata": {}}
        self._input_file = config_yaml
        if config_yaml:
            if not os.path.exists(config_yaml):
                raise fdp_exc.FileNotFoundError(
                    f"Cannot load job configuration from file '{config_yaml}', "
                    "file does not exist"
                )

            self._logger.debug("Loading file '%s'", config_yaml)
            self._config: typing.Dict = yaml.safe_load(open(config_yaml))

        self._fill_missing()

        self._now = datetime.datetime.now()
        self._parsed = {"namespace": [], "author": []}
        self.env = None
        self._job_dir = None
        self._log_file = None

    def _get_local_namespaces(self) -> typing.List[str]:
        _namespaces = fdp_req.get(
            self.local_uri, "namespace", fdp_req.local_token()
        )
        return (
            _namespaces
            if not _namespaces
            else [n["name"] for n in _namespaces]
        )

    def _get_local_authors(self) -> typing.List[str]:
        _authors = fdp_req.get(self.local_uri, "author", fdp_req.local_token())
        return _authors if not _authors else [a["name"] for a in _authors]

    def __contains__(self, key_addr: str) -> bool:
        return any(
            [
                key_addr in self._config,
                key_addr in fdp_util.flatten_dict(self._config),
            ]
        )

    def __setitem__(
        self, key_addr: str, value: typing.Any, separator: str = "."
    ) -> None:
        if key_addr in self:
            self._config[key_addr] = value
        _flat_cfg = fdp_util.flatten_dict(self._config, separator)
        _flat_cfg[key_addr] = value
        self._logger.debug(f"Set value {key_addr.replace('.', ':')}={value}'")
        self._config = fdp_util.expand_dict(_flat_cfg)

    def __delitem__(self, key_addr: str, separator: str = ".") -> None:
        if key_addr in self._config:
            del self._config[key_addr]
        _flat_cfg = fdp_util.flatten_dict(self._config, separator)
        if key_addr not in _flat_cfg:
            raise fdp_exc.KeyPathError(
                key_addr, f"UserConfig[{self._input_file}]"
            )
        self._logger.debug(f"Removing '{key_addr}'")
        del _flat_cfg[key_addr]
        self._config = fdp_util.expand_dict(_flat_cfg)

    def __getitem__(self, key_addr: str, separator: str = ".") -> None:
        if key_addr in self._config:
            return self._config[key_addr]
        _flat_cfg = fdp_util.flatten_dict(self._config, separator)
        if key_addr not in _flat_cfg:
            raise fdp_exc.KeyPathError(
                key_addr, f"UserConfig[{self._input_file}]"
            )
        return _flat_cfg[key_addr]

    def __len__(self) -> int:
        raise fdp_exc.NotImplementedError(
            "Cannot get length of a Job Configuration, attribute has no meaning in this context"
        )

    def __iter__(self) -> typing.Any:
        raise fdp_exc.NotImplementedError(
            "Cannot iterate through a Job Configuration, operation is ambiguous"
        )

    def _fill_missing(self) -> None:
        self._logger.debug("Filling missing metadata")
        if "run_metadata" not in self:
            self._logger.debug(
                "Failed to find 'run_metadata' in configuration, creating"
            )
            self["run_metadata"] = {}

        _required = [
            ("run_metadata.public", True),
            (
                "run_metadata.default_read_version",
                "${{" + fdp_ver.DEFAULT_READ_VERSION + "}}",
            ),
            (
                "run_metadata.default_write_version",
                "${{" + fdp_ver.DEFAULT_WRITE_VERSION + "}}",
            ),
        ]

        for item in _required:
            try:
                self[item[0]]
            except fdp_exc.KeyPathError:
                self[item[0]] = item[1]

    def _handle_register_authors(
        self, register_block: typing.List[typing.Dict]
    ) -> typing.Dict:
        """Store authors on registry from 'register' block"""
        self._logger.debug("Handling 'register:author'")
        _new_register_block: typing.List[typing.Dict] = []

        # Register explicit author objects and remove from config
        for item in register_block:
            # Not a namespace
            if "author" not in item:
                _new_register_block.append(item)
                continue

            if any(
                n in item
                for n in ["data_product", "external_object", "namespace"]
            ):
                raise fdp_exc.UserConfigError(
                    "Invalid use of tag 'author' in non-author registration"
                )

            _author_metadata = {
                "name": item["author"],
                "identifier": item.get("identifier", None),
                "uuid": item.get("uuid", None),
            }

            if item["author"] in self._parsed["author"]:
                self._logger.warning(
                    "Ignoring registration of author '%s' as entry already exists",
                    item["author"],
                )
                continue

            fdp_store.store_author(
                self.local_uri, fdp_req.local_token(), **_author_metadata
            )

            self._parsed["author"].append(_author_metadata["name"])

        return _new_register_block

    def _handle_register_namespaces(
        self, register_block: typing.List[typing.Dict]
    ) -> typing.Dict:
        """Store namespaces on registry from 'register' block"""
        self._logger.debug("Handling 'register:namespace'")
        _new_register_block: typing.List[typing.Dict] = []

        # Register explicit namespace objects and remove from config
        for item in register_block:
            # Not a namespace
            if "namespace" not in item:
                _new_register_block.append(item)
                continue

            if any(
                n in item
                for n in ["data_product", "external_object", "author"]
            ):
                raise fdp_exc.UserConfigError(
                    "Invalid use of tag 'namespace' in non-namespace registration",
                    "Did you mean 'namespace_name'?",
                )

            _namespace_metadata = {
                "name": item["namespace"],
                "full_name": item.get("full_name", None),
                "website": item.get("website", None),
            }

            if item["namespace"] in self._parsed["namespace"]:
                self._logger.warning(
                    "Ignoring registration of namespace '%s' as entry already exists",
                    item["namespace"],
                )
                continue

            fdp_store.store_namespace(
                self.local_uri, fdp_req.local_token(), **_namespace_metadata
            )

            self._parsed["namespace"].append(_namespace_metadata["name"])

        return _new_register_block

    def _fill_namespaces(self, block_type: str) -> typing.List[typing.Dict]:
        self._logger.debug("Filling all namespaces")
        _entries: typing.List[typing.Dict] = []

        if block_type == "read":
            _default_ns = self.default_input_namespace
        else:
            _default_ns = self.default_output_namespace

        for item in self[block_type]:
            if all(i not in item for i in ("data_product", "external_object")):
                _entries.append(item)
                continue
            _new_item = copy.deepcopy(item)
            _new_item["use"] = item.get("use", {})

            if "namespace" not in _new_item["use"]:
                _new_item["use"]["namespace"] = _default_ns

            _entries.append(_new_item)

        return _entries

    def _switch_namespace_name_to_use(self, register_block: typing.List):
        """
        Checks namespace listed in 'namespace_name' if given, if there is a
        match the entry is removed and replaced with a 'use:namespace' entry
        """
        _new_register_block: typing.List[typing.Dict] = []
        for register_entry in register_block:
            _new_entry = register_entry.copy()
            if "namespace_name" not in register_entry:
                continue
            if (
                register_entry["namespace_name"]
                not in self._parsed["namespace"]
            ):
                self._logger.error(
                    "'%s' not in available namespaces:\n\t-%s",
                    register_entry["namespace_name"],
                    "\n\t-".join(self._parsed["namespace"]),
                )
                raise fdp_exc.UserConfigError(
                    "Attempt to register object with unknown namespace "
                    "'" + register_entry["namespace_name"] + "'",
                    "Add new 'namespace' as separate 'register' entry",
                )
            if "use" not in _new_entry:
                _new_entry["use"] = {}
            _new_entry["use"]["namespace"] = register_entry["namespace_name"]
            _new_register_block.append(_new_entry)

        return _new_register_block

    def _update_namespaces(self) -> None:
        self._logger.debug("Updating namespace list")

        # Only allow namespaces to be registered directly
        if "register" in self:
            self["register"] = self._handle_register_namespaces(
                self["register"]
            )
            self["register"] = self._handle_register_authors(self["register"])
            self["register"] = self._switch_namespace_name_to_use(
                self["register"]
            )

        if not self.default_input_namespace:
            raise fdp_exc.UserConfigError("Input namespace cannot be None")

        if not self.default_output_namespace:
            raise fdp_exc.UserConfigError("Output namespace cannot be None")

        for block_type in self._block_types:
            if block_type not in self:
                continue
            self[block_type] = self._fill_namespaces(block_type)

    def _globular_registry_search(
        self,
        registry_uri: str,
        registry_token: str,
        block_entry: typing.Dict[str, typing.Any],
        block_type: str,
    ) -> typing.List[typing.Dict]:
        """Performs globular search in the specified registry

        Any '*' wildcards are used to perform
        """
        _vals_to_check = (
            i for i in block_entry.values() if isinstance(i, str)
        )
        if all("*" not in v for v in _vals_to_check):
            return [block_entry]

        _new_entries: typing.List[typing.Dict] = []
        _obj_type = None
        for obj in fdp_valid.VALID_OBJECTS:
            # Identify object type
            if obj in block_entry:
                _obj_type = obj
                break

        if not _obj_type:
            raise fdp_exc.UserConfigError(
                f"Unrecognised object type for wildcard search in: {block_entry}"
            )

        _search_key = SEARCH_KEYS[_obj_type]

        try:
            _results_local = fdp_req.get(
                registry_uri,
                _obj_type,
                registry_token,
                params={_search_key: block_entry[_obj_type]},
            )
        except fdp_exc.RegistryAPICallError:
            raise fdp_exc.UserConfigError(
                f"Failed to retrieve entries on local registry for {_obj_type}"
                f" wildcard '{block_entry[_obj_type]}'"
            )

        if _obj_type in ("namespace", "author"):
            # If the object is a namespace or an author then there is no
            # additional info in the registry so we can just add the entries
            # as they are
            _new_entries = fdp_glob.get_single_layer_objects(
                _results_local, _obj_type
            )

        elif _obj_type == "external_object":
            # If the object is an external_object we firstly need to get the
            # name of the data product, version and the namespace of this object
            # as well as the identifier
            _version = block_entry.get("version", None)

            _new_entries = fdp_glob.get_external_objects(
                registry_token, _results_local, block_type, _version
            )

        elif _obj_type == "data_product":
            _version = block_entry.get("version", None)

            _new_entries = fdp_glob.get_data_product_objects(
                registry_token, _results_local, block_type, _version
            )

        if block_type == "write":
            _new_entries.append(block_entry)

        return _new_entries

    def _expand_wildcards(
        self, registry_uri: str, registry_token: str
    ) -> None:
        self._logger.debug(
            f"Expanding wildcards using registry '{registry_uri}"
        )
        for block in self._block_types:
            if block not in self:
                continue
            _new_block: typing.List[typing.Dict] = []
            for block_entry in self[block]:
                _new_block_entries = self._globular_registry_search(
                    registry_uri, registry_token, block_entry, block
                )
                _new_block += _new_block_entries

            self[block] = _new_block

    def _fetch_latest_commit(self, allow_dirty: bool = False) -> None:
        self._logger.debug(
            f"Retrieving latest commit SHA with allow_dirty={allow_dirty}"
        )
        try:
            _repository = git.Repo(
                fdp_com.find_git_root(self.local_repository)
            )
        except git.InvalidGitRepositoryError:
            raise fdp_exc.FDPRepositoryError(
                f"Location '{self._local_repository}' is not a valid git repository"
            )

        try:
            _latest = _repository.head.commit.hexsha
        except git.InvalidGitRepositoryError:
            raise fdp_exc.FDPRepositoryError(
                f"Location '{self.local_repository}' is not a valid git repository"
            )
        except ValueError:
            raise fdp_exc.FDPRepositoryError(
                "Failed to retrieve latest commit for local "
                f"repository '{self.local_repository}'",
                hint="Have any changes been committed "
                "in the project repository?",
            )

        if _repository.is_dirty():
            if not allow_dirty:
                _changes = [i.a_path for i in _repository.index.diff(None)]
                self._logger.error(
                    "Cannot retrieve latest commit for repository with allow_dirty=False, "
                    "the follow files have uncommitted changes:\n\t- %s",
                    "\n\t- ".join(_changes),
                )
                raise fdp_exc.FDPRepositoryError(
                    "Cannot retrieve latest commit, "
                    "repository contains uncommitted changes"
                )
            _latest = f"{_latest}-dirty"

        return _latest

    def setup_job_script(self) -> typing.Dict[str, typing.Any]:
        """Setup a job script from the given configuration.

        Checks the user configuration file for the required 'script' or 'script_path'
        keys and determines the process to be executed. Also sets up an environment
        usable when executing the submission script.

        Parameters
        ----------
        local_repo : str
            local FAIR repository
        script : str
            script to write to file
        config_dir : str
            final location of output config.yaml
        output_dir : str
            location to store submission/job script

        Returns
        -------
        Dict[str, Any]
            a dictionary containing information on the command to execute,
            which shell to run it in and the environment to use
        """
        self._logger.debug("Setting up job script for execution")
        _cmd = None

        if not self._job_dir:
            raise fdp_exc.InternalError("Job directory initialisation failed")

        config_dir = self._job_dir

        if config_dir[-1] != os.path.sep:
            config_dir += os.path.sep

        # Check if a specific shell has been defined for the script
        _shell = None
        _out_file = None

        if "shell" in self["run_metadata"]:
            _shell = self["run_metadata.shell"]
        else:
            _shell = "batch" if platform.system() == "Windows" else "sh"

        self._logger.debug("Will use shell: %s", _shell)

        if "script" in self["run_metadata"]:
            _cmd = self["run_metadata.script"]

            if "extension" not in SHELLS[_shell]:
                raise fdp_exc.InternalError(
                    f"Failed to retrieve an extension for shell '{_shell}'"
                )
            _ext = SHELLS[_shell]["extension"]
            _out_file = os.path.join(self._job_dir, f"script.{_ext}")
            if _cmd:
                with open(_out_file, "w") as f:
                    f.write(_cmd)

        elif "script_path" in self["run_metadata"]:
            _path = self["run_metadata.script_path"]
            if not os.path.exists(_path):
                raise fdp_exc.CommandExecutionError(
                    f"Failed to execute run, script '{_path}' was not found, or"
                    " failed to be created.",
                    exit_code=1,
                )
            _cmd = open(_path).read()
            _out_file = os.path.join(self._job_dir, os.path.basename(_path))
            if _cmd:
                with open(_out_file, "w") as f:
                    f.write(_cmd)

        self._logger.debug("Script command: %s", _cmd)
        self._logger.debug("Script written to: %s", _out_file)

        if not _cmd or not _out_file:
            raise fdp_exc.UserConfigError(
                "Configuration file must contain either a valid "
                "'script' or 'script_path' entry under 'run_metadata'"
            )

        self.set_script(_out_file)

        return {"shell": _shell, "script": _out_file}

    def update_from_fair(
        self, fair_repo_dir: str = None, remote_label: str = None
    ) -> None:
        """Update any missing entries from defaults defined by FAIR CLI configurations

        Reads CLI configurations both globally and optionally from a specified FAIR
        repository and updates assigning any keys

        Parameters
        ----------
        fair_repo_dir : str, optional
            local FAIR repository directory
        remote_label : str, optional
            specify alternate remote registry by label, default is 'origin'
        """
        self._logger.debug(f"Updating configuration from {fair_repo_dir}")
        if fair_repo_dir and not os.path.exists(fair_repo_dir):
            raise fdp_exc.FileNotFoundError(
                "Cannot update configuration from repository, location"
                f" '{fair_repo_dir}' does not exist"
            )

        # Combine global and remote CLI configurations by overwriting global
        # with changes in the local version
        _fdpconfig = fdp_util.flatten_dict(fdp_conf.read_global_fdpconfig())
        if fair_repo_dir:
            _local_fdpconfig = fdp_util.flatten_dict(
                fdp_conf.read_local_fdpconfig(fair_repo_dir)
            )
            _fdpconfig.update(_local_fdpconfig)
        for key in JOB2CLI_MAPPINGS:
            if key not in self:
                self[key] = _fdpconfig[JOB2CLI_MAPPINGS[key]]

        if remote_label:
            _key = "run_metadata.remote_data_registry_url"
            _other_key = JOB2CLI_MAPPINGS[_key].replace("origin", remote_label)
            self[_key] = _fdpconfig[_other_key]

        if fair_repo_dir and "run_metadata.remote_repo" not in self:
            _remote = _fdpconfig["git.remote"]

            # If local repository stated in loaded config use that, else if
            # already defined use existing location, else use specified directory
            try:
                _local_repo = _fdpconfig["git.local_repo"]
            except KeyError:
                if "run_metadata.local_repo" in self:
                    _local_repo = self["run_metadata.local_repo"]
                else:
                    _local_repo = fair_repo_dir

            try:
                _git_repo = git.Repo(_local_repo)
            except git.InvalidGitRepositoryError:
                raise fdp_exc.FDPRepositoryError(
                    f"Failed to update job configuration from location '{fair_repo_dir}', "
                    "not a valid git repository."
                )
            _url = _git_repo.remotes[_remote].url
            self["run_metadata.remote_repo"] = _url

    def pop(
        self, key: str, default: typing.Optional[typing.Any] = None
    ) -> typing.Any:
        """Remove item from configuration"""
        try:
            del self[key]
        except fdp_exc.KeyPathError:
            return default

    def get(
        self, key: str, default: typing.Optional[typing.Any] = None
    ) -> typing.Any:
        """Retrieve item if exists, else return default"""
        try:
            _value = self[key]
        except fdp_exc.KeyPathError:
            _value = default
        self._logger.debug(f"Returning '{key}={_value}'")
        return _value

    def set_command(
        self, cmd: str, shell: typing.Optional[str] = None
    ) -> None:
        """Set a BASH command to be executed"""
        if not shell:
            shell = "batch" if platform.system() == "Windows" else "sh"
        self._logger.debug(f"Setting {shell} command to '{cmd}'")
        self["run_metadata.script"] = cmd
        self["run_metadata.shell"] = shell
        self.pop("run_metadata.script_path")

    def _create_environment(self) -> None:
        """Create the environment for running a job"""
        _environment = os.environ.copy()
        _environment["FDP_LOCAL_REPO"] = self.local_repository
        if "PYTHONPATH" in _environment:
            _new_py_path = _environment["PYTHONPATH"]
            _new_py_path += os.pathsep + self.local_repository
        else:
            _new_py_path = self.local_repository
        _environment["PYTHONPATH"] = _new_py_path
        _environment["FDP_CONFIG_DIR"] = self._job_dir
        _environment["FDP_CONFIG_NAME"] = fdp_com.USER_CONFIG_FILE
        _environment["FDP_DATA_STORE"] = self.default_data_store
        _environment["FDP_SCRIPT"] = self.script
        _environment["FDP_LOCAL_TOKEN"] = fdp_req.local_token()
        return _environment

    def set_script(self, command_script: str) -> None:
        """Set a BASH command to be executed"""
        self._logger.debug(f"Setting command script to '{command_script}'")
        self["run_metadata.script_path"] = command_script
        self.pop("run_metadata.script")

    def _create_log(self, command: str = None) -> None:
        _logs_dir = fdp_hist.history_directory(self.local_repository)

        if not os.path.exists(_logs_dir):
            os.makedirs(_logs_dir)

        _time_stamp = self._now.strftime("%Y-%m-%d_%H_%M_%S_%f")
        self._log_file_path = os.path.join(_logs_dir, f"job_{_time_stamp}.log")
        self._logger.debug(
            f"Will write session log to '{self._log_file_path}'"
        )
        command = command or self.command
        self._log_file = open(self._log_file_path, "w")

    def prepare(
        self,
        job_mode: CMD_MODE,
        allow_dirty: bool = False,
        remote_uri: str = None,
        remote_token: str = None,
    ) -> str:
        """Initiate a job execution"""
        _time_stamp = self._now.strftime("%Y-%m-%d_%H_%M_%S_%f")
        self._job_dir = os.path.join(fdp_com.default_jobs_dir(), _time_stamp)

        # For push we do not need to do anything to the config as information
        # is taken from staging
        if job_mode == CMD_MODE.PUSH:
            self._create_log()
            return os.path.join(self._job_dir, fdp_com.USER_CONFIG_FILE)

        self._logger.debug("Preparing configuration")
        self._update_namespaces()

        os.makedirs(self._job_dir)
        self._create_log()
        self._subst_cli_vars(self._now)

        self._fill_all_block_types()

        if job_mode == CMD_MODE.PULL:
            _cmd = f"pull {self._input_file}"
            self._pull_push_log_header(_cmd)
        elif job_mode == CMD_MODE.PUSH:
            _cmd = "push"
            self._pull_push_log_header(_cmd)

        # If pulling glob from the remote, else glob from local
        if job_mode == CMD_MODE.PULL:
            if not remote_uri:
                raise fdp_exc.InternalError(
                    "Expected URI during wildcard unpacking for 'pull'"
                )
            if not remote_token:
                raise fdp_exc.InternalError(
                    "Expected token during wildcard unpacking for 'pull'"
                )
            self._expand_wildcards(remote_uri, remote_token)
        else:
            self._expand_wildcards(self.local_uri, fdp_req.local_token())

        for block_type in self._block_types:
            if block_type in self:
                self[block_type] = self._fill_versions(block_type)

        if job_mode == CMD_MODE.PULL and "register" in self:
            self._logger.debug("Fetching registrations")
            _objs = fdp_reg.fetch_registrations(
                local_uri=self.local_uri,
                repo_dir=self.local_repository,
                write_data_store=self.default_data_store,
                user_config_register=self["register"],
            )
            self._logger.debug("Fetched objects:\n %s", _objs)

        self._parsed["namespace"] = self._get_local_namespaces()
        self._parsed["author"] = self._get_local_authors()

        if "register" in self:
            if "read" not in self:
                self["read"] = []
            self["read"] += self._register_to_read(self["register"])

        if "read" in self:
            self["read"] = self._update_use_sections(self["read"])

        self._config = self._clean()

        self._check_for_unparsed()

        self["run_metadata.latest_commit"] = self._fetch_latest_commit(
            allow_dirty
        )

        # Perform config validation
        self._logger.debug("Running configuration validation")

        return os.path.join(self._job_dir, fdp_com.USER_CONFIG_FILE)

    def _pull_push_log_header(self, _cmd):
        _cmd = f"fair {_cmd}"
        _out_str = self._now.strftime("%a %b %d %H:%M:%S %Y %Z")
        _user = fdp_conf.get_current_user_name(self.local_repository)
        _email = fdp_conf.get_current_user_email(self.local_repository)
        self._log_file.writelines(
            [
                "--------------------------------\n",
                f" Commenced = {_out_str}\n",
                f" Author    = {' '.join(_user)} <{_email}>\n",
                f" Command   = {_cmd}\n",
                "--------------------------------\n",
            ]
        )

    def _check_for_unparsed(self) -> typing.List[str]:
        self._logger.debug("Checking for unparsed variables")
        _conf_str = yaml.dump(self._config)

        # Additional parser for formatted datetime
        _regex_fmt = re.compile(r"\$\{\{\s*([^}${\s]+)\s*\}\}")

        _unparsed = _regex_fmt.findall(_conf_str)

        if _unparsed:
            raise fdp_exc.InternalError(
                f"Failed to parse variables '{_unparsed}'"
            )

    def _subst_cli_vars(self, job_time: datetime.datetime) -> str:
        self._logger.debug("Searching for CLI variables")

        def _get_id():
            try:
                return fdp_conf.get_current_user_uri(self._job_dir)
            except fdp_exc.CLIConfigurationError:
                return fdp_conf.get_current_user_uuid(self._job_dir)

        def _tag_check():
            _repo = git.Repo(fdp_conf.local_git_repo(self.local_repository))
            if len(_repo.tags) < 1:
                fdp_exc.UserConfigError(
                    "Cannot use GIT_TAG variable, no git tags found."
                )
            return _repo.tags[-1].name

        _substitutes: typing.Dict[str, typing.Callable] = {
            "DATE": lambda: job_time.strftime("%Y%m%d"),
            "DATETIME": lambda: job_time.strftime("%Y-%m-%dT%H:%M:%S%Z"),
            "USER": lambda: fdp_conf.get_current_user_name(
                self.local_repository
            ),
            "USER_ID": lambda: _get_id(),
            "REPO_DIR": lambda: self.local_repository,
            "CONFIG_DIR": lambda: self._job_dir + os.path.sep,
            "LOCAL_TOKEN": lambda: fdp_req.local_token(),
            "SOURCE_CONFIG": lambda: os.path.basename(self._input_file),
            "GIT_BRANCH": lambda: self.git_branch,
            "GIT_REMOTE": lambda: self.git_remote_uri,
            "GIT_TAG": _tag_check,
        }

        # Additional parser for formatted datetime
        _regex_dt_fmt = re.compile(r"\$\{\{\s*DATETIME\-[^}${\s]]+\s*\}\}")
        _regex_fmt = re.compile(r"\$\{\{\s*DATETIME\-([^}${\s]+)\s*\}\}")

        _config_str: str = yaml.dump(self._config)

        _dt_fmt_res: typing.Optional[typing.List[str]] = _regex_dt_fmt.findall(
            _config_str
        )
        _fmt_res: typing.Optional[typing.List[str]] = _regex_fmt.findall(
            _config_str
        )

        self._logger.debug(
            "Found datetime substitutions: %s %s",
            _dt_fmt_res or "",
            _fmt_res or "",
        )

        # The two regex searches should match lengths
        if len(_dt_fmt_res) != len(_fmt_res):
            raise fdp_exc.UserConfigError(
                "Failed to parse formatted datetime variable"
            )

        if _dt_fmt_res:
            for i, _ in enumerate(_dt_fmt_res):
                _time_str = job_time.strftime(_fmt_res[i].strip())
                _config_str = _config_str.replace(_dt_fmt_res[i], _time_str)

        _regex_dict = {
            var: r"\$\{\{\s*" + f"{var}" + r"\s*\}\}" for var in _substitutes
        }

        # Perform string substitutions
        for var, subst in _regex_dict.items():
            # Only execute functions in var substitutions that are required
            if re.findall(subst, _config_str):
                _value = _substitutes[var]()
                if not _value:
                    raise fdp_exc.InternalError(
                        f"Expected value for substitution of '{var}' but returned None",
                    )
                _config_str = re.sub(subst, str(_value), _config_str)
                self._logger.debug("Substituting %s: %s", var, str(_value))

        self._config = yaml.safe_load(_config_str)

    def _register_to_read(
        self, register_block: typing.List[typing.Dict]
    ) -> typing.List[typing.Dict]:
        """Construct 'read' block entries from 'register' block entries

        Parameters
        ----------
        register_block : typing.Dict
            register type entries within a config.yaml

        Returns
        -------
        typing.List[typing.Dict]
            new read entries extract from register statements
        """
        _read_block: typing.List[typing.Dict] = []

        for item in register_block:
            _readable = item.copy()
            if "external_object" in item:
                _readable["data_product"] = item["external_object"]
                _readable.pop("external_object")

            # 'public' only valid for writables
            if "public" in _readable:
                _readable.pop("public")

            # Add extra tag for tracking objects which have been registered
            # as opposed to pulled from a remote
            _readable["registered"] = True

            _read_block.append(_readable)
        return _read_block

    def _update_use_sections(
        self, read_block: typing.List[typing.Dict]
    ) -> typing.List[typing.Dict]:
        _new_read_block: typing.List[typing.Dict] = []

        for entry in read_block:
            # Use statement updates only relevant to data_product
            if "data_product" not in entry:
                _new_read_block.append(_new_entry)
                continue
            _new_entry = entry.copy()
            _new_entry["use"]["version"] = fdp_ver.undo_incrementer(
                entry["use"]["version"]
            )
            if "namespace" not in entry["use"]:
                if "namespace_name" in entry:
                    _new_entry["use"]["namespace"] = entry["namespace_name"]
                else:
                    _new_entry["use"][
                        "namespace"
                    ] = self.default_input_namespace
            _new_read_block.append(_new_entry)
        return _new_read_block

    def _clean(self) -> typing.Dict:
        self._logger.debug("Cleaning configuration")
        _new_config: typing.Dict = {
            "run_metadata": copy.deepcopy(self["run_metadata"])
        }

        for action in ("read", "write"):
            if f"default_{action}_version" in _new_config["run_metadata"]:
                del _new_config["run_metadata"][f"default_{action}_version"]

        for block_type in ("read", "write"):
            if block_type not in self:
                continue

            _new_config[block_type] = []

            for item in self[block_type]:
                _new_item = item.copy()
                # Keep only the final permitted keys, this may vary depending
                # on block type, also allow internal status check tags to
                # pass at this stage
                _allowed = list(self._final_permitted["all"])
                if block_type in self._final_permitted:
                    _allowed += list(self._final_permitted[block_type])
                _allowed += list(self._status_tags)

                for key in item.keys():
                    if key not in _allowed:
                        _new_item.pop(key)

                _new_config[block_type].append(_new_item)

        for block in self._block_types:
            if block in _new_config and not _new_config[block]:
                del _new_config[block]

        return _new_config

    def _fill_versions(self, block_type: str) -> typing.List[typing.Dict]:
        self._logger.debug("Filling version information")
        _entries: typing.List[typing.Dict] = []

        if block_type == "read":
            _default_ver = self.default_read_version
        else:
            _default_ver = self.default_write_version

        for item in self[block_type]:
            if all(i not in item for i in ("data_product", "external_object")):
                _entries.append(item)
                continue

            _new_item = copy.deepcopy(item)
            _new_item["use"] = item.get("use", {})

            # --------------------------------------------------------------- #
            # FIXME: Schema needs to be finalised, allowing item:version and
            # also item:use:version will cause confusion.

            if "version" in item and "version" not in _new_item["use"]:
                _new_item["use"]["version"] = _new_item["version"]
                _new_item.pop("version")

            # --------------------------------------------------------------- #

            if "version" not in _new_item["use"]:
                _new_item["use"]["version"] = _default_ver

            _version = item["use"]["version"]

            if "data_product" not in item["use"]:
                if (
                    "external_object" in item
                    and "*" in item["external_object"]
                ):
                    _name = item["external_object"]
                elif "data_product" in item and "*" in item["data_product"]:
                    _name = item["data_product"]
                else:
                    self._logger.warning(f"Missing use:data_product in {item}")
                continue

            _name = item["use"]["data_product"]
            _namespace = item["use"]["namespace"]

            # If no ID exists for the namespace then this object has not yet
            # been written to the target registry and so a version number
            # cannot be deduced this way
            try:
                _id_namespace = fdp_reg.convert_key_value_to_id(
                    self.local_uri,
                    "namespace",
                    _namespace,
                    fdp_req.local_token(),
                )
                if "${{" in _version:
                    _results = fdp_req.get(
                        self.local_uri,
                        "data_product",
                        fdp_req.local_token(),
                        params={"name": _name, "namespace": _id_namespace},
                    )
                    if "LATEST" in _version:
                        _version = fdp_ver.get_latest_version(_results)
                else:
                    _results = fdp_req.get(
                        self.local_uri,
                        "data_product",
                        fdp_req.local_token(),
                        params={
                            "name": _name,
                            "namespace": _id_namespace,
                            "version": _version,
                        },
                    )
            except fdp_exc.RegistryError:
                _results = []

            try:
                _version = fdp_ver.get_correct_version(
                    version=_version,
                    results_list=_results,
                    free_write=block_type != "read",
                )
            except fdp_exc.UserConfigError as e:
                if block_type == "register":
                    self._logger.debug(
                        f"Already found this version ({e}), but may be identical"
                    )
                else:
                    self._logger.error(
                        f"Failed to find version match for {item}"
                    )
                    raise e

            if "${{" in _version:
                self._logger.error(
                    f"Found a version ({_version}) that needs resolving"
                )

            if str(_version) != item["use"]["version"]:
                _new_item["use"]["version"] = str(_version)

            _entries.append(_new_item)

        return _entries

    def _fill_block_data_product(
        self, block_type: str, item: typing.Dict
    ) -> typing.Dict:
        _new_item = copy.deepcopy(item)
        if "namespace" in item:
            _new_item["use"]["namespace"] = item["namespace"]
            _new_item.pop("namespace")

        if "namespace" not in item["use"]:
            if block_type == "read":
                item["use"]["namespace"] = self.default_input_namespace
            elif block_type == "write":
                item["use"]["namespace"] = self.default_output_namespace
            elif block_type == "register":
                item["use"]["namespace"] = item["namespace_name"]
            else:
                raise fdp_exc.NotImplementedError(
                    f"Failed to understand block type '{block_type}'"
                )

        if "version" not in item["use"]:
            if "version" in item:
                _new_item["use"]["version"] = item["version"]
                _new_item.pop("version")
            elif block_type == "read":
                _new_item["use"]["version"] = self.default_read_version
            else:  # 'write' or 'register'
                _new_item["use"]["version"] = self.default_write_version

        if (
            "data_product" not in item["use"]
            and "*" not in item["data_product"]
        ):
            _new_item["use"]["data_product"] = item["data_product"]

        if block_type == "register" and "external_object" in item:
            _new_item.pop("data_product", None)

        return _new_item

    def _fill_block_item(
        self, block_type: str, item: typing.Dict
    ) -> typing.Dict:
        _new_item = copy.deepcopy(item)
        _new_item["use"] = item.get("use", {})

        _reg_obj = [
            block_type == "register",
            "external_object" in item,
            "data_product" not in item,
            "data_product" not in _new_item["use"],
        ]

        if all(_reg_obj):
            _new_item["use"]["data_product"] = item["external_object"]

        if "data_product" in item:
            _new_item = self._fill_block_data_product(block_type, _new_item)

        if block_type in {"write", "register"} and "public" not in item:
            _new_item["public"] = self.is_public_global

        return _new_item

    def _fill_all_block_types(self) -> bool:
        for block_type in self._block_types:
            self._logger.debug(f"Filling '{block_type}' block")
            _new_block: typing.List[typing.Dict] = []

            if block_type not in self:
                continue

            for item in self[block_type]:
                _new_item = self._fill_block_item(block_type, item)
                _new_block.append(_new_item)

            self[block_type] = _new_block

    def _remove_status_tags(self) -> bool:
        """
        Removes any internal tags added by the config class for
        tracking status of objects
        """
        for block in self._block_types:
            if block not in self:
                continue
            for i, _ in enumerate(self[block]):
                for key in self._status_tags:
                    self[block][i].pop(key, None)

    @property
    def script(self) -> str:
        """Retrieve path of session executable script"""
        return self.get("run_metadata.script_path", None)

    @property
    def content(self) -> typing.Dict:
        """Return a copy of the internal dictionary"""
        return copy.deepcopy(self._config)

    @property
    def shell(self) -> str:
        """Retrieve the shell choice"""
        _shell_default = "batch" if platform.system() == "Windows" else "sh"
        return self.get("shell", _shell_default)

    @property
    def local_repository(self) -> str:
        """Retrieves the local project repository"""
        return self["run_metadata.local_repo"]

    @property
    def default_data_store(self) -> str:
        """Retrieves the default write data store"""
        return self["run_metadata.write_data_store"]

    @property
    def default_input_namespace(self) -> str:
        """Retrieves the default write data store"""
        return self["run_metadata.default_input_namespace"]

    @property
    def default_output_namespace(self) -> str:
        """Retrieves the default write data store"""
        return self["run_metadata.default_output_namespace"]

    @property
    def default_read_version(self) -> str:
        """Retrieves the default read version"""
        return self.get(
            "run_metadata.default_read_version",
            "${{" + fdp_ver.DEFAULT_READ_VERSION + "}}",
        )

    @property
    def default_write_version(self) -> str:
        """Retrieves the default write version"""
        return self.get(
            "run_metadata.default_write_version",
            "${{" + fdp_ver.DEFAULT_WRITE_VERSION + "}}",
        )

    @property
    def is_public_global(self) -> bool:
        """Retrieves global publicity setting"""
        return self.get("run_metadata.public", True)

    @property
    def local_uri(self) -> str:
        """Retrieves the local URI for registry for this session"""
        return self.get(
            "run_metadata.local_data_registry_url", fdp_conf.get_local_uri()
        )

    @property
    def remote_uri(self) -> str:
        """Retrieves the remote URI for registry for this session"""
        return self.get(
            "run_metadata.remote_data_registry_url",
            fdp_conf.get_remote_uri(self.local_repository),
        )

    @property
    def git_remote_uri(self) -> str:
        """Retrieves the URI of the remote repository on git"""
        return self["run_metadata.remote_repo"]

    @property
    def git_branch(self) -> str:
        """Retrieves the current git repository branch"""
        _git_loc = fdp_conf.local_git_repo(self.local_repository)
        return git.Repo(_git_loc).active_branch.name

    @property
    def command(self) -> typing.Optional[str]:
        """Returns either the script or script path to be executed"""
        for key in ("script", "script_path"):
            if key in self["run_metadata"]:
                return self[f"run_metadata.{key}"]
        return None

    @property
    def environment(self) -> typing.Dict:
        """Returns the job execution environment"""
        return self.env

    def execute(self) -> int:
        """Execute script/command if specified

        Returns
        -------
        int
            exit code of the executed process
        """
        if not self.command:
            raise fdp_exc.UserConfigError("No command specified to execute")
        _out_str = self._now.strftime("%a %b %d %H:%M:%S %Y %Z")
        _user = fdp_conf.get_current_user_name(self.local_repository)
        _email = fdp_conf.get_current_user_email(self.local_repository)

        self._log_file.writelines(
            [
                "--------------------------------\n",
                f" Commenced = {_out_str}\n",
                f" Author    = {' '.join(_user)} <{_email}>\n",
                f" Command   = {self.command}\n",
                "--------------------------------\n",
            ]
        )

        if not self.env:
            raise fdp_exc.InternalError(
                "Command execution environment setup failed"
            )

        _exec = SHELLS[self.shell]["exec"].format(self.script)

        self._logger.debug("Executing command: %s", _exec)

        _log_tail: typing.List[str] = []

        _process = subprocess.Popen(
            _exec.split(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1,
            text=True,
            shell=False,
            env=self.env,
            cwd=self.local_repository,
        )

        # Write any stdout to the job log
        for line in iter(_process.stdout.readline, ""):
            self._log_file.writelines([line])
            _log_tail.append(line)
            click.echo(line, nl=False)
            sys.stdout.flush()

        _process.wait()

        if _process.returncode != 0:
            self.close_log()
            self._logger.error(
                "Command '%s' failed with exit code %s, log tail:\n\t%s",
                _exec,
                _process.returncode,
                "\n\t".join(_log_tail),
            )
            raise fdp_exc.CommandExecutionError(
                f"Executed 'run' command failed with exit code {_process.returncode}",
                _process.returncode,
            )

    def close_log(self) -> None:
        _time_finished = datetime.datetime.now()
        _duration = _time_finished - self._now
        self._log_file.writelines(
            [
                f"------- time taken {_duration} -------\n",
            ]
        )
        self._log_file.close()

    def get_readables(self) -> typing.List[str]:
        """Returns list form of items to retrieve

        Returns
        -------
        typing.List[str]
            list of data products to retrieve
        """
        self._logger.debug("Retrieving list of 'read' items")
        _readables: typing.List[str] = []
        if "read" not in self:
            return _readables

        # TODO: For now only supports data products
        for readable in self["read"]:
            # In this context readables are items to be read from a remote
            # registry, not items registered locally
            if "registered" in readable:
                continue

            if "data_product" not in readable:
                continue
            if "use" not in readable:
                self._logger.error(
                    "Incomplete read block, expected key 'use' in:\n"
                    f"\t{readable}"
                )
                raise fdp_exc.UserConfigError(
                    "Attempt to access 'read' listings before parsing complete"
                )
            if any(v not in readable["use"] for v in ("version", "namespace")):
                self._logger.error(
                    "Incomplete read block, expected keys 'namespace' and 'version' in:\n"
                    f'\t{readable["use"]}'
                )
                raise fdp_exc.UserConfigError(
                    "Attempt to access 'read' listings before parsing complete"
                )
            _version = readable["use"]["version"]
            _namespace = readable["use"]["namespace"]
            _name = readable["data_product"]

            # If the user has requested to use a cached version, do not
            # add to the list of items to read externally
            if "cache" not in readable["use"]:
                _readables.append(f"{_namespace}:{_name}@v{_version}")

        return _readables

    @property
    def hash(self) -> str:
        """Get job hash"""
        return fdp_run.get_job_hash(self._job_dir)

    def write_log_lines(self, log_file_lines: typing.List[str]) -> None:
        """Add lines to the current session log file"""
        self._log_file.writelines(log_file_lines)

    def write(self, output_file: str = None) -> str:
        """Write job configuration to file"""
        self._remove_status_tags()

        # Validate the file before writing
        try:
            fdp_valid.UserConfigModel(**self._config)
        except pydantic.ValidationError as e:
            self._logger.error(
                "Validation of generated user configuration file failed:"
                "\nCONFIG:\n%s\n\nRESULT:\n%s",
                self._config,
                json.loads(e.json()),
            )
            raise fdp_exc.ValidationError(e.json())

        if not output_file:
            if not self._job_dir:
                raise fdp_exc.UserConfigError(
                    "Cannot write new user configuration file, "
                    "no job directory created and no alternative filename provided"
                )
            output_file = os.path.join(self._job_dir, fdp_com.USER_CONFIG_FILE)
        with open(output_file, "w") as out_f:
            yaml.dump(self._config, out_f)

        self.env = self._create_environment()

        self._logger.debug(f"Configuration written to '{output_file}'")

        return output_file
