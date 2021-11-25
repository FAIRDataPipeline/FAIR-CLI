import copy
import datetime
import logging
import os
import os.path
import platform
import re
import typing
from collections.abc import MutableMapping

import git
import yaml
import pydantic

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.parsing.globbing as fdp_glob
import fair.register as fdp_reg
import fair.registry.requests as fdp_req
import fair.registry.storage as fdp_store
import fair.registry.versioning as fdp_ver
import fair.utilities as fdp_util
from fair.common import CMD_MODE
from fair.user_config.validation import UserConfigModel

JOB2CLI_MAPPINGS = {
    "run_metadata.local_repo": "git.local_repo",
    "run_metadata.default_input_namespace": "namespaces.input",
    "run_metadata.default_output_namespace": "namespaces.output",
    "run_metadata.local_data_registry_url": "registries.local.uri",
    "run_metadata.remote_data_registry_url": "registries.origin.uri",
    "run_metadata.write_data_store": "registries.local.data_store",
}


class JobConfiguration(MutableMapping):
    _logger = logging.getLogger("FAIRDataPipeline.ConfigYAML")
    _block_types = ("register", "write", "read")

    def __init__(self, config_yaml: str) -> None:
        if not os.path.exists(config_yaml):
            raise fdp_exc.FileNotFoundError(
                f"Cannot load job configuration from file '{config_yaml}', "
                "file does not exist"
            )

        self._logger.debug("Loading file '%s'", config_yaml)

        self._config: typing.Dict = yaml.safe_load(open(config_yaml))

        self._fill_missing()

        self.env = None

    def __contains__(self, key_addr: str) -> bool:
        return key_addr in fdp_util.flatten_dict(self._config)

    def __setitem__(
        self, key_addr: str, value: typing.Any, separator: str = "."
    ) -> None:
        if key_addr in self._config:
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
            raise fdp_exc.KeyPathError(key_addr, fdp_com.USER_CONFIG_FILE)
        self._logger.debug(f"Removing '{key_addr}'")
        del _flat_cfg[key_addr]
        self._config = fdp_util.expand_dict(_flat_cfg)

    def __getitem__(self, key_addr: str, separator: str = ".") -> None:
        if key_addr in self._config:
            return self._config[key_addr]
        _flat_cfg = fdp_util.flatten_dict(self._config, separator)
        if key_addr not in _flat_cfg:
            raise fdp_exc.KeyPathError(key_addr, fdp_com.USER_CONFIG_FILE)
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
        if "run_metadata" not in self._config:
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

    def _handle_register_namespaces(self) -> typing.Dict:
        self._logger.debug("Handling 'register' namespaces")
        _new_register_block: typing.List[typing.Dict] = []
        for item in self["register"]:
            if any(i in item for i in ("external_object", "data_product")):
                if "namespace_name" in item:
                    _namespace_store_args = {
                        "namespace_label": item["namespace_name"],
                        "full_name": item.get("namespace_full_name", None),
                        "website": item.get("namespace_website", None),
                    }
                elif "namespace" in item and isinstance(item["namespace"], dict):
                    _namespace_store_args = item["namespace"]
                _new_register_block.append(item)
            elif "namespace" in item:
                _namespace_store_args = {
                    "namespace_label": item["namespace"],
                    "full_name": item.get("full_name", None),
                    "website": item.get("website", None),
                }
            else:
                _new_register_block.append(item)
            fdp_store.store_namespace(self.local_uri, **_namespace_store_args)
        return _new_register_block

    def _unpack_register_namespaces(self) -> None:
        self._logger.debug("Unpacking 'register' namespaces")
        for i, item in enumerate(self._config["register"]):
            if all(it not in item for it in ["external_object", "data_product"]):
                continue

            if "namespace" not in item:
                continue

            if isinstance(item["namespace"], str):
                self._config["register"][i]["namespace_name"] = item["namespace"]
            elif isinstance(item["namespace"], dict):
                self._config["register"][i]["namespace_name"] = item["namespace"][
                    "name"
                ]
                self._config["register"][i]["namespace_full_name"] = None
                self._config["register"][i]["namespace_website"] = None
                del self._config["register"][i]["namespace"]

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

            # --------------------------------------------------------------- #
            # FIXME: Schema needs to be finalised, allowing item:namespace and
            # also item:use:namespace will cause confusion.

            if "namespace" in item and "namespace" not in _new_item["use"]:
                _new_item["use"]["namespace"] = _new_item["namespace"]
                _new_item.pop("namespace")

            # --------------------------------------------------------------- #

            if "namespace" not in _new_item["use"]:
                _new_item["use"]["namespace"] = _default_ns

            _entries.append(_new_item)

        return _entries

    def _update_namespaces(self) -> None:
        self._logger.debug("Updating namespace list")
        if "register" in self._config:
            self["register"] = self._handle_register_namespaces()

        if not self.default_input_namespace:
            raise fdp_exc.UserConfigError("Input namespace cannot be None")

        if not self.default_output_namespace:
            raise fdp_exc.UserConfigError("Output namespace cannot be None")

        fdp_store.store_namespace(self.local_uri, self.default_input_namespace)
        fdp_store.store_namespace(self.local_uri, self.default_output_namespace)

        for block_type in self._block_types:
            if block_type not in self:
                continue
            self[block_type] = self._fill_namespaces(block_type)

        if "register" in self._config:
            self._unpack_register_namespaces()

    def _expand_wildcards_from_local_reg(self, block_type: str) -> None:
        self._logger.debug("Expanding wildcards using local registry")
        _version = (
            self.default_read_version
            if block_type == "read"
            else self.default_write_version
        )
        fdp_glob.glob_read_write(
            user_config=self._config,
            blocktype=block_type,
            search_key=None,
            registry_url=self.local_uri,
            version=_version,
            remove_wildcard=block_type == "read",
        )

    def _fetch_latest_commit(self, allow_dirty: bool = False) -> None:
        self._logger.debug(
            f"Retrieving latest commit SHA with allow_dirty={allow_dirty}"
        )
        _repository = git.Repo(fdp_com.find_git_root(self.local_repository))

        try:
            _latest = _repository.head.commit.hexsha
        except git.InvalidGitRepositoryError:
            raise fdp_exc.FDPRepositoryError(
                f"Location '{self.local_repository}' is not a valid git repository"
            )
        except ValueError:
            raise fdp_exc.FDPRepositoryError(
                f"Failed to retrieve latest commit for local repository '{self.local_repository}'",
                hint="Have any changes been committed in the project repository?",
            )

        if _repository.is_dirty():
            if not allow_dirty:
                raise fdp_exc.FDPRepositoryError(
                    "Repository contains uncommitted changes"
                )
            _latest = f"{_latest}-dirty"

        return _latest

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
            _git_repo = git.Repo(fair_repo_dir)
            _url = _git_repo.remotes[_remote].url
            self["run_metadata.remote_repo"] = _url

    def pop(self, key: str, default: typing.Optional[typing.Any] = None) -> typing.Any:
        """Remove item from configuration"""
        try:
            del self[key]
        except fdp_exc.KeyPathError:
            return default

    def get(self, key: str, default: typing.Optional[typing.Any] = None) -> typing.Any:
        """Retrieve item if exists, else return default"""
        try:
            _value = self[key]
        except fdp_exc.KeyPathError:
            _value = default
        self._logger.debug(f"Returning '{key}={_value}'")
        return _value

    def set_command(self, cmd: str, shell: typing.Optional[str] = None) -> None:
        """Set a BASH command to be executed"""
        if not shell:
            shell = "batch" if platform.system() == "Windows" else "sh"
        self._logger.debug(f"Setting {shell} command to '{cmd}'")
        self["run_metadata.script"] = cmd
        self["run_metadata.shell"] = shell
        self.pop("run_metadata.script_path")

    def _create_environment(self, output_dir: str) -> None:
        """Create the environment for running a job"""
        _environment = os.environ.copy()
        _environment["FDP_LOCAL_REPO"] = self.local_repository
        _environment["FDP_CONFIG_DIR"] = output_dir
        _environment["FDP_LOCAL_TOKEN"] = fdp_req.local_token()
        return _environment

    def set_script(self, command_script: str) -> None:
        """Set a BASH command to be executed"""
        self._logger.debug(f"Setting command script to '{command_script}'")
        self["run_metadata.script_path"] = command_script
        self.pop("run_metadata.script")

    def _substitute_variables(
        self, job_dir: str, time_stamp: datetime.datetime
    ) -> None:
        self._logger.debug("Performing variable substitution")
        _config_str = self._subst_cli_vars(job_dir, time_stamp)
        self._config = yaml.safe_load(_config_str)

    def prepare(
        self, job_dir: str, time_stamp: datetime.datetime, job_mode: CMD_MODE
    ) -> None:
        """Initiate a job execution"""
        self._logger.debug("Preparing configuration")
        self._update_namespaces()
        self._substitute_variables(job_dir, time_stamp)

        self._fill_all_block_types()

        if job_mode in [CMD_MODE.PULL, CMD_MODE.PUSH]:
            self._pull_metadata()

        for block_type in ("read", "write"):
            if block_type not in self:
                continue
            try:
                self._expand_wildcards_from_local_reg(block_type)
            except fdp_exc.InternalError:
                continue

        for block_type in self._block_types:
            if block_type in self:
                self[block_type] = self._fill_versions(block_type)

        if "register" in self:
            if "read" not in self:
                self["read"] = []
            self["read"] += self._register_to_read()

        if job_mode in [CMD_MODE.PULL, CMD_MODE.PUSH]:
            self._pull_data()

        if job_mode == CMD_MODE.PULL and "register" in self:
            _objs = fdp_reg.fetch_registrations(
                local_uri=self.local_uri,
                repo_dir=self.local_repository,
                write_data_store=self.default_data_store,
                user_config_register=self["register"],
            )
            self._logger.debug("Fetched objects:\n %s", _objs)

        self._config = self._clean()

        _unparsed = self._check_for_unparsed()

        if _unparsed:
            raise fdp_exc.InternalError(f"Failed to parse variables '{_unparsed}'")

        self["run_metadata.latest_commit"] = self._fetch_latest_commit()

        # Perform config validation
        self._logger.debug("Running configuration validation")

        try:
            UserConfigModel(**self._config)
        except pydantic.ValidationError as e:
            raise fdp_exc.ValidationError(e.json())

    def _check_for_unparsed(self) -> typing.List[str]:
        self._logger.debug("Checking for unparsed variables")
        _conf_str = yaml.dump(self._config)

        # Additional parser for formatted datetime
        _regex_fmt = re.compile(r"\$\{\{\s*([^}${\s]+)\s*\}\}")

        return _regex_fmt.findall(_conf_str)

    def _subst_cli_vars(self, job_dir: str, job_time: datetime.datetime) -> str:
        self._logger.debug("Searching for CLI variables")

        def _get_id():
            try:
                return fdp_conf.get_current_user_uri(job_dir)
            except fdp_exc.CLIConfigurationError:
                return fdp_conf.get_current_user_uuid(job_dir)

        def _tag_check(*args, **kwargs):
            _repo = git.Repo(fdp_conf.local_git_repo(self.local_repository))
            if len(_repo.tags) < 1:
                fdp_exc.UserConfigError(
                    "Cannot use GIT_TAG variable, no git tags found."
                )
            return _repo.tags[-1].name

        _substitutes: typing.Dict[str, typing.Callable] = {
            "DATE": lambda: job_time.strftime("%Y%m%d"),
            "DATETIME": lambda: job_time.strftime("%Y-%m-%dT%H:%M:%S%Z"),
            "USER": lambda: fdp_conf.get_current_user_name(self.local_repository),
            "USER_ID": lambda: _get_id(),
            "REPO_DIR": lambda: self.local_repository,
            "CONFIG_DIR": lambda: job_dir + os.path.sep,
            "LOCAL_TOKEN": lambda: fdp_req.local_token(),
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
        _fmt_res: typing.Optional[typing.List[str]] = _regex_fmt.findall(_config_str)

        self._logger.debug(
            "Found datetime substitutions: %s %s", _dt_fmt_res or "", _fmt_res or ""
        )

        # The two regex searches should match lengths
        if len(_dt_fmt_res) != len(_fmt_res):
            raise fdp_exc.UserConfigError("Failed to parse formatted datetime variable")

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

        # Load the YAML (this also verifies the write was successful) and return it
        return _config_str

    def _register_to_read(self) -> typing.Dict:
        """Construct 'read' block entries from 'register' block entries

        Parameters
        ----------
        register_block : typing.Dict
            register type entries within a config.yaml

        Returns
        -------
        typing.Dict
            new read entries extract from register statements
        """
        _read_block: typing.List[typing.Dict] = []

        for item in self._config["register"]:
            _readable = {}
            if "use" in item:
                _readable["use"] = copy.deepcopy(item["use"])
            if "external_object" in item:
                _readable["data_product"] = item["external_object"]
            elif "data_product" in item:
                _readable["data_product"] = item["data_product"]
            elif "namespace" in item:
                fdp_store.store_namespace(**item)
            else:  # unknown
                raise fdp_exc.UserConfigError(
                    f"Found registration for unknown item with keys {[*item]}"
                )
            _readable["use"]["version"] = fdp_ver.undo_incrementer(
                _readable["use"]["version"]
            )

            _read_block.append(_readable)

        return _read_block

    def _clean(self) -> typing.Dict:
        self._logger.debug("Cleaning configuration")
        _new_config: typing.Dict = {}
        _new_config["run_metadata"] = copy.deepcopy(self["run_metadata"])

        for action in ("read", "write"):
            if f"default_{action}_version" in _new_config["run_metadata"]:
                del _new_config["run_metadata"][f"default_{action}_version"]

        _namespaces = (self.default_input_namespace, self.default_output_namespace)

        for namespace, block_type in zip(_namespaces, ["read", "write"]):
            if block_type not in self._config:
                continue

            _new_config[block_type] = []

            for item in self[block_type]:
                for use_item in [*item["use"]]:
                    # Get rid of duplicates
                    if use_item in item and item["use"][use_item] == item[use_item]:
                        item["use"].pop(use_item)

                if block_type == "write" and item["public"] == self.is_public_global:
                    item.pop("public")

                if item["use"]["namespace"] == namespace:
                    item["use"].pop("namespace")

                _new_config[block_type].append(item)

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

        for item in self._config[block_type]:
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
                if "external_object" in item and "*" in item["external_object"]:
                    _name = item["external_object"]
                elif "data_product" in item and "*" in item["data_product"]:
                    _name = item["data_product"]
                else:
                    self._logger.warning(f"Missing use:data_product in {item}")
                continue

            _name = item["use"]["data_product"]
            _namespace = item["use"]["namespace"]

            if "${{" in _version:
                _results = fdp_req.get(
                    self.local_uri,
                    "data_product",
                    params={"name": _name, "namespace": _namespace},
                )
            else:
                _results = fdp_req.get(
                    self.local_uri,
                    "data_product",
                    params={
                        "name": _name,
                        "namespace": _namespace,
                        "version": _version,
                    },
                )

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
                    self._logger.error(str(item))
                    raise e

            if "${{" in _version:
                self._logger.error(f"Found a version ({_version}) that needs resolving")

            if str(_version) != item["use"]["version"]:
                item["use"]["version"] = str(_version)

            _entries.append(item)

        return _entries

    def _pull_metadata(self) -> None:
        self._logger.debug("Pulling metadata from remote registry")
        self._logger.warning("Remote registry pulls are not yet implemented")

    def _pull_data(self) -> None:
        self._logger.debug("Pulling data from remote registry")
        self._logger.warning("Remote registry pulls are not yet implemented")

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

        if "data_product" not in item["use"] and "*" not in item["data_product"]:
            _new_item["use"]["data_product"] = item["data_product"]

        if block_type == "register" and "external_object" in item:
            _new_item.pop("data_product", None)

        return _new_item

    def _fill_block_item(self, block_type: str, item: typing.Dict) -> typing.Dict:
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

            self._config[block_type] = _new_block

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

    def write(self, output_file: str) -> None:
        """Write job configuration to file"""
        with open(output_file, "w") as out_f:
            yaml.dump(self._config, out_f)

        self.env = self._create_environment(os.path.dirname(output_file))

        self._logger.debug(f"Configuration written to '{output_file}'")
