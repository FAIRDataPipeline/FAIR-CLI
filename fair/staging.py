#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
Staging
=======

Manage object staging for pushing to a remote registry from the local registry

Contents
========

Classes
-------

    Staging - class handles staging of objects ready to be synchronised

"""

__date__ = "2021-07-13"

import logging
import os
import typing

import yaml

import fair.common as fdp_com
import fair.configuration as fdp_conf
import fair.exceptions as fdp_exc
import fair.registry.requests as fdp_req
import fair.run as fdp_run


class Stager:
    """
    The stager handles staging of jobs and other objects for synchronising
    between local and remote registries. In a manner similar to git, objects
    such as jobs can be tracked but not staged, for example if a job is a trial
    or has issues.
    """

    _logger = logging.getLogger("FAIRDataPipeline.Staging")

    def __init__(self, repo_root: str) -> None:
        """Create a new stager instance for a given FAIR repository

        Parameters
        ----------
        repo_root : str, optional
            the root of the FAIR repository, by default find the root from the
            current working directory
        """
        self._root = repo_root
        self._staging_file = fdp_com.staging_cache(self._root)
        self._logger.debug(
            "Creating stager for FAIR repository '%s'", repo_root
        )

    def initialise(self) -> None:
        """Initialise the stager, creating a staging cache file if one does not exist"""
        # Only create the staging file if one is not already present within the
        # specified directory
        if not os.path.exists(self._staging_file):
            self._logger.debug("Creating new staging cache file")
            # If the stager is called before the rest of the directory tree
            # has been created make the parent directories first
            if not os.path.exists(os.path.dirname(self._staging_file)):
                os.makedirs(os.path.dirname(self._staging_file), exist_ok=True)
            self._create_staging_file()
        else:
            self._logger.debug("Existing staging cache found")

    def _create_file_label(self, file_to_stage: str) -> str:
        return os.path.relpath(
            file_to_stage,
            os.path.dirname(self._staging_file),
        )

    def _create_staging_file(self) -> None:
        _staging_dict = {"job": {}, "file": {}}
        yaml.dump(_staging_dict, open(self._staging_file, "w"))

    def change_job_stage_status(self, job_id: str, stage: bool = True) -> None:
        """Stage a local code job ready to be pushed to the remote registry

        Parameters
        ----------
        job_id : str
            a valid uuid for the given job
        stage : bool, optional
            whether job is staged, default True
        """
        self._logger.debug(
            "Setting job '%s' status to staged=%s", job_id, stage
        )

        if not os.path.exists(self._staging_file):
            raise fdp_exc.FileNotFoundError(
                "Failed to update tracking, expected staging file"
                f" '{self._staging_file}' but it does not exist"
            )

        # Open the staging dictionary first
        _staging_dict = yaml.safe_load(open(self._staging_file))

        # When a job is completed by a language implementation the CLI should
        # have already registered it into staging with a status of staged=False
        if not fdp_run.get_job_dir(job_id):
            raise fdp_exc.StagingError(
                f"Failed to recognise job with ID '{job_id}'"
            )

        _staging_dict["job"][job_id] = stage

        with open(self._staging_file, "w") as f:
            yaml.dump(_staging_dict, f)

    def find_registry_entry_for_file(
        self, local_uri: str, file_path: str
    ) -> str:
        """Performs a rough search for a file in the local registry

        Parameters
        ----------
        file_path : str
            local file system path

        Returns
        -------
        str
            URL of local registry entry
        """
        self._logger.debug(
            "Retrieving registry entry for file '%s'", file_path
        )

        # Will search storage locations for a similar path by using
        # parent_directory/file_name
        _obj_type = "storage_location"

        _results = fdp_req.get(
            local_uri, _obj_type, params={"path": file_path}
        )

        if not _results:
            raise fdp_exc.StagingError(
                "Failed to find local registry entry for file "
                f"'{file_path}'"
            )

        if len(_results) > 1:
            raise fdp_exc.StagingError(
                "Expected single result for local registry entry relating to "
                f" file '{file_path} but got {len(_results)}"
            )

        self._logger.debug("Found config.yaml URL: %s", _results[0])

        return _results[0]

    def _get_code_run_entries(
        self, local_uri: str, job_dir: str
    ) -> typing.List[str]:
        """Retrieve code_run URL list from a given CLI run directory

        Parameters
        ----------
        local_uri : str
            local registry endpoint
        job_dir : str
            CLI run directory

        Returns
        -------
        typing.List[str]

        Raises
        ------
        fdp_exc.ImplementationError
            If the expected registry entries have not been created by an API
            implementation
        """
        self._logger.debug(
            "Retrieving code run URL for job '%s'", os.path.basename(job_dir)
        )
        # Check if any code_runs are present for the given job
        _code_run_file = os.path.join(job_dir, "coderuns.txt")
        _code_run_urls = []

        if (
            os.path.exists(_code_run_file)
            and open(_code_run_file).read().strip()
        ):
            self._logger.debug("Found coderuns file, extracting runs")
            _runs = [i.strip() for i in open(_code_run_file).readlines()]

            for run in _runs:
                _results = fdp_req.get(
                    local_uri, "code_run", params={"uuid": run}
                )

                if not _results:
                    raise fdp_exc.ImplementationError(
                        "Expected code_run with uuid "
                        f"'{run}' in local registry, but no result found."
                    )

                _code_run_urls.append(_results[0]["url"])

        return _code_run_urls

    def _get_written_obj_entries(
        self, local_uri: str, config_dict: typing.Dict
    ):
        if "write" not in config_dict:
            return []

        _data_product_urls: typing.List[str] = []

        for write_obj in config_dict["write"]:
            _data_product = write_obj["data_product"]
            _results = fdp_req.get(
                local_uri, "data_product", params={"name": _data_product}
            )

            if not _results:
                raise fdp_exc.InternalError(
                    "Expected data_product "
                    f"'{_data_product}' in local registry, but no result found."
                )

            _data_product_urls.append(_results[0]["url"])

        return _data_product_urls

    def get_job_data(
        self, local_uri, identifier: str
    ) -> typing.Dict[str, str]:
        self._logger.debug("Fetching job data for job %s", identifier)
        # Firstly find the job directory
        _directory = fdp_run.get_job_dir(identifier)
        if not _directory:
            raise fdp_exc.StagingError(
                f"Could not retrieve directory for job '{identifier}'"
            )

        # Check for a config.yaml file
        _config_yaml = os.path.join(_directory, fdp_com.USER_CONFIG_FILE)
        if not os.path.exists(_config_yaml):
            raise fdp_exc.FileNotFoundError(
                f"Cannot stage job '{identifier}' "
                f"Expected config.yaml in '{_directory}'"
            )

        # Find this config.yaml on the local registry, this involves
        # firstly getting the path commencing from the 'jobs' folder
        _config_rel_path = _config_yaml.split(fdp_com.JOBS_DIR)[1]
        _config_rel_path = f"{fdp_com.JOBS_DIR}{_config_rel_path}"

        _config_url = self.find_registry_entry_for_file(
            fdp_conf.get_local_uri(), _config_rel_path
        )["url"]

        # Check for job script file
        _config_yaml = os.path.join(_directory, fdp_com.USER_CONFIG_FILE)
        if not os.path.exists(_config_yaml):
            raise fdp_exc.FileNotFoundError(
                f"Cannot stage job '{identifier}' "
                f"Expected config.yaml in '{_directory}'"
            )

        # Find this job script on the local registry, as the script
        # can have any name obtain this information from the config.yaml
        _config_dict = yaml.safe_load(open(_config_yaml))

        if (
            "run_metadata" not in _config_dict
            or "script_path" not in _config_dict["run_metadata"]
        ):
            _script_url = None
        else:

            # Find the relevant script path on the local registry, this involves
            # firstly getting the path commencing from the 'jobs' folder
            self._logger.debug("Finding job script within local registry")
            _script_path = _config_dict["run_metadata"]["script_path"]
            _rel_script_path = _script_path.split(fdp_com.JOBS_DIR)[1]
            _rel_script_path = f"{fdp_com.JOBS_DIR}{_rel_script_path}"

            _script_url = self.find_registry_entry_for_file(
                local_uri, _rel_script_path
            )["url"]
            self._logger.debug("Script URL: %s", _script_url)

        self._logger.debug("Retrieving code runs and written objects")

        _code_run_urls = self._get_code_run_entries(local_uri, _directory)
        _user_written_obj_urls = self._get_written_obj_entries(
            local_uri, _config_dict
        )

        return {
            "jobs": _code_run_urls,
            "user_written_objects": _user_written_obj_urls,
            "config_file": _config_url,
            "script_file": _script_url,
        }

    def remove_staging_entry(
        self, identifier: str, stage_type: str = "job"
    ) -> None:
        """Remove an item of type 'stage_type' from staging

        Parameters
        ----------
            identifier : str
                name or ID of item
            stage_type: str, optional
                type of stage item either job (default) or file
        """
        # Open the staging dictionary first
        _staging_dict = yaml.safe_load(open(self._staging_file))

        if stage_type not in _staging_dict:
            raise fdp_exc.StagingError(
                f"Cannot remove staging item of unrecognised type '{stage_type}'"
            )

        if identifier not in _staging_dict[stage_type]:
            raise fdp_exc.StagingError(
                f"Cannot remove item '{identifier}' of stage type '{stage_type}', "
                "item is not in staging."
            )

        del _staging_dict[stage_type][identifier]

        with open(self._staging_file, "w") as f:
            yaml.dump(_staging_dict, f)

    def get_item_list(
        self, staged: bool = True, stage_type: str = "job"
    ) -> typing.List[str]:
        """Returns a list of items of type 'stage_type' which are staged/unstaged

        Parameters
        ----------
            staged : bool
                list staged/unstaged items
            stage_type : str, optional
                type of stage item either job (default) or file
        """
        _staging_dict = yaml.safe_load(open(self._staging_file))

        if stage_type not in _staging_dict:
            raise fdp_exc.StagingError(
                f"Cannot remove staging item of unrecognised type '{stage_type}'"
            )

        return [k for k, v in _staging_dict[stage_type].items() if v == staged]
