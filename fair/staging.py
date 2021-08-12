import os
from posixpath import basename
import typing
import yaml
import fair.common as fdp_com
import fair.run as fdp_run
import fair.registry.requests as fdp_req
import fair.exceptions as fdp_exc


class Stager:
    def __init__(self, repo_root: str = fdp_com.find_fair_root(os.getcwd())) -> None:
        self._root = repo_root
        self._staging_file = fdp_com.staging_cache(self._root)

        # Only create the staging file if one is not already present within the
        # specified directory
        if not os.path.exists(self._staging_file):
            # If the stager is called before the rest of the directory tree
            # has been created make the parent directories first
            if not os.path.exists(os.path.dirname(self._staging_file)):
                os.makedirs(os.path.dirname(self._staging_file), exist_ok=True)
            self._create_staging_file()

    def _create_file_label(self, file_to_stage: str) -> str:
        return os.path.relpath(
            file_to_stage,
            os.path.dirname(self._staging_file),
        )

    def _create_staging_file(self) -> None:
        _staging_dict = {
            'run': {},
            'file': {}
        }
        yaml.dump(_staging_dict, open(self._staging_file, 'w'))

    def change_run_stage_status(self, run_uuid: str, stage: bool = True) -> None:
        """Stage a local code run ready to be pushed to the remote registry

        Parameters
        ----------
        run_uuid : str
            a valid uuid for the given run
        stage : bool
            whether run is staged

        Returns
        -------
            bool
                success if staging/unstaging complete, else fail if uuid not recognised
        """

        # Open the staging dictionary first
        _staging_dict = yaml.safe_load(open(self._staging_file))

        # When a run is completed by a language implementation the CLI should
        # have already registered it into staging with a status of staged=False
        if run_uuid not in _staging_dict['run']:
            raise fdp_exc.StagingError(f"Failed to recognise run with ID '{run_uuid}'")

        _local_config = fdp_com.local_fdpconfig(self._root)
        _local_url = yaml.safe_load(open(_local_config))['remotes']['local']

        # Now check run actually exists on local registry
        try:
            _results = fdp_req.get(
                _local_url, ('code_run',), params={'uuid': run_uuid}
            )

            # Possible for query to return empty list
            if not _results:
                raise fdp_exc.RegistryAPICallError

        except fdp_exc.RegistryAPICallError:
            raise fdp_exc.StagingError(
                f"Cannot stage run '{run_uuid}' as it"
                " does not exist on the local registry"
            )

        _staging_dict['run'][run_uuid] = stage

        with open(self._staging_file, 'w') as f:
            yaml.dump(_staging_dict, f)


    def find_registry_entry_for_file(self, local_uri: str, file_path: str) -> str:
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
        # Will search storage locations for a similar path by using
        # parent_directory/file_name
        _obj_type = ('storage_location', )
        _params = {
            "path": os.path.join(
                os.path.dirname(file_path),
                os.path.basename(file_path)
            )
        }
        _results = fdp_req.get(local_uri, obj_path=_obj_type, params=_params)

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

        return _results[0]

    
    def _get_code_run_entries(
        self,
        local_uri: str,
        cli_run_dir: str
        ) -> typing.List[str]:
        """Retrieve code_run URL list from a given CLI run directory

        Parameters
        ----------
        local_uri : str
            local registry endpoint
        cli_run_dir : str
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
        # Check if any code_runs are present for the given cli run
        _code_run_file = os.path.join(cli_run_dir, 'coderuns.txt')

        if ( os.path.exists(_code_run_file) and
            open(_code_run_file).read().strip()
        ):
            _code_runs = [i.strip() for i in open(_code_run_file).readlines()]
            _code_run_urls = []
            
            for run in _code_runs:
                _results = fdp_req.get(
                    local_uri,
                    ('code_run', ),
                    params={"uuid": run}
                )

                if not _results:
                    raise fdp_exc.ImplementationError(
                        "Expected code_run with uuid "
                        f"'{run}' in local registry, but no result found."
                    )
                
                _code_run_urls.append(_results[0]["url"])
            
        return _code_run_urls

    def _get_written_obj_entries(
        self,
        local_uri: str,
        config_dict: typing.Dict
    ):
        if 'write' not in config_dict:
            return []

        _data_product_urls: typing.List[str] = []
        
        for write_obj in config_dict['write']:
            _data_product = write_obj['data_product']
            _results = fdp_req.get(
                local_uri,
                ('data_product', ),
                params={"name": _data_product}
            )

            if not _results:
                raise fdp_exc.InternalError(
                    "Expected data_product "
                    f"'{_data_product}' in local registry, but no result found."
                )
            
            _data_product_urls.append(_results[0]["url"])
        
        return _data_product_urls
    
    def get_run_data(
            self, local_uri: str, identifier: str
        ) -> typing.Dict[str, str]:
        # Firstly find the CLI run directory
        _directory = fdp_run.get_cli_run_dir(identifier)
        if not _directory:
            raise fdp_exc.StagingError(
                f"Could not retrieve directory for run '{identifier}'"
            )
        
        # Check for a config.yaml file
        _config_yaml = os.path.join(_directory, "config.yaml")
        if not os.path.exists(_config_yaml):
            raise fdp_exc.FileNotFoundError(
                f"Cannot stage run '{identifier}'"
                f"Expected config.yaml in '{_directory}'"
            )

        # Find this config.yaml on the local registry
        _config_url = self.find_registry_entry_for_file(
            local_uri,
            _config_yaml
        )["url"]

        # Check for run script file
        _config_yaml = os.path.join(_directory, "config.yaml")
        if not os.path.exists(_config_yaml):
            raise fdp_exc.FileNotFoundError(
                f"Cannot stage run '{identifier}'"
                f"Expected config.yaml in '{_directory}'"
            )

        # Find this run script on the local registry, as the script
        # can have any name obtain this information from the config.yaml
        _config_dict = yaml.safe_load(_config_yaml)

        if (
            'run_metadata' not in _config_dict
            or 'script_path' not in _config_dict['run_metadata']
        ):
            raise fdp_exc.InternalError(
                "Expected 'script_path' under 'run_metadata' within "
                f" config file '{_config_yaml}'"
            )

        _script_path = _config_yaml['run_metadata']['script_path']

        _script_url = self.find_registry_entry_for_file(
            local_uri,
            _script_path
        )["url"]

        _code_run_urls = self._get_code_run_entries(local_uri, _directory)
        _written_obj_urls = self._get_written_obj_entries(local_uri, _config_dict)

        return {
            "code_runs": _code_run_urls,
            "written_objects": _written_obj_urls,
            "config_file": _config_url,
            "script_file": _script_url
        }

    
    def remove_staging_entry(
        self, identifier: str, stage_type: str = "run"
        ) -> None:
        """Remove an item of type 'stage_type' from staging

        Parameters
        ----------
            identifier : str
                name or ID of item
            stage_type: str, optional
                type of stage item either run (default) or file
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

        with open(self._staging_file, 'w') as f:
            yaml.dump(_staging_dict, f)

    def get_item_list(
        self, staged: bool = True, stage_type: str = "run"
        ) -> typing.List[str]:
        """Returns a list of items of type 'stage_type' which are staged/unstaged
        
        Parameters
        ----------
            staged : bool
                list staged/unstaged items
            stage_type : str, optional
                type of stage item either run (default) or file
        """
        _staging_dict = yaml.safe_load(open(self._staging_file))

        if stage_type not in _staging_dict:
            raise fdp_exc.StagingError(
                f"Cannot remove staging item of unrecognised type '{stage_type}'"
            )

        _items = [k for k, v in _staging_dict[stage_type].items() if v == staged]

        return _items
