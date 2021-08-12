import os
import typing
import yaml
import fair.common as fdp_com
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

    
    def remove_staging_entry(self, identifier: str, stage_type: str = "run") -> None:
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

    def get_item_list(self, staged: bool = True, stage_type: str = "run") -> typing.List[str]:
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
