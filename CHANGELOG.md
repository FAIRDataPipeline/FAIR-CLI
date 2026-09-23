# Unreleased

## Changed behaviour
- `fair remote add` takes the remote's token file with `--token FILE`, and prompts for it if omitted.
  The command never worked before: the label was stored as the token path.
- `fair run --ci` prepares the job directory and prints it without running the script, as documented.
- `fair init --ci` leaves an existing `.fair/` alone, as `fair init` does; it used to delete it, data
  store included. Use `fair purge` to start afresh.
- Wildcards: each `*` matches one segment of a name (not `/`), anchored at both ends, in `read:` and
  `write:` alike, matching DataPipeline.jl. `data_product/*` no longer matches `data_product/1/2`.
- `${{GIT_TAG}}` is the nearest tag in the history of the current commit (`git describe`), not the
  last tag by name; with no such tag it is an error rather than a crash.
- `run_metadata.shell` now chooses the interpreter the script runs with.

## Fixed
- `write:` entries with a wildcard were dropped from the working config (#140). The pattern is now kept
  for new names, and existing matches take the `use: version` token.
- `fair push` from a project whose data store was not the local registry's first storage root recorded
  its files on the remote under the pusher's `file://` path (data-registry #229).
- `fair pull` of a `read:` entry from a remote registry failed.
- A DOI whose publisher refuses automated requests (e.g. Wiley, HTTP 403) was rejected as an invalid
  identifier. An identifier that resolves is now accepted; one that does not (404) is still refused.
- `${{DATETIME-<format>}}` always failed with "Failed to parse formatted datetime variable".
- `fair remote remove` crashed.
- Run from a subdirectory of a project, commands lost configuration changes and left a stale session
  file, so the next command did not start the registry and `fair registry stop` needed `--force`.
- Staging ignored the configured local registry and always used port 8000.
- A download that failed with an HTTP error registered the error page as the data.
- `fair run` could crash on Windows when the model printed a character the console cannot encode.
- `fair run` in a git repository without the configured remote gave a bare `IndexError`; it now says
  which remote is missing, or reads `run_metadata: remote_repo:` if given.
- `fair init` in a subdirectory of a FAIR repository named it as `'True'`.

## Development
- CI ran no tests: an unused dev dependency (`pylama`) crashed `pytest --markers`, leaving the test loop
  empty. It is removed, and a failure to list the tests now fails the job; all 8 jobs run the full suite.
- numpy is chosen per Python version (>= 2.3 from 3.11), so Python 3.14 gets a wheel, not a source build.
- Download tests use a local HTTP server rather than github.com.
- `test_configuration.py::test_local_config_query` is marked `xfail`: it has never passed, and its intent
  is unknown.

# 2026-09-22 [v0.9.9](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.9.9)
- Requires Python >= 3.10, in step with data-registry v1.2.2.
- `${{RUN_ID}}` in a `write:` name is left for the language API to fill in at `finalise`.
- ROR/GRID organisation lookups failed after the ROR API moved to its v2 schema.

# Between v0.2.3 and v0.9.8
Not recorded release by release; see the [releases](https://github.com/FAIRDataPipeline/FAIR-CLI/releases)
and the git log.
- Fix virtual environment bug in `fair` binaries where `venv` assumes the main executable is `python`.
- Registration of `author` and `namespace` objects.
- Allow specification of tag to install registry from during `fair registry install`.
- Wildcard '*' parsing introduced for data products.
- Ability to `push` to a registry added.
- Added `--dirty` option to `fair run` to allow running with uncommitted changes.
- Added `config.yaml` file validation.
- Added initialisation from existing registry.
- Switch to setting port not local URI during initialisation.
- Added option to specify port on `fair registry start`.
- Added `cli-config.yaml` file validation during initialisation.
- Added `fair reset` to list of commands.
- Add tab completion for staging data products.

# 2021-11-17 [v0.2.3](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.2.3)
- Move handling of the user `config.yaml` file to a separate class `JobConfiguration`.
- Added various fixes to improve functionality within Windows.
- Move registry installation from script execution to internal function which sets up virtual environment etc.
- Added a test suite to the project.
- Added additional recognised identifiers for author setup from an organisation GRID and ROR.

# 2021-10-06 [v0.2.2](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.2.2)
- Update to package metadata for PyPi

# 2021-10-06 [v0.2.1](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.2.1)
- Automatic starter `config.yaml` generation.
- Local and Global CLI configurations.
- Start/stop local registry either explicitly or during synchronisations.
- Run logs available via git-like interface.
- Added ability to add/remove files.
- Repository style handling, acts like another git-like tool per project.
- Creation of an interface for `fair` using `click`.
