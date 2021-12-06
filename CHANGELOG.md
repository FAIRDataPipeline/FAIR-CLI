# Unreleased
- Added `--dirty` option to `fair run` to allow running with uncommitted changes.
- Added `config.yaml` file validation.
- Added initialisation from existing registry.
- Switch to setting port not local URI during initialisation.
- Added option to specify port on `fair registry start`.

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
