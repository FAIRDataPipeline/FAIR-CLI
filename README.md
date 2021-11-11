# FAIR Data Pipeline Command Line Interface

[![PyPI](https://img.shields.io/pypi/v/fair-cli)](https://pypi.org/project/fair-cli/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fair-cli)](https://pypi.org/project/fair-cli/) [![DOI](https://zenodo.org/badge/377398464.svg)](https://zenodo.org/badge/latestdoi/377398464) ![PyPI - License](https://img.shields.io/pypi/l/fair-cli)

[![FAIR Data Pipeline CLI](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml/badge.svg?branch=main)](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml)  [![codecov](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI/branch/dev/graph/badge.svg?token=h93TkTiiWf)](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=FAIRDataPipeline_FAIR-CLI&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=FAIRDataPipeline_FAIR-CLI)

FAIR-CLI forms the main interface for synchronising changes between local and shared remote FAIR Data Pipeline registries, it is also used to instantiate model runs/data submissions to the pipeline. Full documentation of the FAIR Data Pipeline can be found on the project [website](https://www.fairdatapipeline.org/).

## Installation

The package is installed using Pip:

```sh
pip install fair-cli
```

## The User Configuration File
Job runs are configured via `config.yaml` files. Upon initialisation of a project, FAIR-CLI automatically generates a starter configuration file with all requirements in place. To execute a process (e.g. perform a model run from a compiled binary/script) an additional key of either `script` or `script_path` must be provided. Alternatively the command `fair run bash` can be used to append the key and run a command directly.

By default the shell used to execute a process is `sh` or `pwsh` for UNIX and Windows systems respectively. This can be overwritten by assigning the optional `shell` key with one of the following values (where `{0}` is the script file):

| **Shell**    | **Command**                     |
| ------------ | ------------------------------- |
| `bash`       | `bash -eo pipefail {0}`         |
| `java`       | `java {0}`                      |
| `julia`      | `julia {0}`                     |
| `powershell` | `powershell -command ". '{0}'"` |
| `pwsh`       | `pwsh -command ". '{0}'"`       |
| `python2`    | `python2 {0}`                   |
| `python3`    | `python3 {0}`                   |
| `python`     | `python {0}`                    |
| `R`          | `R -f {0}`                      |
| `sh`         | `sh -e {0}`                     |

A full description of `config.yaml` files can be found [here](https://www.fairdatapipeline.org/docs/interface/config/).

## Tab Completion
To enable tab completion you need to modify your shell:

```
_FAIR_COMPLETE=bash_source fair > ~/config/fair-complete.bash
echo '. ~/config/fair-complete.bash'
```

## Available Commands

### `init`

Initialises a new FAIR repository within the given directory. This should ideally be the same location as the `.git` folder for the current project, however during setup an option is given to specify an alternative. The command will ask the user a series of questions which will provide metadata for tracking run authors, and also allow for the creation of a starter `config.yaml` file. Initialisation will also configure the CLI itself.

#### Custom CLI Configuration
After setup is complete, the current CLI configuration can also be saved using the command:
```
fair init --export
```
the created file can then be re-read at a later point during setup. Alternatively, if creating a configuration from scratch the YAML file should contain the following information:

```yaml
namespaces: 
  input: testing
  output: testing
registries:
  local:
    data_store: /path/to/local/data_store/,
    directory: /local/registry/install/directory
    uri: http://127.0.0.1:8000/api/
  origin:
    data_store: /remote/registry/data/store/path/
    token: /path/to/remote/token
    uri: https://data.scrc.uk/api/'
user:
  email: 'test@noreply',
  family_name: 'Test'
  given_names: 'Interface'
  orcid: None,
  uuid: '2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1'
git:
  local_repo: /local/repo/path
  remote: origin
description: Testing Project
```
this file is then read during the initialisation:

```sh
fair init --using <cli-config.yaml file>
```

For integration into a CI workflow, the setup can be skipped by running:

```sh
fair init --ci
```

which will create temporary directories for some of the required location paths.


### `run`

The purpose of `run` is to execute a model/submission run and submit results to the local registry. Outputs of a run will be stored within the `coderun` folder in the directory specified under the `data_store` tag in the `config.yaml`, by default this is `$HOME/.fair/data/coderun`.

```sh
fair run
```

If you wish to use an alternative `config.yaml` then specify it as an additional argument:

```sh
fair run /path/to/config.yaml
```

You can also launch a bash command directly, this will be automatically written into the `config.yaml`:

```sh
fair run --script "echo \"Hello World\""
```

note the command itself must be quoted as it is a single argument.

### `pull`

Currently `pull` will update any entries within the `config.yaml` under the `register` heading creating `external_object` and `data_product` objects on the registry and downloading the data to the local data storage. Any data required for a run is downloaded  and stored within the local registry.

### `purge`

The `purge` command removes setup of the current project so it can bereinitialised:

```sh
fair purge
```

To remove all configurations entirely (including those global to all projects) run:

```sh
fair purge --global
```

Finally the data directory itself can be removed by running:

```sh
fair purge --data
```

**WARNING**: This is not recommended as the registry may still have entries pointing to this location!

You can skip any confirmation messages by running:

```sh
fair purge --yes
```

### `registry`

By default the CLI will launch the registry whenever a synchronisation or run is called. The server will only be halted once all ongoing CLI processes (in the case of multiple parallel calls) have been completed.

However the user may also specify a manual launch that will override this behaviour, instead leaving the server running constantly allowing them to view the registry in the browser.

The commands:

```sh
fair registry start
```

and

```sh
fair registry stop
```

will launch and halt the server respectively.

### `log`

Runs are logged locally within the local FAIR repository. A full list of runs is shown by running:

```sh
fair log
```

This will present a list of runs in a summary analogous to a `git log` call:

```yaml
run 0db35c20946a1ebeaafdc3b30103cd74a57eb6b6
Author: Joe Bloggs <jbloggs@noreply.uk>
Date:   Wed Jun 30 09:09:30 2021
```

| **NOTE**                                                                                                                            |
| ----------------------------------------------------------------------------------------------------------------------------------- |
| The SHA for a job is *not* related to a registry code run identifier as multiple code runs can be executed within a single job. |

### `view`

To view the `stdout` of a run given its SHA as shown by running `fair log` use the command:

```sh
fair view <sha>
```

you do not need to specify the full SHA but rather the first few unique characters.

## Template Variables

Within the `config.yaml` file, template variables can be specified by using the notation `${{ VAR }}`, the following variables are currently recognised:

| **Variable**        | **Description**                                                                  |
| ------------------- | -------------------------------------------------------------------------------- |
| `DATE`              | Date in the form `%Y%m%d`                                                        |
| `DATETIME`          | Date and time in the form `%Y-%m-%sT%H:%M:S`                                     |
| `DATETIME-%Y%H%M`   | Date and time in custom format (where `%Y%H%M` can be any valid form)            |
| `USER`              | The current user as defined in the CLI                                           |
| `REPO_DIR`          | The FAIR repository root directory                                               |
| `CONFIG_DIR`        | The directory containing the `config.yaml` after template substitution           |
| `LOCAL_TOKEN`       | The token for access to the local registry                                       |
| `SOURCE_CONFIG`     | Path of the user defined `config.yaml`                                           |
| `GIT_BRANCH`        | Current branch of the `git` repository                                           |
| `GIT_REMOTE`        | The URI of the git repository specified during setup                             |
| `GIT_TAG`           | The latest tag on `git`                                                          |
