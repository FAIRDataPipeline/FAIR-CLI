# FAIR Data Pipeline Command Line Interface

[![PyPI](https://img.shields.io/pypi/v/fair-cli)](https://pypi.org/project/fair-cli/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fair-cli)](https://pypi.org/project/fair-cli/) [![DOI](https://zenodo.org/badge/377398464.svg)](https://zenodo.org/badge/latestdoi/377398464) ![PyPI - License](https://img.shields.io/pypi/l/fair-cli)  [![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/5411/badge)](https://bestpractices.coreinfrastructure.org/projects/5411)

[![FAIR Data Pipeline CLI](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml/badge.svg?branch=main)](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml)  [![codecov](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI/branch/dev/graph/badge.svg?token=h93TkTiiWf)](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=FAIRDataPipeline_FAIR-CLI&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=FAIRDataPipeline_FAIR-CLI)

FAIR-CLI forms the main interface for synchronising changes between local and shared remote FAIR Data Pipeline registries, it is also used to instantiate model runs/data submissions to the pipeline. Full documentation of the FAIR Data Pipeline can be found on the project [website](https://www.fairdatapipeline.org/).

## Installation

The package is installed using Pip:

```sh
pip install fair-cli
```

It needs Python 3.10 or later. A virtual environment is recommended: the local registry is installed with the same Python as the CLI, in an environment of its own.

To enable tab completion you need to modify your shell:

### Bash
```
_FAIR_COMPLETE=bash_source fair > ~/.config/.fair-complete.bash
echo '. ~/.config/.fair-complete.bash' >> ~/.bashrc
```

### zsh
```
_FAIR_COMPLETE=zsh_source fair > ~/.fair-complete.zsh
echo '. ~/.fair-complete.zsh' >> ~/.bashrc
```

### Fish
```
_FAIR_COMPLETE=bash_source fair > ~/.config/fish/.fair-complete.fish
echo '. ~/.config/fish/.fair-complete.fish' >> ~/.bashrc
```

## Upgrading and reinstalling

The CLI and the local registry are installed separately - the CLI by `pip`, the registry by the CLI - so upgrading one does not upgrade the other.

### The CLI

To move to the newest release, or to a particular one:

```sh
pip install --upgrade fair-cli
pip install fair-cli==0.10.1
```

To install a branch of this repository, give `pip` its name after an `@`. A tag or a commit works the same way:

```sh
pip install git+https://github.com/FAIRDataPipeline/FAIR-CLI@<branch>
```

or clone the repository, check the branch out and install from the clone:

```sh
git clone -b <branch> https://github.com/FAIRDataPipeline/FAIR-CLI
pip install ./FAIR-CLI
```

A branch keeps its version number from one commit to the next, and `pip` does not replace an installed package with another of the same version. To bring an installed branch up to date, or to install one branch over another of the same version, add `--force-reinstall --no-deps`:

```sh
pip install --force-reinstall --no-deps git+https://github.com/FAIRDataPipeline/FAIR-CLI@<branch>
```

`fair --version` gives the version number alone, which a branch may share with a release. `pip freeze` says what is installed: `fair-cli==0.10.1` for a release, and `fair-cli @ git+https://github.com/FAIRDataPipeline/FAIR-CLI@<commit>` for a branch.

### The local registry

To replace the local registry with the newest release of it, stop it if it is running, install with `--force`, and start it again:

```sh
fair registry stop
fair registry install --force
fair registry start
```

`--version` installs a tag or a branch of the [registry's repository](https://github.com/FAIRDataPipeline/data-registry) in place of the newest release:

```sh
fair registry install --force --version v1.3.0
fair registry install --force --version <branch>
```

Without `--force` an existing registry is left as it is and the command stops. `fair registry uninstall`, which asks first, followed by `fair registry install` does the same in two steps, and is the way with `fair-cli` 0.10.1 or earlier, where `--force` is refused.

Check the name given to `--version` before using it with `--force`: the old registry is removed before the name is looked for, so one that does not exist leaves no working registry until the command is run again with one that does.

The registry is installed in `~/.fair/registry`, or in the directory given with `--directory`, which has to be given again to replace a registry that was installed elsewhere. A reinstall replaces that directory:

- every record in the local registry goes with its database: the data products, the code runs, and what `fair init` put there;
- the local registry has a new token, written to `token` in its directory when it is next started, and anything else that was kept in that directory is gone;
- the data store, the projects and `~/.fair/cli` are not touched.

A project that was initialised before the reinstall has to be initialised again, as the new registry does not know its user and `fair init` does nothing in a project that is already initialised. Run `fair purge` in the project, which removes its `.fair` folder and any data store kept inside it, then `fair init`, and `fair pull` for what the project registers or reads. What had been pushed to a remote registry can be pulled back from it. The records of anything that had not been pushed cannot be recovered, though its files are still in the data store.

## Uninstallation
To uninstall the CLI run:
```
fair purge --all
pip uninstall fair-cli
```
`fair purge --all` removes the `.fair` folder of the current project and the whole of `~/.fair`: the local registry, the default data store, the CLI's configuration and any token kept there. Leave it out to keep them.

## The User Configuration File
Job runs are configured via `config.yaml` files. Upon initialisation of a project, FAIR-CLI automatically generates a starter configuration file with all requirements in place. To execute a process (e.g. perform a model run from a compiled binary/script) an additional key of either `script` or `script_path` must be provided. Alternatively the command `fair run bash` can be used to append the key and run a command directly.

By default the shell used to execute a process is `sh` or `batch` for UNIX and Windows systems respectively. This can be overwritten by assigning the optional `shell` key with one of the following values (where `{0}` is the script file):

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
| `batch`      | `{0}`                           |

A full description of `config.yaml` files can be found [here](https://www.fairdatapipeline.org/docs/interface/config/).

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
    uri: https://data.fairdatapipeline.org/api/'
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
fair run --script 'echo "Hello World"'
```

note the command itself must be quoted as it is a single argument.

By default the CLI will not allow the user to perform a run if the state of the analysis repository is such that it is behind the git remote, or contains uncommitted changes. To override this behaviour use the `--dirty` flag.

### `pull`

The command `pull` will update any entries within the `config.yaml` under the `register` heading creating `external_object` and `data_product` objects on the registry and downloading the data to the local data storage. Any data required for a run is downloaded and stored within the local registry. In addition any data products requested that are available on the remote registry are pulled locally.

```sh
fair pull /path/to/config.yaml
```

### `status`
This command displays objects which are awaiting staging or have been staged behaving in a manner similar to `git status`:
```sh
fair status
```
staged changes are displayed in green, and unstaged in red.

### `add`
Before changes can be pushed to the remote registry they must be staged. This command allows you to stage objects displayed when running `fair status` so that they can be sent to the remote registry. Data products are displayed and staged in the form `namespace:data_product_name@version`:
```sh
fair add my_namespace:data_object@v0.1.0
```

### `push`
The `push` command will push any staged data products to the remote registry:

```sh
fair push
```

### `purge`

The `purge` command removes setup of the current project so it can bereinitialised:

```sh
fair purge
```

To remove all configurations entirely (including those global to all projects) run:

```sh
fair purge --global
```

To remove the data directory itself run:

```sh
fair purge --data
```

**WARNING**: This is not recommended as the registry may still have entries pointing to this location!

Finally to remove everything run:

```sh
fair purge --all
```

this will remove the current repository `.fair` folder and the global FAIR directory which also contains the local registry.

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

The registry can be installed using the CLI as well by running:
```sh
fair registry install
```
with the additional options to specify the installation location, and the data registry repository tag or branch to install from:
```sh
fair registry install --directory ~/.fair/my_registry --version v1.4.0
```
To replace a registry that is already installed, see [Upgrading and reinstalling](#upgrading-and-reinstalling).

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
| `USER_ID`           | The unique identifier for the current user        |
| `REPO_DIR`          | The FAIR repository root directory                                               |
| `CONFIG_DIR`        | The directory containing the `config.yaml` after template substitution           |
| `LOCAL_TOKEN`       | The token for access to the local registry                                       |
| `SOURCE_CONFIG`     | Path of the user defined `config.yaml`                                           |
| `GIT_BRANCH`        | Current branch of the `git` repository                                           |
| `GIT_REMOTE`        | The URI of the git repository specified during setup                             |
| `GIT_TAG`           | The latest tag on `git`                                                          |
