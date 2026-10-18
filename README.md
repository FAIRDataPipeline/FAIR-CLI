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

With `--force` the old registry is kept until the new one is built. If the install fails - a `--version` that does not exist, no network, a build that fails - the old registry is put back as it was, and the configuration still names it. A running registry is stopped first, and nothing is replaced while a command is using it.

The registry is installed in `~/.fair/registry`, or in the directory given with `--directory`, which has to be given again to replace a registry that was installed elsewhere. A reinstall replaces that directory:

- every record in the local registry goes with its database: the data products, the code runs, and what `fair init` put there;
- the local registry has a new token, written to `token` in its directory when it is next started, and anything else that was kept in that directory is gone;
- the data store, the projects and `~/.fair/cli` are not touched.

The new registry does not know the user of a project that was initialised before the reinstall, and a run in that project fails until it does. There are two ways to put that right:

- run `fair init` in the project again, which leaves the project as it is - its configuration and any data store kept inside it - and registers its user in the new registry. This needs `fair-cli` 0.10.2 or later: before that, `fair init` does nothing in a project that is already initialised;
- or start the project afresh: run `fair purge` in it, which removes its `.fair` folder and any data store kept inside it, and then `fair init`.

After either, `fair pull` registers again what the project registers or reads. What had been pushed to a remote registry can be pulled back from it. The records of anything that had not been pushed cannot be recovered, though its files are still in the data store unless that was removed with the project's `.fair` folder.

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

In a repository that is already initialised the command asks nothing and leaves the repository as it is. It registers the repository's user in the local registry, which a registry [reinstalled](#upgrading-and-reinstalling) since the repository was initialised no longer holds.

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

A detached checkout - a tag or a commit checked out, or a pull request on a CI system - has no branch to compare, so there the commit has to be one the remote has, on a branch or a tag; `--dirty` lets one the remote does not have through.

To prepare a job without running it, as a language API's rehearsal of a run does, use `--rehearse`:

```sh
fair run --rehearse
```

It writes the job's folder, its working config and its script exactly as a run would, and runs nothing. It makes no git check - the commit is recorded as it is, marked `-dirty` where there are uncommitted changes - and stages nothing. Its only output on stdout is one JSON object naming the job's files, its log going to stderr:

```json
{"job_dir": "...", "config": "...", "script": "...", "data_store": "...", "token_file": "..."}
```

`token_file` is the file holding the local registry's token; the token itself is never printed. Like every command, a rehearsal holds the local registry only while it runs, so a program that goes on to run the job's script holds a [session](#registry) of its own around both. `fair run --ci` also prepares a job without running it, but stages it, warns about the repository's state, and prints the job's folder as its last line.

### `pull`

The command `pull` will update any entries within the `config.yaml` under the `register` heading creating `external_object` and `data_product` objects on the registry and downloading the data to the local data storage. An `external_object` entry with `no_file: true`, and no `root`, `path` or `file_type`, registers a source that has no file of its own, such as a data store a run queries, as a data product with no file; it must give its `version`. Any data required for a run is downloaded and stored within the local registry. In addition any data products requested that are available on the remote registry are pulled locally.

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

A command that uses the local registry - `run`, `pull`, `push`, `status`, `list`, `find`, `identify`, `reset` - starts it, on the port the configuration names, if it is not running, and a registry started that way is stopped when the last command using it finishes. Each command holds a *session* on the registry while it runs, and the registry is not stopped while any session is open.

To keep the registry running - to look at it in the browser, or to save each command the few seconds a start takes - start it yourself:

```sh
fair registry start
```

It then runs until

```sh
fair registry stop
```

`fair registry start` takes the port in the configuration (`registries.local.uri` in `~/.fair/cli/cli-config.yaml`), or the one given with `--port`, which becomes the configured one when the registry starts there. Run while a command has the registry running, it keeps that registry running from then on rather than starting another. `fair registry stop` refuses while a command is using the registry; `--force` stops it anyway. `fair registry status` says where the registry is running, whether it was started by `fair registry start` or by a command, and which processes are using it.

An install's registry runs on one host at a time, since its database is a file in its directory: where that directory is shared between machines, as a home directory on a cluster may be, the registry cannot be started, stopped or used from a machine other than the one running it. The refusal names that machine, and the file in the registry's directory that records it. If that machine no longer runs the registry - it has shut down or restarted - delete the file and try again.

`fair purge --all`, `fair registry uninstall` and `fair registry install --force` stop the registry before removing it, and remove nothing while a command is using it or while it is running on another machine.

A program that uses the registry outside a `fair` command, such as a language API, can hold a session of its own:

```sh
fair registry session open --pid <process id> --json
fair registry session close <session id>
```

`session open` starts the registry if it is not running, and prints the session's id, the registry's address and the file holding its token, as one JSON object (`{"session": ..., "registry_url": ..., "token_file": ...}`). The session ends when it is closed or when the process it names ends. A script run by `fair run` is told the run's session in `FDP_SESSION`, and needs none of its own.

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

## What a program may read of the CLI's configuration

A language API, or any other program working with the pipeline, may rely on these parts of the CLI's configuration files:

- `registries.local.uri` and `registries.local.token` in `~/.fair/cli/cli-config.yaml`: the local registry's API address, and the file holding its token.
- `registries.<name>.uri` and `registries.<name>.token` in a project's `.fair/cli-config.yaml`: the same for each remote registry, by its name - `origin`, or one added with `fair remote add`.
- A token file holds that registry's token alone, read as it is with surrounding white space stripped. Use it only for the registry whose entry names it.
- `git.remote` in a project's `.fair/cli-config.yaml`: the name of the project's git remote. Its URL is the git repository's to give (`git remote get-url <name>`); `git.remote_repo` beside it is a copy taken by `fair init`, and goes out of date when the remote moves.
- A directory with a `.fair` folder is a FAIR project, found by searching upwards from the working directory, as every `fair` command does.

Within a run the working config holds what a script needs - among it `run_metadata.local_data_registry_url` and `run_metadata.remote_repo`, the git remote's URL - and the script's environment holds `FDP_CONFIG_DIR`, `FDP_LOCAL_TOKEN` and `FDP_SESSION`.
