# FAIR Data Pipeline Command Line Interface

[![FAIR Data Pipeline CLI](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml/badge.svg?branch=dev)](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml)
[![codecov](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI/branch/dev/graph/badge.svg?token=h93TkTiiWf)](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=FAIRDataPipeline_FAIR-CLI&metric=alert_status)](https://sonarcloud.io/dashboard?id=FAIRDataPipeline_FAIR-CLI)

| **DISCLAIMER:**                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The following document is largely conceptual and therefore does *not* represent a manual for the final interface. Statements within the following are likely to change, further details of possible changes are given throughout. Please either open an issue or pull request on the [source repository](https://github.com/FAIRDataPipeline/FAIR-CLI) raising any changes/issues. |

FAIR-CLI forms the main interface for synchronising changes between your local and shared remote FAIR Data Pipeline registries, it is also used to instantiate model runs/data submissions to the pipeline.

The project is still under development with many features still to be implemented and checked. Available commands are summarised below along with their usage.

## Installation

The project makes use of [Poetry](https://python-poetry.org/) for development which allows quick and easy mangement of dependencies, and provides a virtual environment exclusive to the project. Ultimately the project will be built into a pip installable module (using `poetry build`) meaning users will not need Poetry. You can access this environment by installing poetry:

```sh
pip install poetry
```

and, ensuring you are in the project repository, running:

```sh
poetry install
```

which will setup the virtual environment and install requirements. You can then either launch the environment as a shell using:

```sh
poetry shell
```

or run commands within it externally using:

```sh
poetry run <command>
```

## Publishing
If publishing a new version of `fair-cli` it is recommended that you first upload to [test.pypi.org](https://test.pypi.org/) before performing a final submission to [PyPi](https://pypi.org/) itself. Note also that both sites only allow submission of a file once, even if the file is deleted a re-submission is not allowed so the built tarball/wheel files will require renaming should a re-upload be required. After upload is complete create a release from the tag on GitHub and verify the release has been detected by Zenodo.

|**NOTE**|
|----|
|Before creating a release you must ensure that the version numbers stated within the `CITATION.cff` and `pyproject.toml` files match the git tag to be assigned.|

### Building the Module
To build the module run:
```
poetry build
```
this will produce a `dist` folder containing a tarball archive and a wheels file.

### Publishing to PyPi
Although Poetry does allow you to publish via the `poetry publish` command it is recommended that `twine` be used as it performs a couple of validation checks. To use `twine`:
```
pip install twine
```
if you have an account on [test.pypi.org](https://test.pypi.org/) and you are a member of the `fair-cli` project you can then perform a test upload:
```
twine upload --repository-url https://test.pypi.org/legacy/ dist/* -u <user-name> -p <password>
```
if submission is successful the resultant URL should be displayed. If you are satisfied with the result you can then upload to [PyPi](https://pypi.org/) itself by running:
```
twine upload dist/* -u <user-name> -p <password>
```

## Structure

The layout of FAIR-CLI on a simplified system looks like this:

```sh
$HOME
├── .fair
│   ├── cli
│   │   ├── cli-config.yaml
│   │   └── sessions
│   ├── data
│   │   └── jobs
│   └── $REGISTRY_HOME
│
└─ Documents
   └─ my_project
      ├── config.yaml
      └── .fair
          ├── cli-config.yaml
          ├── logs
          └── staging
```

### Global and Local Directories

FAIR-CLI stores information for projects in two locations. The first is a *global* directory stored in the user's home folder in the same location as the registry itself `$HOME/.fair/cli`, and the second is a *local* directory which exists within the model project itself `$PROJECT_HOME/.fair`.

The CLI holds metadata for the user in it's own configuration file (not to be confused with the user modifiable `config.yaml`), `cli-config.yaml`, the *global* version of which is initialised during first use. In a manner similar to `git`, FAIR-CLI has repositories which allow the user to override these *global* configurations, this then forming a *local* variant.

### Data Directory

The directory `$HOME/.fair/data` is the default data store initialised by FAIR-CLI. During setup an alternative can be provided and this can be later changed on a per-run basis if the user so desires. The subdirectory `$HOME/data/jobs` contains timestamped directories of jobs.

### Sessions Directory

The directory `$HOME/.fair/sessions` is used to keep track of ongoing queries to the registry as a safety mechanism to ensure the registry is not shutdown whilst processes are still occuring.

### Logs Directory

The directory `$PROJECT/.fair/logs` stores `stdout` logs for jobs also giving information on who launched the job and how long it lasted.

### Staging File

The staging file, `$PROJECT/.fair/staging`, contains information of what jobs are being tracked, by default all jobs are added to this file after completion and are set to "unstaged". Simply contains a dictionary of booleans where items for sync (staged) are marked true `True` and those to be held only locally `False`. The file uses paths relative to the *local* `.fair` folder as keys, to behave in a manner identical to `git` staging.

### `config.yaml`

This is the main file the user will interact with to customise their run. FAIR-CLI automatically generates a starter version of this file with everything in place. The only addition required is setting of either `script` or `script_path` (with the exception of running using `fair run bash` - see [below](#run)) under `run_metadata`.
|                                                                                                                                                      |
| ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| **`script`**                                                                                                                                         |
| This should be a command callable by a shell for running a model/submitting data to the registry. This script is saved to a file prior to execution. |
|                                                                                                                                                      |
| **`script_path`**                                                                                                                                    |
| This is a direct path to an existing script to use for submission.                                                                                   |

By default the shell used will be `sh` or `pwsh` for UNIX and Windows systems respectively, however this can be overwritten with the optional `shell` key which recognises the following values (where `{0}` is the script file):

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

| **NOTE**                                                                                                                                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| This layout is subject to possible change depending on whether or not multiple aliases for the same user will be allowed in the registry itself. The main reason for having a *local* version is to support separate handling of multiple projects. |

## Registry Interaction

Currently `FAIR-CLI` sets up the write data storage location on the local registry if it does not exist. Entries are created for the YAML file type, current user as an author, and object for a given run.

## Command Line Usage

As mentioned, all of the subcommands within FAIR-CLI are still under review with many still serving as placeholders for future features. Running `fair` without arguments or `fair --help` will show all of these.

### `init`

Initialises a new FAIR repository within the given directory. This should ideally be the same location as the `.git` folder for the current project, although setup will ask if you want to use an alternative location. The command will ask the user a series of questions which will provide metadata for tracking run authors, and also allow for the creation of a starter `config.yaml`.

The first time this command is launched the *global* CLI configuration will be populated. In subsequent calls the *global* will provide default suggestions towards creating the CLI configuration for the repository (*local*).

A repository directory matching the structure above will be placed in the current location and a starter `config.yaml` file will be generated (see below).

#### Example: First call to `fair init`

This example shows the process of setting up for the first time. Note the default suggestions for each prompt, in the case of `Full name` and `Default output namespace` this is the hostname of the system and an abbreviated version of this name.

```sh
$ fair init
Initialising FAIR repository, setup will now ask for basic info:

Checking for local registry
Local registry found
Remote Data Storage Root [http://data.scrc.uk/data/]:
Remote API Token File: $HOME/scrc_token.txt
Local API URL [http://127.0.0.1:8000/api/]:
Local registry is offline, would you like to start it? [y/N]: y
Default Data Store:  [/home/joebloggs/.fair/data]:
Email: jbloggs@noreply.uk
ORCID [None]:
Full Name: Joe Bloggs
Default output namespace [joebloggs]: 
Default input namespace [joebloggs]: SCRC
Project description: Test project
Local Git repository [/home/joebloggs/Documents/AnalysisProject]:
Git remote name [origin]:
Using git repository remote 'origin': git@notagit.com:jbloggs/AnalysisProject.git
Initialised empty fair repository in /home/joebloggs/Documents/AnalysisProject/.fair
```

#### Example: Subsequent runs

In subsequent runs the first time setup will provide further defaults.

```sh
$ fair init
Initialising FAIR repository, setup will now ask for basic info:

Project description: Test Project
Local Git repository [/home/joebloggs/Documents/AnalysisProject]:
Git remote name [origin]:
Using git repository remote 'origin': git@nogit.com:joebloggs/AnalysisProject.git
Remote API URL [http://data.scrc.uk/api/]: 
Remote API Token File [/home/kristian/scrc_token.txt]: 
Local API URL [http://127.0.0.1:8000/api/]: 
Default output namespace [joebloggs]: 
Default input namespace [joebloggs]: 
Initialised empty fair repository in /home/joebloggs/Documents/AnalysisProject/.fair
```

#### Generated `config.yaml`

```yaml
run_metadata:
  default_input_namespace: SCRC
  default_output_namespace: joebloggs
  description: Test Project
  local_data_registry: http://127.0.0.1:8000/api/
  local_repo: /home/joebloggs/Documents/AnalysisProject
  write_data_store: /home/joebloggs/.fair/data/
```

the user then only needs to add a `script` or `script_path` entry to execute a code run. This is only required for `run`.

#### Advanced usage

CLI configuration can be read directly from a file which should contain the following:

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

this file is then read during initialisation:

```sh
fair init --using <cli-config.yaml file>
```

For the purposes of CI runs, the initialisation can be "skipped" by running:

```sh
fair init --ci
```

which will create temporary directories for some locations.

### `run`

The purpose of `run` is to execute a model/submission run to the local registry. The command fills any specified template variables of the form `${{ VAR }}` to match those outlined [below](#template-variables). Outputs of a run will be stored within the `coderun` folder in the directory specified under the `data_store` tag in the `config.yaml`, by default this is `$HOME/.fair/data/coderun`.

```sh
fair run
```

If you wish to use an alternative `config.yaml` then specify it as an additional argument:

```sh
fair run /path/to/config.yaml
```

You can also launch a bash command directly which will then be automatically written into the `config.yaml` for you:

```sh
fair run --script "echo \"Hello World\""
```

note the command itself must be quoted as it is a single argument.

### `pull`

Currently `pull` will update any entries within the `config.yaml` under the `register` heading creating `external_object` and `data_product` objects on the registry and downloading the data to the local data storage. For example:

```yaml
run_metadata:
  default_input_namespace: SCRC
  default_output_namespace: joebloggs
  description: Test project
  local_data_registry: http://127.0.0.1:8000/api/
  local_repo: /home/joebloggs/Documents/SCRC/FAIR-CLI
  write_data_store: /home/joebloggs/.fair/data/
register:
- external_object: records/SARS-CoV-2/scotland/human-mortality
  namespace_name: Scottish Government Open Data Repository
  namespace_full_name: Scottish Government Open Data Repository
  namespace_website: https://statistics.gov.scot/
  root: https://statistics.gov.scot/sparql.csv?query=
  path: |-
    PREFIX qb: <http://purl.org/linked-data/cube#>
    PREFIX data: <http://statistics.gov.scot/data/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dim: <http://purl.org/linked-data/sdmx/2009/dimension#>
    PREFIX sdim: <http://statistics.gov.scot/def/dimension/>
    PREFIX stat: <http://statistics.data.gov.uk/def/statistical-entity#>
    PREFIX mp: <http://statistics.gov.scot/def/measure-properties/>
    SELECT ?featurecode ?featurename ?areatypename ?date ?cause ?location ?gender ?age ?type ?count
    WHERE {
     ?indicator qb:dataSet data:deaths-involving-coronavirus-covid-19;
       mp:count ?count;
       qb:measureType ?measType;
       sdim:age ?value;
       sdim:causeOfDeath ?causeDeath;
       sdim:locationOfDeath ?locDeath;
       sdim:sex ?sex;
       dim:refArea ?featurecode;
       dim:refPeriod ?period.

       ?measType rdfs:label ?type.
       ?value rdfs:label ?age.
       ?causeDeath rdfs:label ?cause.
       ?locDeath rdfs:label ?location.
       ?sex rdfs:label ?gender.
       ?featurecode stat:code ?areatype;
         rdfs:label ?featurename.
       ?areatype rdfs:label ?areatypename.
       ?period rdfs:label ?date.
    }
  title: Deaths involving COVID19
  description: Nice description of the dataset
  unique_name: Scottish deaths involving COVID19
  file_type: csv
  release_date: ${{DATETIME}}
  version: 0.${{DATE}}.0
  primary: True
```

if run on `10/10/2021` would download the data from the given `root`/`path` URL and store in a file:

```sh
/home/joebloggs/.fair/data/records/SARS-CoV-2/scotland/human-mortality/0.20211010.0.csv
```

and register all required objects into the local registry.

### `purge`

Removes the local `.fair` (FAIR repository) folder by default so the user can reinitialise:

```sh
fair purge
```

You can remove the global configuration and start again entirely by running:

```sh
fair purge --global
```

and also the data directory by running:

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
| The SHA for a job is *not* yet related to a registry code run identifier as multiple code runs can be executed within a single job. |

### `view`

To view the `stdout` of a run given its SHA as shown by running `fair log` use the command:

```sh
fair view <sha>
```

you do not need to specify the full SHA but rather the first few characters:

```text
--------------------------------
 Commenced = Wed Jun 30 09:09:30 2021
 Author    = Joe Bloggs <jbloggs@noreply.uk>
 Namespace = joebloggs
 Command   = bash -eo pipefail /home/jbloggs/.fair/data/coderun/2021-06-30_09_09_30_721358/script.sh
--------------------------------
0
1
2
3
4
5
6
7
8
9
10
------- time taken 0:00:00.011910 -------
```

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
