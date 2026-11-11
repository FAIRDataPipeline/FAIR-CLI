# FAIR Data Pipeline Developer Notes

[![FAIR Data Pipeline CLI](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml/badge.svg?branch=dev)](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml)
[![codecov](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI/branch/dev/graph/badge.svg?token=h93TkTiiWf)](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=FAIRDataPipeline_FAIR-CLI&metric=alert_status)](https://sonarcloud.io/dashboard?id=FAIRDataPipeline_FAIR-CLI)

| **DISCLAIMER:**                                                                                                                                                                                                                                                                                                                                                                    |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| The following document is largely conceptual and therefore does *not* represent a manual for the final interface. Statements within the following are likely to change, further details of possible changes are given throughout. Please either open an issue or pull request on the [source repository](https://github.com/FAIRDataPipeline/FAIR-CLI) raising any changes/issues. |

FAIR-CLI is the main interface for synchronising changes between a local FAIR Data Pipeline registry and shared remote ones. It also launches model runs and data submissions to the pipeline.

The project is still under development, with features still to be implemented and checked. The commands are summarised below, with their usage. The [README](https://github.com/FAIRDataPipeline/FAIR-CLI#readme) is the user's guide.

## Installation

The project is developed with [Poetry](https://python-poetry.org/), which manages its dependencies and gives it a virtual environment of its own. Users do not need Poetry: `poetry build` makes the package that `pip` installs. To use the environment, install Poetry:

```sh
pip install poetry
```

Then, in the project repository, run:

```sh
poetry install
```

This sets up the virtual environment and installs the requirements. Then either start a shell in the environment:

```sh
poetry shell
```

or run a command in it from outside:

```sh
poetry run <command>
```

## Publishing

A release is made on GitHub. Its tag starts two workflows, which publish the package to PyPI and attach its files to the GitHub release.

### Preparing a release

Work is merged into the `Dev` branch. When `Dev` is ready, one commit there sets the new version in three files:

- `pyproject.toml`: `version`.
- `CITATION.cff`: `version`, and `date-released`, the day of the release.
- `CHANGELOG.md`: the "Unreleased" heading becomes the release's date and version, linked to its page on GitHub, as the earlier releases' headings are.

Then `Dev` is merged into `main` by a pull request. The merge closes each issue that the pull request's description or a commit message names with `Fixes #<number>`.

### Making the release

Make a release on GitHub from `main`, with a new tag `v<version>`, such as `v0.11.0`. The tag must match the version in `pyproject.toml` and `CITATION.cff`. The new tag starts two workflows:

- `.github/workflows/release.yaml` builds the package, checks it with `twine check --strict`, and publishes it to PyPI. It stops if the tag is not the version in `pyproject.toml`. It publishes through PyPI's trusted publishing, so it needs no token or password.
- `.github/workflows/deploy.yaml` builds a single-file binary with PyInstaller on Linux, macOS (Apple Silicon) and Windows, and checks that each one runs. It then checks that the tag matches the versions in `pyproject.toml` and `CITATION.cff`. Last, it attaches the wheel, the source archive and the three binaries to the GitHub release. If any binary fails its check, nothing is attached.

When both have finished, check that the release is on [PyPI](https://pypi.org/project/fair-cli/), and that Zenodo has archived it.

PyPI accepts each file only once, even after it is deleted. So a release with a fault is mended by a new version, not by uploading again.

### Building the package yourself

To check the package before a release, build it and check it as `release.yaml` does:

```sh
pip install build twine
python -m build
python -m twine check --strict dist/*
```

`python -m build` makes a `dist` folder holding a source archive and a wheel.

## Structure

The layout of FAIR-CLI on a simplified system looks like this:

```sh
$HOME
├── .fair
│   ├── cli
│   │   └── cli-config.yaml
│   ├── data
│   │   └── jobs
│   └── $REGISTRY_HOME
│       └── sessions
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

FAIR-CLI stores information about projects in two places. The *global* directory is `$HOME/.fair/cli`, in the user's home folder beside the registry. The *local* directory is `$PROJECT_HOME/.fair`, inside the model project itself.

The CLI holds metadata about the user in its own configuration file, `cli-config.yaml`. This is not the `config.yaml` that the user edits. The *global* version is made on first use. As in `git`, each project can override the *global* configuration with a *local* one.

### Data Directory

`$HOME/.fair/data` is the default data store. Setup can give another, and a run's configuration can give another again. The subdirectory `jobs` of the default data store holds a timestamped directory for each job.

### Sessions Directory

The directory `sessions`, in the registry's own directory, records the registry's server. The record gives the host the server runs on, its address, and whether `fair registry start` or a command started it. The directory also holds a record of each session, naming the process that holds it. The registry is not stopped while a session is open. A registry that a command started stops when the last session closes. A file `lock` there lets only one process at a time start or stop the registry.

### Logs Directory

The directory `$PROJECT/.fair/logs` holds each job's log. The log records the job's output, who launched it and how long it took.

### Staging File

The staging file, `$PROJECT/.fair/staging`, records what can be pushed: jobs, files, data products and code runs. Each item is marked `True` if it is staged, to be pushed, or `False` if it is to stay local. A job is added after it finishes, unstaged. Jobs and files are keyed by their paths relative to the *local* `.fair` folder, much as `git` stages files.

### `config.yaml`

This is the file the user edits to configure a run. `fair create`, or `fair init --config <file>`, writes a starter version of it. `run_metadata` must say what to run, with one of these keys, unless the run is given `fair run --script` (see [below](#run)):

- **`script`**: commands for a shell, which run a model or submit data to the registry. The CLI saves them to a file before running it.
- **`script_path`**: the path of an existing script to run.

By default a script runs in `sh` on UNIX and in `batch` on Windows. The optional `shell` key chooses another shell from this table, in which `{0}` stands for the script file:

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

Flags for a shell's program follow its name, separated by spaces. They go straight after the program. So `shell: julia --project=@.` runs `julia --project=@. {0}`, which starts Julia in the repository's project. A flag cannot contain a space. `batch` has no program, so it takes no flags.

| **NOTE**                                                                                                                                                                                                                                            |
| --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| This layout is subject to possible change depending on whether or not multiple aliases for the same user will be allowed in the registry itself. The main reason for having a *local* version is to support separate handling of multiple projects. |

## Registry Interaction

`fair init` registers the file types, and the project's user as an author, in the local registry. The CLI registers nothing about a run itself: the run's language API registers what the run reads and writes.

## Command Line Usage

Running `fair` without arguments, or `fair --help`, lists the commands. Some are still under review.

### `init`

`fair init` makes the current directory a FAIR project. The directory should ideally be the root of the project's git repository, the folder holding `.git`. Setup asks where the git repository is, so another location can be given. The command asks the user a series of questions. The answers record who ran each run. With `--config <file>`, it also writes a starter `config.yaml`.

The first `fair init` on a machine fills in the *global* CLI configuration. Later calls offer the *global* values as defaults for the project's own, *local*, configuration.

`fair init` makes a `.fair` directory, laid out as above, in the current location.

#### Example: First call to `fair init`

The first `fair init` on a machine asks for the global configuration, then for the project's. Pressing Enter takes the default in square brackets. In this example the local registry was installed already, by `fair registry install`. If none is installed, `fair init` asks whether to use an existing installation, and otherwise installs one.

The remote registry in this example is a test one on the same machine. Taking the default gives the FAIR Data Pipeline's registry. To get a token for it, log in to [its website](https://data.fairdatapipeline.org/) with GitHub, and create an API token there. Answering no to starting the local registry starts it only while `fair init` needs it. The ORCID is the one ORCID uses for its example researcher, whose name `fair init` looks up.

```text
$ fair init
Initialising FAIR repository, setup will now ask for basic info (leave blank for default value):

Setup will now ask you questions regarding the global configuration
Checking for local registry
Local registry found at '/home/jcarberry/.fair/registry'
Local Registry Port [8000]:
Remote API URL [https://data.fairdatapipeline.org/api/]: http://127.0.0.1:8001/api/
Remote Data Storage Root [http://127.0.0.1:8001/data/]:
Remote API Token: <your 40-character token>
Local registry is offline, would you like to start it and for it to remain running? [y/N]:
Temporarily launching local registry to retrieve API token.
Default Data Store [/home/jcarberry/.fair/data/]:
User ID system (GITHUB/GITLAB/ORCID/ROR/GRID/None) [GITHUB]: ORCID
ORCID: 0000-0002-1825-0097
Found entry: Josiah Carberry
Email (optional) []: jcarberry@example.org
Default output namespace [josiahcarberry]:
Default input namespace [josiahcarberry]:
Setup will now ask you questions regarding this repo configuration
Local Git repository [/home/jcarberry/Documents/AnalysisProject]:
Git remote name [origin]:
Using git repository remote 'origin': https://git.example.org/jcarberry/AnalysisProject.git
Remote API URL [http://127.0.0.1:8001/api/]:
Remote API Token File [/home/jcarberry/.fair/cli/remotetoken.txt]:
Default output namespace [josiahcarberry]:
Default input namespace [josiahcarberry]:
Initialised empty fair repository in /home/jcarberry/Documents/AnalysisProject/.fair
```

#### Example: A second project

On a machine already set up, `fair init` asks only for the project's configuration. It offers the global values as defaults:

```text
$ fair init
Initialising FAIR repository, setup will now ask for basic info (leave blank for default value):

Setup will now ask you questions regarding this repo configuration
Local Git repository [/home/jcarberry/Documents/AnotherProject]:
Git remote name [origin]:
Using git repository remote 'origin': https://git.example.org/jcarberry/AnotherProject.git
Remote API URL [http://127.0.0.1:8001/api/]:
Remote API Token File [/home/jcarberry/.fair/cli/remotetoken.txt]:
Default output namespace [josiahcarberry]:
Default input namespace [josiahcarberry]:
Initialised empty fair repository in /home/jcarberry/Documents/AnotherProject/.fair
```

#### Generated `config.yaml`

`fair create` then writes a starter `config.yaml` in the project:

```text
$ fair create
Generating new user configuration file '/home/jcarberry/Documents/AnalysisProject/config.yaml'
```

```yaml
run_metadata:
  default_input_namespace: josiahcarberry
  default_output_namespace: josiahcarberry
  description: Project description
  script: echo "Hello World!"
```

The user then replaces the `description`, and the `script` with what the project runs, or gives a `script_path` instead.

#### Advanced usage

`fair init --using <file>` reads the CLI configuration from a file. `fair init --export <file>` writes one. On a machine with no CLI configuration yet, the file's settings become the machine's as well as the project's. On a machine set up already, they are the project's alone. One local registry, with its data store, serves every project on a machine, so a file naming another one is refused. Each key below is required, except `orcid` and `uri` under `user`, and `registries: local` on a machine set up already:

```yaml
namespaces:
  input: testing
  output: testing
registries:
  local:
    data_store: /path/to/local/data_store/
    directory: /path/to/local/registry/
    token: /path/to/local/registry/token
    uri: http://127.0.0.1:8000/api/
  origin:
    data_store: https://data.fairdatapipeline.org/data/
    token: /path/to/remote/token
    uri: https://data.fairdatapipeline.org/api/
user:
  email: jbloggs@example.org
  family_name: Bloggs
  given_names: Joe
  orcid: 0000-0002-1825-0097
  uri: https://orcid.org/0000-0002-1825-0097
  uuid: 2ddb2358-84bf-43ff-b2aa-3ac7dc3b49f1
  remote_user: jbloggs
git:
  local_repo: /path/to/project/
  remote: origin
  remote_repo: https://github.com/jbloggs/project
```

```sh
fair init --using <file>
```

For CI runs, `fair init --ci` skips the questions:

```sh
fair init --ci
```

It gives the project the user "Interface Test" and both namespaces `testing`, and expects the remote registry at `http://127.0.0.1:8001/api/`. On a machine with no CLI configuration yet, such as a CI runner, these settings become the machine's too, with the local registry on port 8000 and its data store in the project's `.fair/data_store` folder. On a machine set up already, the project uses the machine's local registry and data store.

### `run`

`fair run` runs a model, or a data submission, as `config.yaml` configures it. It fills in each template variable of the form `${{ VAR }}`, as listed [below](#template-variables). The run's language API registers what the run reads and writes in the local registry. The outputs go in the data store that `run_metadata: write_data_store` names, by default `$HOME/.fair/data/`. Each run's job folder, holding its working config and its script, is under `jobs` in the default data store.

```sh
fair run
```

If you wish to use an alternative `config.yaml` then specify it as an additional argument:

```sh
fair run /path/to/config.yaml
```

`--script` runs a command directly, and writes it into the `config.yaml`:

```sh
fair run --script "echo \"Hello World\""
```

Quote the command, since it is one argument.

### `pull`

`fair pull` registers the entries under `register:` in `config.yaml`. It downloads each entry's file into the local data store, and registers it in the local registry as an external object and a data product. For example:

```yaml
run_metadata:
  default_input_namespace: SCRC
  default_output_namespace: joebloggs
  description: Test project
  local_data_registry_url: http://127.0.0.1:8000/api/
  local_repo: /home/joebloggs/Documents/SCRC/FAIR-CLI
  write_data_store: /home/joebloggs/.fair/data/
register:
- namespace: scottish_government
  full_name: Scottish Government Open Data Repository
  website: https://statistics.gov.scot/
- external_object: records/SARS-CoV-2/scotland/human-mortality
  namespace_name: scottish_government
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
  alternate_identifier_type: ods_name
  file_type: csv
  release_date: ${{DATETIME}}
  version: 0.${{DATE}}.0
  primary: True
```

Run on 10 October 2021, this downloads the data from the URL that `root` and `path` give, and stores it in this file:

```sh
/home/joebloggs/.fair/data/scottish_government/records/SARS-CoV-2/scotland/human-mortality/0.20211010.0.csv
```

It then registers the file in the local registry. A namespace named by `namespace_name` must have a `namespace:` entry of its own in the same `register:` block.

An entry's `version` may instead be a placeholder, which `fair pull` resolves once it has fetched the file:

- With `${{PATCH}}`, `${{MINOR}}` or `${{MAJOR}}`, the highest version registered is kept if the file is that version's file. A changed file is registered at the next version. `${{PATCH}}` is the default, when no version is given.
- With `${{LATEST}}`, the highest version is kept. A changed file is refused, and so is a name with no version yet. This pins an input that is expected not to change.

`fair run` reads the version that `fair pull` registered, and refuses an entry that `fair pull` has not registered.

Some sources have no file of their own, such as a data store, named by its DOI, that a run queries for the files it needs. Such a source is registered with `no_file: true` in place of `root`, `path` and `file_type`. It becomes a data product with no file, whose source is the external object. A run may then name it in its `read` block. Its `version` must be given, and stays as it is, since with no file nothing else says when it changes. `cache` and `public` describe a file, so they are refused with `no_file`:

```yaml
register:
- namespace: provider
  full_name: The data store's provider
- external_object: provider/store
  namespace_name: provider
  identifier: https://doi.org/<the data store's DOI>
  title: <the data store's title>
  release_date: <the data store's release date>
  primary: True
  version: 1.0.0
  no_file: true
```

### `purge`

`fair purge` removes the project's `.fair` folder, so that the project can be initialised again:

```sh
fair purge
```

`--global` also removes the global configuration, to start again entirely:

```sh
fair purge --global
```

`--data` also removes the default data store:

```sh
fair purge --data
```

**Warning**: this is not recommended. The registry may still hold records of the files in that data store.

`--yes` answers every question for you:

```sh
fair purge --yes
```

### `registry`

A command that uses the local registry starts it if it is not running, on the configured port. A registry started this way stops when the last command using it finishes. `fair registry start` starts the registry, or keeps the one a command started, until `fair registry stop`. The [README](https://github.com/FAIRDataPipeline/FAIR-CLI#registry) says more. It includes the session commands for programs that use the registry outside a `fair` command.

### `log`

Each job's log is kept in the project's `.fair/logs` folder. `fair log` lists the 10 most recent jobs that this repository ran:

```sh
fair log
```

Each job is shown much as `git log` shows a commit:

```text
job # 0db35c20946a1ebeaafdc3b30103cd74a57eb6b6
Author: Joe Bloggs <jbloggs@noreply.uk>
Date:   Wed Jun 30 09:09:30 2021
```

A job's SHA is not a code run's identifier on the registry: one job can hold several code runs.

### `view`

`fair view` shows a job's log, given its SHA from `fair log`. The first few characters of the SHA are enough:

```sh
fair view <sha>
```

```text
--------------------------------
 Commenced = Wed Jun 30 09:09:30 2021
 Author    = Joe Bloggs <jbloggs@noreply.uk>
 Command   = bash -eo pipefail /home/jbloggs/.fair/data/jobs/2021-06-30_09_09_30_721358/script.sh
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

In `config.yaml`, `${{ VAR }}` stands for the value of a template variable. `fair run` substitutes these:

| **Variable**      | **Description**                                                   |
| ----------------- | ----------------------------------------------------------------- |
| `DATE`            | The date, in the form `%Y%m%d`                                    |
| `DATETIME`        | The date and time, in the form `%Y-%m-%dT%H:%M:%S`                |
| `DATETIME-%Y%H%M` | The date and time in a form of your own, here `%Y%H%M`            |
| `USER`            | The user's name, from the CLI's configuration                     |
| `USER_ID`         | The user's identifier as a URL, else their UUID                   |
| `REPO_DIR`        | The project's root directory                                      |
| `CONFIG_DIR`      | The job's folder, which holds the working config                  |
| `LOCAL_TOKEN`     | The local registry's token                                        |
| `SOURCE_CONFIG`   | The file name of the user's `config.yaml`                         |
| `GIT_BRANCH`      | The git repository's current branch                               |
| `GIT_REMOTE`      | The URL of the git remote chosen at setup                         |
| `GIT_TAG`         | The nearest tag in the history of the current commit              |

`${{ RUN_ID }}` is left in place for the language API, which puts the code run's UUID there.
