# FAIR Data Pipeline Command Line Interface

[![PyPI](https://img.shields.io/pypi/v/fair-cli)](https://pypi.org/project/fair-cli/) [![PyPI - Python Version](https://img.shields.io/pypi/pyversions/fair-cli)](https://pypi.org/project/fair-cli/) [![DOI](https://zenodo.org/badge/377398464.svg)](https://zenodo.org/badge/latestdoi/377398464) ![PyPI - License](https://img.shields.io/pypi/l/fair-cli)  [![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/5411/badge)](https://bestpractices.coreinfrastructure.org/projects/5411)

[![FAIR Data Pipeline CLI](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml/badge.svg?branch=main)](https://github.com/FAIRDataPipeline/FAIR-CLI/actions/workflows/fair-cli.yaml)  [![codecov](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI/branch/dev/graph/badge.svg?token=h93TkTiiWf)](https://codecov.io/gh/FAIRDataPipeline/FAIR-CLI) [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=FAIRDataPipeline_FAIR-CLI&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=FAIRDataPipeline_FAIR-CLI)

FAIR-CLI is the main interface for synchronising changes between a local FAIR Data Pipeline registry and shared remote ones. It also launches model runs and data submissions to the pipeline. The project's [website](https://www.fairdatapipeline.org/) documents the FAIR Data Pipeline in full.

## Installation

Install the package with `pip`:

```sh
pip install fair-cli
```

It needs Python 3.10 or later. A virtual environment is recommended. The CLI installs the local registry in an environment of its own, made with the CLI's Python.

On an older Python, `pip install fair-cli` gives no error. Instead it installs the last release that Python allows: 0.9.8 on Python 3.9, such as the `python3` that comes with macOS. The registry needs Python 3.10 as well, so that release cannot install the current registry. Check what you have with `fair --version`. Or ask `pip` for a recent release, which it refuses on an older Python:

```sh
pip install "fair-cli>=0.11"
```

To enable tab completion, add a completion script to your shell's start-up.

### Bash

Completion needs Bash 4.4 or later. The Bash that comes with macOS is older.

```sh
_FAIR_COMPLETE=bash_source fair > ~/.config/.fair-complete.bash
echo '. ~/.config/.fair-complete.bash' >> ~/.bashrc
```

### zsh

```sh
_FAIR_COMPLETE=zsh_source fair > ~/.fair-complete.zsh
echo '. ~/.fair-complete.zsh' >> ~/.zshrc
```

### Fish

Fish loads a script from its completions folder by itself:

```sh
_FAIR_COMPLETE=fish_source fair > ~/.config/fish/completions/fair.fish
```

## Upgrading and reinstalling

The CLI and the local registry are installed separately. `pip` installs the CLI, and the CLI installs the registry. So upgrading one does not upgrade the other.

### The CLI

To move to the newest release, or to a particular one:

```sh
pip install --upgrade fair-cli
pip install fair-cli==0.12.0
```

To install a branch of this repository, give `pip` its name after an `@`. A tag or a commit works the same way:

```sh
pip install git+https://github.com/FAIRDataPipeline/FAIR-CLI@<branch>
```

Or clone the repository with the branch checked out, and install from the clone:

```sh
git clone -b <branch> https://github.com/FAIRDataPipeline/FAIR-CLI
pip install ./FAIR-CLI
```

A branch keeps its version number from one commit to the next. `pip` does not replace an installed package with another of the same version. So to bring an installed branch up to date, or to install one branch over another of the same version, add `--force-reinstall --no-deps`:

```sh
pip install --force-reinstall --no-deps git+https://github.com/FAIRDataPipeline/FAIR-CLI@<branch>
```

`fair --version` gives only the version number, and a branch may share it with a release. `pip freeze` says what is installed: `fair-cli==0.12.0` for a release, and `fair-cli @ git+https://github.com/FAIRDataPipeline/FAIR-CLI@<commit>` for a branch.

### The local registry

To move the local registry to the newest release, keeping its records:

```sh
fair registry upgrade
```

`--version` gives a tag or a branch of the [registry's repository](https://github.com/FAIRDataPipeline/data-registry) in place of the newest release:

```sh
fair registry upgrade --version v1.5.0
fair registry upgrade --version <branch>
```

`upgrade` builds the new version in the registry's place. It keeps the registry's records if the new version's tables are the same as the installed version's, as they are between most releases. If the tables differ, `upgrade` leaves the registry as it was, and its refusal says how to install that version with an empty registry. A running registry is stopped for the rebuild and started again afterwards. Nothing is changed while another command is using the registry.

Given the version already installed, `upgrade` rebuilds the registry and keeps its records. This mends a registry whose Python has gone, for example after an upgrade of the Python it was built with.

To replace the local registry with an empty one instead, at the newest release or at a given version:

```sh
fair registry install --force
fair registry install --force --version <tag or branch>
```

Without `--force`, `install` leaves an existing registry as it is and stops. `fair registry uninstall` followed by `fair registry install` does the same as `--force` in two steps, and `uninstall` asks first. `fair-cli` 0.10.1 and earlier refuse `--force`, so with them use the two steps. To empty the registry but keep its install, see `fair registry reset` under [`registry`](#registry).

`install` refuses a version that the registry's repository does not have, before it stops or replaces anything. With `--force`, the old registry is kept until the new one is built. If the install fails, for example with no network or a failed build, the old registry is put back as it was. The configuration then still names it. A running registry is stopped first. Nothing is replaced while another command is using the registry.

`install` puts the registry in the directory given with `--directory`. Without `--directory`, it uses the directory the configuration names, or `~/.fair/registry` if there is no configuration yet. A reinstall replaces that directory:

- Every record in the local registry is lost with its database: the data products, the code runs, and what `fair init` put there.
- The local registry gets a new token. The registry writes it to `token` in its directory when it next starts. Anything else kept in that directory is gone.
- The data store, the projects and `~/.fair/cli` are not touched.

The new registry does not know the user of a project initialised before the reinstall. A run in that project fails until it does. There are two ways to put that right:

- Run `fair init` in the project again. It registers the project's user in the new registry, and leaves the project as it is, with its configuration and any data store kept inside it. This needs `fair-cli` 0.10.2 or later. In earlier versions, `fair init` does nothing in a project that is already initialised.
- Or start the project afresh. Run `fair purge` in it, which removes its `.fair` folder and any data store kept inside it. Then run `fair init`.

After either, `fair pull` registers again what the project registers or reads. Anything pushed to a remote registry can be pulled back from it. The records of anything not pushed cannot be recovered. Its files are still in the data store, unless the store was removed with the project's `.fair` folder.

## Uninstallation

To uninstall the CLI:

```sh
fair purge --all
pip uninstall fair-cli
```

`fair purge --all` removes the current project's `.fair` folder and the whole of `~/.fair`. That includes the local registry, the default data store, the CLI's configuration and any token kept there. Leave that step out to keep them.

## The User Configuration File

A run is configured by a `config.yaml` file. `fair create` writes a starter one, and so does `fair init --config <file>` as it initialises a project. The configuration says what to run in `run_metadata`: either `script`, holding commands for the shell, or `script_path`, naming a script file. Alternatively, `fair run --script '<command>'` writes the command into the configuration and runs it.

By default a script runs in `sh` on UNIX and in `batch` on Windows. The optional `shell` key in `run_metadata` chooses another shell from this table, in which `{0}` stands for the script file:

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

Flags for a shell's program follow its name, separated by spaces. They go straight after the program. So `shell: julia --project=@.` runs `julia --project=@. {0}`, which starts Julia in the repository's project. `shell: bash -x` runs `bash -x -eo pipefail {0}`. A flag cannot contain a space. `batch` has no program, so it takes no flags.

A run's configuration describes the run in `run_metadata: description:`. The registry requires a description of every code run. So `fair run` refuses a configuration without one, or with a blank one, before it registers anything.

The names that a run or a pull would register must keep to a naming convention. These are the names in `write:` and `register:` entries, and their namespaces. `fair run` and `fair pull` refuse a configuration that breaks the convention, before they register anything.

- A namespace holds ASCII letters, digits, `_` and `-`.
- A data product's name is one or more parts joined by single `/`. A part holds the same characters as a namespace, and also `.`, but a part may not start or end with `.`.
- A name may also hold `*` within a part, as a wildcard, and `${{RUN_ID}}`.

So `PSU`, `SEIRS_model/parameters` and `DataPipeline.jl/release` are names, and `my data` and `results/.hidden` are not. A `read:` entry names something already registered, so it is not checked.

The website's [description of `config.yaml` files](https://www.fairdatapipeline.org/docs/interface/config/) covers them in full.

## Available Commands

### `init`

`fair init` makes the current directory a FAIR project. The directory should ideally be the root of the project's git repository, the folder holding `.git`. Setup asks where the git repository is, so you may give another. `fair init` asks a series of questions about you, which give each run its author. It also configures the CLI itself. With `--config <file>`, it also writes a starter `config.yaml` file.

`fair init` asks for your identifier before your email. If the identifier's record makes an email address public, as an ORCID record or a GitHub account may, `init` offers that address. If you give no email, it uses a GitHub no-reply address.

In a repository that is already initialised, `fair init` asks nothing and leaves the repository as it is. It only registers the repository's user in the local registry again. A registry [reinstalled](#upgrading-and-reinstalling) since the repository was initialised no longer holds that user.

If `fair init` fails or is interrupted before it writes the repository's configuration, it leaves things as they were. There is no `.fair` folder, and the global configuration is unchanged. If it fails after that, for example while registering the user in a local registry that is not running, the repository stays initialised. Running `fair init` again completes it.

A process killed during `fair init` can leave a `.fair` folder with no configuration in it. The next `fair init` removes that folder and starts afresh. If the folder holds anything more, `fair init` names it and leaves it for you to look through.

#### Custom CLI Configuration

After setup, `fair init --export <file>` saves the CLI configuration to a file. A later setup can read it back:

```sh
fair init --using <file>
```

To write such a file from scratch, give it these keys. Each is required, except `orcid` and `uri` under `user`, and `registries: local` on a machine set up already:

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

On a machine with no CLI configuration yet, the file's settings become the machine's as well as the project's. On a machine set up already, they are the project's alone, and the machine's configuration stays as it is. One local registry, with its data store, serves every project on a machine. So the file may leave `registries: local` out, and anything it gives there must be the machine's. A file naming another local registry, port or data store is refused. A project can still keep its outputs apart: `run_metadata: write_data_store` in its `config.yaml` names a data store of its own.

In a CI workflow, `fair init --ci` skips the questions. It gives the project the user "Interface Test" and both namespaces `testing`. It expects the remote registry at `http://127.0.0.1:8001/api/`, with its token in a file `token` in the directory the command runs from. The user it records for the remote is the one that token belongs to. If the remote cannot be asked, that user is `FAIRDataPipeline`.

On a machine with no CLI configuration yet, such as a CI runner, `--ci`'s settings become the machine's too. The local registry is then on port 8000, with its data store in the project's `.fair/data_store` folder. On a machine set up already, the project uses the machine's local registry and data store, and the machine's configuration stays as it is. `--registry` may then name only the machine's registry.

### `run`

`fair run` runs a model, or a data submission, as `config.yaml` configures it. The run's language API registers what the run reads and writes in the local registry. The outputs go in the data store that `run_metadata: write_data_store` names, else the one chosen at `fair init`, by default `~/.fair/data/`. Each run also gets a job folder, holding its working config and its script. Job folders are under `jobs` in the data store chosen at `fair init`.

```sh
fair run
```

That reads the `config.yaml` at the project's root. To use another, give its path:

```sh
fair run /path/to/config.yaml
```

`--script` runs a command directly, and writes it into the configuration:

```sh
fair run --script 'echo "Hello World"'
```

Quote the command, since it is one argument.

By default `fair run` refuses to run if the project's git repository has uncommitted changes, or if its branch is ahead of or behind the git remote's. `--dirty` turns both refusals into warnings.

A detached checkout has no branch to compare. Examples are a checked-out tag or commit, and a pull request on a CI system. There the commit must be one the remote has, on a branch or a tag. `--dirty` lets through a commit the remote does not have.

`--rehearse` prepares a job without running it, as a language API does to rehearse a run:

```sh
fair run --rehearse
```

It writes the job's folder, working config and script exactly as a run would, and runs nothing. It makes no git check: it records the commit as it is, marked `-dirty` if there are uncommitted changes. It stages nothing. Its only output on stdout is one JSON object naming the job's files. Its log goes to stderr:

```json
{"job_dir": "...", "config": "...", "script": "...", "data_store": "...", "token_file": "..."}
```

`token_file` is the file holding the local registry's token. The token itself is never printed. Like every command, a rehearsal holds the local registry only while it runs. So a program that goes on to run the job's script should hold a [session](#registry) of its own around both.

`fair run --ci` also prepares a job without running it. Unlike `--rehearse`, it stages the job, warns about the repository's state, and prints the job's folder as its last line.

### `pull`

`fair pull` reads `config.yaml`, and registers each entry under `register:`. It downloads the entry's file into the local data store, and registers the file in the local registry as an external object and a data product. It also pulls from the remote registry each data product that a `read:` entry asks for, if the remote holds it.

```sh
fair pull /path/to/config.yaml
```

An `external_object` entry with `no_file: true` registers a source that has no file of its own, such as a data store that a run queries. The source becomes a data product with no file. Such an entry gives no `root`, `path` or `file_type`, and must give its `version`.

A file you already hold can stand in for a download, as `cache: <file>`:

- In a `register:` entry, `cache` registers your file in place of the download. `root` and `path` still record where the file came from.
- In a `read:` entry, `use: cache` is for a data product that the remote registry holds but cannot send, such as a private one. A private file is not uploaded, so the remote records it on the machine that made it. `fair pull` pulls the entry as any other read, and copies your file into the local data store in place of the download. It refuses a file without the hash that the remote records, and registers nothing for it. A wildcard entry cannot give `cache`, since one file cannot stand for several data products.

Your file is left as it was. A relative path is taken from the current directory, and `${{REPO_DIR}}` starts one at the project's root.

### `status`

`fair status` lists what is staged and what is not, much as `git status` does:

```sh
fair status
```

Staged items are shown in green, and unstaged ones in red.

### `add`

A push sends only what is staged. `fair add` stages an item that `fair status` lists. Give a data product as `namespace:name@v<version>`, or a code run by its UUID:

```sh
fair add my_namespace:data_object@v0.1.0
```

`fair unstage` takes an item off the stage again, and `fair reset` unstages everything.

### `push`

`fair push` sends what is staged to the remote registry, `origin` unless you name another:

```sh
fair push
```

A data product is identified by its namespace, name and version. A push refuses a data product if the remote registry holds one with the same three but a different file. For a data product with no file, a different source counts in the same way. A pull refuses the same mismatch in the other direction. The command checks every data product it would send before it sends anything, including those of the staged code runs. So a refusal sends nothing. The refusal names each such data product and both files. To push yours, register it under another version and push that.

A push also sends the records attached to each data product and code run that it sends. These are named components, licences, keywords, key-values, quality-control records, code releases, and the issues on components. A pull brings them the other way. A record added after its data product or run was sent goes with the next push.

An issue is not sent again if the other registry holds it already. The command looks for it there by its UUID. Failing that, it looks for an issue with the same severity and description on the same component, raised there separately. If it finds one, it links that issue to any further components the issue is on. If the other registry holds any record with different values, the record there is kept as it is, and the command warns.

A push also warns of earlier code runs that the remote does not hold. Each such run wrote a pushed data product, an input of a pushed run, or an input of another such run. The warning names the `fair add` command that would send it.

When a push or pull sends a code run, it gives the run the object that the other registry already holds for the run's commit, if there is one. So runs of one commit from several machines share one object for the code repository. The command finds that object by the repository's host and the commit, whatever path it was registered at. It sends the repository's records, such as its release, onto that object. If there are several such objects, it takes the one with a release, else the oldest.

### `purge`

`fair purge` removes the current project's `.fair` folder, so that the project can be initialised again. It asks first:

```sh
fair purge
```

`--global` also removes the configuration that all projects share, the whole of `~/.fair/cli`, including any remote's token kept there:

```sh
fair purge --global
```

`--data` also removes the default data store:

```sh
fair purge --data
```

**Warning**: this is not recommended. The registry may still hold records of the files in that data store.

`--all` removes everything: the project's `.fair` folder and the whole of `~/.fair`, including the local registry:

```sh
fair purge --all
```

`--yes` answers every question for you:

```sh
fair purge --yes
```

### `registry`

Commands that use the local registry start it if it is not running, on the port the configuration names. These are `run`, `pull`, `push`, `add`, `status`, `list`, `find`, `identify` and `reset`. A registry started this way stops when the last command using it finishes. Each command holds a *session* on the registry while it runs. The registry is not stopped while any session is open.

To keep the registry running, start it yourself. You might do so to look at it in a browser, or to save each command the few seconds a start takes:

```sh
fair registry start
```

It then runs until you stop it:

```sh
fair registry stop
```

`fair registry start` uses the port in the configuration, `registries.local.uri` in `~/.fair/cli/cli-config.yaml`. `--port` gives another port, which becomes the configured one once the registry starts there. If a command already has the registry running, `fair registry start` keeps that registry running from then on, rather than starting another. `fair registry stop` refuses while a command is using the registry, and `--force` stops it anyway. `fair registry status` says where the registry is running, whether `fair registry start` or a command started it, and which processes are using it.

A registry runs on one machine at a time, because its database is a file in its directory. That directory may be shared between machines, as a home directory on a cluster may be. Then the registry cannot be started, stopped or used from any machine but the one running it. The refusal names that machine, and the file in the registry's directory that records it. If that machine no longer runs the registry, for example because it has shut down or restarted, delete the file and try again.

`fair purge --all`, `fair registry uninstall` and `fair registry install --force` stop the registry before removing it. They remove nothing while a command is using the registry, or while it runs on another machine.

A program that uses the registry outside a `fair` command, such as a language API, can hold a session of its own:

```sh
fair registry session open --pid <process id> --json
fair registry session close <session id>
```

`session open` starts the registry if it is not running. It prints one JSON object holding the session's id, the registry's address and the file holding its token: `{"session": ..., "registry_url": ..., "token_file": ...}`. The session ends when it is closed, or when the process it names ends. A script run by `fair run` needs no session of its own: `FDP_SESSION` gives it the run's session.

`fair registry install` installs the registry:

```sh
fair registry install
```

`--directory` gives the folder to install it in, and `--version` the tag or branch of the registry's repository to install:

```sh
fair registry install --directory ~/.fair/my_registry --version v1.5.0
```

To replace a registry that is already installed, see [Upgrading and reinstalling](#upgrading-and-reinstalling).

The data stores can hold files that the registry does not know about. One may be a file put there by hand, or the output of a run that failed before registering it. To delete such files:

```sh
fair registry clean
```

`clean` runs only while the registry is stopped and no command is using it, so that no run is writing to a store. It asks before deleting anything (`--yes` answers for you). `--dry-run` lists what it would delete and deletes nothing. By default it keeps the files that a run in progress could still be writing: outputs named `dat-...` or by their hash, and everything under a store's `jobs/`. `--delete-all` deletes those too.

The data stores are the folders on this machine where the registry holds files. `clean` never touches a code repository's folder inside one. It does not clean a store that is your home folder, or a folder above it.

To empty the registry instead, deleting every record but keeping the install:

```sh
fair registry reset
```

`reset` asks first (`--yes` answers for you). It refuses while another command is using the registry. If the registry was running, `reset` starts it again afterwards. If emptying it fails, or is stopped by Ctrl-C, the registry keeps its records. It registers the file types again. Run in a project, it also registers the project's user again and clears the project's staging. In each other project, run `fair reset` to unstage what that project had staged.

`--delete-files` also deletes every file in the registry's data stores, except code repositories' folders. Without it the files stay. `fair registry clean` cannot delete them later, because an emptied registry knows no data stores.

While `fair registry clean` or `reset` runs, any other command that would start, stop or use the registry is refused. The refusal names the command that is running.

### `log`

`fair log` lists the 10 most recent jobs that `fair run` ran from this repository:

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

By default every project on a machine shares one data store, so other repositories' jobs may sit beside this one's. `fair log` says how many of those it skipped, and which repositories ran them.

### `view`

`fair view` shows a job's log, given its SHA from `fair log`. It also lists the code runs the job recorded:

```sh
fair view <sha>
```

The first few characters of the SHA are enough. If several jobs' SHAs start with them, `fair view` shows the newest.

## Template Variables

In `config.yaml`, `${{ VAR }}` stands for the value of a template variable. `fair run` substitutes these:

| **Variable**      | **Description**                                                   |
| ----------------- | ----------------------------------------------------------------- |
| `DATE`            | The date, in the form `%Y%m%d`                                    |
| `DATETIME`        | The date and time, in the form `%Y-%m-%dT%H:%M:%S`                |
| `DATETIME-%Y%H%M` | The date and time in a form of your own, here `%Y%H%M`            |
| `USER`            | Your name, from the CLI's configuration                           |
| `USER_ID`         | Your identifier as a URL, else your UUID                          |
| `REPO_DIR`        | The project's root directory                                      |
| `CONFIG_DIR`      | The job's folder, which holds the working config                  |
| `LOCAL_TOKEN`     | The local registry's token                                        |
| `SOURCE_CONFIG`   | The file name of your `config.yaml`                               |
| `GIT_BRANCH`      | The git repository's current branch                               |
| `GIT_REMOTE`      | The URL of the git remote chosen at setup                         |
| `GIT_TAG`         | The nearest tag in the history of the current commit              |

`${{ RUN_ID }}` is left in place for the language API, which puts the code run's UUID there.

## What a program may read of the CLI's configuration

A language API, or any other program working with the pipeline, may rely on these parts of the CLI's configuration files:

- `registries.local.uri` and `registries.local.token` in `~/.fair/cli/cli-config.yaml`: the local registry's API address, and the file holding its token.
- `registries.<name>.uri` and `registries.<name>.token` in a project's `.fair/cli-config.yaml`: the same for each remote registry, by its name. The name is `origin`, or one added with `fair remote add`.
- A token file holds that registry's token alone. Read it as it is, with surrounding white space stripped. Use it only for the registry whose entry names it.
- `git.remote` in a project's `.fair/cli-config.yaml`: the name of the project's git remote. Ask git for its URL (`git remote get-url <name>`). `git.remote_repo` beside it is a copy that `fair init` took, and goes out of date when the remote moves.
- A directory with a `.fair` folder is a FAIR project. Every `fair` command finds its project by searching upwards from the working directory.

Within a run, the working config holds what a script needs, including `run_metadata.local_data_registry_url` and `run_metadata.remote_repo`, the git remote's URL. The script's environment holds `FDP_CONFIG_DIR`, `FDP_LOCAL_TOKEN` and `FDP_SESSION`.
