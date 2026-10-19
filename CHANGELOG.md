# 2026-10-19 [v0.11.0](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.11.0)

## Changed behaviour
- A command that starts the local registry stops it again when the last command using it finishes; it used to leave
  it running. Each command holds a session on the registry while it runs. `fair registry start` keeps it running
  until `fair registry stop`, and run while a command has it running keeps that registry rather than refusing.
- `fair registry start` with no `--port`, and a command that starts the registry, use the configured port, where
  they used 8000 whatever the configuration said.
- An install's registry runs on one host at a time: where its directory is shared between machines, starting,
  stopping or using it from another machine is refused, naming the machine running it and the file that records it.
- `fair purge --all`, `fair registry uninstall` and `fair registry install --force` stop the registry first, and
  remove nothing while a command is using it or it runs on another machine.
- Two versions of the CLI sharing one registry install are not supported: one of an earlier version neither records
  its sessions nor respects this one's.
- `fair init` asks for the user's identifier before their email, and offers as the email the address the
  identifier's record makes public - an ORCID's or a GitHub account's. Anything answering its questions in order
  must put the email after the identifier.
- A `read:` wildcard's own entry is kept in the working config beside the entries it expands to, as a `write:`
  wildcard's is, so that an API can tell a pattern that matched nothing from one never given. `fair pull` fetches
  nothing for it. Its version - its matches' highest, the version it names, or 0.0.0 - means nothing.
- A `*` in a wildcard no longer matches `[`, `]`, `?` or `*`, which the registry refuses in a name, so a pattern
  never matches itself.

## Added
- `fair registry session open --pid <pid> --json` and `fair registry session close <id>`, for a program using the
  registry outside a `fair` command, such as a language API. A script run by `fair run` is told the run's session
  in `FDP_SESSION`.
- `fair registry status` says who holds the registry and which processes are using it.
- `fair run --rehearse` prepares a job as a run would without running it - no git check, nothing staged - and prints
  its files as one JSON object, `{"job_dir", "config", "script", "data_store", "token_file"}`, for a language API's
  rehearsal of a run.
- The README says which parts of the CLI's configuration a program such as a language API may read.
- A `register:` entry for an external object may have `no_file: true` in place of `root`, `path` and `file_type`,
  for a source with no file of its own, such as a data store a run queries: `fair pull` registers it as a data
  product with no file, which a run may read. Its `version` must be given.

## Fixed
- `fair purge --all` answered "no" removed the project's `.fair` folder, and with it a data store kept there; it now
  removes nothing. `fair purge --yes` asked its question anyway, and now answers every question as the README says.
- `fair registry install --force` removed the old registry before it had the new one, so a `--version` that does not
  exist left no working registry. The old one is now kept until the new one is built, and put back if the install
  fails; a first install that fails leaves nothing behind.
- `fair push` and `fair pull` left out a code repository's release (`CodeRepoRelease`), so the remote's record of a
  run or a data product lost it. The release now goes with its repository's object.
- A data product on a code repository's object, such as a release of the code registered with its DOI, was pushed
  with the repository's web page as its file, and could not then be pulled. Its location is the repository at a
  commit, and it is now pushed and pulled as that, with nothing downloaded.
- A `register:` entry with no version was registered again at the next patch version on every `fair pull`, whether
  or not its file had changed, and `fair run` then read a version that did not exist (#317). The highest version is
  now kept while the file is the same, and `fair run` reads it. `${{LATEST}}` on such an entry keeps the highest
  version and refuses a changed file.
- `fair run` did not check that `fair pull` had registered what a `register:` entry names, so the model found out;
  it now refuses to start.
- A ROR identifier (`https://ror.org/...`) of an author or a source was checked by fetching ROR's website, which
  refuses automated requests that come too often (429), and was then rejected. It is now checked through ROR's API.
- On Windows, `fair registry start` could fail - no response, or no token - when the registry's start script gave up
  waiting for the server, leaving the server running. The CLI now waits for the server itself, up to 30 seconds, and
  writes the registry's token if the script did not.
- A command that crashed, or a reboot after `fair registry start`, left a session file behind that made every later
  command skip starting the registry and `fair registry stop` refuse. A session now names its process, and one whose
  process has gone is removed. This needs a new dependency, `psutil`.
- A failed `fair registry start` left a session file behind, and a server on the port that never answered was waited
  on for ever; neither now happens. Another install's registry on the port is named as such.
- An internal error, and a registry's refusal of a request (HTTP 403), printed their message and exited 0, so a
  script or a CI step saw success. Both now exit non-zero, the refusal shown as an error.
- `fair pull` of a `register:` entry at a version given outright, registered already with another file, copied the
  new file over the registered one before the registry refused it, so the registered file no longer matched its
  record. It is now refused before anything is copied; the same file is passed over, as before.
- `fair init --ci` named the remote user `FAIRDataPipeline`, which a freshly built remote registry does not have, so
  `fair push` stopped. It now takes the user the remote's token belongs to, where the remote answers.
- `fair run` refused a detached checkout - a tag, a commit, a pull request on CI - unless given `--dirty`. It now runs
  where the commit is one the remote has, on a branch or a tag, and refuses one the remote does not have.
  `${{GIT_BRANCH}}` there is an error saying there is no branch.

# 2026-10-05 [v0.10.2](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.10.2)

## Changed behaviour
- `fair pull` refuses a `register:` entry for an external object whose identifier or unique name, title and
  release version are those of a source already registered from a different file: a source is one file. Give
  the entry a `title` of its own, or its `release_version`; the same file under another name still registers.
  With a local registry before data-registry v1.4.0, where each data product has its own source, nothing is
  checked.
- `fair init` in a repository that is already initialised registers its user in the local registry, where it
  did nothing. A registry reinstalled since the repository was initialised has no record of the user, and a
  run failed until the repository was purged and initialised afresh. The repository itself is left as it is.

## Added
- A `register:` entry for an external object may give `release_version`, the version of the source it was taken
  from, beside `release_date`. A local registry at data-registry v1.4.0 or later records it as the external
  object's version, 1.0.0 without it; an earlier one records the data product's version there whatever is
  given. It is not the entry's `version`, which is the data product's.

## Fixed
- A GitHub author or user was refused as "not a recognised github" once the GitHub API's limit for
  anonymous requests (60 an hour per address) was spent. GitHub lookups now send the token in `GITHUB_TOKEN`
  or `GITHUB_PAT` if set, retry without it if GitHub refuses it (expired or revoked), and say when the rate
  limit is the problem.
- The FAIR-CLI binaries could not install a local registry, so `fair init` and `fair registry install` failed:
  the registry's virtual environment was built from the binary in place of a Python. They now use a Python
  >= 3.10 from `FAIR_PYTHON` or the `PATH`, or have `uv` fetch one, and say so if there is none. A missing
  Python is found before anything is installed.
- `fair push` failed, with "Failed to access [] on remote registry", for a data product registered from a web
  address once a code run had used it, when the local registry was data-registry v1.4.0. The remote records
  such a file in two places with one hash - where it is stored and where it came from - and the wrong one
  could be taken.
- `fair registry install --force` refused an existing install, as it does without `--force`, instead of
  replacing it.
- A file that was already registered was not registered again under a second data product name, or in a
  second namespace: `fair pull` passed over the entry without a word, and a run that read it then failed. An
  entry is now passed over only when that data product, in that namespace and at that version, already holds
  the file.
- `fair push` and `fair pull` left out a data product that shares its external object with another, as
  data-registry v1.4.0 allows, while reporting it synchronised: its file was moved and its record was not made.
- Starting the local registry reported success when another registry already held its port, and the commands
  that followed went to that one. The start now fails, naming the address: the server that answers there
  must accept the installed registry's token.
- `fair push` and `fair pull` stopped with a Python error at a data product that has no file - one whose
  object has no storage location, as an entry for a deposit as a whole may be - and so at a code run that
  read one. Its records are now synchronised, and a `register:` entry of the same name is told it exists.
- `fair push` gave a code run an input it did not read when two data products held the same file, as one
  file registered under two names does: a run that read one of them and wrote something new arrived on the
  remote as having read both. A run pushed before this keeps the extra input.
- A file registered under a second data product name, or in a second namespace, was copied into the data store
  again, though the registry records a file once there and nothing referred to the copy. The copy is no
  longer kept: the data product's file is the one already in the store.
- A data product pulled from a remote could not be read through an API. `fair pull` copied the remote's record
  of where the file is - the remote's own store - and fetched the file to a place no record named, so a model
  was handed an address and no file. A pulled file is now recorded at its place in the local data store, and
  a file that two data products share is fetched once. Data products pulled with an earlier version keep
  the old record.

## Development
- The test of a data product's dependencies allows for any a registry adds; data-registry v1.4.0 adds one.

# 2026-10-05 [v0.10.1](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.10.1)

## Changed behaviour
- A wildcard entry keeps to one namespace: the one its `use:` names, else the default for its block. It
  used to match names in every namespace, so a `write:` pattern could write a name into a namespace
  other than its own.
- A wildcard entry gives one entry for each name it matches: for a `read:`, the version its `use:` names,
  else the highest; for a `write:`, the version to be written. It used to give one for every version, and
  to ignore a version on a `read:`. A `read:` pattern that matches nothing is not an error.
- `fair run` refuses a `read:` of a data product, or of a version of one, that is not in the registry,
  naming the namespace and registry it looked in. Such a read used to pass unchecked, or as version 0.0.0.
- `fair push` fails if a file cannot be uploaded. It used to warn and carry on, leaving the remote with
  the file's records and no file. A file is now uploaded before its records are written, so a push that
  failed can be run again.
- `fair pull` checks each file it fetches against the hash the remote registry holds for it, and refuses
  one that differs.

## Added
- Registering an external object records where its file was fetched from: the `root` and `path` of its
  `register:` entry, as the object's `original_store`, with the hash of the file found there. `fair push`
  takes that record to the remote. A file registered from the machine itself has no such record.
- `fair push` takes a refusal of an upload address with HTTP 409 to mean that the remote's store already
  holds the file, sends nothing, and carries on. No released registry answers so yet.

## Fixed
- `fair push` sent no value that was false, leaving the remote to apply its default: an external object
  registered with `primary: false` arrived as primary, and a location with `public: false` as public.
- A list from a registry stopped at its first 100 rows, so wildcards, staging and version lookups went
  wrong, with no message, in a registry holding more.
- `fair pull` looked for the version of a `read:` entry in the local registry only, so an entry at the
  default version, for something not yet local, asked the remote for version 0.0.0 and failed.
- `fair push` held each file whole in memory, `fair pull` more than twice over, and both left a copy of
  it in the temporary directory. Files are now sent and received a block at a time, whatever their
  size, and nothing is left behind.
- An upload of 2 GiB or more over plain http failed on macOS.
- An external object could not be registered with a `unique_name` and `alternate_identifier_type` in
  place of an `identifier`: `fair pull` stopped, reporting that the object already existed.
- `fair identify` looked at only the newest of a file's storage locations, so it could miss the data
  products of a file the registry records in more than one place.
- `fair run` could not start the script when the path of the data store, which holds the job
  directory, had a space in it.

## Development
- The Python job of the implementations workflow starts an object store for its remote registry, so
  that its `fair push` uploads the files. It had none, and passed while a failed upload was a warning.

# 2026-10-03 [v0.10.0](https://github.com/FAIRDataPipeline/FAIR-CLI/releases/tag/v0.10.0)

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
  for new names, and existing matches are written as the pattern entry describes them (its `use: version`,
  `file_type`, `description`).
- `fair push` from a project whose data store was not the local registry's first storage root recorded
  its files on the remote under the pusher's `file://` path (data-registry #229).
- `fair pull` of a `read:` entry from a remote registry failed.
- A DOI whose publisher refuses automated requests (e.g. Wiley, HTTP 403) was rejected as an invalid
  identifier. An identifier that resolves is now accepted; one that does not (404) is still refused.
- `${{DATETIME-<format>}}` always failed with "Failed to parse formatted datetime variable".
- Substituting a path with backslashes, such as `${{CONFIG_DIR}}` on Windows, failed `fair pull` and `fair run`
  (#267). Variables are now substituted into the configuration's values, so any text is inserted as it is.
- `fair remote remove` crashed.
- A ROR or GRID ID the ROR API could not parse (e.g. one containing `!`) crashed with `KeyError`
  instead of being reported as not found.
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
