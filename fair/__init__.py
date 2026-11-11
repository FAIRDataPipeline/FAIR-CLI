#!/usr/bin/env python3
"""
FAIR Data Pipeline Command Line Interface Tool
----------------------------------------------
Contains definitions for the synchronisation tool for communication between
remote and local FAIR Data Pipeline registries.

License
-------

BSD 2-Clause License

Copyright (c) 2021, Scottish COVID Response Consortium
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
"""

__author__ = "Scottish COVID-19 Response Consortium"
__credits__ = [
    "Richard Reeve (University of Glasgow)",
    "Nathan Cummings (UKAEA)",
    "Kristian Zarebski (UKAEA)",
    "Dennis Reddyhoff (University of Sheffield)",
]
__license__ = "BSD-2-Clause"
__status__ = "Development"
__copyright__ = "Copyright 2021, SCRC"

import logging
import os

logging.basicConfig()

# GitPython looks for the 'git' program as it is imported, and without one
# refuses to be imported, so that no command could run - '--version' and
# '--help' included. Imported here without that look, unless the user has set
# it, the commands that run git say when it is missing. The setting is not left
# for the programs a command starts
_GIT_REFRESH = "GIT_PYTHON_REFRESH"
_git_refresh_set = _GIT_REFRESH in os.environ
os.environ.setdefault(_GIT_REFRESH, "quiet")
try:
    import git  # noqa: F401, E402
finally:
    if not _git_refresh_set:
        del os.environ[_GIT_REFRESH]
