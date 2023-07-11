#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
FAIR-CLI Templates
==================

Contains methods for assembling templates towards creation of configurations
and user information displays.
"""

__date__ = "2021-06-24"

import os

import jinja2

templates_dir = os.path.dirname(__file__)

config_template = jinja2.Template(
    open(os.path.join(templates_dir, "config.jinja"), encoding='utf-8').read()
)

hist_template = jinja2.Template(
    open(os.path.join(templates_dir, "hist.jinja"), encoding='utf-8').read()
)
