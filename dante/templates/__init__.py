import jinja2
import os

templates_dir = os.path.dirname(__file__)

config_template = jinja2.Template(
    open(os.path.join(templates_dir, "config.jinja")).read()
)
