from .base_settings import *
import os

MIDDLEWARE += ['whitenoise.middleware.WhiteNoiseMiddleware', ]

STATICFILES_STORAGE = 'whitenoise.storage.CompressedStaticFilesStorage'
ALLOWED_HOSTS.extend(
    filter(None, os.environ.get("FAIR_ALLOWED_HOSTS", "").split(","))
)
DOMAIN_URL = 'http://127.0.0.1:8001/'
DEBUG = True
CONFIG_LOCATION = os.environ["FAIR_CONFIG"]