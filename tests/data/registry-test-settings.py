from .base_settings import *
import os

MIDDLEWARE += ['whitenoise.middleware.WhiteNoiseMiddleware', ]

STATICFILES_STORAGE = 'whitenoise.storage.CompressedStaticFilesStorage'
ALLOWED_HOSTS.extend(
    filter(None, os.environ.get("FAIR_ALLOWED_HOSTS", "").split(","))
)

BUCKETS = {
    'default': {
       'url' : 'http://127.0.0.1:3005/',
       'bucket_name': 'fair',
       'access_key': 'AccessKey',
       'secret_key': 'SecretKey',
       'duration': '60'
    }
}

REMOTE = True
DOMAIN_URL = 'http://127.0.0.1:8001/'
DEBUG = True