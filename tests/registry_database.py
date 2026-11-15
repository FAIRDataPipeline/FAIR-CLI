"""A local registry's database made for tests, without installing a registry"""

import pathlib
import sqlite3
import typing

import fair.registry.database as fdp_db

# The tables of a registry's database that these tests need, as Django makes
# them, and one of Django's own
SCHEMA = [
    'CREATE TABLE "custom_user_user" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "username" varchar(150) NOT NULL UNIQUE)',
    'CREATE TABLE "data_management_author" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "name" varchar(1024) NULL, "identifier" varchar(255) NULL)',
    'CREATE TABLE "data_management_userauthor" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "author_id" bigint NOT NULL REFERENCES "data_management_author" '
    '("id"), "user_id" bigint NOT NULL REFERENCES "custom_user_user" ("id"))',
    'CREATE TABLE "data_management_filetype" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "name" varchar(64) NOT NULL, "extension" varchar(64) NOT NULL)',
    'CREATE TABLE "data_management_namespace" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "name" varchar(1024) NOT NULL UNIQUE)',
    'CREATE TABLE "data_management_storageroot" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "root" varchar(1024) NOT NULL, "local" bool NOT NULL)',
    'CREATE TABLE "data_management_storagelocation" ("id" integer NOT NULL PRIMARY '
    'KEY AUTOINCREMENT, "path" text NOT NULL, "storage_root_id" bigint NOT NULL '
    'REFERENCES "data_management_storageroot" ("id"))',
    'CREATE TABLE "data_management_object" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "description" text NULL, "storage_location_id" bigint NULL '
    'REFERENCES "data_management_storagelocation" ("id"))',
    'CREATE INDEX "data_management_object_storage_location_id_9f2e" ON '
    '"data_management_object" ("storage_location_id")',
    'CREATE TABLE "django_migrations" ("id" integer NOT NULL PRIMARY KEY '
    'AUTOINCREMENT, "app" varchar(255) NOT NULL, "name" varchar(255) NOT NULL)',
]

# What an install and 'fair init' put in it
FRESH = [
    "INSERT INTO custom_user_user (username) VALUES ('admin')",
    "INSERT INTO data_management_author (name) VALUES ('Interface Test')",
    "INSERT INTO data_management_userauthor (author_id, user_id) VALUES (1, 1)",
    "INSERT INTO data_management_filetype (name, extension) VALUES ('csv', 'csv')",
    "INSERT INTO django_migrations (app, name) VALUES ('data_management', '0001')",
]


def make_install(
    registry_dir: pathlib.Path,
    schema: typing.List[str] = SCHEMA,
    rows: typing.List[str] = FRESH,
) -> str:
    """Make an install's directory with a database built from statements"""
    registry_dir.mkdir(parents=True)
    _db = sqlite3.connect(registry_dir.joinpath(fdp_db.DATABASE_FILE))
    for _statement in schema + rows:
        _db.execute(_statement)
    _db.commit()
    _db.close()
    return str(registry_dir)
