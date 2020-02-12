-- in database mast

create schema "entities";

create table "entities"."database" (
  id serial primary key,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp,
  database_id text constraint database_database_id_unique unique,
  database_name text,
  note text
);

create table "entities"."table" (
  id serial primary key,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp,
  database_id text,
  database_name text,
  schema_name text,
  table_id text constraint entities_table_table_id_unique unique,
  table_name text,
  note text
);

create table "entities"."field" (
  id serial primary key,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp,
  database_id text,
  database_name text,
  schema_name text,
  table_id text,
  table_name text,
  field_id text constraint entities_field_field_id_unique unique,
  field_name text,
  note text
);

-- TODO: add versioning to these records
create table "entities"."field_transform" (
  id serial primary key,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp,
  field_id text,
  transform_id text,
  transform bytea
);
