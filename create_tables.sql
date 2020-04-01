-- in database mast

--drop schema "entities" cascade;
create schema "entities";

set search_path = "entities";

/*
 * database 
 */

drop table "database" cascade;
create table "database" (
  database_id char(20),
  database_name text,
  note text,
  name_search tsvector,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp default null,
  constraint entities_database_pk primary key (database_id)
);

/* -- don't need this for just names
create function database_tsvector_fn() returns trigger as $body$
begin
	new.name_search := to_tsvector(new.database_name);
	return new;
end;
$body$ language plpgsql;

create trigger database_tsvector_tr before insert or update on "entities"."database"
	for each row execute function database_tsvector_fn();
*/

create or replace function put_database(
	 _database_id char(20)
    ,_database_name text
    ,_note text default null
    ,_created_at timestamp default now()
    ,_updated_at timestamp default now()
    )
  returns table (
    database_id char(20),
    database_name text,
    note text,
    name_search tsvector,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
	return query insert into "entities"."database" as d (database_id, database_name, note, created_at, updated_at) 
		values (_database_id, _database_name, _note, _created_at, _updated_at)
		on conflict on constraint entities_database_pk do update 
		set 
			database_name = _database_name
			,note = _note
			,updated_at = now()
		where d.database_id = _database_id
		returning *;
end;
$$ language 'plpgsql';

create or replace function get_database(
	 _database_id char(20)
	)
  returns table (
    database_id char(20),
    database_name text,
    note text,
    name_search tsvector,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	select d.database_id, d.database_name, d.note, d.created_at, d.updated_at, d.deleted_at
  	from "entities"."database" d
  	where d.database_id = _database_id;
end;
$$ language 'plpgsql';

create or replace function del_database(
	 _database_id char(20)
	)
  returns table (
    database_id char(20),
    database_name text,
    note text,
    name_search tsvector,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	update "entities"."database" d set deleted_at = now()
  	where d.database_id = _database_id
  	returning *;
end;
$$ language 'plpgsql';

/*
 * table 
 */

create table "table" (
  table_id char(20),
  database_id char(20),
  table_name text,
  schema_name text,
  note text,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp null,
  constraint entities_table_pk primary key (table_id),
  constraint entities_table_database_fk foreign key (database_id) references "entities"."database" (database_id)
);


create or replace function put_table(
	 _table_id char(20)
	,_database_id char(20)
    ,_table_name text
    ,_schema_name text default null
    ,_note text default null
    ,_created_at timestamp default now()
    ,_updated_at timestamp default now()
    )
  returns table (
    table_id char(20),
  	database_id char(20),
    table_name text,
    schema_name text,
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
	return query insert into "entities"."table" as t ("table_id", "database_id", "table_name", "schema_name", "note", "created_at", "updated_at") 
		values (_table_id, _database_id, _table_name, _schema_name, _note, _created_at, _updated_at)
		on conflict on constraint entities_table_pk do update 
		set 
			"database_id" = _database_id
			,"schema_name" = _schema_name
			,"table_name" = _table_name
			,"note" = _note
			,"updated_at" = now()
		where t."table_id" = _table_id
		returning *;
end;
$$ language 'plpgsql';

create or replace function get_table(
	 _table_id char(20)
	)
  returns table (
    table_id char(20),
  	database_id char(20),
    table_name text,
    schema_name text,
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	select t.table_id, t.database_id, t.table_name, t.schema_name, t.note, t.created_at, t.updated_at, t.deleted_at
  	from "entities"."table" t
  	where t.table_id = _table_id;
end;
$$ language 'plpgsql';

create or replace function del_table(
	 _table_id char(20)
	)
  returns table (
    table_id char(20),
  	database_id char(20),
    table_name text,
    schema_name text,
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	update "entities"."table" as t set deleted_at = now()
  	where t.table_id = _table_id
  	returning *;
end;
$$ language 'plpgsql';

/*
 * field 
 */

--drop table "field";
create table "field" (
  field_id char(20),
  --database_id char(20),
  table_id char(20),
  field_name text,
  data_type varchar(50),
  note text,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp null,
  constraint entities_field_pk primary key (field_id),
  --constraint entities_field_database_fk foreign key (database_id) references "entities"."database" (database_id),
  constraint entities_field_table_fk foreign key (table_id) references "entities"."table" (table_id)
);

create or replace function put_field(
	 _field_id char(20)
	,_table_id char(20)
    ,_field_name text
    ,_data_type varchar(50)
    ,_note text default null
    ,_created_at timestamp default now()
    ,_updated_at timestamp default now()
    )
  returns table (
    field_id char(20),
  	table_id char(20),
    field_name text,
    data_type varchar(50),
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
	return query insert into "entities"."field" as f ("field_id", "table_id", "field_name", "data_type", "note", "created_at", "updated_at") 
		values (_field_id, _table_id, _field_name, _data_type, _note, _created_at, _updated_at)
		on conflict on constraint entities_field_pk do update 
		set 
			"table_id" = _table_id
			,"field_name" = _field_name
			,"data_type" = _data_type
			,"note" = _note
			,"updated_at" = now()
		where f."field_id" = _field_id
		returning *;
end;
$$ language 'plpgsql';

create or replace function get_field(
	 _field_id char(20)
	)
  returns table (
    field_id char(20),
  	table_id char(20),
    field_name text,
    data_type varchar(50),
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	select f.field_id, f.table_id, f.field_name, f.data_type, f.note, f.created_at, f.updated_at, f.deleted_at
  	from "entities"."field" f
  	where f.field_id = _field_id;
end;
$$ language 'plpgsql';

create or replace function del_field(
	 _field_id char(20)
	)
  returns table (
    field_id char(20),
  	table_id char(20),
    field_name text,
    data_type varchar(50),
    note text,
    created_at timestamp,
    updated_at timestamp,
    deleted_at timestamp
  )
as $$
begin
  return query 
  	update "entities"."field" as f set deleted_at = now()
  	where f.field_id = _field_id
  	returning *;
end;
$$ language 'plpgsql';

/*
 * field_transform 
 */

-- TODO: add versioning to these records
create table "entities"."field_transform" (
  field_transform_id char(20) primary key,
  field_id char(20),
  transform bytea,
  created_at timestamp,
  updated_at timestamp,
  deleted_at timestamp,
  foreign key (field_id) references "entities"."field" (field_id)
);
