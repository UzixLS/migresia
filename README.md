Migresia
========

A simple Erlang tool to automatically migrate Mnesia databases between versions. It's loosely based on Active Record Migrations from Ruby on Rails.

Multiple nodes are supported. Function check/1 will return the list of migrations still to be run so long as Mnesia knows itself to be consistent. Function migrate/1 will perform migrations across distributed nodes provided all of them are up at the time it is called; it will return an error if any nodes are down.

## How it works

When migrating the database:

1. List all available migrations.
2. Check which migrations haven't yet been applied.
3. Load required migrations.
4. For each loaded migration:

* Execute the `up` / `down`\* function as required to bring the database to the desired version.
* Mark the migration as applied if it has been executed successfully.

\* - migrating the database backward hasn't yet been implemented.

Migresia stores all applied migrations in table `schema_migrations` which it creates in case it doesn't yet exist.

## Migrations

Each migration is an Erlang `*.beam` module stored in `ebin/` folder, under a given application, or in a custom folder which you can pass to migresia api as argument.

#### Migration names

The name of each migration starts with a `db_` prefix followed by timestamp, which is treated as its version, and ends with a short information of the purpose of that migration. For example:

    20130731163300_convert_permissions.beam

The timestamp is always 14 numbers, but otherwise can contain any value. Migresia doesn't interpret it in any way, but it uses it to sort the migrations according to the value of that timestamp, assuming timestamps with the lower values are younger.

The remaining part of the name, after the timestamp, is simply ignored. A migration name consisting of just the timestamp is also a valid name.

You can generate a timestamped stub using `./priv/new_migration.esh test_message`

#### Implementing migrations

Migresia doesn't provide any special support for transactions. If any operations should be executed within a transaction, it just should be created and handled accordingly in the `up` or `down` functions.

Migresia provides a behaviour which all migrations should implement: `db_migration`. It expects two functions: `up/0` and `down/0`. Please note, that at this moment migrations can't be applied backward, so the `down/0` function is unused. However, both functions are present for completeness as it is expected that in the future Migresia will support migrating databases in both directions.

## API Calls

Migresia exports 3 functions. All functions take an first parameter of an application name (as an atom) in which scope migrations will be searched.

##### `migresia:check/1`

This function does two things:

* Loads all unapplied migrations.
* Returns a list of all unapplied migrations, or a reason why it was unable to get this list.

This is the method that should be used to verify which migrations will be applied if `migresia:migrate/1` is executed to migrate the database.

##### `migresia:migrate/1` 

Works almost exactly like `migresia:check/1` but in the end, instead of returning information which migrations are going to be applied, just applies them by executing the required `up` or `down` functions.

##### `migresia:list_disc_copy_nodes/0`

Returns the output of `mnesia:table_info(schema, disc_copies)` which could be used in migrations to migrate databases on multiple nodes.
