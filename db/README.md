In order to run database migrations, you need to install the migrate program.
You can download and install it from their [releases][1] page on github.

In order to create a migration, run:

``` sh
migrate create -ext sql -dir db/migrations -seq <migration_name>
```

This will create a two `.sql` files with a sequential counter added to the given
name: one for "up" migration, and one for "down" migration.

Edit the files as you wish. Then, in order to run the migrations use this
command:

``` sh
migrate -database <POSTGRES_DB_URL> -path db/migrations up
```

[1]: https://github.com/golang-migrate/migrate/releases/
