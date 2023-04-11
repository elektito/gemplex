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

## Database Backup

You can use the provided `backup.sh` script to create daily, rotating backups.
You can, for example, put the script in `/etc/cron.daily`, or run it using
any cron-like facility.

NOTE: If you intend to copy the file to directories like `/etc/cron.daily`, make
sure you remove the `.sh` extension, since the `run-parts` program used to run
the scripts in these directories, does not seem to like files with a dot in
their names.

The script uses the following environment variables (if present):

 - `DB_NAME`: The name of the postgresql database to backup. Defaults to
   `gemplex`.
 - `MAX_BACKUPS`: The maximum number of backups to keep. Defaults to 7. Oldest
   backups are deleted if we reach this number.
 - `BACKUP_PREFIX`: The prefix for backup file names. Defaults to `gemplex-`.
 - `BACKUP_SUFFIX`: The suffix for backup file names. Defaults to `.dump`.
 - `DEST_DIR`: The directory to store backups in. Defaults to
   `/var/lib/gemplex/backups`.

In order to restore from a backup file, you can use the `pg_restore` command.
For example:

``` sh
$ createdb newdb
$ pg_restore -d newdb /var/lib/backups/gemplex-2023-01-02.dump
```

This example, restores the backup to a newly created database named `newdb`.

[1]: https://github.com/golang-migrate/migrate/releases/
