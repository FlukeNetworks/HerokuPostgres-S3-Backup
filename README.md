HerokuPostgres-S3-Backup
=================

*Early Stage Software - unstable and unrefined... for now!*

Download a Heroku Postgres backup and upload to Amazon S3

##Usage
Install Requirements

```bash
$ pip install -r requirements.txt

```

Ensure that the Heroku toolbelt is installed and you are logged in.
```bash
$ heroku login
```

View help

```bash
$ herokupostgres_s3_backup.py --help

```

To get the URL of your pgbackups, The program uses the Heroku Toolbelt.
```bash
$ heroku pgbackups:url --app myAppName
"https://some-pgbackups-url.com/"
```
This must be done because the output of pgbackups:url changes as new backups are created.
The Heroku toolbelt is used because there is no standard api to fetch PG backups.

##Contributing
Pull requests and suggestions welcome!
