HerokuPostgres-S3-Backup
=================

*Early Stage Software - unstable and unrefined... for now!*

Download a Heroku Postgres backup and upload to Amazon S3

##Usage
Install Requirements

```bash
$ pip install -r requirements.txt

```

View help

```bash
$ herokupostgres_s3_backup.py --help

```

To get the URL of your pgbackups, use the Heroku Toolbelt.
```bash
$ heroku pgbackups:url --app myAppName
"https://some-pgbackups-url.com/"
```
This URL will then be part of the script parameters.

##Contributing
Pull requests and suggestions welcome!
