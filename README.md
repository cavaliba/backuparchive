backuparchive - cavaliba.com
=============================

(c) cavaliba.com 2020-2022  - Version 1.0 - 2022/09/25

Manage archive rotatation and backup policy (yearly, weekly ...)


Setup

    * download python code from GIT in /opt/backuparchive/ ; virtualenv, prerequisite 
    * or download binary from https://www.cavaliba.com/download
    * and copy to /usr/local/bin/backuparchive (chown root: , chmod 755)


Cron

    20 21 * * * /usr/local/bin/backuparchive -c /opt/backuparchive/conf.yml
