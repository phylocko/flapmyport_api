# What is this?

This is a daemon that provides API for [FlapMyPort](https://flapmyport.com) monitoring system clients. It use the same database as [snmpflapd](http://github.com/phylocko/snmpflapd) does. In other words, **flapmyport_api** shows data that **snmpflapd** collects.


# What do you need to deploy it?
- [snmpflapd](http://github.com/phylocko/snmpflapd) running and set up

# Quick start #

## 1. Create a config file

**settings.conf:**
```
ListenAddress = "0.0.0.0"
ListenPort = 8080
DBHost = "localhost"
DBName = "traps"
DBUser = "flapmyport"
DBPassword = "flapmyport"
LogFilename = "flapmyport_api.log"
```

> settings.conf is optional. You may use environment variables instead.
> Available environment variables are
> LISTEN_ADDRESS, LISTEN_PORT, DBHOST, DBNAME, DBUSER, DBPASSWORD

`DBHost` and `DBName` must be the same as in **snmpflapd**'s settings.py.

## 2. Run flapmyport API
```
> ./flapmyport_api -f settings.py
```

# How to build #

Use `build.sh` instead of `go build`!

If you wish to make a build for a Linux 64-bit machine:

```
GOOS=linux GOARCH=amd64 ./build.sh
```

---
*And may a stable network be with you!*
