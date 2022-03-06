# questdb dremio connector

> note without  schema auth discovery

## Usage

### prepare
- 1 install dremio

docker run --name dremio -p 9047:9047 -p 31010:31010 -v D:\docker-repo\dremio\jars:/tmp -p 45678:45678 dremio/dremio-oss


root权限登录

docker exec -it --user root <container id> /bin/bash


- 2 install questdb

docker run --name questdb -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 -v D:\docker-repo\questdb:/root/.questdb questdb/questdb

-p parameter#
```
This parameter will publish a port to the host, you can specify:

-p 9000:9000 - REST API and Web Console
-p 9009:9009 - InfluxDB line protocol
-p 8812:8812 - Postgres wire protocol
-p 9003:9003 - Min health server
```



- 3 create table in questdb

create TABLE demoapp (
id int,
name STRING
)

insert into demoapp  values(1,'a'),(2,'b')

select * from demoapp


* build

```cpde
mvn clean package
```

* copy jars

```code
copy target/questdb-driver-20.0.0-202201050826310141-8cc7162b.jar to dremio jars dir
```


### 参考资料

[how-to-create-an-arp-connector](https://www.dremio.com/tutorials/how-to-create-an-arp-connector)
[questdb](https://questdb.io/docs/develop/connect)
