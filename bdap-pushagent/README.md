
Usage: java -jar pushagent-VERSION.jar [Config_JSON_URL]

If provide the Config_JSON_URL, it will read the configuration from that, otherwise, it will read configuration from class-path:/default-config.json.

In the config file, the expression language is apache jexl: http://commons.apache.org/proper/commons-jexl/

Below is the example config:
```
{
  "test" : {
    "id" : "test",
    "directory" : "/Dev/log.analysis/bdap-schemagen/src/test",
    "elements" : [ {
      "name" : "FEMTO",
      "hostname" : "localhost",
      "ip" : "127.0.0.1"
    } ],
    "category" : "OM",
    "timeZone" : "CST",
    "cronExpr" : "0/1 * * * * ?",
    "recursive" : true,
    "filenameFilterExpr" : "new('org.apache.commons.io.filefilter.WildcardFileFilter', '*.csv')",
    "filesPerBatch" : 1,
    "processRecordFile" : "/tmp/process_record",
    "destServer" : "localhost",
    "destServerPort" : 22,
    "destServerUser" : "player",
    "destServerPrvKey" : "/tmp/login.ppk",
    "destServerPass" : "123456",
    "destServerDirRule" : "`/tmp/${new('java.util.Date')}/${WorkingElement.IP}`"
  }
}
```

For the config item - destServerPrvKey, if it's set, the sftp will login with key authentication, the destServerPass will be used as key pass-phrase.

For the config item - destServerDirRule, has below variable context:
* WorkingElement {Name, Hostname, IP}
* WorkingDir
* DestServer
