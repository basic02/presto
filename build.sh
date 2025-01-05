./mvnw clean install -DskipTests -pl :presto-cache -am
#./mvnw clean install -DskipTests -pl :presto-hive-hadoop2 -am  -s ~/.m2/settings.xml.aliyun
#./mvnw clean install -DskipTests -rf :presto-hive-hadoop2   -s ~/.m2/settings.xml.aliyun
scp presto-cache/target/presto-cache-0.275.jar root@ccycloud-1.prestopoc.root.comops.site:/opt/presto/plugin/hive-hadoop2/presto-cache-0.275.jar
