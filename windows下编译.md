# Windows 下编译代码

在FLink目录下，执行
mvn clean install -DskipTests -Dfast -T 6 -Dmaven.compile.fork=true  -Dscala-2.11

编译完成后，这个目录就是我们的二进制包
flink/flink-dist/target/flink-1.12-SNAPSHOT-bin

如果要打tar包，就
mvn package -DskipTests -Dfast -T 6
