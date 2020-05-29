FROM maven:3.6.3-jdk-8

WORKDIR /src

COPY . /src/

EXPOSE 8080
EXPOSE 5005

CMD mvn package && java -jar -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005 /src/kestrel-example/target/kestrel-example-0-SNAPSHOT-jar-with-dependencies.jar