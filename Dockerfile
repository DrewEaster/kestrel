FROM maven

WORKDIR /src

COPY . /src/

EXPOSE 8080

CMD mvn package && java -jar /src/kestrel-example/target/kestrel-example-0-SNAPSHOT-jar-with-dependencies.jar