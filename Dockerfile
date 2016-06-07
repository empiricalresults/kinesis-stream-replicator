FROM empiricalresults/minimal-java8

ADD worker.sh /

ADD ./target/app-boot.jar /app.jar

CMD ["sh", "/worker.sh"]
