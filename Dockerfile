FROM openjdk

WORKDIR /home/app

COPY ./target/scala*/*-assembly*.jar ./app.jar

ARG JAVA_OPTS="-Xms128m -Xmx384m"
ENV JAVA_OPTS=$JAVA_OPTS

CMD java -jar $JAVA_OPTS app.jar