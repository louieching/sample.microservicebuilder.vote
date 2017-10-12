FROM mycluster.icp:8500/default/websphere-liberty:latest
COPY server.xml /config/server.xml
RUN installUtility install --acceptLicense defaultServer
COPY target/microservice-vote-1.0.0-SNAPSHOT.war /config/apps/vote.war
