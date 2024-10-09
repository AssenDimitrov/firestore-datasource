FROM grafana/grafana
RUN mkdir /var/lib/grafana/plugins/custom-firestore-datasource
ADD ./dist/* /var/lib/grafana/plugins/custom-firestore-datasource/
ADD ./dist/* /opt/

ENTRYPOINT [ "/run.sh" ]
