FROM quay.io/ceph/ceph:v19

RUN mkdir -p /var/lib/ceph/bootstrap-osd && chown -R ceph:ceph /var/lib/ceph

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8080

ENTRYPOINT ["/entrypoint.sh"]
