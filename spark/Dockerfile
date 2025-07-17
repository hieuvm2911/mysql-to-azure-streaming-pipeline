    FROM bitnami/spark:3.4

    # Install any extra dependencies here if needed
    USER root
    RUN apt-get update && apt-get install -y curl

    # Copy jobs into image
    COPY jobs/ /opt/spark/jobs/
    COPY submit.sh /opt/spark/submit.sh
    RUN chmod +x /opt/spark/submit.sh

    CMD ["/opt/spark/submit.sh"]
