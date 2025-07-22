    FROM bitnami/spark:3.4

    # Install any extra dependencies here if needed
    USER root
    RUN apt-get update && apt-get install -y curl

    # Copy jobs into image
    COPY jobs/ /opt/spark/jobs/
    COPY submit_to_adls.sh /opt/spark/submit_to_adls.sh
    RUN chmod +x /opt/spark/submit_to_adls.sh

    CMD ["/opt/spark/submit_to_adls.sh"]
