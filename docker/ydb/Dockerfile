FROM ydbplatform/local-ydb:latest

# Set environment variables for YDB configuration
ENV YDB_GRPC_ENABLE_TLS=0
ENV MON_PORT=8765
ENV GRPC_PORT=2136
ENV GRPC_TLS_PORT=2135
ENV YDB_KAFKA_PROXY_PORT=9092

# Expose the necessary ports
EXPOSE 2135 2136 8765 9092
