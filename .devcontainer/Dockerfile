FROM bitnami/kubectl:1.25.6 as kubectl


FROM mcr.microsoft.com/devcontainers/python:0-3.9
COPY --from=kubectl /opt/bitnami/kubectl/bin/kubectl /usr/local/bin/