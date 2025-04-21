FROM mcr.microsoft.com/devcontainers/python:dev-3

# *************************************
#         SYSTEM DEPENDENCIES
# *************************************
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    fuse \
    gcc \
    git \
    pkg-config \
    sudo \
    unixodbc-dev

# *************************************
#         PYTHON PACKAGES
# *************************************
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# *************************************
#         USER SETUP
# *************************************
USER vscode

# *************************************
#          DEVCONTAINER SETUP
# *************************************
# Default folder for dev containers
WORKDIR /workspace

# Entrypoint
CMD ["python", "--version"]