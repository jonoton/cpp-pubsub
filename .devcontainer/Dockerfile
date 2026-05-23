FROM mcr.microsoft.com/devcontainers/cpp:1-debian-12

# Install minimal additional tools if needed
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    cmake \
    build-essential \
    gdb \
    xclip \
    ruby-full \
    zlib1g-dev \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Install Jekyll and Bundler
RUN gem install jekyll bundler
