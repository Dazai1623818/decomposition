# Stage 1: Build the application
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Copy Gradle files
COPY gradlew build.gradle settings.gradle gradle.properties ./
COPY gradle gradle

# Copy source and libs
COPY src src
COPY lib lib
COPY libs libs

# Grant execution rights on gradlew
RUN chmod +x gradlew

# Build the application
# Note: If dependencies (like cpq-native-index) are not in Maven Central,
# they must be placed in the 'libs' directory before building.
RUN ./gradlew installDist --no-daemon

# Stage 2: Run the application
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

# Install micromamba and recreate the default conda environment
ENV MAMBA_ROOT_PREFIX=/opt/conda \
    PATH=/opt/conda/bin:$PATH
COPY environment-default.yml /tmp/environment-default.yml
RUN apt-get update && apt-get install -y curl bzip2 ca-certificates && rm -rf /var/lib/apt/lists/* && \
    curl -L https://micro.mamba.pm/api/micromamba/linux-64/latest -o /usr/local/bin/micromamba && \
    chmod +x /usr/local/bin/micromamba && \
    micromamba create -y -n default -f /tmp/environment-default.yml && \
    micromamba clean -ay
ENV CONDA_DEFAULT_ENV=default \
    CONDA_PREFIX=/opt/conda/envs/default \
    PATH=/opt/conda/envs/default/bin:$PATH

# Copy the installed application from the build stage
COPY --from=build /app/build/install/decomposition /app

# Ensure the native library is found.
# The distribution config in build.gradle puts 'lib/libnauty.so' into '/app/lib/libnauty.so'.
ENV LD_LIBRARY_PATH=/app/lib:$LD_LIBRARY_PATH

# Entrypoint
ENTRYPOINT ["/app/bin/decomposition"]
