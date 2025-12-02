# Stage 1: Build the application
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /app

# Install Python for helper scripts used during build/dev
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

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

# Stage 2: Run the application / dev container (JDK so Gradle can compile inside)
FROM eclipse-temurin:21-jdk-jammy
WORKDIR /app
ARG USER_ID=1012
ARG GROUP_ID=1012
ENV USERNAME=appuser

# Install Python for running helper scripts like scripts/generate.py
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python-is-python3 \
    && rm -rf /var/lib/apt/lists/*

# Create a non-root user to match the host UID/GID for mounted files
RUN groupadd -g ${GROUP_ID} ${USERNAME} \
    && useradd -m -u ${USER_ID} -g ${USERNAME} -s /bin/bash ${USERNAME}

# Copy the installed application from the build stage
COPY --from=build /app/build/install/decomposition /app

# Include helper scripts
COPY scripts scripts

# Ensure the native library is found.
# The distribution config in build.gradle puts 'lib/libnauty.so' into '/app/lib/libnauty.so'.
ENV LD_LIBRARY_PATH=/app/lib:$LD_LIBRARY_PATH

# Drop privileges by default
USER ${USERNAME}

# Entrypoint
ENTRYPOINT ["/app/bin/decomposition"]
