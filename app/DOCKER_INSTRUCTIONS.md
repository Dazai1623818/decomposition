# Docker Setup Instructions

This repository is set up to run with Docker. However, due to external dependencies that are not in Maven Central, you need to manually provide the required JAR files.

## Prerequisites

1.  **Docker** and **Docker Compose** installed on your server.
2.  **Dependency JARs**: You need to obtain the following JAR file (as it is not in Maven Central):
    *   `cpq-native-index-1.0.jar`
    *   (Note: `gmark` is available on Maven Central, so you don't need to provide it manually.)

## Setup Steps

1.  **Clone the repository**:
    ```bash
    git clone <repo-url>
    cd decomposition
    ```

2.  **Add Dependencies**:
    Copy the `cpq-native-index` JAR file into the `libs/` directory in the project root.
    ```bash
    cp /path/to/cpq-native-index-1.0.jar libs/
    ```

3.  **Build and Run**:
    You can use Docker Compose to build and run the application.
    ```bash
    docker compose up --build
    ```

    Or build manually:
    ```bash
    docker build -t decomposition-app .
    docker run -it -v $(pwd)/graphs:/app/graphs decomposition-app
    ```

## Notes

*   **Native Library**: The application uses `libnauty.so`. A version is included in `lib/` and configured to be used in the Docker container. Ensure this library is compatible with the container's architecture (x86_64 Linux).
*   **Graphs**: The `graphs/` directory is mounted to `/app/graphs` in the container, so you can access your graph files.
