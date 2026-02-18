# load_batch - Spring Batch XML Loader

This mini app loads XML invoices in parallel, converts them to JSON, and persists them into MongoDB (collection `invoices`).

## Prerequisites
- Java 17
- Gradle
- MongoDB connection string

## Ubuntu Dependencies
Install dependencies to run both `mock_data.py` (mock XML generation) and `load_batch`:

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip openjdk-17-jdk ca-certificates
```

Check versions:

```bash
python3 --version
java -version
```

## Gradle Wrapper Setup (gradlew)
This project already includes Gradle Wrapper (`gradlew`), so no global Gradle install is required.

```bash
cd /Users/vagnerpontes/Documents/demos/load_xml_batch
chmod +x gradlew
./gradlew --version
```

## Compile / Build
Compile the code and build artifacts:

```bash
cd /Users/vagnerpontes/Documents/demos/load_xml_batch
APP_PATH=/Users/vagnerpontes/Documents/demos/load_xml_batch ./gradlew clean build -x test
```

## Setup
1. Edit `src/main/resources/application.yml`:
   - `load-batch.input-dir`: directory with XML invoices
   - `load-batch.threads`: number of parallel load threads
   - `load-batch.failed-dir`: where failed XML files are moved
   - `load-batch.error-log`: CSV file for failed items
   - `load-batch.summary-log`: CSV summary file for batch results
   - `spring.data.mongodb.uri`: MongoDB connection string

2. Ensure the input directory exists and contains `.xml` files.

## Run
```bash
cd /Users/vagnerpontes/Documents/demos/load_xml_batch
APP_PATH=/Users/vagnerpontes/Documents/demos/load_xml_batch ./gradlew bootRun
```

## Generate Mock XMLs
```bash
cd /Users/vagnerpontes/Documents/demos/load_xml_batch
python3 mock_data.py --count 1000 --due-date 2026-02-01 --output mock_invoices
```

## Logging & Retries
- Each file conversion is logged by filename.
- Failed items are retried up to 3 times.
- Items still failing after retries are skipped (up to 100).
- MongoDB inserts are logged.
- Failed XML files are moved to `failed-dir`.
- A CSV skip list is written to `error-log` with filename + error stage.
- A CSV summary is written to `summary-log` with read/write/skip counts.
