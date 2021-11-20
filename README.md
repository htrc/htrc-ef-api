# HTRC-ExtractedFeatures-API
An API for accessing the HTRC Extracted Features dataset

# Build
`sbt clean stage`
Then find the result in `target/universal/stage/`

# Deploy
Copy the folder `target/universal/stage/` to the deployment location and rename as desired (henceforth referred to as `DEPLOY_DIR`).

# Setup
0. Generate an application secret by running `sbt playGenerateSecret`
1. Set `EFAPI_MONGODB_URI` environment variable to point to the Mongo instance holding the EF data
2. Set `EFAPI_SECRET` environment variable to the value generated by step 0

(alternatively, these settings can also be configured by editing `target/universal/stage/conf/application.conf`)

# Run
*Note:* You must have the environment variables set before running (or edited the `application.conf` accordingly)
```bash
$DEPLOY_DIR/bin/htrc-extractedfeatures-api -Dhttp.address=HOST -Dhttp.port=PORT -Dplay.http.context=/api
```
where `HOST` is the desired hostname or IP to bind to, and `PORT` is the desired port to run on.

# API

WIP