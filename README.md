# dup

## Database unattended pipelines

Unattended scheduled pipelines to databases hosted on `scidb.mpio.orn.mpg.de`.

The API clients live in the `apis` package. `dup` reads database watermarks,
prepares API records for the destination schema, and writes them to MariaDB.

## Argos pipeline

`ARGOS2.pipeline()` retrieves each platform's recent Argos window. Argos returns
one SOAP/CSV response per platform request rather than cursor pages.

```text
authenticate and retrieve platform list
    ↓
read each platform watermark from MariaDB
    ↓
request one platform and recent time window
    ↓
parse the SOAP/CSV response
    ↓
prepare location and sensor rows
    ↓
append rows to ARGOS2 in MariaDB
    ↓
request next platform
```

## Ecotopia/DRUID pipeline

`DRUID_update()` updates the device list, GPS, ODBA, environmental, and raw
structured-behaviour tables. Each device and data layer is isolated so an
ordinary device error does not discard other devices.

```text
authenticate and update device list
    ↓
read device/layer watermark from MariaDB
    ↓
request telemetry page
    ↓
append raw page and select its timestamp cursor
    ↓
request next cursor
    ↺ until the API returns no more records
    ↓
prepare and deduplicate layer rows
    ↓
write through a temporary stage into DRUID in MariaDB
    ↓
request next device/layer
```

Existing devices restart two days before their latest stored timestamp.
MariaDB primary keys make this overlap idempotent.

## Kinéis pipeline

`KINEIS_update()` requests sensors and Doppler together, then updates both
tables from each page. Kinéis pagination occurs inside each device: a device
with many messages can require many bulk API requests.

```text
obtain or renew JWT
    ↓
read shared telemetry checkpoint from MariaDB
    ↓
request one page with sensors and Doppler in ascending msgDatetime order
    ↓
prepare sensor and Doppler rows
    ↓
write both outputs and advance telemetry_progress in one transaction
    ↓
read pageInfo$endCursor
    ↓
request next cursor
    ↺ while pageInfo$hasNextPage is true
    ↓
request next device
```

Each Kinéis page and its shared checkpoint are committed before the next cursor
is requested. The initial combined run starts from 2000 for every device so it
can recover historical Doppler rows that the former sensor-first pipeline never
reached. If rate limiting remains active after automatic retries,
`KINEIS_update()` stops making requests and returns normally with
`status = "deferred"`. The next scheduled run resumes from the latest persisted
page with a two-day overlap. API calls are paced and retried; expired JWTs are
renewed automatically.
