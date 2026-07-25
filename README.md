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

`KINEIS_update_bulk()` performs the historical backfill. It requests sensors
and Doppler together for all authorized devices and updates both tables from
each page. The bulk-count endpoint keeps dense historical windows bounded.

```text
obtain or renew JWT
    ↓
read active bulk window and cursor from MariaDB
    ↓
count all-device messages in the window
    ↓
halve the window when it exceeds the target size
    ↓
request one all-device page with sensors and Doppler
    ↓
prepare sensor and Doppler rows
    ↓
write both outputs and the next cursor in one transaction
    ↓
read pageInfo$endCursor
    ↓
request next cursor
    ↺ while pageInfo$hasNextPage is true
    ↓
mark the bounded window complete
    ↓
advance to the next window
```


Each Kinéis page and its cursor are committed before the next cursor is
requested. The first run starts at 2000-01-01 unless `bulk_progress` already
contains a checkpoint. Empty windows are advanced without a data request. If
rate limiting or a temporary API failure remains active after automatic
retries, `KINEIS_update_bulk()` returns normally with `status = "deferred"`.
The next scheduled run resumes the exact window and cursor. API calls are paced
and expired JWTs are renewed automatically.
