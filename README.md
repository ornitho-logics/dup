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

`KINEIS_update()` is the only Kinéis database pipeline. It requests sensors and
Doppler together for all authorized devices and writes both tables from each
page. It runs once per day and processes fixed one-day windows.

```text
obtain or renew JWT
    ↓
read active one-day window and cursor from MariaDB
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
requested. On an empty database, `KINEIS_update()` starts at 2024-01-01 by
default; use `KINEIS_update(start_date = "YYYY-MM-DD")` to choose another
initial date. Once created, the stored checkpoint controls subsequent runs.
If rate limiting or a temporary API failure remains active after automatic
retries, the update returns normally with `status = "deferred"`. The next
daily run resumes the exact window and cursor. API calls are paced and expired
JWTs are renewed automatically.

## GM Movebank pipeline

`GM_update()` downloads all GPS events including
events outside a defined deployment. It stores only the GM measurements in the
`GM.locations` table.

```text
read the latest location timestamp from MariaDB
    ↓
start two days before the watermark
    ↓
request one day of raw Movebank GPS events
    ↓
discard deployment and other reference metadata
    ↓
write through a temporary stage into GM.locations
    ↓
request the next one-day window
    ↺ until the current time is reached
```
