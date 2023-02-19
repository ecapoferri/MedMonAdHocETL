
# Media Monitors 2022 All Market Analysis, Sales Lead Identification

## Abstract

The resulting analysis is to answer these questions:

- Which companies purchased advertising on cable television outlets but NOT on
  broadcast television outlets.
- Which companies purchased advertising on broadcast television outlets
  comprising less than 20% of their total yearly ad spend on all media types.
- Which companies are advertising only within a single market.

The intended use of the output table is to identify companies falling in to some
combination of those classifications as target leads for local broadcast
television sales calls.

The source files were provided to me by a manager who assigned the project. (A
better approach would have been to extract the data from Media Monitors' API,
but these source files were ready to go as this project was assigned and this
manager preferred to access the Media Monitors platform himself.)

In a normal work environment, for an adhoc project, I would first attempt to
just do a quick adhoc ETL within the production platform. However, with this
dataset structured as it is, applications (such as Excel) buckle under the
weight of so much file parsing, string joins, etc.

For data in this sort of state, I would most likely clean the data with in a
Jupyter notebook and load to a local DB, cloud DB, or portable file DB (e.g.
SQLite) for use in production. Basic usage of the final XLSX file does not
really even require an open connection to the production dataset as the data is
contained in the Power Pivot Data Model within the file, and, as it is a static
dataset, does not require refresh unless the production dataset is restructured.

This example is, obviously, overly engineered and overly documented for the task
at hand. I took it as an opportunity to hone my understanding of a few features
and rework/clean up some platforms and features I'd used in other contexts to
make a somewhat portable, working demonstration of this particular solution.

## Source Data

---

### Data (Fact Table Source) - `bind/source_data/MediaMonitors-Details/*.xlsx`

- Supplied by Media Monitors, tracks media spending by a wide array of companies
  using proprietary techniques. As this is proprietary data licensed for private
  use, ***this underlying source or resulting analysis is not to be shared or
  used outside of this demonstration***.

---

### Lookup Keys

#### Outlet Key - `bind/source_data/MediaMonitors-OutletKey.xlsx`

- Manually prepared, using Excel features, from in a separate xlsx using unique
  outlet names in the source data.
- Outlet types can be determined by the name format:
  - If formatted as a call sign XXXX\[-X\[X\]\]): Lead character and suffixes
    determine the outlet type.
  - Remaining, natural word based outlet names can be verified by web search.

#### Category Key - `bind/source_data/MediaMonitors-CategoryKey.xlsx`

- Manually prepared; An arbitrary grouping of categories from the source data
  used to simplify filtering of target clients.

---

## Functional Outline

Upon activating the docker compose stack:

- A container service ('pg') is launched with a PostgresSQL server running on
  port 5435 on the host machine. The standard postgres:postgres
  username:password combination are used and a db, 'medmon' is created as a
  local sink from which to query data for production analysis. A volume is
  created to persist the db data for future use on the host machine.
- A second container service ('script-worker'):
  - runs to execute the 'ETL' script and loads data to the 'pg/medmon' db.
  - then runs a series of SQL queries to create production ready tables within
    the 'pg/medmon' db.
- A connector must be established on the host machine via ODBC or PostgreSQL
  connector in the production environment and Analysis can be conducted
  thereon/therein.

[The 'pg' service can be stopped and started as needed](#relaunch-the-local-db).

## Use

### Requirements

The host machine must have Docker Desktop and Docker Compose installed.

The host machine must have Excel installed within a MS Office 365 instance.

Data source files must be provided from the author: [Evan Capoferri:
ecapoferri@gmail.com]("mailto:Evan%20Capoferri<ecapoferri@gmail.com?subject=Portfolio%3A%20Media%20Monitors>")
(available upon request)

The host production environment must be able to connect to the PostgreSQL
cluster via ODBC or other PostgreSQL connector.

### Initial Launch

1. Clone the source directory onto the host machine: [`git clone
  https://github.com/ecapoferri/MedMonAdHocETL`](https://github.com/ecapoferri/MedMonAdHocETL)
1. Add the source data (available upon request, [see above](#requirements)). Add
  to the repo directory in `bind/source_data/` e.g.:
    - `MedMonAdHocETL/bind/source_data/MediaMonitors-Details/AdExpenditure-All_LOCAL_Markets-01`...,
      `02`..., *`.xlsx`
    - `MedMonAdHocETL/bind/source_data/MediaMonitors-CategoryKey.xlsx`
    - `MedMonAdHocETL/bind/source_data/MediaMonitors-OutletKey.xlsx`
      - \[Files will be provided as a directory structured tar ball or other
      archive for quick insertion.\]
1. Run `$ docker compose up` within the cloned directory at the top level
   (`MedMonAdHocETL/`).

### Relaunch the local DB

If necessary, relaunch the 'pg' database service container via your preferred Docker
interface: CLI, UI, or, within the top docker compose build context
(`MedMonAdHocETL/`), using Docker Compose: `$ docker compose run pg`, provided
the local Docker Volume has not been deleted.

## License (None)

This code repository and any accompanying data is provided only for private
demonstration and should not be otherwise distributed.
