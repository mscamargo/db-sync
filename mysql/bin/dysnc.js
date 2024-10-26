import mysql from "mysql2";
import { Writable } from "node:stream";

const sourceConnection = mysql.createPool({
  host: "db_source",
  port: 3306,
  user: "root",
  password: "root",
  database: "employees",
  waitForConnections: true,
  connectionLimit: 100,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

let sourcePools = 0;

sourceConnection.on("acquire", () => {
  sourcePools++;
});
sourceConnection.on("release", () => {
  sourcePools--;
});

const targetConnection = mysql.createPool({
  host: "db_target",
  port: 3306,
  user: "root",
  password: "root",
  database: "target_db",
  waitForConnections: true,
  connectionLimit: 100,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

let targetPools = 0;

sourceConnection.on("acquire", () => {
  targetPools++;
});
sourceConnection.on("release", () => {
  targetPools--;
});

targetConnection.on("connection", function (conn) {
  conn.query("SET FOREIGN_KEY_CHECKS = 0");
});

async function getTableNames(conn) {
  const sql = "SHOW TABLES";
  const rows = await query(conn, sql);
  const tableNames = rows.map((row) => Object.values(row)[0]);
  return tableNames;
}

async function getTableDDL(conn, tableName) {
  const sql = `SHOW CREATE TABLE ${tableName}`;
  const rows = await query(conn, sql);
  const ddl = rows[0]["Create Table"];
  return ddl;
}

async function getTableColumns(conn, tableName) {
  const sql = `
        SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, EXTRA
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
    `;
  const rows = await query(conn, sql, ["beerorcoffee", tableName]);
  const columNames = rows.map((row) => row.COLUMN_NAME);
  return columNames;
}

async function query(conn, sql, values = []) {
  return new Promise((resolve, reject) => {
    if (values.length) {
      conn.execute(sql, values, (err, rows) => {
        if (err) {
          return reject(err);
        }
        resolve(rows);
      });
    } else {
      conn.query(sql, (err, rows) => {
        if (err) {
          return reject(err);
        }
        resolve(rows);
      });
    }
  });
}

async function getTableRowCount(conn, tableName) {
  const sql = `SELECT count(*) as count FROM ${tableName}`;
  const [{ count }] = await query(conn, sql);
  return count;
}

async function getTables(conn) {
  const tableNames = await getTableNames(conn);
  const tables = await Promise.all(
    tableNames.map(async (tableName) => {
      const [tableDDL, tableRowCount, tableColumns] = await Promise.all([
        getTableDDL(conn, tableName),
        getTableRowCount(conn, tableName),
        getTableColumns(conn, tableName),
      ]);
      const table = {
        name: tableName,
        ddl: tableDDL,
        rowCount: tableRowCount,
        columns: tableColumns,
      };
      return table;
    })
  );
  return tables;
}

async function recreateTablesStructure(conn, tables) {
  await Promise.all(
    tables.map(async (table) => {
      console.log("Recreating table", table.name);
      const dropTableSql = `DROP TABLE IF EXISTS ${table.name}`;
      await query(conn, dropTableSql);
      await query(conn, table.ddl);
      console.log("Recreated table", table.name);
    })
  );
}

function getProgress(startTime, processedRows, totalRows) {
  const percentage = ((processedRows / totalRows) * 100).toFixed(2);
  const elapsedInSeconds = (Date.now() - startTime) / 1000;
  const estimatedTotalTimeInSeconds =
    (elapsedInSeconds / processedRows) * totalRows;
  const remainingTime = estimatedTotalTimeInSeconds - elapsedInSeconds;
  const minutes = Math.floor(remainingTime / 60);
  const seconds = Math.floor(remainingTime % 60);
  const sec = `${seconds}`.padStart(2, "0");
  const rps = (processedRows / elapsedInSeconds).toFixed(2);
  return `Progress ${percentage}% | ETA: ${minutes}:${sec} | Rows/sec: ${rps}`;
}

async function copyTablesData(sourceConn, targetConn, tables) {
  return new Promise((resolve, reject) => {
    const totalRecords = tables.reduce((sum, table) => sum + table.rowCount, 0);
    let totalRecordsProcessed = 0;
    const startTime = Date.now();
    for (const table of tables) {
      console.log("reading table", table.name);
      const sql = `SELECT '${table.name}' as table_name, ${table.name}.* FROM ${table.name}`;
      const sourceReadableStream = sourceConn.query(sql).stream();
      const batchSize = 1000;
      let batch = [];
      const targetWritableStream = new Writable({
        objectMode: true,
        write(row, _, callback) {
          batch.push(row);

          // If batch size reached, or the stream ends, insert the batch
          if (batch.length >= batchSize) {
            flushBatch()
              .then(() => callback())
              .catch((err) => callback(err));
          } else {
            callback();
          }
        },
        final(callback) {
          // Insert any remaining rows in the batch when the stream ends
          flushBatch()
            .then(() => callback())
            .catch((err) => callback(err));
        },
      });

      async function flushBatch() {
        if (batch.length === 0) return;

        const tableName = batch[0].table_name;
        const values = batch.map((row) => Object.values(row).slice(1));

        // Construct the SQL for batch insert
        const placeholders = values
          .map((row) => `(${Array(row.length).fill("?").join(", ")})`)
          .join(", ");
        const insertSql = `INSERT INTO ${tableName} VALUES ${placeholders}`;

        return new Promise((resolve, reject) => {
          targetConn.execute(insertSql, values.flat(), (err) => {
            if (err) {
              reject(err);
            }
            totalRecordsProcessed += batch.length;
            process.stdout.write(
              `${totalRecordsProcessed}/${totalRecords} - ${getProgress(
                startTime,
                totalRecordsProcessed,
                totalRecords
              )} | Source Pool: ${sourcePools} Target Pool: ${targetPools}\n`
            );
            batch = []; // Clear the batch after insert
              resolve()
          });
        });
      }

      sourceReadableStream
        .pipe(targetWritableStream)
        .on("finish", () => {
          console.log(`Finished copying table: ${table.name}`);
          resolve();
        })
        .on("error", (err) => {
          console.error(`Error copying table: ${err}`);
          reject(err);
        });
    }
  });
}

const tables = await getTables(sourceConnection);
await recreateTablesStructure(targetConnection, tables);
await copyTablesData(sourceConnection, targetConnection, tables);

// sourceConnection.end();
// targetConnection.end();
