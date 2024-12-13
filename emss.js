const sql = require("mssql");
const net = require("net");

const sqlConfig = {
  user: "admin9",
  password: "admin9",
  database: "EMS_thermax",
  server: "DESKTOP-5UJJEQ0",
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000,
  },
  options: {
    encrypt: false,
    trustServerCertificate: false,
  },
  requestTimeout: 30000,
};


// Date Trackers
let lastHour = new Date().getHours();
let lastDate = new Date().toISOString().slice(0, 10);
let lastWeekMonday = getLastWeekMonday();
let lastMonth = getLastMonth();

// Get Last Week's Monday
function getLastWeekMonday() {
  const now = new Date();
  const dayOfWeek = now.getDay();
  const diff = now.getDate() - dayOfWeek + (dayOfWeek === 0 ? -6 : 1); // Adjust to last Monday
  const lastMonday = new Date(now.setDate(diff));
  return lastMonday.toISOString().slice(0, 10); // Format: YYYY-MM-DD
}

// Get Last Month
function getLastMonth() {
  const now = new Date();
  return now.getFullYear() + "-" + String(now.getMonth() + 1).padStart(2, "0"); // Format: YYYY-MM
}

// Start the server to handle live meter data
const server = net.createServer((conn) => {
  conn.on("data", async (data) => {
    try {
      const values = data.toString().split(",");

      const meterId = parseInt(values[0]);
      if (isNaN(meterId)) {
        console.error("Invalid meter ID received:", values[0]);
        return; // Stop further processing if meterId is invalid
      }
      const currentDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD
      const currentHour = new Date().getHours();
      const currentDay = new Date().getDay();
      const currentMonth = getLastMonth();
      const currentDayOfMonth = new Date().getDate();

      // Insert live data
      const loadName = await getLoadName(meterId);
      await insertLiveData(meterId, loadName, values);

      // Hourly Processing
      if (currentHour !== lastHour) {
        console.log("HOUR CHANGED");
        await calculateAndInsertHourlyAveragesForAllMeters();
        lastHour = currentHour;
      }

    //   // Daily Processing
    //   if (currentDate !== lastDate) {
    //     console.log(`DATE CHANGED: Processing data for ${currentDate}`);
    //     await calculateAndInsertDailyData(meterId, lastDate);
    //     lastDate = currentDate;
    //   }
// Modify this in your main server function:
if (currentDate !== lastDate) {
    console.log(`DATE CHANGED: Processing data for ${currentDate}`);
    await calculateAndInsertDailyDataForAllMeters(lastDate);
    lastDate = currentDate;
  }
      // Weekly Processing
      if (currentDay === 1 && currentDate !== lastWeekMonday) {
        console.log("WEEK CHANGED: Processing weekly data.");
        await calculateAndInsertWeeklyData(meterId);
        lastWeekMonday = currentDate;
      }

      // Monthly Processing
      if (currentDayOfMonth === 1 && currentMonth !== lastMonth) {
        console.log("MONTH CHANGED: Processing monthly data.");
        await calculateAndInsertMonthlyData(meterId);
        lastMonth = currentMonth;
      }
    } catch (err) {
      console.error("Error processing data:", err);
    }
  });

  conn.on("error", (err) => {
    console.error("Connection error:", err);
  });
});
// Fetch Load Name for Meter ID
async function getLoadName(meterId) {
  try {
    const query = `SELECT load_name FROM [EMS_thermax].[dbo].[ems_master] WHERE meter_id = ${meterId}`;
    const pool = await sql.connect(sqlConfig);
    const result = await pool.request().query(query);
    return result.recordset.length > 0 ? result.recordset[0].load_name : "Unknown";
  } catch (err) {
    console.error("Error fetching load name:", err);
    return "Unknown";
  }
}

// Insert live data into the database dynamically
// async function insertLiveData(meterId, loadName, values) {

//   console.log("values", values);
//   try {
//     const pool = await sql.connect(sqlConfig);

//     // Dynamically construct the table name based on the meter ID
//     const tableName = `[EMS_thermax].[dbo].[master_live_data_m${meterId}]`;
//     if (!/^\d+$/.test(meterId.toString())) {
//       throw new Error(`Invalid meter ID '${meterId}' for SQL table name.`);
//     }

//     // Fetch the previous kWh value to calculate the current kWh increment
//     const previousKWhQuery = `SELECT TOP 1 kWh FROM ${tableName} ORDER BY date_time DESC`;
//     const previousKWhResult = await pool.request().query(previousKWhQuery);
//     const previousKWh = previousKWhResult.recordset.length > 0 ? parseFloat(previousKWhResult.recordset[0].kWh) : 0;

//     console.log("previousKWh", previousKWh);

//     console.log("parseFloat(values[15])", parseFloat(values[15]));

//     // Calculate current kWh
//     const currentKWh = previousKWh + parseFloat(values[15]);
//     const mdValue = parseFloat(values[18]) || 0; // Safe parse for MD
// // console.log("tableName",tableName);


//     const insertQuery = `
//       INSERT INTO ${tableName}
//         (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, VLL1, VLL2, VLL3, Ir, Iy, Ib, PF, F, kW, kWh, kVAh, date_time, MD)
//       VALUES
//         (${meterId}, '${loadName}', '${values[1]}', '${values[2]}', '${values[3]}', '${values[4]}', '${values[5]}', '${values[6]}',
//          '${values[7]}', '${values[8]}', '${values[9]}', '${values[10]}', '${values[11]}', '${values[12]}',
//          '${values[13]}', '${values[14]}', '${values[15]}', ${values[16]}, '${values[17]}', GETDATE(), '${values[18]}');
//     `;
//     await pool.request().query(insertQuery);
//     console.log(`Live data inserted successfully for Meter ${meterId}.`);
//   } catch (err) {
//     console.error(`Error inserting live data for Meter ${meterId}:`, err);
//   }
// }
// Insert live data into the database dynamically
// Insert live data into the database dynamically
// Insert live data into the database dynamically
async function insertLiveData(meterId, loadName, values) {

  console.log("values", values);
  try {
    const pool = await sql.connect(sqlConfig);

    // Dynamically construct the table name based on the meter ID
    const tableName = `[EMS_thermax].[dbo].[master_live_data_m${meterId}]`;
    if (!/^\d+$/.test(meterId.toString())) {
      throw new Error(`Invalid meter ID '${meterId}' for SQL table name.`);
    }

    // Fetch the previous kWh value to calculate the current kWh increment
    const previousKWhQuery = `SELECT TOP 1 kWh FROM ${tableName} ORDER BY date_time DESC`;
    const previousKWhResult = await pool.request().query(previousKWhQuery);
    const previousKWh = previousKWhResult.recordset.length > 0 ? parseFloat(previousKWhResult.recordset[0].kWh) : 0;

    console.log("previousKWh", previousKWh);
    console.log("parseFloat(values[15])", parseFloat(values[15]));

    // Auto-fill missing values with default '0' to prevent incomplete data errors
    while (values.length < 19) {
      values.push('0');
    }
    console.log("Processed values:", values);

    // Validate numeric values
    for (let i = 1; i <= 18; i++) {
      if (isNaN(parseFloat(values[i]))) {
        console.error(`Invalid numeric value at index ${i}: ${values[i]}`);
        throw new Error(`Invalid numeric data received for meter ${meterId}`);
      }
    }

    // Construct and execute the insert query
    const insertQuery = `
      INSERT INTO ${tableName}
        (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, VLL1, VLL2, VLL3, Ir, Iy, Ib, PF, F, kW, kWh, kVAh, date_time, MD)
      VALUES
        (${meterId}, '${loadName}', 
         ${parseFloat(values[1]) || 0}, ${parseFloat(values[2]) || 0}, ${parseFloat(values[3]) || 0}, 
         ${parseFloat(values[4]) || 0}, ${parseFloat(values[5]) || 0}, ${parseFloat(values[6]) || 0}, 
         ${parseFloat(values[7]) || 0}, ${parseFloat(values[8]) || 0}, ${parseFloat(values[9]) || 0}, 
         ${parseFloat(values[10]) || 0}, ${parseFloat(values[11]) || 0}, ${parseFloat(values[12]) || 0}, 
         ${parseFloat(values[13]) || 0}, ${parseFloat(values[14]) || 0}, 
         ${parseFloat(values[15]) || 0}, ${parseFloat(values[16]) || 0}, 
         ${parseFloat(values[17]) || 0}, GETDATE(), ${parseFloat(values[18]) || 0});
    `;

    await pool.request().query(insertQuery);
    console.log(`Live data inserted successfully for Meter ${meterId}.`);
  } catch (err) {
    console.error(`Error inserting live data for Meter ${meterId}:`, err);
  }
}



function getCurrentDateFormattedMinusOneHour() {
  const now = new Date();
  now.setHours(now.getHours() - 1); // Subtract 1 hour

  // Format the date and time, including minutes as 00
  const year = now.getFullYear();
  const month = (now.getMonth() + 1).toString().padStart(2, '0');
  const day = now.getDate().toString().padStart(2, '0');
  const hours = now.getHours().toString().padStart(2, '0');

  // Return formatted date as 'YYYY-MM-DD HH'
  return `${year}-${month}-${day} ${hours}`;
}


// async function calculateAndInsertHourlyAverages(meterId, lastHour) {
//   try {
//     const pool = await sql.connect(sqlConfig);
//     const liveTable = `[EMS_thermax].[dbo].[master_live_data_m${meterId}]`;
//     const averageTable = `[EMS_thermax].[dbo].[average_m${meterId}]`;
//     const energyConsumptionTable = `[EMS_thermax].[dbo].[energy_consumption]`;

//     console.log("Processing hourly averages for table:", liveTable);
//     let startDatetime = `${getCurrentDateFormattedMinusOneHour()}:00:00`;
//     let endDatetime = `${getCurrentDateFormattedMinusOneHour()}:59:59`;
//     // Fetch the last inserted date in energy_consumption
//     const lastDateQuery = `
//       SELECT TOP 1 CONVERT(VARCHAR(10), date_time, 120) AS lastDate
//       FROM ${energyConsumptionTable}
//       WHERE meter_id = ${meterId}
//       ORDER BY date_time DESC;
//     `;
//     const lastDateResult = await pool.request().query(lastDateQuery);
//     const lastDate = lastDateResult.recordset.length > 0 ? lastDateResult.recordset[0].lastDate : null;

//     // Get the current date for comparison
//     const currentDate = new Date().toISOString().slice(0, 10); // YYYY-MM-DD

//     // Calculate hourly kWh consumption difference
//     const Hr_C_kwh_query = `
//       WITH HourlyData AS (
//           SELECT
//               FORMAT(date_time, 'yyyy-MM-dd HH') AS hour_group,
//               meter_id,
//               kWh,
//               ROW_NUMBER() OVER (PARTITION BY FORMAT(date_time, 'yyyy-MM-dd HH') ORDER BY date_time ASC) AS rn_first,
//               ROW_NUMBER() OVER (PARTITION BY FORMAT(date_time, 'yyyy-MM-dd HH') ORDER BY date_time DESC) AS rn_last
//           FROM ${liveTable}
//       ),
//       FirstLastEntries AS (
//           SELECT
//               hour_group,
//               meter_id,
//               MAX(CASE WHEN rn_first = 1 THEN kWh END) AS kWh_first,
//               MAX(CASE WHEN rn_last = 1 THEN kWh END) AS kWh_last
//           FROM HourlyData
//           GROUP BY hour_group, meter_id
//       )
//       SELECT
//           MAX(kWh_last - kWh_first) AS kWh_difference
//       FROM FirstLastEntries
//       WHERE meter_id = ${meterId};
//     `;
//     const Hr_C_kwh = await pool.request().query(Hr_C_kwh_query);
//     const kWhDifference = Hr_C_kwh.recordset[0]?.kWh_difference || 0;

//     console.log("Calculated hourly kWh difference:", kWhDifference);

    
// const avgQuery = `
//   SELECT 
//     meter_id, 
//     MAX(load_name) AS load_name, 
//     AVG(CAST(Vry AS FLOAT)) AS avgVry,
//     AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
//     AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
//     AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
//     AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
//     AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
//     AVG(CAST(VLL1 AS FLOAT)) AS avgVLL1,
//     AVG(CAST(VLL2 AS FLOAT)) AS avgVLL2,
//     AVG(CAST(VLL3 AS FLOAT)) AS avgVLL3,
//     AVG(CAST(Ir AS FLOAT)) AS avgIr,
//     AVG(CAST(Iy AS FLOAT)) AS avgIy,
//     AVG(CAST(Ib AS FLOAT)) AS avgIb,
//     AVG(CAST(PF AS FLOAT)) AS avgPF,
//     AVG(CAST(F AS FLOAT)) AS avgF,
//     AVG(CAST(kW AS FLOAT)) AS avgKW,
//     AVG(CAST(kWh AS FLOAT)) AS avgKWh,
//     AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
//      AVG(CAST(MD AS FLOAT)) AS avgMD,
//       MIN(CAST(MD AS FLOAT)) AS minMD,
//     MAX(CAST(MD AS FLOAT)) AS maxMD,
//     FORMAT(MAX(date_time), 'yyyy-MM-dd HH:mm:ss') AS avgDate
//   FROM ${liveTable}
//   WHERE TRY_CAST(date_time AS DATETIME) IS NOT NULL
//     AND date_time BETWEEN '${startDatetime}' AND '${endDatetime}' -- Use BETWEEN for the time range
//   GROUP BY meter_id;
// `;

// console.log("avgQuery",avgQuery);

//     const avgResult = await pool.request().query(avgQuery);

//     console.log("Avarge Query result", avgResult);

    
//     for (const avgData of avgResult.recordset) {
//       const { meter_id, load_name, avgDate } = avgData;
     
      
//       console.log(startDatetime); // Outputs: 2024-12-01 03:00:00
      
//       // Insert hourly averages into the average table
//       const insertAvgQuery = `
//         INSERT INTO ${averageTable}
//           (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, VLL1, VLL2, VLL3, Ir, Iy, Ib, PF, F, kW, kWh, kVAh, kwh_C, date_time, MD, minMD, maxMD)
//         VALUES
//           (${meter_id}, '${load_name}', ${avgData.avgVry}, ${avgData.avgVyb}, ${avgData.avgVbr}, ${avgData.avgVLN1}, ${avgData.avgVLN2}, ${avgData.avgVLN3},
//            ${avgData.avgVLL1}, ${avgData.avgVLL2}, ${avgData.avgVLL3}, ${avgData.avgIr}, ${avgData.avgIy}, ${avgData.avgIb},
//            ${avgData.avgPF}, ${avgData.avgF}, ${avgData.avgKW}, ${avgData.avgKWh}, ${avgData.avgKVAh}, ${kWhDifference}, '${startDatetime}', ${avgData.avgMD}, ${avgData.minMD}, ${avgData.maxMD});
//       `;
//       await pool.request().query(insertAvgQuery);

//       console.log("Hourly average data inserted:", insertAvgQuery);
//     }

 
//   try {
//     console.log(`Hourly averages processed successfully for Meter ${meterId}.`);
  
//     // Delete live data after processing
//     const deleteQuery = `
//       DELETE FROM ${liveTable}
//       WHERE date_time BETWEEN '${startDatetime}' AND '${endDatetime}';
//     `;
//   console.log("deleteQuery",deleteQuery);
  
//     const deleteResult = await pool.request().query(deleteQuery);
//     console.log(`Hourly data deleted from live table for Meter ${meterId}. Rows affected: ${deleteResult.rowsAffected}`);
//   } catch (err) {
//     console.error(`Error processing hourly averages for Meter ${meterId}:`, err);
//   } finally {
//   }
// } catch (err) {
//   console.error(`Error processing daily data for Meter ${meterId}:`, err);
// }
// }


// /////////////////////////////////////////daily data //////////////////



async function calculateAndInsertHourlyAveragesForAllMeters() {
    try {
      const pool = await sql.connect(sqlConfig);
      const startDatetime = `${getCurrentDateFormattedMinusOneHour()}:00:00`;
      const endDatetime = `${getCurrentDateFormattedMinusOneHour()}:59:59`;
  
      for (const meterId of meter_id_list) {
        try {
          const liveTable = `[EMS_thermax].[dbo].[master_live_data_m${meterId}]`;
          const averageTable = `[EMS_thermax].[dbo].[average_m${meterId}]`;
          const energyConsumptionTable = `[EMS_thermax].[dbo].[energy_consumption]`;
  
          console.log(`Processing hourly averages for Meter ID ${meterId}, Table: ${liveTable}`);
  
          // Fetch the last inserted date in energy_consumption
          const lastDateQuery = `
            SELECT TOP 1 CONVERT(VARCHAR(10), date_time, 120) AS lastDate
            FROM ${energyConsumptionTable}
            WHERE meter_id = ${meterId}
            ORDER BY date_time DESC;
          `;
          const lastDateResult = await pool.request().query(lastDateQuery);
          const lastDate = lastDateResult.recordset.length > 0 ? lastDateResult.recordset[0].lastDate : null;
  
          // Calculate hourly kWh consumption difference
          const Hr_C_kwh_query = `
            WITH HourlyData AS (
                SELECT
                    FORMAT(date_time, 'yyyy-MM-dd HH') AS hour_group,
                    meter_id,
                    kWh,
                    ROW_NUMBER() OVER (PARTITION BY FORMAT(date_time, 'yyyy-MM-dd HH') ORDER BY date_time ASC) AS rn_first,
                    ROW_NUMBER() OVER (PARTITION BY FORMAT(date_time, 'yyyy-MM-dd HH') ORDER BY date_time DESC) AS rn_last
                FROM ${liveTable}
            ),
            FirstLastEntries AS (
                SELECT
                    hour_group,
                    meter_id,
                    MAX(CASE WHEN rn_first = 1 THEN kWh END) AS kWh_first,
                    MAX(CASE WHEN rn_last = 1 THEN kWh END) AS kWh_last
                FROM HourlyData
                GROUP BY hour_group, meter_id
            )
            SELECT
                MAX(kWh_last - kWh_first) AS kWh_difference
            FROM FirstLastEntries
            WHERE meter_id = ${meterId};
          `;
          const Hr_C_kwh = await pool.request().query(Hr_C_kwh_query);
          const kWhDifference = Hr_C_kwh.recordset[0]?.kWh_difference || 0;
  
          console.log(`Calculated hourly kWh difference for Meter ${meterId}: ${kWhDifference}`);
  
          // Calculate averages
          const avgQuery = `
            SELECT 
              meter_id, 
              MAX(load_name) AS load_name, 
              AVG(CAST(Vry AS FLOAT)) AS avgVry,
              AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
              AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
              AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
              AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
              AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
              AVG(CAST(VLL1 AS FLOAT)) AS avgVLL1,
              AVG(CAST(VLL2 AS FLOAT)) AS avgVLL2,
              AVG(CAST(VLL3 AS FLOAT)) AS avgVLL3,
              AVG(CAST(Ir AS FLOAT)) AS avgIr,
              AVG(CAST(Iy AS FLOAT)) AS avgIy,
              AVG(CAST(Ib AS FLOAT)) AS avgIb,
              AVG(CAST(PF AS FLOAT)) AS avgPF,
              AVG(CAST(F AS FLOAT)) AS avgF,
              AVG(CAST(kW AS FLOAT)) AS avgKW,
              AVG(CAST(kWh AS FLOAT)) AS avgKWh,
              AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
              AVG(CAST(MD AS FLOAT)) AS avgMD,
              MIN(CAST(MD AS FLOAT)) AS minMD,
              MAX(CAST(MD AS FLOAT)) AS maxMD,
              FORMAT(MAX(date_time), 'yyyy-MM-dd HH:mm:ss') AS avgDate
            FROM ${liveTable}
            WHERE TRY_CAST(date_time AS DATETIME) IS NOT NULL
              AND date_time BETWEEN '${startDatetime}' AND '${endDatetime}'
            GROUP BY meter_id;
          `;
          const avgResult = await pool.request().query(avgQuery);
  
          for (const avgData of avgResult.recordset) {
            const { meter_id, load_name } = avgData;
  
            // Insert hourly averages into the average table
            const insertAvgQuery = `
              INSERT INTO ${averageTable}
                (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, VLL1, VLL2, VLL3, Ir, Iy, Ib, PF, F, kW, kWh, kVAh, kwh_C, date_time, MD, minMD, maxMD)
              VALUES
                (${meter_id}, '${load_name}', ${avgData.avgVry}, ${avgData.avgVyb}, ${avgData.avgVbr}, ${avgData.avgVLN1}, ${avgData.avgVLN2}, ${avgData.avgVLN3},
                 ${avgData.avgVLL1}, ${avgData.avgVLL2}, ${avgData.avgVLL3}, ${avgData.avgIr}, ${avgData.avgIy}, ${avgData.avgIb},
                 ${avgData.avgPF}, ${avgData.avgF}, ${avgData.avgKW}, ${avgData.avgKWh}, ${avgData.avgKVAh}, ${kWhDifference}, '${startDatetime}', ${avgData.avgMD}, ${avgData.minMD}, ${avgData.maxMD});
            `;
            await pool.request().query(insertAvgQuery);
            console.log(`Hourly average data inserted for Meter ${meter_id}.`);
          }
  
          // Delete live data after processing
          const deleteQuery = `
            DELETE FROM ${liveTable}
            WHERE date_time BETWEEN '${startDatetime}' AND '${endDatetime}';
          `;
          const deleteResult = await pool.request().query(deleteQuery);
          console.log(`Hourly data deleted for Meter ${meterId}. Rows affected: ${deleteResult.rowsAffected}`);
        } catch (err) {
          console.error(`Error processing hourly averages for Meter ${meterId}:`, err);
        }
      }
  
      console.log("Hourly averages processed successfully for all meters.");
    } catch (err) {
      console.error("Error processing hourly averages for meters:", err);
    }
  }
  



// async function calculateAndInsertDailyData(meterId, last_date) {
//   try {
//     const pool = await sql.connect(sqlConfig);

//     // Define table names
//     const averageTable = `[EMS_thermax].[dbo].[average_m${meterId}]`;
//     const energyConsumptionTable = `[EMS_thermax].[dbo].[energy_consumption]`;

//     console.log(`Processing daily data for meter ID: ${meterId}`);

//     // Fetch the last inserted date in the average table
//     const lastDateQuery = `
//         SELECT TOP 1 CONVERT(VARCHAR(10), date_time, 120) AS lastDate
//         FROM ${averageTable}
//         WHERE meter_id = ${meterId}
//         ORDER BY sr_no DESC;
//       `;
//     const lastDateResult = await pool.request().query(lastDateQuery);

//     console.log("lastdate data query  ", lastDateQuery);

//     console.log("lastdate data ", lastDateResult);

//     // const lastDate = lastDateResult.recordset.length > 0 ? lastDateResult.recordset[0].lastDate : null;

//     const lastDate = last_date;
//     // Get the current date
//     //   const currentDate = new Date().toISOString().slice(0, 10); // Format: YYYY-MM-DD
//     const now = new Date();
//     const year = now.getFullYear();
//     const month = String(now.getMonth() + 1).padStart(2, '0');
//     const day = String(now.getDate()).padStart(2, '0');
//     const currentDate = `${year}-${month}-${day}`;
//     const previesDate = String(now.getDate() - 1).padStart(2, '0');
//     console.log("previesDate", previesDate);
//     // Process data only if the date has changed
//     if (currentDate !== lastDate) {
//       console.log(`Date changed: Processing data for ${currentDate}. Previous processed date was ${lastDate}.`);

//       // Query to calculate daily averages and sums
//       const dailyQuery = `
//           SELECT 
//             meter_id,
//             MAX(load_name) AS load_name,
//             AVG(CAST(kWh AS FLOAT)) AS avgKWh, -- Average of kWh
//             SUM(CAST(kwh_C AS FLOAT)) AS sumKWh, -- Sum of kWh
//             AVG(CAST(Vry AS FLOAT)) AS avgVry,
//             AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
//             AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
//             AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
//             AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
//             AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
//             AVG(CAST(Ir AS FLOAT)) AS avgIr,
//             AVG(CAST(Iy AS FLOAT)) AS avgIy,
//             AVG(CAST(Ib AS FLOAT)) AS avgIb,
//             AVG(CAST(PF AS FLOAT)) AS avgPF,
//             AVG(CAST(F AS FLOAT)) AS avgF,
//             AVG(CAST(kW AS FLOAT)) AS avgKW,
//             AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
//             AVG(CAST(MD AS FLOAT)) AS avgMD,   
//             MIN(CAST(MD AS FLOAT)) AS minMD,
//            MAX(CAST(MD AS FLOAT)) AS maxMD,

//             CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120) AS avgDate
//           FROM ${averageTable}
//           WHERE CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120) = '${lastDate}'
//           GROUP BY meter_id, CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120);
//         `;
//       console.log("dailyQuery", dailyQuery);

//       // Execute the query
//       const dailyResult = await pool.request().query(dailyQuery);

//       // Insert the results into the energy consumption table
//       for (const dailyData of dailyResult.recordset) {
//         const { meter_id, load_name, avgDate, avgKWh, sumKWh } = dailyData;


//         // const insertQuery = `
//         //     INSERT INTO ${energyConsumptionTable}
//         //       (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3,  Ir, Iy, Ib, PF, F, kW, kVAh, kWh, kWh_C, date_time, MD, minMD, maxMD)
//         //     VALUES
//         //       (${meter_id}, '${load_name}', ${dailyData.avgVry}, ${dailyData.avgVyb}, ${dailyData.avgVbr}, 
//         //        ${dailyData.avgVLN1}, ${dailyData.avgVLN2}, ${dailyData.avgVLN3},  ${dailyData.avgIr}, ${dailyData.avgIy}, ${dailyData.avgIb}, ${dailyData.avgPF}, 
//         //        ${dailyData.avgF}, ${dailyData.avgKW}, ${dailyData.avgKVAh}, ${avgKWh}, ${sumKWh}, '${avgDate}', ${dailyData.avgMD}, ${avgData.minMD}, ${avgData.maxMD});
//         //   `;

//         const insertQuery = `
//     INSERT INTO ${energyConsumptionTable}
//       (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3,  Ir, Iy, Ib, PF, F, kW, kVAh, kWh, kWh_C, date_time, MD, minMD, maxMD)
//     VALUES
//       (${meter_id}, '${load_name}', ${dailyData.avgVry}, ${dailyData.avgVyb}, ${dailyData.avgVbr}, 
//        ${dailyData.avgVLN1}, ${dailyData.avgVLN2}, ${dailyData.avgVLN3},  
//        ${dailyData.avgIr}, ${dailyData.avgIy}, ${dailyData.avgIb}, ${dailyData.avgPF}, 
//        ${dailyData.avgF}, ${dailyData.avgKW}, ${dailyData.avgKVAh}, ${avgKWh}, ${sumKWh}, 
//        '${avgDate}', ${dailyData.avgMD}, ${dailyData.minMD}, ${dailyData.maxMD});
//   `;

//         await pool.request().query(insertQuery);
//         console.log("insertQuery", insertQuery);

//         console.log(`Inserted daily data for Meter ${meter_id} on ${avgDate}`);
//       }
//     } else {
//       console.log(`No new date to process for Meter ${meterId}. Current date is ${currentDate}, last processed date was ${lastDate}.`);
//     }
//   } catch (err) {
//     console.error(`Error processing daily data for Meter ${meterId}:`, err);
//   }
// }

////////////////////////////////////////////daily end //////////////////////////////////////////////////
// /////////////////////////////////////////////weekly start /////////////////////////////////////////////




let meter_id_list = [1, 2, 3, 4, 5, 6, 7, 8]; // List of meter IDs

async function calculateAndInsertDailyDataForAllMeters(lastDate) {
  try {
    const pool = await sql.connect(sqlConfig);
    const energyConsumptionTable = `[EMS_thermax].[dbo].[energy_consumption]`;

    for (const meterId of meter_id_list) {
      try {
        const averageTable = `[EMS_thermax].[dbo].[average_m${meterId}]`;

        console.log(`Processing daily data for Meter ID: ${meterId}`);

        // Query to calculate daily averages and sums
        const dailyQuery = `
          SELECT 
            meter_id,
            MAX(load_name) AS load_name,
            AVG(CAST(kWh AS FLOAT)) AS avgKWh, -- Average of kWh
            SUM(CAST(kwh_C AS FLOAT)) AS sumKWh, -- Sum of kWh
            AVG(CAST(Vry AS FLOAT)) AS avgVry,
            AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
            AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
            AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
            AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
            AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
            AVG(CAST(Ir AS FLOAT)) AS avgIr,
            AVG(CAST(Iy AS FLOAT)) AS avgIy,
            AVG(CAST(Ib AS FLOAT)) AS avgIb,
            AVG(CAST(PF AS FLOAT)) AS avgPF,
            AVG(CAST(F AS FLOAT)) AS avgF,
            AVG(CAST(kW AS FLOAT)) AS avgKW,
            AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
            AVG(CAST(MD AS FLOAT)) AS avgMD,   
            MIN(CAST(MD AS FLOAT)) AS minMD,
            MAX(CAST(MD AS FLOAT)) AS maxMD,
            CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120) AS avgDate
          FROM ${averageTable}
          WHERE CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120) = '${lastDate}'
          GROUP BY meter_id, CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120);
        `;
        console.log("Daily Query for Meter:", dailyQuery);

        const dailyResult = await pool.request().query(dailyQuery);

        // Insert the results into the energy consumption table
        for (const dailyData of dailyResult.recordset) {
          const { meter_id, load_name, avgDate, avgKWh, sumKWh } = dailyData;

          const insertQuery = `
            INSERT INTO ${energyConsumptionTable}
              (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, Ir, Iy, Ib, PF, F, kW, kVAh, kWh, kWh_C, date_time, MD, minMD, maxMD)
            VALUES
              (${meter_id}, '${load_name}', ${dailyData.avgVry}, ${dailyData.avgVyb}, ${dailyData.avgVbr}, 
               ${dailyData.avgVLN1}, ${dailyData.avgVLN2}, ${dailyData.avgVLN3}, ${dailyData.avgIr}, ${dailyData.avgIy}, ${dailyData.avgIb}, 
               ${dailyData.avgPF}, ${dailyData.avgF}, ${dailyData.avgKW}, ${dailyData.avgKVAh}, ${avgKWh}, ${sumKWh}, 
               '${avgDate}', ${dailyData.avgMD}, ${dailyData.minMD}, ${dailyData.maxMD});
          `;
          await pool.request().query(insertQuery);

          console.log(`Inserted daily data for Meter ${meter_id} on ${avgDate}`);
        }
      } catch (err) {
        console.error(`Error processing daily data for Meter ${meterId}:`, err);
      }
    }

    console.log("Daily data processed successfully for all meters.");
  } catch (err) {
    console.error("Error processing daily data for all meters:", err);
  }
}





async function calculateAndInsertWeeklyData(meterId) {
  try {
    const pool = await sql.connect(sqlConfig);

    // Define table names
    const energyConsumptionTable = `[EMS_thermax].[dbo].[energy_consumption]`;
    const energyConsumptionWeeklyTable = `[EMS_thermax].[dbo].[energy_consumption_weekly]`;

    console.log(`Processing weekly data for meter ID: ${meterId}`);

    // Determine the start and end dates for the previous week
    const now = new Date();
    const dayOfWeek = now.getDay();
    const mondayOffset = dayOfWeek === 0 ? -6 : 1 - dayOfWeek; // Adjust for Monday
    const previousMonday = new Date(now.getFullYear(), now.getMonth(), now.getDate() + mondayOffset - 6);
    const previousSunday = new Date(previousMonday.getFullYear(), previousMonday.getMonth(), previousMonday.getDate() + 7);


    const startDate = previousMonday.toISOString().slice(0, 10); // Format: YYYY-MM-DD
    const endDate = previousSunday.toISOString().slice(0, 10);

    // const startDate = '2024-11-24'; // Format: YYYY-MM-DD
    // const endDate = '2024-12-1';
    console.log('startDate', startDate);
    console.log('endDate', endDate);
    // Query to calculate weekly averages for kWh and sums for kwh_C
    const weeklyQuery = `
      SELECT 
        meter_id,
        MAX(load_name) AS load_name,
        AVG(CAST(kWh AS FLOAT)) AS avgKWh, -- Weekly average of kWh
        SUM(CAST(kwh_C AS FLOAT)) AS sumKwhC, -- Weekly sum of kwh_C
        AVG(CAST(Vry AS FLOAT)) AS avgVry,
        AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
        AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
        AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
        AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
        AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
        AVG(CAST(Ir AS FLOAT)) AS avgIr,
        AVG(CAST(Iy AS FLOAT)) AS avgIy,
        AVG(CAST(Ib AS FLOAT)) AS avgIb,
        AVG(CAST(PF AS FLOAT)) AS avgPF,
        AVG(CAST(F AS FLOAT)) AS avgF,
        AVG(CAST(kW AS FLOAT)) AS avgKW,
        AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
        AVG(CAST(MD AS FLOAT)) AS avgMD

      FROM ${energyConsumptionTable}
      WHERE 
        CONVERT(VARCHAR(10), TRY_CAST(date_time AS DATETIME), 120) BETWEEN '${startDate}' AND '${endDate}'
      GROUP BY meter_id;
    `;

    console.log("Weekly Query:", weeklyQuery);

    // Execute the query
    const weeklyResult = await pool.request().query(weeklyQuery);

    // Insert the results into the weekly table
    for (const weeklyData of weeklyResult.recordset) {
      const {
        meter_id,
        load_name,
        avgKWh, sumKwhC,
        avgVry, avgVyb, avgVbr,
        avgVLN1, avgVLN2, avgVLN3,
        avgIr, avgIy, avgIb,
        avgPF, avgF, avgKW, avgKVAh,avgMD,
      } = weeklyData;

      // Log the calculated sum of kWh_C
      console.log(`Calculated weekly kWh_C for Meter ${meter_id}: ${sumKwhC}`);

      // Use FORMAT(GETDATE(), 'yyyy-MM-dd') to insert only the date
      const insertQuery = `
        INSERT INTO ${energyConsumptionWeeklyTable}
          (meter_id, load_name, Vry, Vyb, Vbr, VLN1, VLN2, VLN3, Ir, Iy, Ib, PF, F, kW, kVAh, kWh, kwh_C, date_time, MD)
        VALUES
          (${meter_id}, '${load_name}', ${avgVry}, ${avgVyb}, ${avgVbr}, 
           ${avgVLN1}, ${avgVLN2}, ${avgVLN3}, ${avgIr}, ${avgIy}, ${avgIb}, 
           ${avgPF}, ${avgF}, ${avgKW}, ${avgKVAh}, ${avgKWh}, ${sumKwhC}, CAST('${endDate}' AS DATE), ${avgMD});
      `;
      console.log("Weekly insertQuery:", insertQuery);
      await pool.request().query(insertQuery);

      console.log(`Inserted weekly data for Meter ${meter_id} (Start: ${startDate}, End: ${endDate})`);
    }
  } catch (err) {
    console.error(`Error processing weekly data for Meter ${meterId}:`, err);
  }
}


////////////////////////////////////////////////////weekly end ///////////////////////////////////////////////
////////////////////////////////////////////////////////monthly start ///////////////////////////////////////////

// Calculate and Insert Monthly Data
async function calculateAndInsertMonthlyData(meterId) {
  try {
    const pool = await sql.connect(sqlConfig);

    // Define table names
    const energyConsumptionWeeklyTable = `[EMS_thermax].[dbo].[energy_consumption]`;
    const energyConsumptionMonthlyTable = `[EMS_thermax].[dbo].[energy_consumption_monthly]`;

    console.log(`Processing monthly data for meter ID: ${meterId}`);

    // Determine the start and end dates for the previous month
    const now = new Date();
    const previousMonth = new Date(now.getFullYear(), now.getMonth() - 1, 1); // First day of the previous month
    const lastDayPreviousMonth = new Date(previousMonth.getFullYear(), previousMonth.getMonth() + 1, 0); // Last day of the previous month

    // Format the dates manually
    const formatDate = (date) => {
      const year = date.getFullYear();
      const month = date.getMonth() + 1; // Months are zero-indexed
      const day = date.getDate(); // Day of the month
      return `${year}-${month}-${day}`;
    };

    const startDate = formatDate(previousMonth); // First day of the previous month
    const endDate = formatDate(lastDayPreviousMonth); // Last day of the previous month

    console.log('First Day of Previous Month:', startDate);
    console.log('Last Day of Previous Month:', endDate);

    // Query to calculate monthly averages for kWh and sums for kwh_C
    const monthlyQuery = `
      SELECT 
        AVG(CAST(kWh AS FLOAT)) AS avgKWh, -- Monthly average of kWh
        SUM(CAST(kwh_C AS FLOAT)) AS sumKwhC, -- Monthly sum of kwh_C
        AVG(CAST(Vry AS FLOAT)) AS avgVry,
        AVG(CAST(Vyb AS FLOAT)) AS avgVyb,
        AVG(CAST(Vbr AS FLOAT)) AS avgVbr,
        AVG(CAST(VLN1 AS FLOAT)) AS avgVLN1,
        AVG(CAST(VLN2 AS FLOAT)) AS avgVLN2,
        AVG(CAST(VLN3 AS FLOAT)) AS avgVLN3,
        AVG(CAST(Ir AS FLOAT)) AS avgIr,
        AVG(CAST(Iy AS FLOAT)) AS avgIy,
        AVG(CAST(Ib AS FLOAT)) AS avgIb,
        AVG(CAST(PF AS FLOAT)) AS avgPF,
        AVG(CAST(F AS FLOAT)) AS avgF,
        AVG(CAST(kW AS FLOAT)) AS avgKW,
        AVG(CAST(kVAh AS FLOAT)) AS avgKVAh,
        AVG(CAST(MD AS FLOAT)) AS avgMD

      FROM ${energyConsumptionWeeklyTable}
       WHERE date_time BETWEEN '${startDate}' AND '${endDate}'
    `;

    console.log("Monthly Query:", monthlyQuery);

    // Execute the query
    const monthlyResult = await pool.request().query(monthlyQuery);
    console.log("Monthly monthlyResult:", monthlyResult);


    // Insert the results into the monthly table
    for (const monthlyData of monthlyResult.recordset) {
      const {
        avgKWh, sumKwhC,
        avgVry, avgVyb, avgVbr,
        avgVLN1, avgVLN2, avgVLN3,
        avgIr, avgIy, avgIb,
        avgPF, avgF, avgKW, avgKVAh,avgMD,
      } = monthlyData;

      const insertQuery = `
        INSERT INTO ${energyConsumptionMonthlyTable}
          ( Vry, Vyb, Vbr, VLN1, VLN2, VLN3, Ir, Iy, Ib, PF, F, kW, kVAh, kWh, kwh_C, date_time, MD)
        VALUES
          ( ${avgVry}, ${avgVyb}, ${avgVbr}, 
           ${avgVLN1}, ${avgVLN2}, ${avgVLN3}, ${avgIr}, ${avgIy}, ${avgIb}, 
           ${avgPF}, ${avgF}, ${avgKW}, ${avgKVAh}, ${avgKWh}, ${sumKwhC}, CAST('${endDate}' AS DATE), ${avgMD});
      `;

      console.log("Monthly insertQuery:", insertQuery);
      await pool.request().query(insertQuery);

      console.log(`Inserted monthly data for Meter ${meter_id} (Start: ${startDate}, End: ${endDate})`);
    }
  } catch (err) {
    console.error(`Error processing monthly data for Meter ${meterId}:`, err);
  }
}



////////////////////////////////////////////////////monthly end ////////////////////////////////////////

server.listen(5000, () => {
  console.log("Server listening on port 5000 for live meter data.");

});
  
