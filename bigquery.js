import { BigQuery } from "@google-cloud/bigquery";
const bigquery = new BigQuery();

const query1 = `
  SELECT
    time_ref,
    SUM(value) AS trade_value
  FROM
    \`steph-ridnell-ass-1.ass1_bigquery.gsquarterly_september20\`
  GROUP BY
    time_ref
  ORDER BY
    trade_value DESC
  LIMIT
    10
`;

const query2 = `
SELECT
  x.trade_deficit_value,
  country_label,
  x.product_type,
  x.status
FROM (
  SELECT
    imports.sum - exports.sum AS trade_deficit_value,
    imports.country_code,
    imports.product_type,
    imports.status
  FROM (
    SELECT
      SUM(value) AS sum,
      country_code,
      product_type,
      status
    FROM
      ass1_bigquery.gsquarterly_september20 AS gsquarterly
    WHERE
      gsquarterly.account = 'Exports'
      AND gsquarterly.status = 'F'
      AND gsquarterly.product_type = 'Goods'
      AND gsquarterly.time_ref BETWEEN 201401
      AND 201612
    GROUP BY
      country_code,
      product_type,
      status ) AS exports
  JOIN (
    SELECT
      SUM(value) AS sum,
      country_code,
      product_type,
      status
    FROM
      ass1_bigquery.gsquarterly_september20 AS gsquarterly
    WHERE
      gsquarterly.account = 'Imports'
      AND gsquarterly.status = 'F'
      AND gsquarterly.product_type = 'Goods'
      AND gsquarterly.time_ref BETWEEN 201401
      AND 201612
    GROUP BY
      country_code,
      product_type,
      status ) AS imports
  ON
    exports.country_code=imports.country_code ) AS x
JOIN
  ass1_bigquery.country_classification AS countries
ON
  x.country_code = countries.country_code
ORDER BY
  trade_deficit_value DESC
LIMIT
  50
  `;

const query3 = `
  SELECT
    x.trade_surplus_value,
    service_label
  FROM (
  SELECT
    exports.sum - imports.sum AS trade_surplus_value,
    imports.code
  FROM (
    SELECT
      SUM(value) AS sum,
      code
    FROM
      \`steph-ridnell-ass-1.ass1_bigquery.gsquarterly_september20\`
    WHERE
      time_ref IN (
      SELECT
        top_10_time_slots.time_ref
      FROM (
        SELECT
          time_ref,
          SUM(value) AS trade_value
        FROM
          \`steph-ridnell-ass-1.ass1_bigquery.gsquarterly_september20\`
        GROUP BY
          time_ref
        ORDER BY
          trade_value DESC
        LIMIT
          10 ) AS top_10_time_slots )
      AND country_code IN (
      SELECT
        x.country_code
      FROM (
        SELECT
          imports.sum - exports.sum AS trade_deficit_value,
          imports.country_code,
          imports.product_type,
          imports.status
        FROM (
          SELECT
            SUM(value) AS sum,
            country_code,
            product_type,
            status
          FROM
            ass1_bigquery.gsquarterly_september20 AS gsquarterly
          WHERE
            gsquarterly.account = 'Exports'
            AND gsquarterly.status = 'F'
            AND gsquarterly.product_type = 'Goods'
            AND gsquarterly.time_ref BETWEEN 201401
            AND 201612
          GROUP BY
            country_code,
            product_type,
            status ) AS exports
        JOIN (
          SELECT
            SUM(value) AS sum,
            country_code,
            product_type,
            status
          FROM
            ass1_bigquery.gsquarterly_september20 AS gsquarterly
          WHERE
            gsquarterly.account = 'Imports'
            AND gsquarterly.status = 'F'
            AND gsquarterly.product_type = 'Goods'
            AND gsquarterly.time_ref BETWEEN 201401
            AND 201612
          GROUP BY
            country_code,
            product_type,
            status ) AS imports
        ON
          exports.country_code=imports.country_code ) AS x
      ORDER BY
        trade_deficit_value DESC
      LIMIT
        50 )
      AND account = 'Exports'
      AND product_type = 'Services'
    GROUP BY
      code ) AS exports
  JOIN (
    SELECT
      SUM(value) AS sum,
      code
    FROM
      \`steph-ridnell-ass-1.ass1_bigquery.gsquarterly_september20\`
    WHERE
      time_ref IN (
      SELECT
        top_10_time_slots.time_ref
      FROM (
        SELECT
          time_ref,
          SUM(value) AS trade_value
        FROM
          \`steph-ridnell-ass-1.ass1_bigquery.gsquarterly_september20\`
        GROUP BY
          time_ref
        ORDER BY
          trade_value DESC
        LIMIT
          10 ) AS top_10_time_slots )
      AND country_code IN (
      SELECT
        x.country_code
      FROM (
        SELECT
          imports.sum - exports.sum AS trade_deficit_value,
          imports.country_code,
          imports.product_type,
          imports.status
        FROM (
          SELECT
            SUM(value) AS sum,
            country_code,
            product_type,
            status
          FROM
            ass1_bigquery.gsquarterly_september20 AS gsquarterly
          WHERE
            gsquarterly.account = 'Exports'
            AND gsquarterly.status = 'F'
            AND gsquarterly.product_type = 'Goods'
            AND gsquarterly.time_ref BETWEEN 201401
            AND 201612
          GROUP BY
            country_code,
            product_type,
            status ) AS exports
        JOIN (
          SELECT
            SUM(value) AS sum,
            country_code,
            product_type,
            status
          FROM
            ass1_bigquery.gsquarterly_september20 AS gsquarterly
          WHERE
            gsquarterly.account = 'Imports'
            AND gsquarterly.status = 'F'
            AND gsquarterly.product_type = 'Goods'
            AND gsquarterly.time_ref BETWEEN 201401
            AND 201612
          GROUP BY
            country_code,
            product_type,
            status ) AS imports
        ON
          exports.country_code=imports.country_code ) AS x
      ORDER BY
        trade_deficit_value DESC
      LIMIT
        50 )
      AND account = 'Imports'
      AND product_type = 'Services'
    GROUP BY
      code ) AS imports
  ON
    exports.code=imports.code ) AS x
  JOIN
  \`ass1_bigquery.services_classification\` AS services
  ON
  x.code = services.code
  ORDER BY
    trade_surplus_value DESC
  LIMIT
    30
`;

export const getQuery1Data = async () => {
  const rows = await getData(query1);
  return rows;
};

export const getQuery2Data = async () => {
  const rows = await getData(query2);
  return rows;
};

export const getQuery3Data = async () => {
  const rows = await getData(query3);
  return rows;
};

const getData = async (query) => {
  const options = {
    query: query,
    location: "US",
  };

  const [job] = await bigquery.createQueryJob(options);
  console.log(`Job ${job.id} started`);
  const [rows] = await job.getQueryResults();
  console.log(`Job ${job.id} finished`);

  return rows;
};
