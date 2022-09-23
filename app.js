import express from "express";
import { getQuery1Data, getQuery2Data, getQuery3Data } from "./bigquery.js";
const app = express();
const port = parseInt(process.env.PORT) || 8888;

app.set("view engine", "pug");

app.get("/", async function (req, res) {
  const query1Rows = await getQuery1Data();
  const query2Rows = await getQuery2Data();
  const query3Rows = await getQuery3Data();
  res.render("index", { query1Rows, query2Rows, query3Rows });
});

app.listen(port, function () {
  console.log(`Listening on port ${port} !`);
});
