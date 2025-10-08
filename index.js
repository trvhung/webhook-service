import express from "express";
import bodyParser from "body-parser";
import { BigQuery } from "@google-cloud/bigquery";

const app = express();
const bigquery = new BigQuery();

app.use(bodyParser.json());

const datasetId = "Demo";
const tableId = "webhook_attributes";

app.post("/", async (req, res) => {
  try {
    const data = req.body;

    if (!data.insider_id) {
      return res.status(400).send({ error: "Missing insider_id" });
    }

    // Chuẩn hóa dữ liệu để khớp với schema
    const row = {
      insider_id: data.insider_id,
      hook_id: data.hook_id,
      partner: data.partner,
      name: data.name,
      timestamp: data.timestamp,

      attrs: {
        age: data.attrs?.age || null,
        email_optin: data.attrs?.email_optin ?? null,
        name: data.attrs?.name || null,
        surname: data.attrs?.surname || null
      },

      triggers: (data.triggers || []).map(t => ({
        key: t.key,
        current_state: t.current_state || null,
        next: t.next || null
      }))
    };

    await bigquery
      .dataset(datasetId)
      .table(tableId)
      .insert([row]);

    console.log("✅ Inserted row:", row.insider_id);
    res.status(200).send({ success: true });
  } catch (err) {
    console.error("❌ BigQuery insert error:", err);
    res.status(500).send({ error: err.message });
  }
});

const PORT = process.env.PORT || 8080;
app.listen(PORT, () => {
  console.log(`🚀 Webhook service running on port ${PORT}`);
});
