import { BigQuery } from "@google-cloud/bigquery";

/**
 * NOTE:
 * - Add your GCP credentials in environment variable:
 *   VERCEL_ENV -> Production
 *   GOOGLE_APPLICATION_CREDENTIALS_JSON -> JSON key content of your service account
 */

export default async function handler(req, res) {
  if (req.method !== "POST") {
    return res.status(405).json({ error: "Method not allowed" });
  }

  try {
    // Parse BigQuery credentials from env
    const creds = JSON.parse(process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON);
    const bigquery = new BigQuery({ credentials: creds, projectId: creds.project_id });

    const datasetId = "Demo";
    const tableId = "webhook_attributes";

    const data = req.body;

    if (!data.insider_id) {
      return res.status(400).json({ error: "Missing insider_id" });
    }

    // Chuẩn hóa dữ liệu để khớp schema
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

    await bigquery.dataset(datasetId).table(tableId).insert([row]);

    console.log("✅ Inserted row:", row.insider_id);
    res.status(200).json({ success: true });
  } catch (err) {
    console.error("❌ BigQuery insert error:", err);
    res.status(500).json({ error: err.message });
  }
}
