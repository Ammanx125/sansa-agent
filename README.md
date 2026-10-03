# Demo webhook watcher

The demo watcher generates linked vehicle-trip and fuel-purchase business
events and sends each as a separate, HMAC-signed webhook delivery to the
platform. It does not replace the regular filesystem sync command.

1. Create a webhook data source in the platform and copy its webhook URL and
   signing secret. The secret is returned only when the source is created.
2. Set the values in PowerShell without putting them on the command line:

   ```powershell
   $env:SANSA_DEMO_WEBHOOK_URL = "http://127.0.0.1:8000/api/v1/webhooks/<token>"
   $env:SANSA_DEMO_WEBHOOK_SECRET = "<signing-secret>"
   ```

3. Start the generated vehicle-event watcher:

   ```powershell
   .\.venv\Scripts\python.exe -m sansa_agent run-demo-watcher --interval 30
   ```

Each cycle sends a trip and a related fuel-purchase record with the same
vehicle and trip IDs. Fuel-purchase columns use the platform's seeded
procurement and common concepts (`supplier`, `product`, `purchase_order`,
`quantity`, `unit_price`, `amount`, `order_date`, and `currency`); trip
passenger count uses `quantity` and its date uses `date`. Vehicle, route,
distance, and fuel-efficiency fields have no matching concepts in the current
seeded catalog and are sent as descriptive attributes without claiming
misleading mappings. Pending events are saved under the agent's config
directory before delivery and retried if the request fails. If a delivery
times out after the platform accepts it but before the agent receives the
response, it may be sent again; webhook delivery is therefore at-least-once
in that failure case. Keep the terminal open while the watcher runs and press
Ctrl+C to stop.

To forward rows from a CSV instead, use `--mode csv --source-file <path>`.
For example:

```powershell
.\.venv\Scripts\python.exe -m sansa_agent run-demo-watcher `
  --mode csv `
  --source-file "C:\Users\LocalAdmin\Documents\platform\data\demo\procurement_sample.csv"
```
