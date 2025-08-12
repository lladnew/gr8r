# gr8r-grafana-worker
logging worker to log to grafana
workers should use this format:


await fetch(env.GRAFANA_URL, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    level: "info", // or "error"
    message: "Some event",
    meta: {
      source: "gr8r-my-worker",
      service: "my-service-type",
      task: "transcribe",
      user: "admin", // ← anything primitive
      responseTime: 143 // ← number, gets flattened
    }
  })
})
