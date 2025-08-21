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

Grafana can be complicated and confusing.  browse to gr8r.grafana.net --> Connections --> Datasources --> grafanacloud-gr8r-logs It is a Loki service and the URL is listed with it... https://logs-prod-036.grafana.net  When you access by clicking the icon you can see the User (in this case: 1248364) and Password which is not visible but shows configured.  You can reset the password or key here... pending testing