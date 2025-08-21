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

Grafana can be complicated and confusing.  browse to gr8r.grafana.net --> Connections --> Datasources --> grafanacloud-gr8r-logs It is a Loki service and the URL is listed with it... https://logs-prod-036.grafana.net  When you access by clicking the icon you can see the User (in this case: 1248364) and Password which is not visible but shows configured.  You can't view or reset the password here though it seems.  Grafana seems to give me a user and then I can create "policies" with various access permissions.  They get ID's, but I didn't use in my CF workers.  I only use the URL, USER, and token.  It seems you can create multiple tokens under each policy.  I find this rather confusing, but it's the way it seems to work.  I am currently using the token named: GRAFANACLOUD_GR8R_LOGS_KEY since it matches what I name in in CF Secrets Store.