(async function loadRecords() {
  const API_URL = "https://gr8r-videosdb1-worker.gr8r.workers.dev"; // Update if needed

  const res = await fetch(`${API_URL}/videos`, {
    headers: {
      Authorization: `Bearer ${window.GR8R_ADMIN_TOKEN}`,
    },
  });

  if (!res.ok) {
    document.getElementById("recordsTable").innerHTML =
      `<tr><td colspan="4" class="px-6 py-4 text-red-600">Error loading records: ${res.statusText}</td></tr>`;
    return;
  }

  const data = await res.json();

  const rows = data.records.map((record) => {
    return `
      <tr>
        <td class="px-6 py-4 whitespace-nowrap">${record.title || "-"}</td>
        <td class="px-6 py-4 whitespace-nowrap">${record.status || "-"}</td>
        <td class="px-6 py-4 whitespace-nowrap">
          ${record.r2Url ? `<a href="${record.r2Url}" class="text-blue-600 underline" target="_blank">Link</a>` : "-"}
        </td>
        <td class="px-6 py-4 whitespace-nowrap">${record.scheduleDateTime || "-"}</td>
      </tr>
    `;
  });

  document.getElementById("recordsTable").innerHTML = rows.join("");
})();
