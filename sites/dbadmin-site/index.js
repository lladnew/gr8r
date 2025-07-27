(async function loadRecords() {
  const API_URL = "https://gr8r-videosdb1-worker.gr8r.workers.dev"; // Adjust if needed

  const res = await fetch(`${API_URL}/videos`, {
    headers: {
      Authorization: `Bearer ${window.GR8R_ADMIN_TOKEN}`,
    },
  });

  if (!res.ok) {
    document.getElementById("recordsTable").innerHTML =
      `<tr><td colspan="99" class="px-6 py-4 text-red-600">Error loading records: ${res.statusText}</td></tr>`;
    return;
  }

  const records = await res.json();
  if (!records.length) {
    document.getElementById("recordsTable").innerHTML =
      `<tr><td colspan="99" class="px-6 py-4">No records found.</td></tr>`;
    return;
  }

  const tableHead = document.getElementById("recordsTableHead");
  const tableBody = document.getElementById("recordsTable");

  // Generate headers from keys of first record
  const keys = Object.keys(records[0]);
  const headers = keys.map(key =>
    `<th class="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">${key}</th>`
  );
  tableHead.innerHTML = `<tr>${headers.join("")}</tr>`;

  // Generate body rows
  const rows = records.map(record => {
    const cells = keys.map(key => {
      const value = record[key];
      if (key.toLowerCase().includes("url") && typeof value === "string") {
        return `<td class="px-6 py-4 whitespace-nowrap">${value ? `<a href="${value}" class="text-blue-600 underline" target="_blank">Link</a>` : "-"}</td>`;
      }
      return `<td class="px-6 py-4 whitespace-nowrap">${value ?? "-"}</td>`;
    });
    return `<tr>${cells.join("")}</tr>`;
  });

  tableBody.innerHTML = rows.join("");
})();
