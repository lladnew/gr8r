// v1.6.9 Cloudflare Worker: Subscribe worker + MailerSend + WhySubscribe + Confirm
//
// Changelog:
// - FIXED CORS issue by standardizing Access-Control-Allow-Origin headers
// - REFACTORED CORS logic using a shared corsHeaders constant
// - NO OTHER LOGIC CHANGES

const corsHeaders = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type"
};

export default {
  async fetch(request, env, ctx) {
    const {
      AIRTABLE_TOKEN,
      AIRTABLE_BASE_ID,
      AIRTABLE_TABLE_ID,
      MAILERSEND_API_KEY
    } = env;

    const url = new URL(request.url);

    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: corsHeaders
      });
    }

    if (url.pathname === "/api/confirm" && request.method === "GET") {
      const email = url.searchParams.get("email");
      if (!email) {
        return new Response(JSON.stringify({ status: "missing_email" }), {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }

      const headers = {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      };

      const searchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}?filterByFormula={Email}='${email}'`;
      const searchRes = await fetch(searchUrl, { headers });
      const searchData = await searchRes.json();

      if (!searchData.records || searchData.records.length === 0) {
        return new Response(JSON.stringify({ status: "not_found" }), {
          status: 404,
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }

      const record = searchData.records[0];
      const currentStatus = record.fields.Status;

      if (currentStatus === "Subscribed") {
        return new Response(JSON.stringify({ status: "already_subscribed" }), {
          status: 200,
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        });
      }

      const patchRes = await fetch(
        `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}/${record.id}`,
        {
          method: "PATCH",
          headers,
          body: JSON.stringify({ fields: { Status: "Subscribed" } })
        }
      );

      const patchResult = await patchRes.json();
      console.log("Status updated to Subscribed:", patchResult);

      return new Response(JSON.stringify({ status: "subscribed" }), {
        status: 200,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    }

    if (request.method !== "POST") {
      return new Response("Method not allowed", {
        status: 405,
        headers: corsHeaders
      });
    }

    if (url.pathname === "/api/whysubscribe") {
      try {
        const body = await request.json();
        const { email, response, checkOnly } = body;

        if (!email) {
          return new Response(JSON.stringify({ error: "Missing email" }), {
            status: 400,
            headers: corsHeaders
          });
        }

        const headers = {
          Authorization: `Bearer ${AIRTABLE_TOKEN}`,
          "Content-Type": "application/json"
        };

        const searchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}?filterByFormula={Email}='${email}'`;
        const searchRes = await fetch(searchUrl, { headers });
        const searchData = await searchRes.json();

        if (!searchData.records || searchData.records.length === 0) {
          return new Response(JSON.stringify({ found: false }), {
            status: 200,
            headers: corsHeaders
          });
        }

        const record = searchData.records[0];
        const recordId = record.id;

        if (checkOnly) {
          return new Response(JSON.stringify({ found: true }), {
            status: 200,
            headers: corsHeaders
          });
        }

        const existing = record.fields["whysubscribe"] || "";
        const timestamp = new Date().toISOString();
        const appendText = `${existing}\n\n[${timestamp}]\n${response}`;

        const patchRes = await fetch(
          `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}/${recordId}`,
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({ fields: { whysubscribe: appendText } })
          }
        );

        const patchResult = await patchRes.json();
        console.log("Appended to Airtable:", JSON.stringify(patchResult, null, 2));

        try {
          const emailAlert = {
            from: { email: "no-reply@gr8terthings.com", name: "Gr8terThings" },
            to: [{ email: "info@gr8terthings.com" }],
            subject: `New WhySubscribe Response: ${email}`,
            text: `Email: ${email}\n\n---\n\n${response}`
          };

          const mailerRes = await fetch("https://api.mailersend.com/v1/email", {
            method: "POST",
            headers: {
              Authorization: `Bearer ${MAILERSEND_API_KEY}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify(emailAlert)
          });

          const mailerJson = await mailerRes.json();
          console.log("MailerSend response:", mailerJson);
        } catch (mailerErr) {
          console.warn("MailerSend failed:", mailerErr);
        }

        return new Response(JSON.stringify({ success: true }), {
          status: 200,
          headers: corsHeaders
        });
      } catch (error) {
        console.error("Error in /api/whysubscribe:", error);
        return new Response("Internal Server Error", {
          status: 500,
          headers: corsHeaders
        });
      }
    }

    try {
      const body = await request.json();
      const {
        firstName,
        lastName,
        emailAddress,
        phoneNumber,
        DeliveryPreference,
        CampaignInterest,
        source
      } = body;

      console.log("Incoming payload:", JSON.stringify(body, null, 2));

      const headers = {
        Authorization: `Bearer ${AIRTABLE_TOKEN}`,
        "Content-Type": "application/json"
      };

      const searchUrl = `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}?filterByFormula={Email}='${emailAddress}'`;
      const searchRes = await fetch(searchUrl, { headers });
      const searchData = await searchRes.json();
      console.log("Search result:", JSON.stringify(searchData, null, 2));

      const now = new Date().toISOString();
      const tags = CampaignInterest?.split(",").map(tag => tag.trim()).filter(Boolean) || [];
      const delivery = ["Both", "Email", "Text"].includes(DeliveryPreference) ? DeliveryPreference : undefined;

      const baseFields = {
        "First Name": firstName,
        "Last Name": lastName,
        "Email": emailAddress,
        "Phone number": phoneNumber
      };
      if (delivery) baseFields["Delivery Preference"] = delivery;
      if (tags.length > 0) baseFields["Campaign Interest"] = tags;

      if (!searchData.records || searchData.records.length === 0) {
        const fields = {
          ...baseFields,
          "Subscribed Date": now,
          "Source": source || "Direct",
          "Status": "Pending"
        };

        const createRes = await fetch(`https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}`, {
          method: "POST",
          headers,
          body: JSON.stringify({ fields })
        });

        const createResult = await createRes.json();
        console.log("Create result:", JSON.stringify(createResult, null, 2));

        if (MAILERSEND_API_KEY && firstName && emailAddress) {
          const confirmEmail = {
            template_id: "zr6ke4ne1yy4on12",
            from: {
              email: "chad.mowery@gr8terthings.com",
              name: "Chad from GR8R"
            },
            to: [{ email: emailAddress, name: firstName }],
            personalization: [
              {
                email: emailAddress,
                data: {
                  subscriber: {
                    email: emailAddress,
                    first_name: firstName
                  }
                }
              }
            ]
          };

          console.log("MailerSend payload:", JSON.stringify(confirmEmail, null, 2));

          const sendRes = await fetch("https://api.mailersend.com/v1/email", {
            method: "POST",
            headers: {
              Authorization: `Bearer ${MAILERSEND_API_KEY}`,
              "Content-Type": "application/json"
            },
            body: JSON.stringify(confirmEmail)
          });

          if (sendRes.ok) {
            console.log(`✅ MailerSend: ${sendRes.status} ${sendRes.statusText}`);
          } else {
            const errorText = await sendRes.text();
            console.error("❌ MailerSend error:", errorText);
          }
        }
      } else {
        const record = searchData.records[0];
        const patchFields = {};
        for (const [key, value] of Object.entries(baseFields)) {
          if (value !== undefined && value !== "") {
            patchFields[key] = value;
          }
        }

        const patchRes = await fetch(
          `https://api.airtable.com/v0/${AIRTABLE_BASE_ID}/${AIRTABLE_TABLE_ID}/${record.id}`,
          {
            method: "PATCH",
            headers,
            body: JSON.stringify({ fields: patchFields })
          }
        );

        const patchResult = await patchRes.json();
        console.log("Patch result:", JSON.stringify(patchResult, null, 2));
      }

      return new Response(JSON.stringify({ status: searchData.records.length ? "updated" : "created" }), {
        status: 200,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      });
    } catch (error) {
      console.error("Error processing request:", error);
      return new Response("Internal Server Error", {
        status: 500,
        headers: corsHeaders
      });
    }
      return new Response("Not Found", {
      status: 404,
      headers: corsHeaders
    });
  }
};