const API = "http://127.0.0.1:8000";

async function sendMessage() {
    const input = document.getElementById("chatInput");
    const message = input.value.trim();

    if (!message) return;

    addMessage(message, "user");
    input.value = "";

    try {
        const response = await fetch(`${API}/chat?query=${encodeURIComponent(message)}`);
        let data = await response.text();

        // Remove wrapping quotes if present
        if (data.startsWith('"') && data.endsWith('"')) {
            data = data.slice(1, -1);
        }

        // Convert escaped newlines into real newlines
        data = data.replace(/\\n/g, "\n");

        addMessage(data, "ai");

    } catch (err) {
        addMessage("Error connecting to server.", "ai");
    }
}

function addMessage(text, type) {
    const chatBox = document.getElementById("chatBox");

    const msgDiv = document.createElement("div");
    msgDiv.className = `message ${type}`;

    if (type === "ai") {
        msgDiv.innerHTML = formatAIResponse(text);
    } else {
        msgDiv.innerText = text;
    }

    chatBox.appendChild(msgDiv);
    chatBox.scrollTop = chatBox.scrollHeight;
}
function formatAIResponse(text) {
    const lines = text.split("\n").filter(line => line.trim() !== "");

    let html = "";
    let currentListOpen = false;

    lines.forEach(line => {
        const clean = line.trim();

        // Candidate name line (no dash at start)
        if (!clean.startsWith("-")) {

            // Close previous list
            if (currentListOpen) {
                html += "</ul>";
                currentListOpen = false;
            }

            html += `<h3 style="margin-top:15px;">${escapeHtml(clean)}</h3>`;
        }
        else {
            // Open list if not open
            if (!currentListOpen) {
                html += "<ul>";
                currentListOpen = true;
            }

            html += `<li>${escapeHtml(clean.substring(2))}</li>`;
        }
    });

    if (currentListOpen) {
        html += "</ul>";
    }

    return html;
}
function escapeHtml(str) {
    const div = document.createElement("div");
    div.innerText = str;
    return div.innerHTML;
}


