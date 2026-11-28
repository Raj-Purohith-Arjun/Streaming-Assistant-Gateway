/**
 * Zero-dependency browser / Node 18+ client with automatic Last-Event-ID resume.
 *
 * Usage:
 *   import { streamWithResume } from "./browser_client.js";
 *   await streamWithResume({
 *     baseUrl: "http://localhost:8000",
 *     sessionId: "s1",
 *     userId: "u1",
 *     prompt: "Explain resumable SSE streams",
 *     onToken: (text) => process.stdout.write(text),
 *   });
 */

export async function streamWithResume({
  baseUrl,
  sessionId,
  userId,
  prompt,
  onToken,
  onDone,
  onError,
  signal,
  maxRetries = 5,
  backoffMs = 250,
}) {
  const root = baseUrl.replace(/\/$/, "");
  let generationId = null;
  let lastEventId = null;

  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      const resp =
        generationId === null
          ? await fetch(`${root}/v1/stream`, {
              method: "POST",
              headers: { "content-type": "application/json" },
              body: JSON.stringify({ session_id: sessionId, user_id: userId, prompt }),
              signal,
            })
          : await fetch(`${root}/v1/generations/${encodeURIComponent(generationId)}/stream`, {
              headers: lastEventId ? { "Last-Event-ID": lastEventId } : {},
              signal,
            });

      if (!resp.ok || !resp.body) {
        throw new Error(`Gateway returned HTTP ${resp.status}`);
      }
      generationId = resp.headers.get("x-generation-id") || generationId;

      const reader = resp.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";

      while (true) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true }).replace(/\r\n/g, "\n");
        while (buf.includes("\n\n")) {
          const idx = buf.indexOf("\n\n");
          const block = buf.slice(0, idx);
          buf = buf.slice(idx + 2);
          if (!block || block.startsWith(":")) continue;

          let evId = null;
          let evType = "message";
          let dataRaw = "{}";
          for (const line of block.split("\n")) {
            if (line.startsWith("id: ")) evId = line.slice(4);
            else if (line.startsWith("event: ")) evType = line.slice(7);
            else if (line.startsWith("data: ")) dataRaw = line.slice(6);
          }
          if (evId) lastEventId = evId;
          const data = JSON.parse(dataRaw);
          if (evType === "token" && typeof data.text === "string" && onToken) {
            onToken(data.text, evId);
          } else if (evType === "done") {
            if (onDone) onDone(data, generationId);
            return generationId;
          } else if (evType === "error") {
            if (onError) onError(data, generationId);
            return generationId;
          }
        }
      }
    } catch (err) {
      if (err?.name === "AbortError" || attempt >= maxRetries) throw err;
    }
    await new Promise((r) => setTimeout(r, backoffMs * 2 ** attempt));
  }
  throw new Error("Stream ended before terminal event");
}
