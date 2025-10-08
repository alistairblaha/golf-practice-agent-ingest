import { createClient } from "@supabase/supabase-js";

const {
  SUPABASE_URL,
  SUPABASE_SERVICE_ROLE,
  TRACKMAN_API_TOKEN,
  TRACKMAN_BASE_URL = "https://api.trackman.example" // replace with real base url
} = process.env;

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE);

// Helper: insert raw log
async function logRaw(endpoint, status, payload) {
  await supabase.from("raw.trackman_events").insert({
    endpoint,
    status_code: status,
    payload
  });
}

export default async function handler(req, res) {
  try {
    // 1) Pull recent sessions (adjust window)
    const since = new Date(Date.now() - 1000 * 60 * 60 * 24 * 7).toISOString(); // last 7 days
    const sessionsResp = await fetch(`${TRACKMAN_BASE_URL}/v1/sessions?since=${encodeURIComponent(since)}`, {
      headers: { Authorization: `Bearer ${TRACKMAN_API_TOKEN}` }
    });

    const sessionsJson = await sessionsResp.json();
    await logRaw("/v1/sessions", sessionsResp.status, sessionsJson);

    if (!sessionsResp.ok) throw new Error(JSON.stringify(sessionsJson));

    // 2) Upsert sessions, then shots per session
    for (const sess of sessionsJson.sessions ?? sessionsJson ?? []) {
      const { data: sRow, error: sErr } = await supabase
        .from("core.sessions")
        .upsert({
          source_session_id: String(sess.id ?? sess.sessionId),
          player_id: String(sess.playerId ?? sess.player_id ?? "me"),
          started_at: sess.startedAt ?? sess.startTime ?? null,
          ended_at: sess.endedAt ?? sess.endTime ?? null,
          session_type: (sess.type ?? "unknown").toLowerCase(),
          notes: sess.meta ? JSON.parse(JSON.stringify(sess.meta)) : {}
        })
        .select("session_id")
        .single();

      if (sErr) throw sErr;
      const session_id = sRow.session_id;

      // Fetch shots for this session
      const shotsResp = await fetch(`${TRACKMAN_BASE_URL}/v1/sessions/${sess.id}/shots`, {
        headers: { Authorization: `Bearer ${TRACKMAN_API_TOKEN}` }
      });
      const shotsJson = await shotsResp.json();
      await logRaw(`/v1/sessions/${sess.id}/shots`, shotsResp.status, shotsJson);
      if (!shotsResp.ok) throw new Error(JSON.stringify(shotsJson));

      // Map and insert shots (batch in chunks of 500)
      const mapShot = (sh) => ({
        session_id,
        club: sh.club ?? null,
        ts: sh.timestamp ?? null,
        ball_speed: sh.ballSpeed ?? null,
        club_speed: sh.clubSpeed ?? null,
        smash_factor: sh.smash ?? null,
        launch_deg: sh.launchDeg ?? sh.launchAngle ?? null,
        spin_rpm: sh.spin ?? sh.spinRpm ?? null,
        aoa_deg: sh.attackAngle ?? sh.aoa ?? null,
        path_deg: sh.path ?? sh.clubPath ?? null,
        face_deg: sh.face ?? sh.faceAngle ?? null,
        face_to_path_deg: sh.faceToPath ?? null,
        carry_yd: sh.carry ?? null,
        total_yd: sh.total ?? null,
        side_yd: sh.side ?? sh.lateral ?? null,
        height_ft: sh.heightFt ?? null,
        curve_yd: sh.curve ?? null,
        raw: JSON.parse(JSON.stringify(sh))
      });

      const shots = (shotsJson.shots ?? shotsJson ?? []).map(mapShot);

      const chunk = 500;
      for (let i = 0; i < shots.length; i += chunk) {
        const slice = shots.slice(i, i + chunk);
        const { error } = await supabase.from("core.shots").insert(slice);
        if (error) throw error;
      }
    }

    res.status(200).json({ ok: true });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e) });
  }
