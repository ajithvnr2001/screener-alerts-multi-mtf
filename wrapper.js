export default {
    // Cron trigger — fires every 1 minute
    async scheduled(event, env, ctx) {
        await triggerAll(env);
    },

    // HTTP handler — for manual testing via browser/curl
    async fetch(request, env, ctx) {
        const result = await triggerAll(env);
        return new Response(JSON.stringify(result), {
            headers: { "Content-Type": "application/json" }
        });
    }
};

async function triggerAll(env) {
    const results = { mtf: "skipped" };

    if (env.MTF_WORKER) {
        try {
            const resp = await env.MTF_WORKER.fetch(
                new Request("https://screener-alerts-mtf.ltimindtree.workers.dev/api/cron", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" }
                })
            );
            results.mtf = resp.ok ? "ok" : `error:${resp.status}`;
        } catch (e) {
            results.mtf = `error:${e.message}`;
        }
    }

    return results;
}

