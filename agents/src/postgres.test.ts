import { expect, test } from "bun:test";
import { SQL } from "bun";
import { actor, createChat } from "@inkwell/core";
import { scriptedModel } from "@inkwell/core/testkit";
import { migratePostgres, postgresConversationStore, postgresTranscript } from "@inkwell/store-postgres";
import { oflAgent } from "./agents";

const url = process.env.OFL_AGENT_TEST_DATABASE_URL;
test.skipIf(!url)("Postgres survives a new agent instance and preserves user ownership", async () => {
  const sql = new SQL(url!);
  const schema = `ofl_agent_test_${crypto.randomUUID().replaceAll("-", "")}`;
  try {
    await migratePostgres(sql, {schema});
    const build = () => createChat({agent: oflAgent("quality", {read: async () => ({published: true})}),
      model: scriptedModel([{content: "Consultando cobertura.", toolCalls: [{name: "read_coverage", arguments: {}}]}]),
      roles: {research: {model: "test"}}, conversations: postgresConversationStore(sql, {schema}), transcript: postgresTranscript(sql, {schema}), policy: {maxModelSteps: 1}});
    await build().turn(actor("ana"), {conversationId: "saved-check", message: "Qual a cobertura?"});
    const restarted = build();
    expect(JSON.stringify(await restarted.get(actor("ana"), "saved-check"))).toContain("published");
    await expect(restarted.get(actor("bia"), "saved-check")).rejects.toThrow();
    expect(await restarted.list(actor("bia"))).toHaveLength(0);
    expect((await restarted.list(actor("ana")))[0]!.conversationId).toBe("saved-check");
  } finally {
    await sql.unsafe(`DROP SCHEMA ${schema} CASCADE`);
    await sql.close();
  }
});
