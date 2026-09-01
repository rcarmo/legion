const input = JSON.parse(await Bun.stdin.text());
process.stdout.write(JSON.stringify({ version: "canary", input }));
