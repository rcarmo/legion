const input = JSON.parse(await Bun.stdin.text());
process.stdout.write(JSON.stringify({
  greeting: `Hello, ${input.name ?? "world"}!`,
  runtime: "bun",
}));
