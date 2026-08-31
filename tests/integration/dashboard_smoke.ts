import { chromium } from "playwright";

const base = process.env.LEGION_TEST_URL ?? "http://127.0.0.1:18084";
const browser = await chromium.launch({ headless: true });
try {
  const page = await browser.newPage();
  await page.goto(`${base}/dashboard`, { waitUntil: "networkidle" });
  if (await page.title() !== "Legion Dashboard") throw new Error("dashboard title missing");
  await page.getByText("Sessions", { exact: true }).first().waitFor();
  await page.locator("tr[data-id]").first().click();
  await page.getByText("Event log", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Cluster" }).click();
  await page.getByText("peers", { exact: true }).waitFor();
  await page.getByRole("button", { name: "Workflow runner" }).click();
  await page.getByRole("button", { name: "Run workflow" }).click();
  await page.getByText('"waves"').waitFor();
  console.log("Dashboard session detail, cluster navigation, and workflow run passed");
} finally {
  await browser.close();
}
