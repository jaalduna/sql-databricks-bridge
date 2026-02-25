import { chromium } from "playwright";

const REPO_URL = "https://github.com/jaalduna/sql-databricks-bridge/releases";
const TAG = "v0.1.17";

// Use Edge with the user's profile so GitHub session is already active
const userDataDir = process.env.LOCALAPPDATA + "/Microsoft/Edge/User Data";

(async () => {
  console.log("Launching browser with existing session...");
  const context = await chromium.launchPersistentContext(userDataDir, {
    channel: "msedge",
    headless: false,
    args: ["--profile-directory=Default"],
  });

  const page = context.pages()[0] || (await context.newPage());

  console.log("Navigating to releases page...");
  await page.goto(REPO_URL, { waitUntil: "networkidle" });

  // Find the draft release for our tag
  console.log(`Looking for draft release ${TAG}...`);

  // Draft releases have a "Draft" badge - find the edit link for our tag
  const editLink = page.locator(`a[href*="/releases/edit/"][href*="${TAG}"]`).first();

  if (await editLink.count() === 0) {
    // Try finding it via the tag text
    const draftSection = page.locator(`text=${TAG}`).first();
    if (await draftSection.count() === 0) {
      console.log("Draft release not found on page. Taking screenshot...");
      await page.screenshot({ path: "release_page.png", fullPage: true });
      await context.close();
      process.exit(1);
    }
    // Click the tag link to go to the release
    await draftSection.click();
    await page.waitForLoadState("networkidle");

    // Look for edit button on the release page
    const editBtn = page.locator('a:has-text("Edit")').first();
    if (await editBtn.count() > 0) {
      await editBtn.click();
      await page.waitForLoadState("networkidle");
    }
  } else {
    await editLink.click();
    await page.waitForLoadState("networkidle");
  }

  console.log("On edit page, looking for publish button...");
  await page.screenshot({ path: "edit_release.png" });

  // Uncheck "Set as a pre-release" if checked
  const preReleaseCheckbox = page.locator('input[name="prerelease"]');
  if (await preReleaseCheckbox.count() > 0 && await preReleaseCheckbox.isChecked()) {
    await preReleaseCheckbox.uncheck();
  }

  // Click "Publish release" button
  const publishBtn = page.getByRole("button", { name: /Publish release/i });
  if (await publishBtn.count() > 0) {
    console.log("Clicking Publish release...");
    await publishBtn.click();
    await page.waitForLoadState("networkidle");
    console.log("Release published!");
    await page.screenshot({ path: "published_release.png" });
  } else {
    console.log("Publish button not found. Taking screenshot...");
    await page.screenshot({ path: "no_publish_btn.png", fullPage: true });
  }

  await context.close();
  console.log("Done.");
})();
