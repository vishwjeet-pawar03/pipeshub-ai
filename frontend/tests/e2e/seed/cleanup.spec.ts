import { test, expect } from '../fixtures/api-context.fixture';

const EMAIL_DOMAIN = 'e2etest.pipeshub.local';

/**
 * The id an entity was listed with. Skipping in silence is how leftovers pile
 * up in the test organisation unseen, so a missing id says so out loud and the
 * caller counts it.
 */
function listedId(
  entity: { _id?: string; id?: string; _key?: string; name?: string; email?: string },
  kind: string,
): string | undefined {
  const id = entity._id ?? entity.id ?? entity._key;
  if (id) return id;
  console.warn(
    `Cannot delete the ${kind} "${entity.name ?? entity.email ?? '(unnamed)'}": ` +
      `the listing gave no id. Delete it by hand, and check whether the ` +
      `listing's shape has changed.`,
  );
  return undefined;
}

test.describe.serial('Cleanup E2E Test Data', () => {
  test('delete seeded users', async ({ apiContext }) => {
    let page = 1;
    let deleted = 0;
    let skipped = 0;

    // Paginate through all users matching e2e pattern
    while (true) {
      const response = await apiContext.get('/api/v1/users', {
        params: { page, limit: 100, search: 'e2e-user' },
      });
      if (!response.ok()) break;

      const data = await response.json();
      const users = data.users ?? [];
      if (users.length === 0) break;

      const e2eUsers = users.filter(
        (u: { email?: string }) => u.email?.endsWith(`@${EMAIL_DOMAIN}`)
      );

      for (const user of e2eUsers) {
        const id = listedId(user, 'user');
        if (!id) {
          skipped++;
          continue;
        }
        const delResponse = await apiContext.delete(`/api/v1/users/${id}`);
        if (delResponse.ok()) deleted++;
      }

      if (users.length < 100) break;
      page++;
    }

    console.log(
      `Deleted ${deleted} e2e users` +
        (skipped > 0 ? `; ${skipped} could not be deleted and need clearing by hand` : ''),
    );
  });

  test('delete seeded groups', async ({ apiContext }) => {
    let page = 1;
    let deleted = 0;
    let skipped = 0;

    while (true) {
      const response = await apiContext.get('/api/v1/userGroups', {
        params: { page, limit: 100, search: 'E2E Group' },
      });
      if (!response.ok()) break;

      const data = await response.json();
      const groups = data.groups ?? [];
      if (groups.length === 0) break;

      const e2eGroups = groups.filter(
        (g: { name?: string }) => g.name?.startsWith('E2E Group')
      );

      for (const group of e2eGroups) {
        const id = listedId(group, 'group');
        if (!id) {
          skipped++;
          continue;
        }
        const delResponse = await apiContext.delete(`/api/v1/userGroups/${id}`);
        if (delResponse.ok()) deleted++;
      }

      if (groups.length < 100) break;
      page++;
    }

    console.log(
      `Deleted ${deleted} e2e groups` +
        (skipped > 0 ? `; ${skipped} could not be deleted and need clearing by hand` : ''),
    );
  });

  test('delete seeded teams', async ({ apiContext }) => {
    let page = 1;
    let deleted = 0;
    let skipped = 0;

    while (true) {
      const response = await apiContext.get('/api/v1/teams/user/teams', {
        params: { page, limit: 100, search: 'E2E Team' },
      });
      if (!response.ok()) break;

      const data = await response.json();
      const teams = data.teams ?? [];
      if (teams.length === 0) break;

      const e2eTeams = teams.filter(
        (t: { name?: string }) => t.name?.startsWith('E2E Team')
      );

      for (const team of e2eTeams) {
        const id = listedId(team, 'team');
        if (!id) {
          skipped++;
          continue;
        }
        const delResponse = await apiContext.delete(`/api/v1/teams/${id}`);
        if (delResponse.ok()) deleted++;
      }

      if (teams.length < 100) break;
      page++;
    }

    console.log(
      `Deleted ${deleted} e2e teams` +
        (skipped > 0 ? `; ${skipped} could not be deleted and need clearing by hand` : ''),
    );
  });

  test('delete e2e upload knowledge bases', async ({ apiContext }) => {
    let pageNum = 1;
    let deleted = 0;
    let skipped = 0;

    while (true) {
      const response = await apiContext.get('/api/v1/knowledgeBase', {
        params: { page: pageNum, limit: 100 },
      });
      if (!response.ok()) break;

      const data = await response.json();
      const kbs: { id?: string; _key?: string; name?: string }[] =
        data.knowledgeBases ?? data.kbs ?? data.items ?? [];
      if (kbs.length === 0) break;

      const e2eKbs = kbs.filter((kb) => kb.name?.startsWith('E2E Upload'));

      for (const kb of e2eKbs) {
        const id = listedId(kb, 'knowledge base');
        if (!id) {
          skipped++;
          continue;
        }
        const delResponse = await apiContext.delete(`/api/v1/knowledgeBase/${id}`);
        if (delResponse.ok()) deleted++;
      }

      if (kbs.length < 100) break;
      pageNum++;
    }

    console.log(
      `Deleted ${deleted} e2e upload knowledge bases` +
        (skipped > 0 ? `; ${skipped} could not be deleted and need clearing by hand` : ''),
    );
  });
});
