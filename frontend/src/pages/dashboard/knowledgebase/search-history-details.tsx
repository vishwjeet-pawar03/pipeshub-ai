import { Helmet } from 'react-helmet-async';

import { CONFIG } from 'src/config-global';
import { UserProvider } from 'src/context/UserContext';

import SeachHistoryDetails from 'src/sections/knowledgebase/search-history-details';

// ----------------------------------------------------------------------

const metadata = { title: `Knowledge Search | ${CONFIG.appName}` };

export default function Page() {
  return (
    <>
      <Helmet>
        <title> {metadata.title}</title>
      </Helmet>
      <UserProvider>
        <SeachHistoryDetails />
      </UserProvider>
    </>
  );
}
