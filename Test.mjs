import { ProxyAgent, fetch } from 'undici';

const url = 'https://www.jobsatamazon.co.uk/graphql';
const auth = process.env.AMAZON_AUTHORIZATION;
const proxyUrl = process.env.AMAZON_PROXY_URL;

if (!auth) {
  console.error('AMAZON_AUTHORIZATION is not set');
  process.exit(1);
}

if (!proxyUrl) {
  console.error('AMAZON_PROXY_URL is not set');
  process.exit(1);
}

const dispatcher = new ProxyAgent(proxyUrl);

const res = await fetch(url, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': auth,
  },
  body: JSON.stringify({ query: '{ __typename }' }),
  dispatcher,
});

console.log('Status code through proxy:', res.status);
