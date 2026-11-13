const url = 'https://www.jobsatamazon.co.uk/graphql';
const auth = process.env.AMAZON_AUTHORIZATION;

if (!auth) {
  console.error('AMAZON_AUTHORIZATION is not set');
  process.exit(1);
}

const res = await fetch(url, {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json',
    'Authorization': auth,
  },
  body: JSON.stringify({ query: '{ __typename }' }),
});

console.log(res.status);
