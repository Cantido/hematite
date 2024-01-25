import { check } from "k6";
import http from "k6/http";

import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";

export const options = {
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(95)<50'],
  },
};

export default function() {
    const payload = JSON.stringify({
        specversion: "1.0",
        type: "dev.cosmicrose.hematite.test",
        id: uuidv4(),
        source: "k6"
    });
    const stream = uuidv4();

    const headers = {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${__ENV.HEMATITE_AUTH_TOKEN}`
    };

    const res = http.post(`http://localhost:8080/streams/${stream}/events`, payload, { headers: headers, tags: { name: "PostEventURL" }});

    check(res, {
        "is status 201": (r) => r.status === 201,
    });
}
