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
    const headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwiYXVkIjoiaGVtYXRpdGUiLCJpYXQiOjE3MDQ3NTY3NTAsImV4cCI6OTk5OTk5OTk5OX0.am-46Z8g0-Ou-lcBPtm8hymjdZYWBl1qfYeFfvTO8CI"
    };

    const stream = uuidv4();

    for (let i = 0; i < 100; i++) {
        const payload = JSON.stringify({
            specversion: "1.0",
            type: "dev.cosmicrose.hematite.test",
            id: uuidv4(),
            source: "k6"
        });

        const res = http.post(`http://localhost:8080/streams/${stream}/events`, payload, { headers: headers, tags: { name: "PostEventURL" }});

        check(res, {
            "is status 201": (r) => r.status === 201,
        });
    }

    for (let i = 0; i < 1000; i++) {
        const revision = i % 100;
        const res = http.get(`http://localhost:8080/streams/${stream}/events/${revision}`, { headers: headers, tags: { name: "GetEventURL" }});

        check(res, {
            "is status 200": (r) => r.status === 200,
        });
    }

}
