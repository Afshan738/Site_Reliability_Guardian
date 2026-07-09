import http from "k6/http";
import { sleep } from "k6";

export const options = {
  vus: 10,
  duration: "30s",
};

export default function () {
  const url = "http://localhost:3000/api/monitoring/monitor";
  const payload = JSON.stringify({
    url: "https://google.com",
    interval: 60,
  });

  const params = {
    headers: {
      "Content-Type": "application/json",
    },
  };
  http.post(url, payload, params);
  sleep(1);
}
