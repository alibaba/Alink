const baseUrl = "http://localhost:8080";

const getResponse = async (request: Promise<Response>) => {
  const response = await request;
  const json = await response.json();
  const { status, data, message } = json;
  if (status != "OK") {
    throw new Error(status + ": " + message);
  } else {
    return data;
  }
};

export const post = async (url: string, data: any) => {
  const request = fetch(baseUrl + url, {
    method: "POST",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
  return await getResponse(request);
};

export const get = async (url: string) => {
  const request = fetch(baseUrl + url, {
    method: "GET",
    headers: {
      Accept: "application/json",
      "Content-Type": "application/json",
    },
  });
  return await getResponse(request);
};
