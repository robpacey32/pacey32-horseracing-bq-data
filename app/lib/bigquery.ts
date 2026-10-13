import { BigQuery } from "@google-cloud/bigquery";

const projectId = "horseracing-pacey32-github";

let bigQueryClient: BigQuery | null = null;

export function getBigQueryClient() {
  if (bigQueryClient) {
    return bigQueryClient;
  }

  const credentialsJson = process.env.GOOGLE_APPLICATION_CREDENTIALS_JSON;

  if (credentialsJson) {
    const credentials = JSON.parse(credentialsJson);

    bigQueryClient = new BigQuery({
      projectId,
      credentials,
    });
  } else {
    // Allows Application Default Credentials locally / on Google Cloud.
    bigQueryClient = new BigQuery({
      projectId,
    });
  }

  return bigQueryClient;
}

export const PROJECT_ID = projectId;
export const RACE_DATASET = "horseracescrape";
export const ANALYTICS_DATASET = "horseraceanalytics";