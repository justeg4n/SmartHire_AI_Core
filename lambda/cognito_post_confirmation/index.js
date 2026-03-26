const { Client } = require("pg");
const {
  SecretsManagerClient,
  GetSecretValueCommand,
} = require("@aws-sdk/client-secrets-manager");
const crypto = require("crypto");

const secretsClient = new SecretsManagerClient({
  region: process.env.AWS_REGION,
});

let cachedSecret = null;

async function getDbSecret() {
  if (cachedSecret) return cachedSecret;

  const command = new GetSecretValueCommand({
    SecretId: process.env.SECRET_NAME,
  });
  const response = await secretsClient.send(command);
  cachedSecret = JSON.parse(response.SecretString);
  return cachedSecret;
}

exports.handler = async (event) => {
  if (event.triggerSource !== "PostConfirmation_ConfirmSignUp") {
    return event;
  }

  const { sub: cognitoSub, email } = event.request.userAttributes;
  const normalizedEmail = (email || "").trim().toLowerCase();
  const role = event.request.userAttributes["custom:role"] || "CANDIDATE";

  const secret = await getDbSecret();

  const client = new Client({
    user: secret.username,
    password: secret.password,
    host: secret.host,
    port: secret.port,
    database: secret.dbname,
    ssl: { rejectUnauthorized: false },
  });

  try {
    await client.connect();

    await client.query(
      `INSERT INTO "Users" ("Id", "CognitoSub", "Email", "Role", "CreatedAt")
       VALUES ($1, $2, $3, $4, NOW())
       ON CONFLICT ("Email") DO UPDATE
       SET "CognitoSub" = EXCLUDED."CognitoSub",
           "Role" = COALESCE("Users"."Role", EXCLUDED."Role")`,
      [crypto.randomUUID(), cognitoSub, normalizedEmail, role],
    );

    console.log(`Synced ${role} user to RDS: ${normalizedEmail}`);
  } catch (err) {
    console.error("RDS insert failed:", err);
    throw err;
  } finally {
    await client.end();
  }

  return event;
};
