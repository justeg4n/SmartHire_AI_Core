const {
  CognitoIdentityProviderClient,
  ListUsersCommand,
  AdminLinkProviderForUserCommand,
} = require("@aws-sdk/client-cognito-identity-provider");

const cognitoClient = new CognitoIdentityProviderClient({
  region: process.env.AWS_REGION,
});

exports.handler = async (event) => {
  // Only handle external (federated) provider sign-ups
  if (event.triggerSource !== "PreSignUp_ExternalProvider") {
    return event;
  }

  const email = (event.request?.userAttributes?.email || "").trim().toLowerCase();
  if (!email) {
    return event;
  }

  // Normalize email in the event
  event.request.userAttributes.email = email;

  // Extract provider info from userName (e.g., "Google_123456789")
  const [providerName, ...rest] = (event.userName || "").split("_");
  const providerUserId = rest.join("_");

  if (!providerName || !providerUserId) {
    console.warn("External provider sign-in missing provider details", {
      triggerSource: event.triggerSource,
      userName: event.userName,
    });
    // Still auto-confirm to prevent orphan federated account
    event.response.autoConfirmUser = true;
    event.response.autoVerifyEmail = true;
    return event;
  }

  // Check if a native (password-based) user with this email already exists
  const listCommand = new ListUsersCommand({
    UserPoolId: event.userPoolId,
    Filter: `email = \"${email.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}\"`,
    Limit: 10,
  });

  const existingUsers = await cognitoClient.send(listCommand);

  if (existingUsers.Users && existingUsers.Users.length > 0) {
    // Find the native Cognito user (not an EXTERNAL_PROVIDER)
    const nativeUser = existingUsers.Users.find(
      (user) => user.UserStatus !== "EXTERNAL_PROVIDER"
    );

    if (nativeUser) {
      // Link the federated identity to the existing native user
      const linkCommand = new AdminLinkProviderForUserCommand({
        UserPoolId: event.userPoolId,
        DestinationUser: {
          ProviderName: "Cognito",
          ProviderAttributeValue: nativeUser.Username,
        },
        SourceUser: {
          ProviderName: providerName,
          ProviderAttributeName: "Cognito_Subject",
          ProviderAttributeValue: providerUserId,
        },
      });

      await cognitoClient.send(linkCommand);

      console.log("Linked external provider to existing native user", {
        email,
        providerName,
        nativeUsername: nativeUser.Username,
      });
    }
  }

  // CRITICAL: Always auto-confirm federated users.
  // Without these flags, Cognito creates a separate federated user account.
  event.response.autoConfirmUser = true;
  event.response.autoVerifyEmail = true;

  return event;
};
