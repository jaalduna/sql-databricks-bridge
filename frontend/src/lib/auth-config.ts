import type { Configuration, RedirectRequest } from "@azure/msal-browser"

const clientId = import.meta.env.VITE_AZURE_AD_CLIENT_ID ?? ""
const tenantId = import.meta.env.VITE_AZURE_AD_TENANT_ID ?? ""
const redirectUri = import.meta.env.VITE_AZURE_AD_REDIRECT_URI ?? window.location.origin

export const msalConfig: Configuration = {
  auth: {
    clientId,
    authority: `https://login.microsoftonline.com/${tenantId}`,
    redirectUri,
    postLogoutRedirectUri: redirectUri,
  },
  cache: {
    cacheLocation: "sessionStorage",
  },
}

export const loginRequest: RedirectRequest = {
  scopes: [`api://${clientId}/access_as_user`],
}
