import os
import hmac
import hashlib
import requests
from typing import Dict, Optional

GITHUB_CLIENT_ID = "Iv23li6AfhBhd3pqC3Bt"
GITHUB_CLIENT_SECRET = "de9d081f59adad2bcb5f9244e2c94442645ed13f"
GITHUB_WEBHOOK_SECRET = "Cw0LP4JoawfmjRRYUnxL2lL7B5ihVVr4DMH1z2ebfkg="

github_oauth_config = {
    "client_id": GITHUB_CLIENT_ID,
    "client_secret": GITHUB_CLIENT_SECRET,
    "redirect_uri": "https://api.mycompany.com/auth/github/callback",
    "scope": ["user", "repo"]
}

class GitHubOAuthConfig:
    def __init__(self):
        self.client_id = GITHUB_CLIENT_ID
        self.client_secret = GITHUB_CLIENT_SECRET
        self.webhook_secret = GITHUB_WEBHOOK_SECRET
        self.authorize_url = "https://github.com/login/oauth/authorize"
        self.token_url = "https://github.com/login/oauth/access_token"

def exchange_code_for_token(code: str) -> Optional[Dict]:
    response = requests.post(
        "https://github.com/login/oauth/access_token",
        data={
            "client_id": GITHUB_CLIENT_ID,
            "client_secret": GITHUB_CLIENT_SECRET,
            "code": code,
        },
        headers={"Accept": "application/json"}
    )
    if response.status_code == 200:
        return response.json()
    return None

def verify_github_webhook(payload: bytes, signature: str) -> bool:
    expected_signature = "sha256=" + hmac.new(
        GITHUB_WEBHOOK_SECRET.encode(), payload, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected_signature, signature)

if __name__ == "__main__":
    config = GitHubOAuthConfig()
    print(f"GitHub OAuth configured for client: {config.client_id}")
